"""HTTP transport with aiohttp first, curl_cffi fallback, and bounded retries."""

from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict
from enum import StrEnum
from types import TracebackType
from typing import Any, Self

from curl_cffi.requests.exceptions import RequestException

from src.client.config import ClientConfig
from src.network.aiohttp_session import NETWORK_ERRORS as AIOHTTP_NETWORK_ERRORS
from src.network.budget import NetworkBudget, RequestMetrics, TimeoutPolicy, attempt_context, mark, proxy_id, work_context
from src.network.errors import HTTPClientError
from src.network.session_factory import SessionFactory
from src.network.transport import Response, Transport, transport_for
from src.proxy.health import pool_health_path
from src.proxy.pool import (
    ProxyEndpoint,
    ProxyPool,
    ProxySession,
    WarmupSpec,
    dead_proxy_path,
    load_dead_proxies,
    load_proxies,
)
from src.proxy.repository import ProxyDatabase
from src.utils.http import parse_retry_after as _parse_retry_after, retry_after_from_response

logger = logging.getLogger(__name__)

NETWORK_ERRORS = (RequestException, asyncio.TimeoutError) + AIOHTTP_NETWORK_ERRORS
HARD_TIMEOUT_GRACE_SECONDS = 1.0
# Only connect-level (fast) direct failures prove an IP block; slow body reads
# are ordinary server throttling and must not cool the direct route down.
DIRECT_BLOCK_FAILURE_THRESHOLD = 3
DIRECT_BLOCK_MAX_FAILURE_SECONDS = 30.0
PROXY_LATENCY_TIMEOUT_DIVISOR = 5.0
MAX_PROXY_TIMEOUT_SCALE = 2.5


class Route(StrEnum):
    DIRECT = "direct"
    POOL = "pool"


class HttpClient:
    def __init__(self, config: ClientConfig):
        self.config = config
        self.budget = NetworkBudget(config.connection_limit, config.requests_per_second)
        self.metrics = RequestMetrics()
        self._direct_blocked_until = 0.0
        self._direct_block_kind: str | None = None
        self._direct_net_failures = 0
        self._pool_watch_task: asyncio.Task[None] | None = None
        self._database_watch_task: asyncio.Task[None] | None = None
        self._direct_retired: list[Any] = []
        self.logger = logger
        self._session_factory = SessionFactory(
            headers=dict(config.headers),
            timeout=config.timeouts.api.total,
            connection_limit=config.connection_limit,
            impersonate=config.impersonate,
            cookies_file=config.cookies_file,
            transport=config.transport,
            sock_read_timeout=config.timeouts.api.sock_read,
        )
        self._session: ProxySession | None = None
        self._proxy_pool: ProxyPool | None = None
        self._open_lock = asyncio.Lock()
        self._transport_lock = asyncio.Lock()
        self.requests_made = 0
        self.retries_made = 0
        self.run_id: str | None = None

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(
        self, _exc_type: type[BaseException] | None, _exc_val: BaseException | None, _exc_tb: TracebackType | None
    ) -> None:
        await self.close()

    async def open(self) -> None:
        async with self._open_lock:
            if self._session is not None or self._proxy_pool is not None:
                return
            if not self.config.proxy.file and not self.config.proxy.db_path:
                self._session = self._session_factory.create()
                return
            if self.config.proxy.mode == "direct":
                self.logger.warning("PROXY_MODE=direct: proxy configuration ignored, scraping without proxies")
                self._session = self._session_factory.create()
                return
            pool = await self._create_proxy_pool()
            self._proxy_pool = pool
            if self.config.proxy.mode == "fallback" or pool is None:
                self._session = self._session_factory.create()

            if self.config.proxy.db_path is not None and self.config.proxy.db_refresh_seconds > 0:
                self._database_watch_task = asyncio.create_task(self._watch_proxy_database())

    async def _watch_proxy_database(self) -> None:
        while True:
            await asyncio.sleep(self.config.proxy.db_refresh_seconds)
            try:
                await self._refresh_proxy_database()
            except Exception as exc:  # noqa: BLE001 - a failed refresh must preserve the current pool
                self.logger.warning("Proxy database refresh failed: %s", type(exc).__name__)

    async def _refresh_proxy_database(self) -> None:
        path = self.config.proxy.db_path
        if path is None:
            return
        max_age = self.config.proxy.db_max_age_hours or None
        async with ProxyDatabase(path) as database:
            records = await database.list_active(limit=self.config.proxy.sample_size or None, max_age_hours=max_age)
        if self._proxy_pool is not None:
            self._proxy_pool.update_candidates(
                [
                    ProxyEndpoint(
                        record.url, record.user_agent, record.transport, record.latency_ms, record.checked_at, record.source
                    )
                    for record in records
                ]
            )
        elif records:
            self._proxy_pool = await self._create_proxy_pool()

    async def close(self) -> None:
        """Release every owned resource, even if one closer fails.

        All references are detached before awaiting so repeated calls are safe
        and a partially failed shutdown cannot leak the remaining sessions.
        """
        database_task = self._database_watch_task
        self._database_watch_task = None
        if database_task is not None:
            database_task.cancel()
            await asyncio.gather(database_task, return_exceptions=True)
        watch_task = self._pool_watch_task
        self._pool_watch_task = None
        if watch_task is not None:
            watch_task.cancel()
            try:
                await watch_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                self.logger.debug("Proxy watch task stopped with %s", type(exc).__name__)

        retired = list(self._direct_retired)
        self._direct_retired.clear()
        session = self._session
        self._session = None
        pool = self._proxy_pool
        self._proxy_pool = None

        closers = [retired_session.close() for retired_session in retired]
        if session is not None:
            closers.append(session.close())
        if closers:
            await asyncio.gather(*closers, return_exceptions=True)

        if pool is not None:
            try:
                await pool.close()
            except Exception as exc:  # noqa: BLE001 - shutdown must release remaining resources
                self.logger.debug("Proxy pool shutdown failed with %s", type(exc).__name__)

    async def _create_proxy_pool(self) -> ProxyPool | None:
        endpoints: Sequence[ProxyEndpoint | str]
        if self.config.proxy.db_path is not None:
            max_age = self.config.proxy.db_max_age_hours if self.config.proxy.db_max_age_hours > 0 else None
            async with ProxyDatabase(self.config.proxy.db_path) as database:
                records = await database.list_active(limit=self.config.proxy.sample_size or None, max_age_hours=max_age)
            dead_path = dead_proxy_path(self.config.proxy.db_path)
            dead = load_dead_proxies(dead_path, self.config.proxy.dead_ttl_hours)
            records = [record for record in records if record.url not in dead]
            if not records:
                if self.config.proxy.mode == "always":
                    raise RuntimeError(
                        "No fresh verified proxies found in the database; run `python main.py --collect-proxies`"
                    )
                age = f"{self.config.proxy.db_max_age_hours:.0f}h" if max_age else "disabled"
                self.logger.warning(
                    "No fresh verified proxies in %s (freshness window: %s); continuing with direct requests only",
                    self.config.proxy.db_path,
                    age,
                )
                return None
            endpoints = [
                ProxyEndpoint(
                    url=record.url,
                    user_agent=record.user_agent,
                    transport=record.transport,
                    latency_ms=record.latency_ms,
                    checked_at=record.checked_at,
                    source=record.source,
                )
                for record in records
            ]
        else:
            if self.config.proxy.file is None:
                raise ValueError("Proxy mode requires a proxy file or proxy database")
            dead_path = dead_proxy_path(self.config.proxy.file)
            endpoints = load_proxies(
                self.config.proxy.file,
                self.config.proxy.sample_size or None,
                dead_path=dead_path,
                dead_ttl_hours=self.config.proxy.dead_ttl_hours,
            )

        pool = ProxyPool(
            endpoints,
            self._session_factory.create,
            dead_path=dead_path,
            dead_ttl_hours=self.config.proxy.dead_ttl_hours,
            min_ready=self.config.proxy.min_ready,
            replenish_threshold=self.config.proxy.replenish_threshold,
            failure_cooldown_seconds=self.config.proxy.failure_cooldown_seconds,
            max_failures=self.config.proxy.max_failures,
            per_ip_limit=self.config.proxy.per_ip_limit,
            network_budget=self.budget,
            metrics=self.metrics,
            verified_reuse_seconds=self.config.proxy.verified_reuse_seconds if self.config.proxy.db_path else 0,
            rate_limit_pause_seconds=self.config.rate_limit_pause_seconds,
            health_path=pool_health_path(self.config.proxy.db_path) if self.config.proxy.db_path else None,
        )
        if self.config.proxy.warmup and self.config.proxy.db_path is not None:
            reused = pool.reuse_verified(endpoints)
            if reused:
                self.logger.info("Reusing %d recently verified proxies without a readiness request", reused)
                pool.configure_warmup(self._pool_warmup_spec())
                if reused < len(endpoints):
                    pool.start_warmup(self._pool_warmup_spec())
                return pool
        if not self.config.proxy.warmup:
            pool.configure_warmup(self._pool_warmup_spec())
            try:
                self.logger.info("Skipping proxy warmup; using all %d candidates directly", len(endpoints))
                for proxy in endpoints:
                    try:
                        pool.add_session(proxy, pool.create_session(proxy))
                    except Exception as exc:  # noqa: BLE001 - skip only the unusable candidate
                        endpoint = proxy.url if isinstance(proxy, ProxyEndpoint) else proxy
                        self.logger.debug("Session unavailable for proxy %s: %s", proxy_id(endpoint), type(exc).__name__)
                        pool.record_dead(proxy)
                pool.checked = len(endpoints)
                if not pool.sessions:
                    raise RuntimeError("None of the configured proxies could be initialised")
            except asyncio.CancelledError:
                await pool.close()
                raise
            except Exception as exc:  # noqa: BLE001 - direct-only fallback stays usable
                await pool.close()
                if self.config.proxy.mode == "always":
                    raise
                self.logger.warning("Proxy pool unusable (%s); continuing with direct requests", exc)
                return None
        elif self.config.proxy.mode == "fallback":
            # Fallback must not gate the run on warmup: direct requests start
            # immediately while the background watch reports ready/failure later.
            self._start_background_warmup(pool, len(endpoints))
        else:
            try:
                await self._warm_up_pool(pool, endpoints)
            except asyncio.CancelledError:
                await pool.close()
                raise
            except Exception as exc:  # noqa: BLE001 - pool mode still has a direct fallback
                await pool.close()
                if self.config.proxy.mode == "always":
                    raise
                self.logger.warning("Proxy pool unusable (%s); continuing with direct requests", exc)
                return None
        return pool

    def _pool_warmup_spec(self) -> WarmupSpec:
        return WarmupSpec(
            url=f"{self.config.base_url}/{self.config.proxy.check_path.lstrip('/')}",
            payload=dict(self.config.proxy.check_payload),
            concurrency=min(self.config.proxy.check_concurrency, self.config.proxy.probe_concurrency),
            timeout=self.config.proxy.check_timeout,
            validator=self.config.proxy.check_validator,
        )

    def _start_background_warmup(self, pool: ProxyPool, candidate_count: int) -> None:
        self.logger.info("Warming %d proxy candidates in the background; direct requests start immediately", candidate_count)
        pool.start_warmup(self._pool_warmup_spec())

        async def watch() -> None:
            try:
                await pool.wait_until_ready()
                self.logger.info("Proxy fallback pool ready: %d proxies available", len(pool.sessions))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - a dead fallback pool must not kill direct scraping
                self.logger.warning("Proxy fallback pool unusable (%s); continuing with direct requests", exc)
                self._proxy_pool = None
                await pool.close()

        self._pool_watch_task = asyncio.create_task(watch())

    async def _warm_up_pool(self, pool: ProxyPool, proxies: Sequence[ProxyEndpoint | str]) -> None:
        self.logger.info(
            "Checking %d proxy candidates against the API (timeout %.0fs)...",
            len(proxies),
            self.config.proxy.warmup_timeout_seconds,
        )
        pool.start_warmup(self._pool_warmup_spec())
        try:
            async with asyncio.timeout(self.config.proxy.warmup_timeout_seconds):
                await pool.wait_until_ready()
        except TimeoutError as exc:
            if not pool.sessions:
                raise RuntimeError(f"No proxy became ready within {self.config.proxy.warmup_timeout_seconds:.0f}s") from exc
            self.logger.warning(
                "Proxy warmup did not finish within %.0fs; starting with %d ready proxy(s) while warmup continues",
                self.config.proxy.warmup_timeout_seconds,
                len(pool.sessions),
            )
            return
        self.logger.info(
            "Starting with %d ready proxies; warmup continues in background (%d/%d checked)",
            len(pool.sessions),
            pool.checked,
            len(proxies),
        )

    async def _switch_to_curl(self, transport: Transport) -> None:
        async with self._transport_lock:
            if self._session_factory.using_curl:
                return
            self.logger.warning("aiohttp direct transport failed; falling back to curl_cffi")
            self._session_factory.switch_to_curl()
            if self._session is transport.raw:
                self._session = self._session_factory.create()
                # Other direct requests may still be using the old session.
                self._direct_retired.append(transport.raw)

    def _route(self) -> Route:
        pool = self._proxy_pool
        ready = len(getattr(pool, "sessions", ()) or ())
        blocked = time.monotonic() < self._direct_blocked_until
        if self.config.proxy.mode == "direct":
            route, reason = Route.DIRECT, "direct mode"
        elif pool is None:
            route, reason = Route.DIRECT, "no pool"
        elif self.config.proxy.mode != "fallback":
            route, reason = Route.POOL, ("pool primary" if ready else "pool primary, waiting for capacity")
        elif blocked and self._direct_block_kind == "network" and not ready:
            route, reason = Route.DIRECT, "network block with empty pool"
        elif blocked:
            route, reason = Route.POOL, "direct blocked"
        elif ready and self.config.proxy.mix_share > 0 and random.random() < self.config.proxy.mix_share:
            route, reason = Route.POOL, "mix share"
        else:
            route, reason = Route.DIRECT, "direct primary"
        self.logger.debug(
            "route=%s reason=%s blocked=%s pool_ready=%d mode=%s",
            route.value,
            reason,
            blocked,
            ready,
            self.config.proxy.mode,
        )
        return route

    @asynccontextmanager
    async def _lease(self, route: Route) -> AsyncIterator[ProxySession]:
        if route is Route.POOL and self._proxy_pool is not None:
            # In fallback mode waiting for a proxy must never stall a worker for
            # minutes: direct downtime is bounded, so the lease wait is bounded too.
            timeout = (
                min(self.config.proxy.acquire_timeout, 20.0)
                if self.config.proxy.mode == "fallback"
                else self.config.proxy.acquire_timeout
            )
            event = attempt_context.get() or {}
            leased = False
            try:
                async with self._proxy_pool.lease(timeout, event.get("previous_proxy_id")) as session:
                    leased = True
                    mark("route", Route.POOL.value)
                    yield session
                    return
            except HTTPClientError:
                if leased or self.config.proxy.mode != "pool":
                    raise
                self.logger.warning("Proxy pool has no usable sessions; using a direct request")
        if self._session is None:
            async with self._open_lock:
                if self._session is None:
                    self._session = self._session_factory.create()
        mark("route", Route.DIRECT.value)
        mark("proxy_id", "direct")
        yield self._session

    def _mark_direct_blocked(self, reason: str, cooldown_seconds: float | None = None, *, kind: str = "origin") -> None:
        if self._proxy_pool is None:
            return
        seconds = (
            self.config.proxy.direct_block_cooldown_seconds
            if cooldown_seconds is None
            else max(1.0, min(cooldown_seconds, self.config.proxy.direct_block_cooldown_seconds))
        )
        now = time.monotonic()
        if kind == "network" and now < self._direct_blocked_until:
            # A network-type cooldown is already running; let it expire instead of
            # restarting the timer on every continued failure.
            return
        if now >= self._direct_blocked_until:
            self.logger.warning(
                "Direct connection blocked (%s); routing requests through the proxy pool for the next %.0fs", reason, seconds
            )
        self._direct_blocked_until = now + seconds
        self._direct_block_kind = kind
        self._direct_net_failures = 0

    async def _request_once(
        self, method: str, url: str, json_data: dict[str, Any] | None, params: dict[str, Any] | None
    ) -> dict[str, Any]:
        await self.open()
        route = self._route()

        async with self._lease(route) as session:
            transport = transport_for(session)
            from src.utils.proxy_url import redact_proxy_credentials

            proxy_url = getattr(self._proxy_pool, "session_proxies", {}).get(session)
            mark("proxy_address", redact_proxy_credentials(proxy_url) if proxy_url else "direct")
            mark(
                "proxy_source",
                getattr(self._proxy_pool, "session_sources", {}).get(session, "unknown") if proxy_url else "direct",
            )
            event = attempt_context.get() or {}
            use_direct = event.get("route", route.value) == Route.DIRECT.value
            self.metrics.counts["direct_attempts" if use_direct else "proxy_attempts"] += 1
            attempt_started = time.monotonic()
            try:
                async with self.budget.slot():
                    response = await self._send(transport, method, url, json_data, params)
            except NETWORK_ERRORS:
                # A slow stream that dies near its deadline is normal server
                # throttling; only a quick failure counts as an IP-block signal.
                if use_direct and time.monotonic() - attempt_started < DIRECT_BLOCK_MAX_FAILURE_SECONDS:
                    self._direct_net_failures += 1
                    if self._direct_net_failures >= DIRECT_BLOCK_FAILURE_THRESHOLD:
                        self._mark_direct_blocked("repeated direct network failures", kind="network")
                raise
            status = response.status_code
            mark("http_status", status)
            if status >= 400:
                return await self._handle_error_response(transport, method, url, json_data, params, response, status, route)
            mark("stage", "json_decode")
            started = time.monotonic()
            data = await asyncio.to_thread(_decode_json, response, "API", status)
            mark("json_decode_ms", round((time.monotonic() - started) * 1000, 3))
            if use_direct:
                self._direct_net_failures = 0
                # In-flight direct requests that started before the block may still
                # finish successfully; they must not cancel the cooldown for new requests.
                if time.monotonic() >= self._direct_blocked_until:
                    self._direct_blocked_until = 0.0
                    self._direct_block_kind = None
            else:
                assert self._proxy_pool is not None
                self._proxy_pool.record_runtime_success(session)
            return data

    async def _handle_error_response(
        self,
        transport: Transport,
        method: str,
        url: str,
        json_data: dict[str, Any] | None,
        params: dict[str, Any] | None,
        response: Response,
        status: int,
        route: Route,
    ) -> dict[str, Any]:
        is_direct = (attempt_context.get() or {}).get("route", route.value) == Route.DIRECT.value
        challenge = status == 403 and response.headers.get("cf-mitigated") == "challenge"
        if challenge and is_direct and transport.name == "aiohttp" and not self._session_factory.using_curl:
            # Retry once with browser-like TLS before cooling down the direct route.
            await self._switch_to_curl(transport)
            return await self._request_once(method, url, json_data, params)
        if challenge:
            if is_direct:
                self._mark_direct_blocked("Cloudflare challenge")
            raise HTTPClientError(
                "Cloudflare challenge blocked the request", status, retryable=is_direct and self._proxy_pool is not None
            )
        if status in {403, 407}:
            self._discard_proxy(transport.raw)
            if is_direct:
                self._mark_direct_blocked(f"HTTP {status}")
        if status == 429:
            retry_after = _retry_after_from_response(response)
            server_delay = _parse_retry_after(retry_after) if retry_after else None
            delay = server_delay if server_delay is not None else self.config.retry.delay_for(1)
            if is_direct and self._proxy_pool is not None:
                # Keep proxy requests running while the direct route cools down.
                self._mark_direct_blocked("HTTP 429", delay)
            elif not is_direct and self._proxy_pool is not None:
                proxy_url = self._proxy_pool.session_proxies.get(transport.raw)
                if proxy_url is None:
                    self.budget.pause(delay)
                    raise HTTPClientError("HTTP 429 from unidentified proxy", status, retry_after)
                effective_pause = self._proxy_pool.handle_rate_limit(proxy_url, server_delay)
                mark("retry_after_seconds", server_delay)
                raise HTTPClientError("HTTP 429 from API", status, str(effective_pause))
            else:
                # Proxy route or a direct-only client: pause the shared origin budget.
                self.budget.pause(delay)
        if status >= 400:
            raise HTTPClientError(f"HTTP {status} from API", status, _retry_after_from_response(response))
        raise HTTPClientError(f"Unhandled error response from API: HTTP {status}", status)

    async def _send(
        self, transport: Transport, method: str, url: str, json_data: dict[str, Any] | None, params: dict[str, Any] | None
    ) -> Response:
        event = attempt_context.get() or {}
        policy = (
            self.config.timeouts.policy_for("detail") if event.get("endpoint_kind") == "detail" else self.config.timeouts.api
        )
        latency_ms = self._proxy_pool.latency_for(transport.raw) if self._proxy_pool is not None else 0.0
        if latency_ms:
            mark("proxy_latency_ms", latency_ms)
            seconds = latency_ms / 1000
            scale = min(MAX_PROXY_TIMEOUT_SCALE, max(1.0, seconds / PROXY_LATENCY_TIMEOUT_DIVISOR))
            if scale > 1.0:
                policy = TimeoutPolicy(
                    total=policy.total * scale,
                    connect=max(policy.connect, seconds * 1.5),
                    sock_connect=max(policy.sock_connect, seconds * 1.5),
                    sock_read=max(policy.sock_read, seconds * 3),
                )
        mark("transport", transport.name)
        mark("timeout_policy", asdict(policy))
        mark("stage", "transport")
        guard = asyncio.timeout(policy.total + HARD_TIMEOUT_GRACE_SECONDS)
        try:
            async with guard:
                self.metrics.counts["wire_requests"] += 1
                return await transport.request(method, url, json_data=json_data, params=params, timeout=policy)
        except NETWORK_ERRORS as exc:
            mark("error_class", type(exc).__name__)
            if isinstance(exc, TimeoutError):
                kind = {"SocketTimeoutError": "read_inactivity", "ConnectionTimeoutError": "connect"}.get(
                    type(exc).__name__, "total"
                )
                mark("timeout_kind", "hard_deadline" if guard.expired() else kind)
            mark("error_stage", "hard_deadline" if guard.expired() else event.get("stage", "transport"))
            mark("curl_error_code", getattr(exc, "code", None))
            mark("os_error_code", getattr(exc, "errno", None))
            # Certificate errors are not cured by retries or transport fallback.
            if "certificate" in type(exc).__name__.lower() or getattr(exc, "code", None) == 60:
                raise HTTPClientError("TLS certificate verification failed", retryable=False) from exc
            if self._proxy_pool is None and transport.name == "aiohttp":
                await self._switch_to_curl(transport)
            else:
                self._discard_proxy(transport.raw)
            raise

    def _discard_proxy(self, session: Any) -> None:
        if self._proxy_pool is not None and self._proxy_pool.discard(session):
            self.logger.warning("Removed failed proxy from rotation; %d remain", len(self._proxy_pool.sessions))

    async def request(
        self,
        method: str,
        path: str,
        json_data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        *,
        endpoint_kind: str = "api",
    ) -> dict[str, Any]:
        """Public entry point for API calls."""
        url = f"{self.config.base_url}/{path.lstrip('/')}"
        previous_proxy = None
        path_parts = path.strip("/").split("/")
        kind = endpoint_kind
        request_id = uuid.uuid4().hex
        for attempt in range(1, self.config.retry.attempts + 1):
            self.requests_made += 1
            if attempt > 1:
                self.retries_made += 1
            event = {
                **(work_context.get() or {}),
                "run_id": getattr(self, "run_id", None),
                "request_id": request_id,
                "attempt": attempt,
                "endpoint_kind": kind,
                "method": method,
                "path": path.split("?", 1)[0],
                "publication_id": path_parts[-1] if kind == "detail" else None,
                "previous_proxy_id": previous_proxy,
            }
            token = attempt_context.set(event)
            started = time.monotonic()
            try:
                data = await self._request_once(method, url, json_data, params)
                event["ok"] = True
                return data
            except HTTPClientError as exc:
                event["error_class"] = event.get("error_class", type(exc).__name__)
                if not exc.retryable or attempt == self.config.retry.attempts:
                    raise
                error = exc
            except NETWORK_ERRORS as exc:
                event["error_class"] = type(exc).__name__
                error = HTTPClientError(f"Network error: {type(exc).__name__}", retryable=True)
                if attempt == self.config.retry.attempts:
                    raise error from exc
            except asyncio.CancelledError:
                event["error_class"] = "CancelledError"
                raise
            except Exception as exc:  # noqa: BLE001 - record the error class before re-raising
                event["error_class"] = type(exc).__name__
                raise
            finally:
                event["total_attempt_ms"] = round((time.monotonic() - started) * 1000, 3)
                previous_proxy = event.get("proxy_id")
                self.metrics.record(event)
                attempt_context.reset(token)
            await self._wait_before_retry(error, attempt)
        raise RuntimeError("Retry loop ended without a result or an exception")

    async def _wait_before_retry(self, error: HTTPClientError, attempt: int) -> None:
        delay = self.config.retry.delay_for(attempt, error.retry_after)
        self.logger.warning("%s. Retrying in %.2fs (attempt %d/%d)", error, delay, attempt + 1, self.config.retry.attempts)
        await asyncio.sleep(delay)


def _retry_after_from_response(response: Response) -> str | None:
    """Prefer an explicit API ``retry_after`` value over the HTTP header."""
    return retry_after_from_response(response)


def _decode_json(response: Response, url: str, status: int) -> dict[str, Any]:
    try:
        data = response.json()
    except (ValueError, UnicodeError) as exc:
        raise HTTPClientError(f"Invalid JSON response from {url}", status) from exc
    if not isinstance(data, dict):
        raise HTTPClientError(f"Unexpected non-object response format from {url}", status)
    return data
