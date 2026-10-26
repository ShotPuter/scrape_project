"""HTTP transport with aiohttp first, curl_cffi fallback, and bounded retries."""

from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import asdict
from enum import StrEnum
from pathlib import Path
from typing import Any, Self

from curl_cffi.requests.exceptions import RequestException

from src.client.aiohttp_session import NETWORK_ERRORS as AIOHTTP_NETWORK_ERRORS
from src.client.config import ClientConfig, ProxyConfig, RetryConfig, TimeoutConfig, parse_retry_after as _parse_retry_after
from src.client.errors import HTTPClientError
from src.client.network import NetworkBudget, RequestMetrics, TimeoutPolicy, attempt_context, mark, proxy_id, work_context
from src.client.proxy_pool import ProxyEndpoint, ProxyPool, dead_proxy_path, load_dead_proxies, load_proxies
from src.client.session_factory import SessionFactory
from src.client.transport import Transport, transport_for
from src.storage.proxy_database import ProxyDatabase

logger = logging.getLogger(__name__)

NETWORK_ERRORS = (RequestException, asyncio.TimeoutError) + AIOHTTP_NETWORK_ERRORS
MAX_ERROR_BODY_CHARS = 2000
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


# Kept as an alias: the validated retry data now lives in ``RetryConfig``.
RetryPolicy = RetryConfig


class HttpClient:
    def __init__(
        self, base_url: str = "", headers: Mapping[str, str] | None = None,
        timeout_seconds: float = 30, retry_attempts: int = 3, retry_backoff_factor: float = 1.5,
        retry_jitter_seconds: float = 0.3, connection_limit: int = 9, impersonate: str = "chrome",
        proxy_file: Path | None = None, proxy_db_path: Path | None = None, proxy_sample_size: int = 0,
        proxy_check_concurrency: int = 5, proxy_check_timeout: float = 10, proxy_min_ready: int = 20,
        proxy_replenish_threshold: int = 10, proxy_failure_cooldown_seconds: float = 120, proxy_max_failures: int = 3,
        proxy_dead_ttl_hours: float = 1, proxy_check_path: str = "", proxy_check_payload: dict | None = None,
        cookies_file: Path | None = None, proxy_warmup: bool = True, transport: str = "aiohttp",
        api_timeout_policy: TimeoutPolicy | None = None, detail_timeout_policy: TimeoutPolicy | None = None,
        requests_per_second: float = 0, proxy_per_ip_limit: int = 1, proxy_probe_concurrency: int = 2,
        proxy_acquire_timeout: float = 150, direct_block_cooldown_seconds: float = 300, proxy_mode: str = "pool",
        proxy_mix_share: float = 0, proxy_db_max_age_hours: float = 24, proxy_warmup_timeout_seconds: float = 90,
        config: ClientConfig | None = None,
    ):
        if config is None:
            api_policy = api_timeout_policy or TimeoutPolicy(total=timeout_seconds)
            config = ClientConfig(
                base_url=base_url, headers=dict(headers or {}),
                timeouts=TimeoutConfig(api=api_policy, detail=detail_timeout_policy or api_policy),
                retry=RetryConfig(retry_attempts, retry_backoff_factor, retry_jitter_seconds),
                proxy=ProxyConfig(
                    file=proxy_file, db_path=proxy_db_path, mode=proxy_mode, sample_size=proxy_sample_size,
                    check_concurrency=proxy_check_concurrency, check_timeout=proxy_check_timeout,
                    min_ready=proxy_min_ready, replenish_threshold=proxy_replenish_threshold,
                    failure_cooldown_seconds=proxy_failure_cooldown_seconds, max_failures=proxy_max_failures,
                    dead_ttl_hours=proxy_dead_ttl_hours, check_path=proxy_check_path,
                    check_payload=dict(proxy_check_payload or {}), warmup=proxy_warmup,
                    per_ip_limit=proxy_per_ip_limit, probe_concurrency=proxy_probe_concurrency,
                    acquire_timeout=proxy_acquire_timeout,
                    direct_block_cooldown_seconds=direct_block_cooldown_seconds, mix_share=proxy_mix_share,
                    db_max_age_hours=proxy_db_max_age_hours,
                    warmup_timeout_seconds=proxy_warmup_timeout_seconds,
                ),
                connection_limit=connection_limit, impersonate=impersonate, cookies_file=cookies_file,
                transport=transport, requests_per_second=requests_per_second,
            )
        self.config = config
        proxy = config.proxy
        self.retry_policy = config.retry
        self.base_url = config.base_url
        self.headers = dict(config.headers)
        self.timeout = config.timeouts.api.total
        self.api_timeout_policy = config.timeouts.api
        self.detail_timeout_policy = config.timeouts.policy_for("detail")
        self.budget = NetworkBudget(config.connection_limit, config.requests_per_second)
        self.metrics = RequestMetrics()
        self.proxy_per_ip_limit = proxy.per_ip_limit
        self.proxy_probe_concurrency = proxy.probe_concurrency
        self.proxy_acquire_timeout = proxy.acquire_timeout
        self.proxy_mode = proxy.mode
        self.direct_block_cooldown_seconds = proxy.direct_block_cooldown_seconds
        self.proxy_mix_share = proxy.mix_share
        self._direct_blocked_until = 0.0
        self._direct_block_kind: str | None = None
        self._direct_net_failures = 0
        self._pool_watch_task: asyncio.Task[None] | None = None
        self._direct_retired = []
        self.retry_attempts = self.retry_policy.attempts
        self.retry_backoff_factor = self.retry_policy.backoff_factor
        self.retry_jitter_seconds = self.retry_policy.jitter_seconds
        self.proxy_file = proxy.file
        self.proxy_db_path = proxy.db_path
        self.proxy_sample_size = proxy.sample_size
        self.proxy_check_concurrency = proxy.check_concurrency
        self.proxy_check_timeout = proxy.check_timeout
        self.proxy_min_ready = proxy.min_ready
        self.proxy_replenish_threshold = proxy.replenish_threshold
        self.proxy_failure_cooldown_seconds = proxy.failure_cooldown_seconds
        self.proxy_max_failures = proxy.max_failures
        self.proxy_dead_ttl_hours = proxy.dead_ttl_hours
        self.proxy_db_max_age_hours = proxy.db_max_age_hours
        self.proxy_warmup_timeout_seconds = proxy.warmup_timeout_seconds
        self.proxy_check_path = proxy.check_path
        self.proxy_check_payload = dict(proxy.check_payload)
        self.proxy_warmup = proxy.warmup
        self.logger = logger
        self._session_factory = SessionFactory(
            headers=self.headers, timeout=self.timeout, connection_limit=config.connection_limit,
            impersonate=config.impersonate, cookies_file=config.cookies_file, transport=config.transport,
            sock_read_timeout=self.api_timeout_policy.sock_read,
        )
        self._session = None
        self._proxy_pool: ProxyPool | None = None
        self._open_lock = asyncio.Lock()
        self._transport_lock = asyncio.Lock()
        self.connection_limit = config.connection_limit
        self._semaphore = self.budget.semaphore
        self.requests_made = 0
        self.retries_made = 0
        self.run_id: str | None = None

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def open(self) -> None:
        async with self._open_lock:
            if self._session is not None or self._proxy_pool is not None:
                return
            if not self.proxy_file and not self.proxy_db_path:
                self._session = self._session_factory.create()
                return
            if self.proxy_mode == "direct":
                self.logger.warning("PROXY_MODE=direct: proxy configuration ignored, scraping without proxies")
                self._session = self._session_factory.create()
                return
            pool = await self._create_proxy_pool()
            self._proxy_pool = pool
            if self.proxy_mode == "fallback" or pool is None:
                # Fallback keeps a healthy direct session; the pool is only used when
                # the origin blocks the direct route. Pool mode creates its direct
                # fallback lazily, only if the pool turns out to be empty.
                self._session = self._session_factory.create()

    async def close(self) -> None:
        """Release every owned resource, even if one closer fails.

        All references are detached before awaiting so repeated calls are safe
        and a partially failed shutdown cannot leak the remaining sessions.
        """
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
        if self.proxy_db_path is not None:
            max_age = self.proxy_db_max_age_hours if self.proxy_db_max_age_hours > 0 else None
            async with ProxyDatabase(self.proxy_db_path) as database:
                records = await database.list_active(
                    limit=self.proxy_sample_size or None, max_age_hours=max_age
                )
            dead_path = dead_proxy_path(self.proxy_db_path)
            dead = load_dead_proxies(dead_path, self.proxy_dead_ttl_hours)
            records = [record for record in records if record.url not in dead]
            if not records:
                if self.proxy_mode == "always":
                    raise RuntimeError(
                        "No fresh verified proxies found in the database; run `python main.py --collect-proxies`"
                    )
                age = f"{self.proxy_db_max_age_hours:.0f}h" if max_age else "disabled"
                self.logger.warning(
                    "No fresh verified proxies in %s (freshness window: %s); continuing with direct requests only",
                    self.proxy_db_path, age,
                )
                return None
            proxies = [
                ProxyEndpoint(
                    url=record.url, user_agent=record.user_agent, transport=record.transport, latency_ms=record.latency_ms
                )
                for record in records
            ]
        else:
            if self.proxy_file is None:
                raise ValueError("Proxy mode requires a proxy file or proxy database")
            dead_path = dead_proxy_path(self.proxy_file)
            proxies = load_proxies(
                self.proxy_file, self.proxy_sample_size or None, dead_path=dead_path,
                dead_ttl_hours=self.proxy_dead_ttl_hours,
            )

        pool = ProxyPool(
            proxies, self._session_factory.create, dead_path=dead_path,
            dead_ttl_hours=self.proxy_dead_ttl_hours, min_ready=self.proxy_min_ready,
            replenish_threshold=self.proxy_replenish_threshold, failure_cooldown_seconds=self.proxy_failure_cooldown_seconds,
            max_failures=self.proxy_max_failures, per_ip_limit=self.proxy_per_ip_limit, network_budget=self.budget,
            metrics=self.metrics,
        )
        pool.configure_warmup(
            f"{self.base_url}/{self.proxy_check_path.lstrip('/')}", self.proxy_check_payload,
            min(self.proxy_check_concurrency, self.proxy_probe_concurrency), self.proxy_check_timeout,
        )
        if not self.proxy_warmup:
            try:
                self._add_proxy_sessions(pool, proxies)
            except asyncio.CancelledError:
                await pool.close()
                raise
            except Exception as exc:  # noqa: BLE001 - direct-only fallback stays usable
                await pool.close()
                if self.proxy_mode == "always":
                    raise
                self.logger.warning("Proxy pool unusable (%s); continuing with direct requests", exc)
                return None
        elif self.proxy_mode == "fallback":
            # Fallback must not gate the run on warmup: direct requests start
            # immediately while the background watch reports ready/failure later.
            self._start_background_warmup(pool, len(proxies))
        else:
            try:
                await self._warm_up_pool(pool, proxies)
            except asyncio.CancelledError:
                await pool.close()
                raise
            except Exception as exc:  # noqa: BLE001 - pool mode still has a direct fallback
                await pool.close()
                if self.proxy_mode == "always":
                    raise
                self.logger.warning("Proxy pool unusable (%s); continuing with direct requests", exc)
                return None
        return pool

    def _start_background_warmup(self, pool: ProxyPool, candidate_count: int) -> None:
        self.logger.info("Warming %d proxy candidates in the background; direct requests start immediately", candidate_count)
        pool.start_warmup(
            f"{self.base_url}/{self.proxy_check_path.lstrip('/')}", self.proxy_check_payload,
            min(self.proxy_check_concurrency, self.proxy_probe_concurrency), self.proxy_check_timeout,
        )

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

    async def _warm_up_pool(self, pool: ProxyPool, proxies: list[ProxyEndpoint | str]) -> None:
        self.logger.info(
            "Checking %d proxy candidates against the API (timeout %.0fs)...",
            len(proxies), self.proxy_warmup_timeout_seconds,
        )
        pool.start_warmup(
            f"{self.base_url}/{self.proxy_check_path.lstrip('/')}", self.proxy_check_payload,
            min(self.proxy_check_concurrency, self.proxy_probe_concurrency), self.proxy_check_timeout,
        )
        try:
            async with asyncio.timeout(self.proxy_warmup_timeout_seconds):
                await pool.wait_until_ready()
        except TimeoutError as exc:
            if not pool.sessions:
                raise RuntimeError(
                    f"No proxy became ready within {self.proxy_warmup_timeout_seconds:.0f}s"
                ) from exc
            self.logger.warning(
                "Proxy warmup did not finish within %.0fs; starting with %d ready proxy(s) while warmup continues",
                self.proxy_warmup_timeout_seconds, len(pool.sessions),
            )
            return
        self.logger.info(
            "Starting with %d ready proxies; warmup continues in background (%d/%d checked)", len(pool.sessions),
            pool.checked, len(proxies),
        )

    def _add_proxy_sessions(self, pool: ProxyPool, proxies: list[ProxyEndpoint | str]) -> None:
        self.logger.info("Skipping proxy warmup; using all %d candidates directly", len(proxies))
        for proxy in proxies:
            try:
                pool.add_session(proxy, pool.create_session(proxy))
            except Exception as exc:  # noqa: BLE001 - skip only the unusable candidate
                endpoint = proxy.url if isinstance(proxy, ProxyEndpoint) else proxy
                self.logger.debug("Session unavailable for proxy %s: %s", proxy_id(endpoint), type(exc).__name__)
                pool.record_dead(proxy)
        pool.checked = len(proxies)
        if not pool.sessions:
            raise RuntimeError("None of the configured proxies could be initialised")

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

    def _backoff_seconds(self, attempt: int, retry_after: str | None = None) -> float:
        return self.retry_policy.delay_for(attempt, retry_after)

    def _timeout_policy_for(self, session, policy: TimeoutPolicy) -> TimeoutPolicy:
        latency_ms = self._proxy_pool.latency_for(session) if self._proxy_pool is not None else 0.0
        if latency_ms <= 0:
            return policy
        seconds = latency_ms / 1000
        scale = min(MAX_PROXY_TIMEOUT_SCALE, max(1.0, seconds / PROXY_LATENCY_TIMEOUT_DIVISOR))
        if scale == 1.0:
            return policy
        return TimeoutPolicy(
            total=policy.total * scale, connect=max(policy.connect, seconds * 1.5),
            sock_connect=max(policy.sock_connect, seconds * 1.5), sock_read=max(policy.sock_read, seconds * 3),
        )

    async def _direct_session(self):
        if self._session is not None:
            return self._session
        async with self._open_lock:
            if self._session is None:
                self._session = self._session_factory.create()
        return self._session

    def _route(self) -> Route:
        pool = self._proxy_pool
        ready = len(getattr(pool, "sessions", ()) or ())
        blocked = time.monotonic() < self._direct_blocked_until

        route, reason = self._route_decision(ready, blocked)
        self.logger.debug(
            "route=%s reason=%s blocked=%s pool_ready=%d mode=%s", route.value, reason, blocked, ready, self.proxy_mode
        )
        return route

    def _route_decision(self, ready: int, blocked: bool) -> tuple[Route, str]:
        if self.proxy_mode == "direct":
            return Route.DIRECT, "direct mode"
        if self._proxy_pool is None:
            return Route.DIRECT, "no pool"
        if self.proxy_mode != "fallback":
            reason = "pool primary" if ready else "pool primary, waiting for capacity"
            return Route.POOL, reason
        if blocked and self._direct_block_kind == "network" and not ready:
            return Route.DIRECT, "network block with empty pool"
        if blocked:
            return Route.POOL, "direct blocked"
        if ready and self.proxy_mix_share > 0 and random.random() < self.proxy_mix_share:
            return Route.POOL, "mix share"
        return Route.DIRECT, "direct primary"

    async def _use_direct_for_attempt(self) -> bool:
        return self._route() is Route.DIRECT

    @asynccontextmanager
    async def _lease(self, route: Route):
        if route is Route.POOL and self._proxy_pool is not None:
            # In fallback mode waiting for a proxy must never stall a worker for
            # minutes: direct downtime is bounded, so the lease wait is bounded too.
            timeout = min(self.proxy_acquire_timeout, 20.0) if self.proxy_mode == "fallback" else self.proxy_acquire_timeout
            event = attempt_context.get() or {}
            leased = False
            try:
                async with self._proxy_pool.lease(timeout, event.get("previous_proxy_id")) as session:
                    leased = True
                    mark("route", Route.POOL.value)
                    yield session
                    return
            except HTTPClientError:
                if leased or self.proxy_mode != "pool":
                    raise
                self.logger.warning("Proxy pool has no usable sessions; using a direct request")
        session = await self._direct_session()
        mark("route", Route.DIRECT.value)
        mark("proxy_id", "direct")
        yield session

    def _note_direct_success(self) -> None:
        self._direct_net_failures = 0
        # In-flight direct requests that started before the block may still finish
        # successfully; they must not cancel the cooldown for new requests.
        if time.monotonic() >= self._direct_blocked_until:
            self._direct_blocked_until = 0.0
            self._direct_block_kind = None

    def _note_direct_network_failure(self) -> None:
        self._direct_net_failures += 1
        if self._direct_net_failures >= DIRECT_BLOCK_FAILURE_THRESHOLD:
            self._mark_direct_blocked("repeated direct network failures", kind="network")

    def _mark_direct_blocked(self, reason: str, cooldown_seconds: float | None = None, *, kind: str = "origin") -> None:
        if self._proxy_pool is None:
            return
        seconds = (
            self.direct_block_cooldown_seconds
            if cooldown_seconds is None
            else max(1.0, min(cooldown_seconds, self.direct_block_cooldown_seconds))
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

    async def _request_once(self, method, url, json_data, params, use_direct=None) -> dict[str, Any]:
        await self.open()
        if use_direct is None:
            route = self._route()
        else:
            route = Route.DIRECT if use_direct else Route.POOL
        async with self._lease(route) as session:
            transport = transport_for(session)
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
                    self._note_direct_network_failure()
                raise
            status = response.status_code
            mark("http_status", status)
            if status >= 400:
                return await self._handle_error_response(
                    transport, method, url, json_data, params, response, status, use_direct
                )
            mark("stage", "json_decode")
            started = time.monotonic()
            data = await asyncio.to_thread(_decode_json, response, "API", status)
            mark("json_decode_ms", round((time.monotonic() - started) * 1000, 3))
            if use_direct:
                self._note_direct_success()
            else:
                self._proxy_pool.record_runtime_success(session)
            return data

    async def _handle_error_response(
        self, transport: Transport, method, url, json_data, params, response, status, use_direct
    ) -> dict[str, Any]:
        challenge = status == 403 and response.headers.get("cf-mitigated") == "challenge"
        if challenge and use_direct and transport.name == "aiohttp" and not self._session_factory.using_curl:
            # Retry once with browser-like TLS before cooling down the direct route.
            await self._switch_to_curl(transport)
            return await self._request_once(method, url, json_data, params)
        if challenge:
            if use_direct:
                self._mark_direct_blocked("Cloudflare challenge")
            raise HTTPClientError(
                "Cloudflare challenge blocked the request", status, retryable=use_direct and self._proxy_pool is not None
            )
        if status in {403, 407}:
            self._discard_proxy(transport.raw)
            if use_direct:
                self._mark_direct_blocked(f"HTTP {status}")
        if status == 429:
            self._handle_rate_limit(response, use_direct)
        if status >= 400:
            raise HTTPClientError(f"HTTP {status} from API", status, _retry_after_from_response(response))
        raise HTTPClientError(f"Unhandled error response from API: HTTP {status}", status)

    def _handle_rate_limit(self, response, use_direct: bool) -> None:
        retry_after = _retry_after_from_response(response)
        server_delay = _parse_retry_after(retry_after) if retry_after else None
        delay = server_delay if server_delay is not None else self._backoff_seconds(1)
        if use_direct and self._proxy_pool is not None:
            # Keep proxy requests running while the direct route cools down.
            self._mark_direct_blocked("HTTP 429", delay)
            return
        # Proxy route or a direct-only client: pause the shared origin budget.
        self.budget.pause(delay)

    async def _select_session(self):
        # Kept for callers inspecting the selected transport. Requests use leases.
        if self._route() is not Route.POOL or self._proxy_pool is None:
            return await self._direct_session()
        if not await self._proxy_pool.wait_for_session(self.proxy_acquire_timeout):
            raise HTTPClientError("All proxies have failed; working proxy pool is empty")
        return self._proxy_pool.choose()

    async def _send(self, transport: Transport, method: str, url: str, json_data, params):
        event = attempt_context.get() or {}
        policy = self.detail_timeout_policy if event.get("endpoint_kind") == "detail" else self.api_timeout_policy
        if self._proxy_pool is not None:
            latency_ms = self._proxy_pool.latency_for(transport.raw)
            if latency_ms:
                mark("proxy_latency_ms", latency_ms)
        policy = self._timeout_policy_for(transport.raw, policy)
        mark("transport", transport.name)
        mark("timeout_policy", asdict(policy))
        mark("stage", "transport")
        guard = asyncio.timeout(policy.total + HARD_TIMEOUT_GRACE_SECONDS)
        try:
            async with guard:
                self.metrics.counts["wire_requests"] += 1
                response = await transport.request(
                    method, url, json_data=json_data, params=params, timeout=policy
                )
            return response
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

    def _discard_proxy(self, session) -> None:
        if self._proxy_pool is not None and self._proxy_pool.discard(session):
            self.logger.warning("Removed failed proxy from rotation; %d remain", len(self._proxy_pool.sessions))

    async def request(
        self, method: str, path: str, json_data: dict[str, Any] | None = None, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Public entry point for API calls."""
        return await self._request(method, path, json_data, params)

    async def _request(
        self, method: str, path: str, json_data: dict[str, Any] | None = None, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        previous_proxy = None
        path_parts = path.strip("/").split("/")
        kind = "detail" if method == "GET" and len(path_parts) == 2 and path_parts[0] == "publicaciones" else "api"
        request_id = uuid.uuid4().hex
        for attempt in range(1, self.retry_attempts + 1):
            self.requests_made += 1
            if attempt > 1:
                self.retries_made += 1
            event = {
                **(work_context.get() or {}),
                "run_id": getattr(self, "run_id", None),
                "request_id": request_id,
                "attempt": attempt,
                "endpoint_kind": kind,
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
                if not exc.retryable or attempt == self.retry_attempts:
                    raise
                error = exc
            except NETWORK_ERRORS as exc:
                event["error_class"] = type(exc).__name__
                error = HTTPClientError(f"Network error: {type(exc).__name__}", retryable=True)
                if attempt == self.retry_attempts:
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
        delay = self._backoff_seconds(attempt, error.retry_after)
        self.logger.warning("%s. Retrying in %.2fs (attempt %d/%d)", error, delay, attempt + 1, self.retry_attempts)
        await asyncio.sleep(delay)


# Backwards-compatible name: the client is designed for composition, not subclassing.
BaseHTTPClient = HttpClient


def _retry_after_from_response(response) -> str | None:
    """Prefer an explicit API ``retry_after`` value over the HTTP header."""
    header = response.headers.get("Retry-After")
    try:
        payload = response.json()
    except (ValueError, UnicodeError):
        return header
    if not isinstance(payload, dict):
        return header
    value = payload.get("retry_after")
    return str(value) if value is not None else header


def _decode_json(response, url: str, status: int) -> dict[str, Any]:
    try:
        data = response.json()
    except (ValueError, UnicodeError) as exc:
        raise HTTPClientError(f"Invalid JSON response from {url}", status) from exc
    if not isinstance(data, dict):
        raise HTTPClientError(f"Unexpected non-object response format from {url}", status)
    return data
