"""Proxy list loading, readiness checks, and dead-proxy bookkeeping."""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import time
import uuid
from collections.abc import AsyncIterator, Callable, Iterable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from curl_cffi.requests import AsyncSession

from src.network.budget import NetworkBudget, RequestMetrics, attempt_context, proxy_id
from src.network.errors import HTTPClientError
from src.proxy.probes import is_success_response
from src.utils.atomic import atomic_write_json
from src.utils.http import parse_retry_after, retry_after_from_response
from src.utils.proxy_url import proxy_host, proxy_url_from_row

logger = logging.getLogger(__name__)

DEAD_PROXY_TTL_HOURS = 1.0
WARMUP_PROGRESS_EVERY = 25
WARMUP_PROGRESS_SECONDS = 10.0
HARD_TIMEOUT_GRACE_SECONDS = 1.0
DEFAULT_FAILURE_COOLDOWN_SECONDS = 120.0
DEFAULT_MAX_FAILURES = 3

ProxySession = AsyncSession[Any]
SessionFactory = Callable[..., ProxySession]


@dataclass(frozen=True, slots=True)
class ProxyEndpoint:
    """A proxy together with the user agent and transport that verified it."""

    url: str
    user_agent: str | None = None
    transport: str | None = None
    latency_ms: float = 0.0
    checked_at: str = ""
    source: str = ""


@dataclass(frozen=True, slots=True)
class WarmupSpec:
    """The readiness probe shared by every proxy warmup."""

    url: str
    payload: dict[str, Any] = field(default_factory=dict)
    concurrency: int = 5
    timeout: float = 10
    validator: Callable[[Any], bool] = is_success_response


def _as_endpoint(proxy: str | ProxyEndpoint) -> ProxyEndpoint:
    return proxy if isinstance(proxy, ProxyEndpoint) else ProxyEndpoint(url=proxy)


def _dedupe_endpoints(proxies: Iterable[str | ProxyEndpoint]) -> list[ProxyEndpoint]:
    """Keep one preferred record per proxy URL (collector may store both transports)."""
    endpoints: list[ProxyEndpoint] = []
    seen: set[str] = set()
    for proxy in proxies:
        endpoint = _as_endpoint(proxy)
        if endpoint.url in seen:
            continue
        seen.add(endpoint.url)
        endpoints.append(endpoint)
    return endpoints


def dead_proxy_path(proxy_path: Path) -> Path:
    return proxy_path.with_suffix(".dead.json")


def load_proxies(
    path: Path, limit: int | None = None, dead_path: Path | None = None, dead_ttl_hours: float = DEAD_PROXY_TTL_HOURS
) -> list[str]:
    if limit is not None and limit < 0:
        raise ValueError("Proxy sample size must be >= 0 (0 = all)")

    dead = set(load_dead_proxies(dead_path, dead_ttl_hours))
    proxies: list[str] = []
    seen: set[str] = set()
    with path.open(encoding="utf-8-sig", newline="") as handle:
        rows = (
            ({"proxy": line.strip()} for line in handle if line.strip() and not line.lstrip().startswith("#"))
            if path.suffix.lower() == ".txt"
            else csv.DictReader(handle)
        )
        for row in rows:
            url = _parse_proxy_row(row)
            if url is None or url in dead or url in seen:
                continue
            seen.add(url)
            proxies.append(url)
            if limit and len(proxies) >= limit:
                break

    if not proxies:
        raise ValueError("No usable proxy addresses found in proxy file")
    return proxies


def _parse_proxy_row(row: dict[str, str]) -> str | None:
    if row.get("alive", "true").lower() == "false":
        return None
    return proxy_url_from_row(row)


class ProxyPool:
    """Keeps healthy proxies in rotation and replenishes them while scraping."""

    def __init__(
        self,
        proxies: Iterable[str | ProxyEndpoint],
        session_factory: SessionFactory,
        dead_path: Path | None = None,
        dead_ttl_hours: float = DEAD_PROXY_TTL_HOURS,
        min_ready: int = 1,
        replenish_threshold: int = 0,
        failure_cooldown_seconds: float = DEFAULT_FAILURE_COOLDOWN_SECONDS,
        max_failures: int = DEFAULT_MAX_FAILURES,
        per_ip_limit: int = 1,
        network_budget: NetworkBudget | None = None,
        metrics: RequestMetrics | None = None,
        verified_reuse_seconds: float = 0,
        rate_limit_pause_seconds: float = 150,
        health_path: Path | None = None,
    ):
        self.proxies = _dedupe_endpoints(proxies)
        self._verified_at = {endpoint.url: endpoint.checked_at for endpoint in self.proxies}
        self.session_factory = session_factory
        self.verified_reuse_seconds = verified_reuse_seconds
        self.rate_limit_pause_seconds = rate_limit_pause_seconds
        self.health_path = health_path
        self._health_task: asyncio.Task[None] | None = None
        self._health_dirty = False
        self._last_health_write = 0.0
        self._recent_rate_limits: dict[str, float] = {}
        self.sessions: list[ProxySession] = []
        self.session_proxies: dict[ProxySession, str] = {}
        self.session_latency: dict[ProxySession, float] = {}
        self.session_sources: dict[ProxySession, str] = {}
        self._last_source_tier: str | None = None
        self.retired: list[ProxySession] = []
        self.next_index = 0
        self.checked = 0
        self.dead_path = dead_path
        self._dead = load_dead_proxies(dead_path, dead_ttl_hours)
        self.min_ready = min_ready
        self.replenish_threshold = replenish_threshold
        self.failure_cooldown_seconds = failure_cooldown_seconds
        self.max_failures = max_failures
        self.per_ip_limit = per_ip_limit
        self.budget = network_budget or NetworkBudget(5)
        self.metrics = metrics or RequestMetrics()
        self.in_flight: dict[ProxySession, int] = {}
        self._host_in_flight: dict[str, int] = {}
        self._changed = asyncio.Event()
        self._closed = False
        self._close_lock = asyncio.Lock()
        self._probe_slots = asyncio.Semaphore(1)
        self._persist_task: asyncio.Task[None] | None = None
        self._dirty = False
        self._close_tasks: set[asyncio.Task[None]] = set()
        self._active: set[str] = set()
        self._scheduled: set[str] = set()
        self._failure_counts: dict[str, int] = {}
        self._cooldown_until: dict[str, float] = {}
        self._rate_limit_until: dict[str, float] = {}
        self._permanent_dead: set[str] = set()
        self._ready = asyncio.Event()
        self._warmup_task: asyncio.Task[None] | None = None
        self._replenish_task: asyncio.Task[None] | None = None
        self._warmup_candidates: list[ProxyEndpoint] = []
        self._warmup_url: str | None = None
        self._warmup_payload: dict[str, Any] = {}
        self._warmup_concurrency = 1
        self._warmup_timeout = 10.0
        self._warmup_validator = is_success_response
        self._warmup_error: RuntimeError | None = None
        self._warmup_started = time.monotonic()
        self._last_progress_log = self._warmup_started
        self._ready_announced = False
        self._refresh_pending = False

    def update_candidates(self, proxies: Iterable[ProxyEndpoint]) -> None:
        """Apply a verified database snapshot without interrupting leased sessions."""
        if self._closed:
            return
        previous = {endpoint.url: endpoint for endpoint in self.proxies}
        endpoints = _dedupe_endpoints(proxies)
        current = {endpoint.url: endpoint for endpoint in endpoints}
        self.proxies = endpoints
        for endpoint in endpoints:
            if (
                endpoint.checked_at
                and endpoint.checked_at > self._verified_at.get(endpoint.url, "")
                and self._rate_limit_until.get(endpoint.url, 0) <= time.monotonic()
            ):
                self._permanent_dead.discard(endpoint.url)
                self._failure_counts.pop(endpoint.url, None)
                self._cooldown_until.pop(endpoint.url, None)
                if self._dead.pop(endpoint.url, None) is not None:
                    self._dirty = True
            self._verified_at[endpoint.url] = max(endpoint.checked_at, self._verified_at.get(endpoint.url, ""))
        for session in list(self.sessions):
            url = self.session_proxies.get(session, "")
            endpoint = current.get(url)
            old = previous.get(url)
            if endpoint is None or (old and (old.transport, old.user_agent) != (endpoint.transport, endpoint.user_agent)):
                self._retire(session)
            elif endpoint:
                self.session_latency[session] = endpoint.latency_ms
                self.session_sources[session] = endpoint.source
        self._flush_dead()
        self._changed.set()
        self.reuse_verified(endpoints)
        self._refresh_pending = True
        self.ensure_capacity()
        self._publish_health()

    def _publish_health(self) -> None:
        if self.health_path is None:
            return
        self._health_dirty = True
        if self._health_task is None or self._health_task.done():
            self._health_task = asyncio.create_task(self._persist_health())

    async def _persist_health(self) -> None:
        path = self.health_path
        if path is None:
            return
        while self._health_dirty:
            self._health_dirty = False
            payload = {
                "updated_at": datetime.now(UTC).isoformat(),
                "status": "stopped" if self._closed else "running",
                "webshare_usable": len(
                    {self.session_proxies[s] for s in self.sessions if self.session_sources.get(s) == "webshare"}
                ),
            }
            try:
                await asyncio.to_thread(atomic_write_json, path, payload)
            except OSError:
                logger.warning("Could not publish proxy pool health")
            self._last_health_write = time.monotonic()

    def reuse_verified(self, proxies: Iterable[str | ProxyEndpoint]) -> int:
        """Create sessions from recent collector receipts without another network probe."""
        if self._closed or self.verified_reuse_seconds <= 0:
            return 0
        now = datetime.now(UTC)
        reused = 0
        for proxy in proxies:
            endpoint = _as_endpoint(proxy)
            url = endpoint.url
            if not endpoint.checked_at or url in self._active or url in self._scheduled:
                continue
            if (
                url in self._permanent_dead
                or url in self._failure_counts
                or self._rate_limit_until.get(url, 0) > time.monotonic()
            ):
                continue
            try:
                stamp = datetime.fromisoformat(endpoint.checked_at)
                if stamp.tzinfo is None:
                    continue
                age = (now - stamp).total_seconds()
                if not 0 <= age <= self.verified_reuse_seconds:
                    continue
                session = self.create_session(endpoint)
            except (ValueError, TypeError):
                continue
            except Exception as exc:  # noqa: BLE001 - an unusable session must not disable the pool
                logger.debug("Verified proxy session unavailable: %s", type(exc).__name__)
                self.record_dead(endpoint)
                continue
            if self.add_session(endpoint, session):
                reused += 1
            else:
                task = asyncio.create_task(_close_quietly(session))
                self._close_tasks.add(task)
                task.add_done_callback(self._close_tasks.discard)
        self.metrics.counts["verified_reuses"] += reused
        return reused

    def configure_warmup(self, spec: WarmupSpec) -> None:
        self._warmup_url = spec.url
        self._warmup_payload = spec.payload
        self._warmup_concurrency = max(1, spec.concurrency)
        self._warmup_timeout = spec.timeout
        self._warmup_validator = spec.validator
        self._probe_slots = asyncio.Semaphore(self._warmup_concurrency)

    async def _wait_changed(self, seconds: float) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(self._changed.wait(), max(0.001, seconds))

    @asynccontextmanager
    async def _host_slot(self, proxy: str) -> AsyncIterator[None]:
        host = proxy_host(proxy)
        while self._host_in_flight.get(host, 0) >= self.per_ip_limit:
            self._changed.clear()
            await self._wait_changed(0.25)
        self._host_in_flight[host] = self._host_in_flight.get(host, 0) + 1
        try:
            yield
        finally:
            self._host_in_flight[host] -= 1
            self._changed.set()

    @asynccontextmanager
    async def lease(self, timeout: float, avoid: str | None = None) -> AsyncIterator[ProxySession]:  # noqa: ASYNC109
        started = time.monotonic()
        deadline = started + timeout
        while True:
            self._changed.clear()
            if self._closed:
                raise HTTPClientError("Proxy pool is closed")
            self.ensure_capacity()
            session = self._choose_available_session(avoid)
            if session is not None:
                host = self._session_host(session)
                self._host_in_flight[host] = self._host_in_flight.get(host, 0) + 1
                self.in_flight[session] = self.in_flight.get(session, 0) + 1
                break
            if not self._recoverable():
                raise HTTPClientError("All proxies have failed; working proxy pool is empty")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise HTTPClientError("Timed out waiting for a proxy slot", retryable=True)
            await self._wait_changed(min(0.25, remaining))
        event = attempt_context.get()
        if event is not None:
            event.update(
                proxy_id=proxy_id(self.session_proxies.get(session)),
                proxy_source=self.session_sources.get(session, ""),
                proxy_wait_ms=round((time.monotonic() - started) * 1000, 3),
                proxy_in_flight=self._host_in_flight[host],
            )
        try:
            yield session
        finally:
            self._host_in_flight[host] -= 1
            self.in_flight[session] -= 1
            self._changed.set()
            if session in self.retired and not self.in_flight[session]:
                await self._close_retired(session)

    def _session_host(self, session: ProxySession) -> str:
        proxy = self.session_proxies.get(session, str(id(session)))
        return proxy_host(proxy)

    def _session_load(self, session: ProxySession) -> int:
        return self._host_in_flight.get(self._session_host(session), 0)

    def _choose_available_session(self, avoid: str | None) -> ProxySession | None:
        if time.monotonic() - self._last_health_write >= 5:
            self._publish_health()
        primary = [session for session in self.sessions if self.session_sources.get(session) in {"webshare", "paid"}]
        # Busy private proxies keep their priority; public proxies are a failure reserve.
        available = [session for session in (primary or self.sessions) if self._session_load(session) < self.per_ip_limit]
        if not available:
            return None

        tier = "private" if primary else "public"
        if tier != self._last_source_tier:
            logger.info("Proxy routing switched to %s proxies (%d usable sessions)", tier, len(primary or self.sessions))
            self._last_source_tier = tier

        preferred = [session for session in available if proxy_id(self.session_proxies.get(session)) != avoid]
        candidates = preferred or available
        minimum_load = min(map(self._session_load, candidates))
        least_busy = [session for session in candidates if self._session_load(session) == minimum_load]
        session = least_busy[self.next_index % len(least_busy)]
        self.next_index += 1
        return session

    def _recoverable(self) -> bool:
        if self.sessions:
            return True
        if any(task and not task.done() for task in (self._warmup_task, self._replenish_task)):
            return True
        return self._warmup_url is not None and any(p.url not in self._permanent_dead for p in self.proxies)

    def record_runtime_success(self, session: ProxySession) -> None:
        proxy = self.session_proxies.get(session)
        if proxy is not None and session in self.sessions:
            self._failure_counts.pop(proxy, None)

    def latency_for(self, session: ProxySession) -> float:
        return self.session_latency.get(session, 0.0)

    def start_warmup(self, spec: WarmupSpec) -> asyncio.Task[None]:
        if self._warmup_task and not self._warmup_task.done():
            return self._warmup_task

        self._warmup_error = None
        self._ready.clear()
        self._ready_announced = False
        self._warmup_started = time.monotonic()
        self._last_progress_log = self._warmup_started
        self.configure_warmup(spec)
        self._warmup_candidates = [
            proxy for proxy in self.proxies if proxy.url not in self._active and proxy.url not in self._permanent_dead
        ]
        self._scheduled.update(proxy.url for proxy in self._warmup_candidates)
        logger.info(
            "Proxy warmup started: candidates=%d, concurrency=%d, per-probe timeout=%.1fs",
            len(self._warmup_candidates),
            self._warmup_concurrency,
            self._warmup_timeout,
        )
        self._warmup_task = asyncio.create_task(self._run_warmup())
        return self._warmup_task

    async def wait_until_ready(self) -> None:
        await self._ready.wait()
        self.reuse_verified(self.proxies)
        if self.sessions:
            return
        if self._warmup_error:
            raise self._warmup_error
        if not self.sessions:
            raise RuntimeError(f"None of {self.checked} tested proxies returned valid API data")

    async def _run_warmup(self) -> None:
        if self._warmup_url is None:
            return

        try:
            await self._probe_candidates(self._warmup_candidates, persist_failure=True)
        finally:
            self._flush_dead()
            if self._persist_task:
                await self._persist_task
            self._ready.set()

        if not self.sessions:
            self._warmup_error = RuntimeError(f"None of {self.checked} tested proxies returned valid API data")
        else:
            logger.info(
                "Proxy warmup finished: %d/%d checked, %d usable", self.checked, len(self.proxies), len(self.sessions)
            )

    def create_session(self, proxy: str | ProxyEndpoint) -> ProxySession:
        endpoint = _as_endpoint(proxy)
        return self.session_factory(proxy=endpoint.url, user_agent=endpoint.user_agent, transport=endpoint.transport)

    async def _check_proxy(
        self,
        proxy: str | ProxyEndpoint,
        url: str,
        payload: dict[str, Any],
        semaphore: asyncio.Semaphore,
        timeout: float,  # noqa: ASYNC109 - forwarded to the readiness request
        *,
        persist_failure: bool,
    ) -> None:
        endpoint = _as_endpoint(proxy)
        proxy_url = endpoint.url
        session = None
        keep_session = False
        cancelled = False
        event = {
            "request_id": uuid.uuid4().hex,
            "endpoint_kind": "probe",
            "attempt": 1,
            "proxy_id": proxy_id(proxy_url),
            "transport": endpoint.transport or "configured",
        }
        token = attempt_context.set(event)
        started = time.monotonic()
        try:
            async with semaphore, self._probe_slots, self._host_slot(proxy_url):
                session = self.create_session(endpoint)
                response = await self._send_probe(session, url, payload, timeout)
                keep_session = self._warmup_validator(response)
                if self._closed or endpoint not in self.proxies:
                    keep_session = False
                event.update(http_status=response.status_code, ok=keep_session)
                self._pause_if_rate_limited(response, proxy_url)
                if not keep_session:
                    event["error_class"] = (
                        f"HTTP{response.status_code}" if response.status_code >= 400 else "InvalidReadinessPayload"
                    )
                    return

                keep_session = self.add_session(endpoint, session)
                if keep_session:
                    session = None
        except asyncio.CancelledError:
            cancelled = True
            event["error_class"] = "CancelledError"
            raise
        except Exception as exc:  # noqa: BLE001 - one bad endpoint must not abort readiness
            event["error_class"] = type(exc).__name__
            logger.debug("Proxy %s failed readiness: %s", proxy_id(proxy_url), type(exc).__name__)
        finally:
            self.checked += 1
            self._scheduled.discard(proxy_url)
            self._changed.set()
            self._log_progress()
            if not keep_session and not cancelled and endpoint in self.proxies:
                self._handle_failure(proxy_url, persist=persist_failure)
            await _close_quietly(session)
            event["total_attempt_ms"] = round((time.monotonic() - started) * 1000, 3)
            self.metrics.record(event)
            attempt_context.reset(token)

    async def _send_probe(self, session: ProxySession, url: str, payload: dict[str, Any], timeout: float) -> Any:  # noqa: ASYNC109
        # Readiness checks share the connection cap but bypass pacing so they
        # cannot hold rate-limiter slots needed by publication downloads.
        async with self.budget.slot(apply_rate=False):
            self.metrics.counts["wire_requests"] += 1
            self.metrics.counts["probe_attempts"] += 1
            return await asyncio.wait_for(
                session.post(url, json=payload, timeout=timeout), timeout=timeout + HARD_TIMEOUT_GRACE_SECONDS
            )

    def _pause_if_rate_limited(self, response: Any, proxy_url: str) -> None:
        if response.status_code != 429:
            return
        delay = parse_retry_after(retry_after_from_response(response))
        self.handle_rate_limit(proxy_url, delay)

    def _handle_failure(self, proxy: str, *, persist: bool) -> None:
        failures = self._failure_counts.get(proxy, 0) + 1
        self._failure_counts[proxy] = failures
        self._cooldown_until[proxy] = time.monotonic() + self.failure_cooldown_seconds

        if failures >= self.max_failures:
            self._permanent_dead.add(proxy)

        if persist and proxy in self._permanent_dead:
            self._record_dead(proxy)

    def _log_progress(self) -> None:
        total = len(self.proxies)
        now = time.monotonic()
        count_due = not total or self.checked % WARMUP_PROGRESS_EVERY == 0 or self.checked == total
        time_due = now - self._last_progress_log >= WARMUP_PROGRESS_SECONDS
        if not count_due and not time_due:
            return
        self._last_progress_log = now
        elapsed = max(0.001, now - self._warmup_started)
        rate = self.checked / elapsed
        remaining = max(0, total - self.checked)
        eta = remaining / rate if rate > 0 else 0.0
        logger.info(
            "Proxy warmup progress: %d/%d checked (%.1f/s), %d usable, elapsed %.0fs, ETA %.0fs",
            self.checked,
            total,
            rate,
            len(self.sessions),
            elapsed,
            eta,
        )

    def add_session(self, proxy: str | ProxyEndpoint, session: ProxySession) -> bool:
        endpoint = _as_endpoint(proxy)
        proxy_url = endpoint.url
        if (
            proxy_url in self._active
            or proxy_url in self._permanent_dead
            or self._rate_limit_until.get(proxy_url, 0) > time.monotonic()
        ):
            return False

        self.sessions.append(session)
        self.session_proxies[session] = proxy_url
        self.session_latency[session] = endpoint.latency_ms
        self.session_sources[session] = endpoint.source
        self._active.add(proxy_url)
        self._publish_health()
        self._changed.set()
        self._cooldown_until.pop(proxy_url, None)
        if self._dead.pop(proxy_url, None) is not None:
            self._dirty = True
            self._flush_dead()

        if len(self.sessions) >= self.min_ready:
            self._ready.set()
            if not self._ready_announced:
                self._ready_announced = True
                logger.info("Proxy readiness threshold reached: %d/%d ready", len(self.sessions), self.min_ready)
        return True

    def record_dead(self, proxy: str | ProxyEndpoint) -> None:
        proxy_url = _as_endpoint(proxy).url
        self._failure_counts[proxy_url] = self.max_failures
        self._permanent_dead.add(proxy_url)
        self._record_dead(proxy_url)

    def _record_dead(self, proxy: str) -> None:
        if self.dead_path is not None:
            self._dead[proxy] = datetime.now(UTC).isoformat()
            self._dirty = True

    def _flush_dead(self) -> None:
        if self.dead_path is None or not self._dirty:
            return
        if self._persist_task is None or self._persist_task.done():
            self._persist_task = asyncio.create_task(self._persist_dead())

    async def _persist_dead(self) -> None:
        dead_path = self.dead_path
        if dead_path is None:
            return
        while self._dirty:
            self._dirty = False
            snapshot = dict(self._dead)
            try:
                await asyncio.to_thread(_save_dead_proxies, dead_path, snapshot)
            except OSError:
                logger.warning("Could not persist proxy exclusions")

    async def _close_retired(self, session: ProxySession) -> None:
        if session not in self.retired:
            return
        self.retired.remove(session)
        self.session_proxies.pop(session, None)
        self.session_latency.pop(session, None)
        self.session_sources.pop(session, None)
        self.in_flight.pop(session, None)
        await _close_quietly(session)

    def discard(self, session: ProxySession) -> bool:
        """Exclude immediately; close later so in-flight requests can finish."""
        if session not in self.sessions:
            return False

        proxy = self.session_proxies.get(session)
        if proxy is not None:
            self._handle_failure(proxy, persist=False)
            self._flush_dead()
        self._retire(session)
        self.ensure_capacity()
        return True

    def handle_rate_limit(self, url: str, server_delay: float | None) -> float:
        """Quarantine the URL; escalate if three distinct URLs hit 429 in ten minutes."""
        now = time.monotonic()
        pause = self.rate_limit_pause_seconds
        quarantine = max(server_delay or 0, pause)
        until = now + quarantine
        self._rate_limit_until[url] = max(until, self._rate_limit_until.get(url, 0))
        self._cooldown_until[url] = max(until, self._cooldown_until.get(url, 0))
        for active_session in list(self.sessions):
            if self.session_proxies.get(active_session) == url:
                self._retire(active_session)
        self.ensure_capacity()
        self._recent_rate_limits = {key: stamp for key, stamp in self._recent_rate_limits.items() if now - stamp < 600}
        self._recent_rate_limits[proxy_id(url)] = now
        shared = len(self._recent_rate_limits) >= 3
        effective_pause = max(pause, server_delay or 0) if shared else pause
        self.budget.pause(effective_pause)
        logger.warning(
            "HTTP 429: proxy=%s excluded for %.1fs; global pause=%.1fs; limited_urls_10m=%d; scope=%s",
            proxy_id(url),
            quarantine,
            effective_pause,
            len(self._recent_rate_limits),
            "suspected_shared" if shared else "proxy",
        )
        return effective_pause

    def _retire(self, session: ProxySession) -> None:
        """Remove from rotation, but keep leased sessions alive until released."""

        self.sessions.remove(session)
        self._publish_health()
        self.retired.append(session)
        proxy = self.session_proxies.get(session)
        if proxy is not None:
            self._active.discard(proxy)

        self._changed.set()
        if not self.in_flight.get(session, 0):
            task = asyncio.create_task(self._close_retired(session))
            self._close_tasks.add(task)
            task.add_done_callback(self._close_tasks.discard)

    def ensure_capacity(self) -> None:
        if self._closed or self._warmup_url is None:
            return
        self.reuse_verified(self.proxies)
        ready_count = len(self.sessions)
        if (
            not self._refresh_pending
            and ready_count
            and (self.replenish_threshold <= 0 or ready_count >= self.replenish_threshold)
        ):
            return
        if self._replenish_task and not self._replenish_task.done():
            return

        candidates = self._replenish_candidates()
        self._refresh_pending = False
        if not candidates:
            return
        self._scheduled.update(proxy.url for proxy in candidates)
        self._replenish_task = asyncio.create_task(self._run_replenish(candidates))

    def _replenish_candidates(self) -> list[ProxyEndpoint]:
        now = time.monotonic()
        return [
            proxy
            for proxy in self.proxies
            if proxy.url not in self._active
            and proxy.url not in self._scheduled
            and proxy.url not in self._permanent_dead
            and self._cooldown_until.get(proxy.url, 0.0) <= now
            and self._rate_limit_until.get(proxy.url, 0.0) <= now
        ]

    async def _run_replenish(self, candidates: list[ProxyEndpoint]) -> None:
        if self._warmup_url is None:
            return

        self._scheduled.update(proxy.url for proxy in candidates)
        try:
            await self._probe_candidates(candidates, persist_failure=False)
        finally:
            self._flush_dead()
            self._replenish_task = None
            if self._refresh_pending:
                self.ensure_capacity()

    async def _probe_candidates(self, candidates: list[ProxyEndpoint], *, persist_failure: bool) -> None:
        warmup_url = self._warmup_url
        if warmup_url is None:
            return
        semaphore = asyncio.Semaphore(self._warmup_concurrency)
        tasks = [
            asyncio.create_task(
                self._check_proxy(
                    proxy, warmup_url, self._warmup_payload, semaphore, self._warmup_timeout, persist_failure=persist_failure
                )
            )
            for proxy in candidates
        ]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def close(self) -> None:
        """Stop background work and close every owned session exactly once."""
        async with self._close_lock:
            self._closed = True
            self._changed.set()
            current = asyncio.current_task()
            tasks = [
                task
                for task in (self._warmup_task, self._replenish_task)
                if task is not None and task is not current and not task.done()
            ]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self._warmup_task = None
            self._replenish_task = None

            self._publish_health()
            if self._health_task is not None:
                await asyncio.gather(self._health_task, return_exceptions=True)
                self._health_task = None

            self._flush_dead()
            if self._persist_task is not None:
                await asyncio.gather(self._persist_task, return_exceptions=True)
                self._persist_task = None
            if self._close_tasks:
                await asyncio.gather(*tuple(self._close_tasks), return_exceptions=True)
                self._close_tasks.clear()

            sessions = [*self.sessions, *self.retired]
            self.sessions.clear()
            self.retired.clear()
            self.session_proxies.clear()
            self.session_latency.clear()
            self.session_sources.clear()
            self.in_flight.clear()
            self._active.clear()
            await asyncio.gather(*(session.close() for session in sessions), return_exceptions=True)


async def _close_quietly(session: ProxySession | None) -> None:
    if session is None:
        return
    with suppress(Exception):  # noqa: BLE001 - cleanup must not mask the original failure
        await session.close()


def load_dead_proxies(path: Path | None, ttl_hours: float) -> dict[str, str]:
    if path is None:
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    if not isinstance(raw, dict):
        return {}

    cutoff = datetime.now(UTC) - timedelta(hours=ttl_hours)
    active: dict[str, str] = {}
    for proxy, stamp in raw.items():
        if not isinstance(proxy, str) or not isinstance(stamp, str):
            continue
        try:
            seen = datetime.fromisoformat(stamp)
        except ValueError:
            continue
        if seen.tzinfo is None:
            seen = seen.replace(tzinfo=UTC)
        if seen >= cutoff:
            active[proxy] = stamp
    return active


def _save_dead_proxies(path: Path, dead: dict[str, str]) -> None:
    atomic_write_json(path, dead, indent=2, sort_keys=True)
