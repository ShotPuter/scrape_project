"""Proxy list loading, readiness checks, and dead-proxy bookkeeping."""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import time
import uuid
from collections.abc import Callable, Iterable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from curl_cffi.requests import AsyncSession

from src.client.errors import HTTPClientError
from src.client.network import NetworkBudget, RequestMetrics, attempt_context, proxy_id
from src.client.readiness import is_ready_response
from src.utils.atomic import atomic_write_json
from src.utils.http import parse_retry_after
from src.utils.proxy_url import SUPPORTED_PROXY_SCHEMES as _SUPPORTED_PROXY_SCHEMES, normalize_proxy_url, proxy_host

logger = logging.getLogger(__name__)

SUPPORTED_PROXY_SCHEMES = _SUPPORTED_PROXY_SCHEMES
DEAD_PROXY_TTL_HOURS = 1.0
WARMUP_PROGRESS_EVERY = 25
WARMUP_PROGRESS_SECONDS = 10.0
HARD_TIMEOUT_GRACE_SECONDS = 1.0
DEFAULT_FAILURE_COOLDOWN_SECONDS = 120.0
DEFAULT_MAX_FAILURES = 3

SessionFactory = Callable[..., AsyncSession]


@dataclass(frozen=True, slots=True)
class ProxyEndpoint:
    """A proxy together with the user agent and transport that verified it."""

    url: str
    user_agent: str | None = None
    transport: str | None = None
    latency_ms: float = 0.0


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


def load_dead_proxies(path: Path | None, ttl_hours: float) -> set[str]:
    return set(_load_dead_proxies(path, ttl_hours))


def load_proxies(
    path: Path, limit: int | None = None, dead_path: Path | None = None, dead_ttl_hours: float = DEAD_PROXY_TTL_HOURS
) -> list[str]:
    if limit is not None and limit < 0:
        raise ValueError("Proxy sample size must be >= 0 (0 = all)")

    dead = set(_load_dead_proxies(dead_path, dead_ttl_hours))
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

    protocol = row.get("protocol", "http").strip().lower()
    address = row.get("proxy", "").strip() or f"{row.get('ip', '')}:{row.get('port', '')}"
    if not address.strip():
        return None
    url = address if "://" in address else f"{protocol}://{address}"
    return normalize_proxy_url(url)


class ProxyPool:
    """Keeps healthy proxies in rotation and replenishes them while scraping."""

    def __init__(
        self, proxies: Iterable[str | ProxyEndpoint], session_factory: SessionFactory,
        dead_path: Path | None = None, dead_ttl_hours: float = DEAD_PROXY_TTL_HOURS, min_ready: int = 1,
        replenish_threshold: int = 0, failure_cooldown_seconds: float = DEFAULT_FAILURE_COOLDOWN_SECONDS,
        max_failures: int = DEFAULT_MAX_FAILURES, per_ip_limit: int = 1, network_budget: NetworkBudget | None = None,
        metrics: RequestMetrics | None = None,
    ):
        if per_ip_limit < 1:
            raise ValueError("per_ip_limit must be positive")
        if min_ready < 1:
            raise ValueError("min_ready must be >= 1")
        if replenish_threshold < 0:
            raise ValueError("replenish_threshold must be >= 0")
        if failure_cooldown_seconds < 0:
            raise ValueError("failure_cooldown_seconds must be >= 0")
        if max_failures < 1:
            raise ValueError("max_failures must be >= 1")

        self.proxies = _dedupe_endpoints(proxies)
        self.session_factory = session_factory
        self.sessions: list[AsyncSession] = []
        self.session_proxies: dict[AsyncSession, str] = {}
        self.session_latency: dict[AsyncSession, float] = {}
        self.retired: list[AsyncSession] = []
        self.next_index = 0
        self.checked = 0
        self.dead_path = dead_path
        self._dead = _load_dead_proxies(dead_path, dead_ttl_hours)
        self.min_ready = min_ready
        self.replenish_threshold = replenish_threshold
        self.failure_cooldown_seconds = failure_cooldown_seconds
        self.max_failures = max_failures
        self.per_ip_limit = per_ip_limit
        self.budget = network_budget or NetworkBudget(5)
        self.metrics = metrics or RequestMetrics()
        self.in_flight: dict[AsyncSession, int] = {}
        self._host_in_flight: dict[str, int] = {}
        self._changed = asyncio.Event()
        self._closed = False
        self._close_lock = asyncio.Lock()
        self._probe_slots = asyncio.Semaphore(1)
        self._persist_task: asyncio.Task[None] | None = None
        self._dirty = False
        self._close_tasks: set[asyncio.Task] = set()
        self._successes: dict[str, int] = {}
        self._failures_total: dict[str, int] = {}

        self._active: set[str] = set()
        self._scheduled: set[str] = set()
        self._failure_counts: dict[str, int] = {}
        self._cooldown_until: dict[str, float] = {}
        self._permanent_dead: set[str] = set()
        self._ready = asyncio.Event()
        self._warmup_task: asyncio.Task[None] | None = None
        self._replenish_task: asyncio.Task[None] | None = None
        self._warmup_candidates: list[ProxyEndpoint] = []
        self._warmup_url: str | None = None
        self._warmup_payload: dict = {}
        self._warmup_concurrency = 1
        self._warmup_timeout = 10.0
        self._warmup_error: RuntimeError | None = None
        self._warmup_started = time.monotonic()
        self._last_progress_log = self._warmup_started
        self._ready_announced = False

    def configure_warmup(self, url: str, payload: dict, concurrency: int, timeout: float) -> None:
        self._warmup_url = url
        self._warmup_payload = payload
        self._warmup_concurrency = max(1, concurrency)
        self._warmup_timeout = timeout
        self._probe_slots = asyncio.Semaphore(self._warmup_concurrency)

    @staticmethod
    def _host(proxy: str) -> str:
        return proxy_host(proxy)

    async def _wait_changed(self, seconds: float) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(self._changed.wait(), max(0.001, seconds))

    @asynccontextmanager
    async def _host_slot(self, proxy: str):
        host = self._host(proxy)
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
    async def lease(self, timeout: float, avoid: str | None = None):  # noqa: ASYNC109 - bounded pool acquisition
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
                proxy_wait_ms=round((time.monotonic() - started) * 1000, 3), proxy_in_flight=self._host_in_flight[host],
            )
        try:
            yield session
        finally:
            self._host_in_flight[host] -= 1
            self.in_flight[session] -= 1
            self._changed.set()
            if session in self.retired and not self.in_flight[session]:
                await self._close_retired(session)

    def _session_host(self, session) -> str:
        proxy = self.session_proxies.get(session, str(id(session)))
        return self._host(proxy)

    def _session_load(self, session) -> int:
        return self._host_in_flight.get(self._session_host(session), 0)

    def _choose_available_session(self, avoid: str | None):
        available = [session for session in self.sessions if self._session_load(session) < self.per_ip_limit]
        if not available:
            return None

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

    def record_runtime_success(self, session) -> None:
        proxy = self.session_proxies.get(session)
        if proxy is not None and session in self.sessions:
            self._failure_counts.pop(proxy, None)
            self._successes[proxy] = self._successes.get(proxy, 0) + 1

    def latency_for(self, session) -> float:
        return self.session_latency.get(session, 0.0)

    def snapshot(self) -> dict:
        return {
            "ready": len(self.sessions),
            "retired": len(self.retired),
            "cooldown": sum(until > time.monotonic() for until in self._cooldown_until.values()),
            "proxies": {
                proxy_id(url): {
                    "active": self._host_in_flight.get(self._host(url), 0),
                    "successes": self._successes.get(url, 0),
                    "failures": self._failures_total.get(url, 0),
                    "consecutive_failures": self._failure_counts.get(url, 0),
                }
                for url in self._active
            },
        }

    def start_warmup(self, url: str, payload: dict, concurrency: int = 5, timeout: float = 10) -> asyncio.Task[None]:
        if self._warmup_task and not self._warmup_task.done():
            return self._warmup_task

        self._warmup_error = None
        self._ready.clear()
        self._ready_announced = False
        self._warmup_started = time.monotonic()
        self._last_progress_log = self._warmup_started
        self.configure_warmup(url, payload, concurrency, timeout)
        self._warmup_candidates = [
            proxy for proxy in self.proxies if proxy.url not in self._active and proxy.url not in self._permanent_dead
        ]
        self._scheduled.update(proxy.url for proxy in self._warmup_candidates)
        logger.info(
            "Proxy warmup started: candidates=%d, concurrency=%d, per-probe timeout=%.1fs",
            len(self._warmup_candidates), self._warmup_concurrency, self._warmup_timeout,
        )
        self._warmup_task = asyncio.create_task(self._run_warmup())
        return self._warmup_task

    async def warmup(
        self, url: str, payload: dict,
        concurrency: int = 5,
        timeout: float = 10,  # noqa: ASYNC109 - forwarded to the request
    ) -> None:
        task = self.start_warmup(url, payload, concurrency, timeout)
        await task
        if self._warmup_error:
            raise self._warmup_error

    async def wait_until_ready(self) -> None:
        await self._ready.wait()
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
                "Proxy warmup finished: %d/%d checked, %d usable",
                self.checked, len(self.proxies), len(self.sessions),
            )

    def create_session(self, proxy: str | ProxyEndpoint):
        endpoint = _as_endpoint(proxy)
        return self.session_factory(proxy=endpoint.url, user_agent=endpoint.user_agent, transport=endpoint.transport)

    async def _check_proxy(
        self, proxy: str | ProxyEndpoint, url: str, payload: dict,
        semaphore: asyncio.Semaphore, timeout: float, *, persist_failure: bool,  # noqa: ASYNC109
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
                keep_session = _is_ready_response(response)
                event.update(http_status=response.status_code, ok=keep_session)
                self._pause_if_rate_limited(response)
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
            if not keep_session and not cancelled:
                self._handle_failure(proxy_url, persist=persist_failure)
            await _close_quietly(session)
            event["total_attempt_ms"] = round((time.monotonic() - started) * 1000, 3)
            self.metrics.record(event)
            attempt_context.reset(token)

    async def _send_probe(self, session, url: str, payload: dict, timeout: float):  # noqa: ASYNC109
        # Readiness checks share the connection cap but bypass pacing so they
        # cannot hold rate-limiter slots needed by publication downloads.
        async with self.budget.slot(apply_rate=False):
            self.metrics.counts["wire_requests"] += 1
            self.metrics.counts["probe_attempts"] += 1
            return await asyncio.wait_for(
                session.post(url, json=payload, timeout=timeout), timeout=timeout + HARD_TIMEOUT_GRACE_SECONDS
            )

    def _pause_if_rate_limited(self, response) -> None:
        if response.status_code != 429:
            return
        delay = parse_retry_after(str(response.headers.get("Retry-After", "1")))
        self.budget.pause(1 if delay is None else delay)

    def _handle_failure(self, proxy: str, *, persist: bool) -> None:
        self._failures_total[proxy] = self._failures_total.get(proxy, 0) + 1
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
            self.checked, total, rate, len(self.sessions), elapsed, eta,
        )

    def add_session(self, proxy: str | ProxyEndpoint, session: AsyncSession) -> bool:
        endpoint = _as_endpoint(proxy)
        proxy_url = endpoint.url
        if proxy_url in self._active or proxy_url in self._permanent_dead:
            return False

        self.sessions.append(session)
        self.session_proxies[session] = proxy_url
        self.session_latency[session] = endpoint.latency_ms
        self._active.add(proxy_url)
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

    async def _close_retired(self, session) -> None:
        if session not in self.retired:
            return
        self.retired.remove(session)
        self.session_proxies.pop(session, None)
        self.session_latency.pop(session, None)
        self.in_flight.pop(session, None)
        await _close_quietly(session)

    def choose(self) -> AsyncSession:
        if not self.sessions:
            raise RuntimeError("Proxy pool is empty")
        session = self.sessions[self.next_index % len(self.sessions)]
        self.next_index += 1
        return session

    def discard(self, session: AsyncSession) -> bool:
        """Exclude immediately; close later so in-flight requests can finish."""
        if session not in self.sessions:
            return False

        self.sessions.remove(session)
        self.retired.append(session)
        proxy = self.session_proxies.get(session)
        if proxy is not None:
            self._active.discard(proxy)
            self._handle_failure(proxy, persist=False)
            self._flush_dead()

        self._changed.set()
        if not self.in_flight.get(session, 0):
            task = asyncio.create_task(self._close_retired(session))
            self._close_tasks.add(task)
            task.add_done_callback(self._close_tasks.discard)
        self.ensure_capacity()
        return True

    def ensure_capacity(self) -> None:
        if self._closed or self._warmup_url is None:
            return
        ready_count = len(self.sessions)
        if ready_count and (self.replenish_threshold <= 0 or ready_count >= self.replenish_threshold):
            return
        if self._replenish_task and not self._replenish_task.done():
            return

        candidates = self._replenish_candidates()
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
        ]

    async def _run_replenish(self, candidates: list[ProxyEndpoint]) -> None:
        if self._warmup_url is None:
            return

        self._scheduled.update(proxy.url for proxy in candidates)
        try:
            await self._probe_candidates(candidates, persist_failure=False)
        finally:
            self._flush_dead()

    async def _probe_candidates(self, candidates: list[ProxyEndpoint], *, persist_failure: bool) -> None:
        warmup_url = self._warmup_url
        if warmup_url is None:
            return
        semaphore = asyncio.Semaphore(self._warmup_concurrency)
        tasks = [
            asyncio.create_task(
                self._check_proxy(
                    proxy, warmup_url, self._warmup_payload,
                    semaphore, self._warmup_timeout, persist_failure=persist_failure,
                )
            )
            for proxy in candidates
        ]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            await self._cancel(tasks)
            raise

    async def wait_for_session(self, timeout: float) -> bool:  # noqa: ASYNC109
        deadline = time.monotonic() + max(0, timeout)
        while not self.sessions and not self._closed:
            self._changed.clear()
            self.ensure_capacity()
            if not self._recoverable() or time.monotonic() >= deadline:
                return False
            await self._wait_changed(min(0.25, deadline - time.monotonic()))
        return bool(self.sessions)

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
            self.in_flight.clear()
            self._active.clear()
            await asyncio.gather(*(session.close() for session in sessions), return_exceptions=True)

    @staticmethod
    async def _cancel(tasks: list[asyncio.Task]) -> None:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _is_ready_response(response) -> bool:
    return is_ready_response(response)


async def _close_quietly(session) -> None:
    if session is None:
        return
    with suppress(Exception):  # noqa: BLE001 - cleanup must not mask the original failure
        await session.close()


def _load_dead_proxies(path: Path | None, ttl_hours: float) -> dict[str, str]:
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
