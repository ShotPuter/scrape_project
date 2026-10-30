"""Shared origin budget, explicit timeouts, and credential-free request telemetry."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections import Counter, deque
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from typing import Any

logger = logging.getLogger("seprec_client")
work_context: ContextVar[dict[str, Any] | None] = ContextVar("publication_work", default=None)
attempt_context: ContextVar[dict[str, Any] | None] = ContextVar("http_attempt", default=None)


def proxy_id(url: str | None) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16] if url else "direct"


@dataclass(frozen=True, slots=True)
class TimeoutPolicy:
    total: float = 30
    connect: float = 10
    sock_connect: float = 10
    sock_read: float = 30

    def __post_init__(self):
        if any(value <= 0 for value in asdict(self).values()):
            raise ValueError("All timeout stages must be positive")

    def aiohttp(self):
        from aiohttp import ClientTimeout

        return ClientTimeout(**asdict(self))


class NetworkBudget:
    """One cap/rate/cooldown shared by discovery, downloads and readiness probes."""

    def __init__(self, limit: int, requests_per_second: float = 0):
        if limit < 1 or requests_per_second < 0:
            raise ValueError("Invalid network budget")
        self.semaphore = asyncio.Semaphore(limit)
        self.limit = limit
        self.rate = requests_per_second
        self.active = 0
        self._next_start = 0.0
        self._paused_until = 0.0
        self._rate_lock = asyncio.Lock()

    def pause(self, seconds: float) -> None:
        self._paused_until = max(self._paused_until, time.monotonic() + max(0, seconds))
        if seconds >= 1:
            logger.warning("Origin requests paused for %.2fs", seconds)

    @asynccontextmanager
    async def slot(self, *, apply_rate: bool = True):
        started = time.monotonic()
        async with self.semaphore:
            if apply_rate:
                await self._wait_for_start()
            else:
                # Probes may bypass pacing, but must still respect an origin cooldown.
                while (delay := self._paused_until - time.monotonic()) > 0:  # noqa: ASYNC110 - deadline can be extended
                    await asyncio.sleep(delay)
            self.active += 1
            event = attempt_context.get()
            if event is not None:
                event["global_slot_wait_ms"] = round((time.monotonic() - started) * 1000, 3)
                event["active_http"] = self.active
            try:
                yield
            finally:
                self.active -= 1

    async def _wait_for_start(self) -> None:
        async with self._rate_lock:
            while True:
                delay = max(self._next_start, self._paused_until) - time.monotonic()
                if delay <= 0:
                    break
                await asyncio.sleep(delay)
            self._next_start = time.monotonic() + (1 / self.rate if self.rate else 0)


@dataclass
class RequestMetrics:
    counts: Counter = field(default_factory=Counter)
    samples: deque = field(default_factory=lambda: deque(maxlen=2048))

    def record(self, event: dict[str, Any]) -> None:
        self.counts["attempts"] += 1
        self.counts["retries"] += int(event.get("attempt", 1) > 1)
        self.counts[f"{event.get('endpoint_kind', 'unknown')}_attempts"] += 1
        if event.get("http_status") is not None:
            self.counts[f"http_{event['http_status']}"] += 1
        self.counts["successes" if event.get("ok") else "failures"] += 1
        if event.get("error_class"):
            self.counts[event["error_class"]] += 1
        self.counts["response_bytes"] += event.get("response_bytes", 0)
        self.samples.append(dict(event))
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("http_attempt %s", json.dumps(event, sort_keys=True))

    def snapshot(self) -> dict[str, Any]:
        result: dict[str, Any] = dict(self.counts)
        result["response_bytes_total"] = self.counts["response_bytes"]
        for key in (
            "total_attempt_ms",
            "global_slot_wait_ms",
            "proxy_wait_ms",
            "worker_queue_ms",
            "connector_queue_ms",
            "connection_setup_ms",
            "response_headers_ms",
            "first_body_chunk_ms",
            "body_read_ms",
            "json_decode_ms",
            "response_bytes",
        ):
            values = sorted(item[key] for item in self.samples if key in item)
            if values:
                result[key] = {"p50": values[(len(values) - 1) // 2], "p95": values[int((len(values) - 1) * 0.95)]}
        return result


def mark(name: str, value: Any) -> None:
    event = attempt_context.get()
    if event is not None:
        event[name] = value


def trace_config():
    from aiohttp import TraceConfig

    trace = TraceConfig()
    for signal, phase, key in (
        ("connection_queued", "connector_queue", "connector_queue_ms"),
        ("dns_resolvehost", "dns", "dns_ms"),
        ("connection_create", "connection_setup", "connection_setup_ms"),
    ):

        async def start(_session, ctx, _params, phase=phase):
            setattr(ctx, phase, time.monotonic())
            mark("stage", phase)

        async def end(_session, ctx, _params, phase=phase, key=key):
            mark(key, round((time.monotonic() - getattr(ctx, phase)) * 1000, 3))
            mark("stage", "response_headers")

        getattr(trace, f"on_{signal}_start").append(start)
        getattr(trace, f"on_{signal}_end").append(end)

    async def reused(_session, _ctx, _params):
        mark("connection_reused", True)
        mark("stage", "response_headers")

    trace.on_connection_reuseconn.append(reused)
    return trace
