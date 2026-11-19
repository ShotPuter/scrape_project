"""Multi-provider proxy fleet: drain paid/free routes to daily quota, park until next day.

Each browser slot leases one route from any configured provider: Webshare,
Bright Data, a static paid list, or the public free catalog. A route is drained
until its per-route document cap, the provider's daily budget, or the per-IP
daily limit. Routes that reach a limit are parked until the next quota reset
(next day). State lives in fleet.sqlite3 so a restart resumes routes and quotas.
"""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
import json
import os
import signal
import sqlite3
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from src.proxy.reputation import ReputationDenied, check_ip, public_collection_allowed, screen_proxy, validate_ip
from src.proxy.sources.catalog import load_public_catalog
from src.proxy.sources.free_proxy import load_free_proxy_candidates
from src.proxy.sources.paid import load_paid_candidates
from src.proxy.sources.registry import resolve_sources
from src.proxy.sources.webshare import load_webshare_candidates
from src.utils.error_details import safe_message

PARK_OUTCOMES = {"daily_limit", "budget"}
PROVIDERS = ("webshare", "brightdata", "paid", "free")
DEFAULT_ROUTE_DOCS = 95
DEFAULT_PROXY_TTL = 600
DEFAULT_PROXY_RETRY_SECONDS = 300
DEFAULT_ROUTE_FAILURES = 3
DEFAULT_CATALOG_SECONDS = 300
DEFAULT_PROXY_RATE_PER_MINUTE = 30
DEFAULT_PROXY_CHECK_CONCURRENCY = 10
DEFAULT_PROXY_TIMEOUT = 15
DEFAULT_SOURCE_CACHE_MIB = 100
DEFAULT_SOURCE_RECORDS = 5000
DEFAULT_MAX_CANDIDATES = 2000
DEFAULT_WORKER_TIMEOUT = 300
DEFAULT_COMPANY_ATTEMPTS = 3
DEFAULT_RETRY_SECONDS = 60
DEFAULT_MAX_FAILURES = 5
DEFAULT_ERROR_COOLDOWN_MINUTES = 5
DEFAULT_MAX_COOLDOWNS = 3
DEFAULT_RESET_HOUR = 0
DEFAULT_BROWSERS = 5
DEFAULT_BUDGET_WEBSHARE = 500
DEFAULT_BUDGET_BRIGHTDATA = 500
DEFAULT_BUDGET_PAID = 500
DEFAULT_BUDGET_FREE = 200
DEFAULT_FREE_KEEP = True  # Park free routes until next quota instead of deleting


def endpoint_exceptions(value: str) -> set[tuple[str, int]]:
    """Parse comma-separated host:port exceptions for free proxy filtering."""
    result = set()
    for item in value.split(","):
        entry = item.strip()
        if not entry:
            continue
        host, port_text = entry.rsplit(":", 1)
        address = validate_ip(host)
        port = int(port_text)
        if ipaddress.ip_address(address).version != 4 or not 1 <= port <= 65535:
            raise ValueError("Proxy exception must be an exact public IPv4:port")
        result.add((address, port))
    return result


def proxy_ports(value: str) -> set[int]:
    """Parse comma-separated allowed proxy ports."""
    ports = {int(item.strip()) for item in value.split(",") if item.strip()}
    if not ports or any(port < 1 or port > 65535 for port in ports):
        raise ValueError("Proxy ports must be a comma-separated list of ports 1..65535")
    return ports


def endpoint_allowed(endpoint: str, exceptions: set[tuple[str, int]], ports: set[int] | None = None) -> bool:
    """Match the host firewall policy before spending any network requests."""
    try:
        parts = urlsplit(endpoint)
        if parts.scheme not in {"http", "socks5", "socks5h"} or parts.username or parts.password:
            return False
        address = ipaddress.ip_address(validate_ip(parts.hostname or ""))
        return address.version == 4 and (
            parts.port in (ports if ports is not None else {80, 443}) or (str(address), parts.port) in exceptions
        )
    except ValueError:
        return False


def probe_error_category(error: Exception) -> str:
    """Distinguish local setup errors from candidate connectivity and target refusals."""
    if isinstance(error, ReputationDenied):
        return "reputation_denied"
    if isinstance(error, ImportError):
        return "missing_socks_dependency" if "socksio" in str(error).lower() else "missing_dependency"
    if isinstance(error, (httpx.TimeoutException, TimeoutError)):
        return "timeout"
    if isinstance(error, httpx.HTTPStatusError):
        return f"http_{error.response.status_code}"
    if isinstance(error, httpx.ProxyError):
        return "proxy_error"
    if isinstance(error, httpx.ConnectError):
        return "connect_error"
    return type(error).__name__


def next_reset(reset_hour: int, now: datetime | None = None) -> float:
    """First local-UTC moment at or after the next daily quota reset."""
    now = now or datetime.now(UTC)
    target = now.replace(hour=reset_hour, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target.timestamp()


def current_day() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


@dataclass(frozen=True, slots=True)
class FleetArgs:
    """Runtime arguments for the fleet."""

    database: Path
    output: Path
    providers: str
    browsers: int
    budget_webshare: int
    budget_brightdata: int
    budget_paid: int
    budget_free: int
    route_docs: int
    proxy_ttl: int
    proxy_retry_seconds: int
    route_failures: int
    catalog_seconds: int
    proxy_rate_per_minute: int
    proxy_check_concurrency: int
    proxy_timeout: float
    source_cache_mib: int
    source_records: int
    max_candidates: int
    worker_timeout: int
    company_attempts: int
    retry_seconds: int
    max_failures: int
    error_cooldown_minutes: int
    max_cooldowns: int
    reset_hour: int
    free_keep: bool
    allowed_proxy_endpoints: str
    proxy_ports: str
    paid_file: Path
    sources: str
    provider_set: frozenset[str]
    run_id: str = field(default_factory=lambda: datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"))


class FleetQueue:
    """Routes, provider budgets and durable state.

    Uses SQLite for persistence across restarts.
    """

    def __init__(self, path: Path, run_id: str):
        self.run_id = run_id
        self.database = sqlite3.connect(path, timeout=30, isolation_level=None)
        self.database.row_factory = sqlite3.Row
        self.database.execute("PRAGMA journal_mode=WAL")
        self.database.execute("PRAGMA busy_timeout=30000")
        self.database.executescript(
            """
            CREATE TABLE IF NOT EXISTS fleet_routes (
                route_id TEXT PRIMARY KEY,provider TEXT NOT NULL,endpoint TEXT NOT NULL,
                proxy_url TEXT,label TEXT,expected_ip TEXT,dynamic_ip INTEGER NOT NULL DEFAULT 0,
                ca_file TEXT,state TEXT NOT NULL DEFAULT 'pending',
                documents INTEGER NOT NULL DEFAULT 0,tokens INTEGER NOT NULL DEFAULT 0,
                failures INTEGER NOT NULL DEFAULT 0,exit_ip TEXT,verified_at REAL,last_success REAL,
                expires_at REAL NOT NULL DEFAULT 0,next_at REAL NOT NULL DEFAULT 0,
                parked_until REAL NOT NULL DEFAULT 0,owner TEXT,last_error TEXT,
                source TEXT,seen_at REAL NOT NULL);
            CREATE UNIQUE INDEX IF NOT EXISTS fleet_exit_unique ON fleet_routes(exit_ip)
                WHERE state IN ('ready','leased') AND exit_ip IS NOT NULL;
            CREATE INDEX IF NOT EXISTS fleet_routes_claim ON fleet_routes(state,provider,documents,last_success,route_id);
            CREATE TABLE IF NOT EXISTS fleet_budgets (
                provider TEXT NOT NULL,day TEXT NOT NULL,documents INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(provider,day));
            CREATE TABLE IF NOT EXISTS fleet_subnets (
                subnet TEXT PRIMARY KEY,timeout_streak INTEGER NOT NULL DEFAULT 0,
                cooldown_until REAL NOT NULL DEFAULT 0,pauses INTEGER NOT NULL DEFAULT 0);
            """
        )

    def close(self) -> None:
        self.database.close()

    @contextmanager
    def write(self) -> Iterator[None]:
        """Explicit write transaction; the connection runs in autocommit mode."""
        self.database.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.database.execute("ROLLBACK")
            raise
        else:
            self.database.execute("COMMIT")

    def recover(self) -> None:
        """Return interrupted leases to the pool; the next start reassigns them."""
        with self.write():
            self.database.execute("UPDATE fleet_routes SET state='pending',owner=NULL,next_at=0 WHERE state='leased'")

    # ---------------------------------------------------------------- routes

    def add_route(
        self,
        provider: str,
        endpoint: str,
        *,
        proxy_url: str | None = None,
        expected_ip: str | None = None,
        label: str | None = None,
        ca_file: str | None = None,
        source: str | None = None,
        state: str = "pending",
    ) -> None:
        with self.write():
            self.database.execute(
                "INSERT INTO fleet_routes("
                "route_id,provider,endpoint,proxy_url,label,expected_ip,ca_file,state,source,seen_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(route_id) DO UPDATE SET "
                "proxy_url=COALESCE(excluded.proxy_url,fleet_routes.proxy_url),"
                "label=COALESCE(excluded.label,fleet_routes.label),"
                "expected_ip=COALESCE(excluded.expected_ip,fleet_routes.expected_ip),"
                "ca_file=COALESCE(excluded.ca_file,fleet_routes.ca_file),"
                "source=COALESCE(excluded.source,fleet_routes.source),seen_at=excluded.seen_at,"
                "state=CASE WHEN fleet_routes.state IN ('leased','ready') THEN fleet_routes.state ELSE excluded.state END",
                (
                    f"{provider}:{endpoint}",
                    provider,
                    endpoint,
                    proxy_url,
                    label,
                    expected_ip,
                    ca_file,
                    state,
                    source,
                    time.time(),
                ),
            )

    def set_route_session(self, route_id: str, proxy_url: str, expected_ip: str | None) -> None:
        with self.write():
            self.database.execute(
                "UPDATE fleet_routes SET proxy_url=?,expected_ip=? WHERE route_id=?", (proxy_url, expected_ip, route_id)
            )

    def provider_spent(self, provider: str, day: str) -> int:
        row = self.database.execute(
            "SELECT documents FROM fleet_budgets WHERE provider=? AND day=?", (provider, day)
        ).fetchone()
        return int(row[0]) if row else 0

    def claim_route(
        self, owner: str, provider_order: list[str], budgets: dict[str, int], day: str, route_docs: int
    ) -> dict[str, Any] | None:
        """Lease the least-drained ready route of the first provider with budget left."""
        now = time.time()
        for provider in provider_order:
            if budgets.get(provider, 0) and self.provider_spent(provider, day) >= budgets[provider]:
                continue
            row = self.database.execute(
                "SELECT * FROM fleet_routes WHERE state='ready' AND provider=? AND documents<? "
                "AND next_at<=? AND parked_until<=? "
                "ORDER BY documents ASC,last_success DESC,route_id LIMIT 1",
                (provider, route_docs, now, now),
            ).fetchone()
            if not row:
                continue
            with self.write():
                cursor = self.database.execute(
                    "UPDATE fleet_routes SET state='leased',owner=?,next_at=0 WHERE route_id=? AND state='ready'",
                    (owner, row["route_id"]),
                )
            if cursor.rowcount != 1:
                continue
            return {**dict(row), "state": "leased", "owner": owner}
        return None

    def finish_route(
        self,
        route: dict[str, Any],
        *,
        documents: int = 0,
        tokens: int = 0,
        outcome: str = "ok",
        error: str | None = None,
        ttl: float = DEFAULT_PROXY_TTL,
        retry_seconds: float = DEFAULT_PROXY_RETRY_SECONDS,
        reset_at: float = 0.0,
        keep_free: bool = True,
        max_failures: int = DEFAULT_ROUTE_FAILURES,
        route_docs: int = DEFAULT_ROUTE_DOCS,
    ) -> str:
        """Apply one lease result; returns the route's next state."""
        now = time.time()
        route_id, provider = route["route_id"], route["provider"]
        documents = max(0, int(documents))
        tokens = max(0, int(tokens))
        total = route["documents"] + documents
        if outcome == "ok" and total >= route_docs:
            outcome = "budget"
        with self.write():
            if documents:
                self.database.execute(
                    "INSERT INTO fleet_budgets(provider,day,documents) VALUES(?,?,?) "
                    "ON CONFLICT(provider,day) DO UPDATE SET documents=documents+excluded.documents",
                    (provider, current_day(), documents),
                )
            if outcome in PARK_OUTCOMES:
                if provider == "free" and not keep_free:
                    self.database.execute("DELETE FROM fleet_routes WHERE route_id=?", (route_id,))
                    return "retired"
                # Paid routes, and free routes when kept, park until the next reset.
                self.database.execute(
                    "UPDATE fleet_routes SET state='parked',owner=NULL,parked_until=?,documents=?,"
                    "tokens=tokens+?,last_error=?,"
                    "proxy_url=CASE WHEN provider='brightdata' THEN NULL ELSE proxy_url END,"
                    "expected_ip=CASE WHEN provider='brightdata' THEN NULL ELSE expected_ip END,next_at=0 WHERE route_id=?",
                    (reset_at, total, tokens, error, route_id),
                )
                return "parked"
            if outcome == "dead":
                failures = int(route["failures"]) + 1
                if failures >= max_failures:
                    if provider == "free" and not keep_free:
                        self.database.execute("DELETE FROM fleet_routes WHERE route_id=?", (route_id,))
                        return "retired"
                    self.database.execute(
                        "UPDATE fleet_routes SET state='parked',owner=NULL,parked_until=?,documents=?,tokens=tokens+?,"
                        "failures=?,last_error=?,next_at=0 WHERE route_id=?",
                        (reset_at, total, tokens, failures, error, route_id),
                    )
                    return "parked"
                self.database.execute(
                    "UPDATE fleet_routes SET state='cooldown',owner=NULL,next_at=?,documents=?,tokens=tokens+?,"
                    "failures=?,last_error=? WHERE route_id=?",
                    (now + retry_seconds, total, tokens, failures, error, route_id),
                )
                return "cooldown"
            if outcome in {"retry", "cooldown"}:
                self.database.execute(
                    "UPDATE fleet_routes SET state='cooldown',owner=NULL,next_at=?,documents=?,"
                    "tokens=tokens+?,last_error=? WHERE route_id=?",
                    (now + retry_seconds, total, tokens, error, route_id),
                )
                return "cooldown"
            # Success - ready for next lease
            if provider == "free":
                self.database.execute(
                    "UPDATE fleet_routes SET state='ready',owner=NULL,documents=?,tokens=tokens+?,"
                    "exit_ip=COALESCE(?,exit_ip),"
                    "last_success=?,verified_at=COALESCE(verified_at,?),expires_at=?,next_at=0,"
                    "last_error=NULL WHERE route_id=?",
                    (total, tokens, route.get("exit_ip"), now, now, now + ttl, route_id),
                )
            else:
                self.database.execute(
                    "UPDATE fleet_routes SET state='ready',owner=NULL,documents=?,tokens=tokens+?,"
                    "last_success=?,verified_at=COALESCE(verified_at,?),next_at=0,last_error=NULL WHERE route_id=?",
                    (total, tokens, now, now, route_id),
                )
            return "ready"

    def park_provider(self, provider: str, reset_at: float) -> int:
        """Park every ready route of an over-budget provider until tomorrow."""
        with self.write():
            cursor = self.database.execute(
                "UPDATE fleet_routes SET state='parked',owner=NULL,parked_until=?,next_at=0 "
                "WHERE provider=? AND state IN ('ready','pending')",
                (reset_at, provider),
            )
            return cursor.rowcount

    def wake_parked(self, now: float) -> int:
        """Re-open parked routes after reset: free ones re-probe, paid ones resume ready."""
        with self.write():
            cursor = self.database.execute(
                "UPDATE fleet_routes SET "
                "state=CASE WHEN provider='free' THEN 'pending' ELSE 'ready' END,"
                "parked_until=0,documents=0,failures=0,next_at=0 "
                "WHERE state='parked' AND parked_until>0 AND parked_until<=?",
                (now,),
            )
            return cursor.rowcount

    def expire_free(self, now: float) -> int:
        """Return free routes whose verification TTL lapsed to the probe queue."""
        with self.write():
            cursor = self.database.execute(
                "UPDATE fleet_routes SET state='pending',owner=NULL,next_at=0 "
                "WHERE provider='free' AND state='ready' AND expires_at>0 AND expires_at<=?",
                (now,),
            )
            return cursor.rowcount

    def wake_cooldown(self, now: float) -> int:
        """Paid cooldowns promote themselves; free ones wait for a fresh probe."""
        with self.write():
            cursor = self.database.execute(
                "UPDATE fleet_routes SET state='ready',next_at=0 WHERE state='cooldown' AND provider<>'free' AND next_at<=?",
                (now,),
            )
            return cursor.rowcount

    def next_wake(self, now: float) -> float:
        """Earliest future moment when a parked or cooling route becomes eligible."""
        row = self.database.execute(
            "SELECT MIN(CASE WHEN state='parked' THEN parked_until WHEN state='cooldown' THEN next_at END) "
            "FROM fleet_routes WHERE state IN ('parked','cooldown') "
            "AND CASE WHEN state='parked' THEN parked_until WHEN state='cooldown' THEN next_at END > ?",
            (now,),
        ).fetchone()
        value = row[0] if row and row[0] is not None else None
        return float(value) if value is not None else now + 30.0

    def counts(self) -> dict[str, dict[str, int]]:
        return {
            "routes": {
                row[0]: row[1] for row in self.database.execute("SELECT state,COUNT(*) FROM fleet_routes GROUP BY state")
            },
            "budgets": {
                row[0]: row[1]
                for row in self.database.execute(
                    "SELECT provider,documents FROM fleet_budgets WHERE day=?", (current_day(),)
                )
            },
        }


class FleetProducer:
    """Discovers and verifies public free candidates for the mixed fleet pool."""

    def __init__(self, args: FleetArgs, root: Path, queue: FleetQueue, changed: asyncio.Event, stop: asyncio.Event):
        self.args, self.root, self.queue, self.changed, self.stop = args, root, queue, changed, stop
        self.exceptions = endpoint_exceptions(args.allowed_proxy_endpoints)
        self.ports = proxy_ports(args.proxy_ports)
        self.stats: dict[str, Any] = {
            "checks": 0,
            "ready": 0,
            "denied_or_failed": 0,
            "catalog_refreshes": 0,
            "reputation_denied": 0,
            "connection_failed": 0,
            "configuration_failed": 0,
            "failure_reasons": {},
            "recent_ready": [],
        }
        self.next_catalog = 0.0
        self.next_probe = 0.0

    async def refresh(self) -> None:
        if not public_collection_allowed():
            return
        sources = resolve_sources(self.args.sources)
        public = [source for source in sources if source != "free-proxy"]
        catalog = await load_public_catalog(
            public,
            timeout=self.args.proxy_timeout,
            cache_dir=self.root / "source-cache",
            download_concurrency=3,
            max_records=self.args.source_records or None,
            max_response_bytes=16 * 1024 * 1024,
        )
        raw = [(candidate.url, candidate.source) for candidate in sorted(catalog.candidates, key=lambda row: row.priority)]
        if "free-proxy" in sources:
            try:
                extra = await asyncio.wait_for(load_free_proxy_candidates(request_timeout=10), 35)
                raw.extend((endpoint, "free-proxy") for endpoint in extra)
            except Exception as error:
                catalog.errors["free-proxy"] = type(error).__name__
        seen: set[str] = set()
        candidates: list[tuple[str, str]] = []
        for endpoint, source in raw:
            if endpoint in seen:
                continue
            seen.add(endpoint)
            try:
                parts = urlsplit(endpoint)
                if parts.scheme not in {"http", "socks5", "socks5h"} or parts.username or parts.password or not parts.port:
                    continue
                address = ipaddress.ip_address(parts.hostname or "")
                if address.version != 4 or not address.is_global:
                    continue
                validate_ip(str(address))
            except (ValueError, ReputationDenied):
                continue
            if not endpoint_allowed(endpoint, self.exceptions, self.ports):
                continue
            candidates.append((endpoint, source))
        limited = candidates[: self.args.max_candidates] if self.args.max_candidates else candidates
        inserted = 0
        for endpoint, source in limited:
            before = self.queue.database.execute(
                "SELECT 1 FROM fleet_routes WHERE route_id=?", (f"free:{endpoint}",)
            ).fetchone()
            if not before:
                self.queue.add_route("free", endpoint, source=source)
                inserted += 1
        self.stats.update(
            catalog_candidates=len(raw),
            catalog_unique=len(seen),
            port_eligible=len(candidates),
            selected_candidates=len(limited),
            queue_inserted=inserted,
        )
        self.stats["catalog_refreshes"] += 1
        # Cache cleanup
        cached = sorted((self.root / "source-cache").glob("*"), key=lambda path: path.stat().st_mtime)
        retained = sum(path.stat().st_size for path in cached if path.is_file())
        for path in cached:
            if retained <= self.args.source_cache_mib * 1024 * 1024:
                break
            if path.is_file():
                retained -= path.stat().st_size
                path.unlink(missing_ok=True)
        self.changed.set()

    async def verify(self, endpoint: str) -> str:
        pinned = await screen_proxy(endpoint)
        async with httpx.AsyncClient(
            proxy=pinned,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(self.args.proxy_timeout, connect=min(8, self.args.proxy_timeout)),
            headers={"User-Agent": "Mozilla/5.0 (compatible; FleetBot/1.0)"},
        ) as client:

            async def get(url: str, limit: int = 2 * 1024 * 1024) -> bytes:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > limit:
                            raise ValueError("Proxy probe response too large")
                    return bytes(body)

            exit_ip = str(ipaddress.ip_address(json.loads(await get("https://api.ipify.org?format=json", 65536))["ip"]))
            await asyncio.to_thread(check_ip, exit_ip)
            # Verify the proxy can reach a generic HTTPS endpoint
            await get("https://httpbin.org/get")
            after = str(ipaddress.ip_address(json.loads(await get("https://api.ipify.org?format=json", 65536))["ip"]))
            if after != exit_ip:
                raise ValueError("Proxy exit changed during preflight")
            return exit_ip

    async def scan(self) -> None:
        while not self.stop.is_set():
            candidate = self.queue.database.execute(
                "SELECT * FROM fleet_routes WHERE provider='free' AND state IN ('pending','cooldown') AND next_at<=? "
                "ORDER BY last_success DESC,next_at,route_id LIMIT 1",
                (time.time(),),
            ).fetchone()
            if not candidate:
                await asyncio.sleep(0.5)
                continue
            with self.queue.write():
                cursor = self.queue.database.execute(
                    "UPDATE fleet_routes SET state='probing' WHERE route_id=? AND state IN ('pending','cooldown')",
                    (candidate["route_id"],),
                )
            if cursor.rowcount != 1:
                continue
            row = dict(candidate)
            # Share one verification rate across all scanners without holding a lock.
            delay = max(0.0, self.next_probe - time.monotonic())
            self.next_probe = time.monotonic() + delay + 60 / self.args.proxy_rate_per_minute
            if delay:
                await asyncio.sleep(delay)
            try:
                exit_ip = await self.verify(row["endpoint"])
                if self._finish_probe(row, exit_ip):
                    self.stats["ready"] += 1
                    self.stats["recent_ready"] = [*self.stats["recent_ready"][-4:], exit_ip]
                    self.changed.set()
                else:
                    self.stats["denied_or_failed"] += 1
            except asyncio.CancelledError:
                self._finish_probe(row, error="probe interrupted")
                raise
            except Exception as error:
                category = probe_error_category(error)
                self.stats["failure_reasons"][category] = self.stats["failure_reasons"].get(category, 0) + 1
                self._finish_probe(row, error=f"{category}: {safe_message(error)}")
                self.stats["denied_or_failed"] += 1
                counter = (
                    "reputation_denied"
                    if isinstance(error, ReputationDenied)
                    else "configuration_failed"
                    if isinstance(error, ImportError)
                    else "connection_failed"
                )
                self.stats[counter] += 1
            finally:
                self.stats["checks"] += 1

    def _finish_probe(self, row: dict[str, Any], exit_ip: str | None = None, error: str | None = None) -> bool:
        now = time.time()
        with self.queue.write():
            if error:
                self.queue.database.execute(
                    "UPDATE fleet_routes SET state='cooldown',owner=NULL,next_at=?,last_error=? WHERE route_id=?",
                    (now + self.args.proxy_retry_seconds, error, row["route_id"]),
                )
                return False
            self.queue.database.execute(
                "UPDATE fleet_routes SET state='ready',owner=NULL,exit_ip=?,verified_at=?,expires_at=?,"
                "next_at=0,last_error=NULL WHERE route_id=?",
                (exit_ip, now, now + self.args.proxy_ttl, row["route_id"]),
            )
            return True

    async def run(self) -> None:
        scanners = [asyncio.create_task(self.scan()) for _ in range(self.args.proxy_check_concurrency)]
        try:
            while not self.stop.is_set():
                if any(scanner.done() and not scanner.cancelled() and scanner.exception() for scanner in scanners):
                    raise RuntimeError("A fleet proxy verifier task failed")
                if time.monotonic() >= self.next_catalog:
                    try:
                        await self.refresh()
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        self.stats["catalog_error"] = safe_message(error)
                    self.next_catalog = time.monotonic() + self.args.catalog_seconds
                    pending = self.queue.counts()["routes"].get("pending", 0)
                    print(
                        f"🧭 Free proxies: {self.stats['ready']} ready, {self.stats['denied_or_failed']} failed, "
                        f"{self.stats['checks']} checks; {pending} pending"
                    )
                await asyncio.sleep(0.5)
        finally:
            for scanner in scanners:
                scanner.cancel()
            await asyncio.gather(*scanners, return_exceptions=True)


@dataclass(frozen=True, slots=True)
class LeaseResult:
    """Result of a browser lease."""

    route: dict[str, Any]
    documents: int = 0
    tokens: int = 0
    outcome: str = "ok"
    error: str | None = None
    completed: set[str] = field(default_factory=set)
    attempted: set[str] = field(default_factory=set)
    quota_limited: bool = False
    retry_connection: bool = False


class FleetDispatcher:
    """Each browser slot serially owns one fleet route."""

    def __init__(
        self,
        args: FleetArgs,
        queue: FleetQueue,
        worker_factory: Callable[[int, dict[str, Any]], Awaitable[LeaseResult]],
        changed: asyncio.Event,
        stop: asyncio.Event,
        session_minter: Callable[[dict[str, Any]], Awaitable[tuple[str, str | None]]] | None = None,
    ):
        if not 1 <= args.browsers <= 10:
            raise ValueError("Fleet dispatcher supports at most ten browsers")
        self.args, self.queue, self.worker_factory, self.changed, self.stop = args, queue, worker_factory, changed, stop
        self.session_minter = session_minter
        self.provider_order = [provider.strip() for provider in args.providers.split(",") if provider.strip() in PROVIDERS]
        self.budgets = {
            "webshare": args.budget_webshare,
            "brightdata": args.budget_brightdata,
            "paid": args.budget_paid,
            "free": args.budget_free,
        }
        self.active: dict[int, str] = {}
        self.proxy_failures = 0
        self.cooldowns = 0
        self.total_documents = 0
        self.day = current_day()
        self.reset_at = next_reset(args.reset_hour)
        self.next_maintenance = 0.0

    def rollover_day(self) -> None:
        if current_day() != self.day:
            self.day = current_day()
            self.reset_at = next_reset(self.args.reset_hour)
            with self.queue.write():
                self.queue.database.execute("UPDATE fleet_routes SET documents=0,failures=0")

    def budgets_exhausted(self) -> bool:
        """True when every configured provider has spent its daily quota."""
        configured = [provider for provider in self.provider_order if self.budgets.get(provider, 0) > 0]
        if not configured:
            return False
        return all(self.queue.provider_spent(provider, self.day) >= self.budgets[provider] for provider in configured)

    def route_outcome(self, result: LeaseResult) -> str:
        if result.quota_limited:
            return "daily_limit"
        if result.retry_connection:
            return "retry"
        if result.outcome != "ok":
            return result.outcome
        error = (result.error or "").lower()
        if error and any(word in error for word in ("auth", "reputation", "denied", "ip_blacklisted")):
            return "dead"
        if result.error:
            return "cooldown"
        return "ok"

    async def _mint_if_needed(self, route: dict[str, Any]) -> dict[str, Any]:
        """Bright Data rows are minted lazily; other providers already carry a URL."""
        if route["provider"] != "brightdata" or route.get("proxy_url"):
            return route
        if self.session_minter is None:
            raise RuntimeError("Bright Data route requires a configured session minter")
        proxy_url, expected_ip = await self.session_minter(route)
        self.queue.set_route_session(route["route_id"], proxy_url, expected_ip)
        return {**route, "proxy_url": proxy_url, "expected_ip": expected_ip}

    async def _idle(self) -> None:
        """Wait for a change or until parked routes wake up; never busy-spin."""
        now = time.time()
        wake = self.reset_at if self.budgets_exhausted() else self.queue.next_wake(now)
        delay = max(0.25, min(30.0, (wake - now) if wake > now else 30.0))
        with suppress(TimeoutError):
            await asyncio.wait_for(self.changed.wait(), delay)
        self.changed.clear()

    async def consume(self, slot: int, *, run_once: bool = False) -> None:
        processed = 0
        while not self.stop.is_set():
            self.rollover_day()
            now = time.time()
            if now >= self.next_maintenance:
                self.queue.wake_parked(now)
                self.queue.wake_cooldown(now)
                self.queue.expire_free(now)
                self.next_maintenance = now + 5
            if run_once and processed:
                return
            owner = uuid.uuid4().hex
            route = self.queue.claim_route(owner, self.provider_order, self.budgets, self.day, self.args.route_docs)
            if not route:
                if run_once:
                    return
                await self._idle()
                continue
            processed += 1
            self.active[slot] = route["route_id"]
            result = LeaseResult(route=route, error="Worker interrupted before processing")
            try:
                route = await self._mint_if_needed(route)
                result = await self.worker_factory(slot, route)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                result = LeaseResult(route=route, error=safe_message(error))
            finally:
                outcome = self.route_outcome(result)
                state = self.queue.finish_route(
                    route,
                    documents=result.documents,
                    tokens=result.tokens,
                    outcome=outcome,
                    error=result.error,
                    ttl=self.args.proxy_ttl,
                    retry_seconds=self.args.proxy_retry_seconds,
                    reset_at=self.reset_at,
                    keep_free=self.args.free_keep,
                    max_failures=self.args.route_failures,
                    route_docs=self.args.route_docs,
                )
                if outcome in PARK_OUTCOMES:
                    print(
                        f"🅿️ {route['provider']} route {route['endpoint']} drained to quota; "
                        f"parked until {datetime.fromtimestamp(self.reset_at, UTC).strftime('%Y-%m-%d %H:%M')} UTC"
                    )
                elif state == "retired":
                    print(f"🗑️ {route['provider']} route {route['endpoint']} retired")
                self.active.pop(slot, None)
                self.changed.set()
            self.total_documents += result.documents
            budget = self.budgets.get(route["provider"], 0)
            if budget and self.queue.provider_spent(route["provider"], self.day) >= budget:
                parked = self.queue.park_provider(route["provider"], self.reset_at)
                if parked:
                    print(
                        f"🅿️ {route['provider']} budget reached; parked {parked} route(s) until "
                        f"{datetime.fromtimestamp(self.reset_at, UTC).strftime('%Y-%m-%d %H:%M')} UTC"
                    )
            self.proxy_failures = 0 if result.completed or outcome in PARK_OUTCOMES else self.proxy_failures + 1
            if self.proxy_failures >= self.args.max_failures and self.args.error_cooldown_minutes > 0:
                self.cooldowns += 1
                if self.args.max_cooldowns and self.cooldowns > self.args.max_cooldowns:
                    print(f"🛑 Fleet halted after {self.cooldowns - 1} cooldowns")
                    self.stop.set()
                    return
                print(
                    f"⏸️ Fleet cooldown {self.cooldowns}/{self.args.max_cooldowns or '∞'} "
                    f"for {self.args.error_cooldown_minutes} min"
                )
                with suppress(TimeoutError):
                    await asyncio.wait_for(self.stop.wait(), self.args.error_cooldown_minutes * 60)
                self.proxy_failures = 0


async def seed_routes(args: FleetArgs, queue: FleetQueue, *, has_minter: bool = False) -> dict[str, int]:
    """Fill the fleet once from every configured paid source; free routes arrive via the producer."""
    seeded: dict[str, int] = {}
    if "webshare" in args.provider_set:
        token = os.getenv("WEBSHARE_API_TOKEN", "").strip()
        if not token:
            raise ValueError("Webshare provider requires WEBSHARE_API_TOKEN")
        candidates = await load_webshare_candidates(token, mode="direct")
        for candidate in candidates:
            parts = urlsplit(candidate.url)
            if not parts.hostname or not parts.port:
                continue
            queue.add_route(
                "webshare",
                f"{parts.hostname}:{parts.port}",
                proxy_url=candidate.url,
                expected_ip=str(ipaddress.ip_address(parts.hostname)),
                label=f"webshare:{parts.hostname}:{parts.port}",
                source="webshare",
                state="ready",
            )
        seeded["webshare"] = len(candidates)
    if "brightdata" in args.provider_set:
        if not has_minter and (
            not os.getenv("BRIGHTDATA_PROXY_USERNAME", "").strip() or not os.getenv("BRIGHTDATA_PROXY_PASSWORD", "").strip()
        ):
            raise ValueError("Bright Data provider requires BRIGHTDATA_PROXY_USERNAME and BRIGHTDATA_PROXY_PASSWORD")
        server = os.getenv("BRIGHTDATA_PROXY_SERVER", "http://brd.superproxy.io:44445")
        parts = urlsplit(server)
        sessions = args.browsers  # One sticky session per browser by default.
        for index in range(sessions):
            queue.add_route(
                "brightdata",
                parts.netloc,
                label=f"brightdata:{index + 1}",
                ca_file="docker/brightdata-proxy-ca.crt",
                source="brightdata",
                state="pending",  # Minted on first lease.
            )
        seeded["brightdata"] = sessions
    if "paid" in args.provider_set:
        paid = args.paid_file
        if not paid.exists():
            if args.provider_set <= {"paid", "free"} and "paid" in args.provider_set:
                raise ValueError(f"Paid provider file not found: {paid}")
            print(f"⚠️ Paid proxies file not found: {paid}; continuing without the paid provider")
        else:
            candidates = await asyncio.to_thread(load_paid_candidates, paid)
            for candidate in candidates:
                parts = urlsplit(candidate.url)
                if not parts.hostname or not parts.port:
                    continue
                try:
                    expected = str(ipaddress.ip_address(parts.hostname or ""))
                except ValueError:
                    expected = None
                endpoint = f"{parts.hostname}:{parts.port}"
                queue.add_route(
                    "paid",
                    endpoint,
                    proxy_url=candidate.url,
                    expected_ip=expected,
                    label=f"paid:{endpoint}",
                    source="paid",
                    state="ready",
                )
            seeded["paid"] = len(candidates)
    return seeded


def build_fleet_args(args: argparse.Namespace, root: Path) -> FleetArgs:
    """Convert argparse namespace to FleetArgs."""
    provider_set = frozenset(p.strip() for p in args.providers.split(",") if p.strip() in PROVIDERS)
    return FleetArgs(
        database=(args.database or (root / "fleet.sqlite3")),
        output=args.output,
        providers=args.providers,
        browsers=args.browsers,
        budget_webshare=args.budget_webshare,
        budget_brightdata=args.budget_brightdata,
        budget_paid=args.budget_paid,
        budget_free=args.budget_free,
        route_docs=args.route_docs,
        proxy_ttl=args.proxy_ttl,
        proxy_retry_seconds=args.proxy_retry_seconds,
        route_failures=args.route_failures,
        catalog_seconds=args.catalog_seconds,
        proxy_rate_per_minute=args.proxy_rate_per_minute,
        proxy_check_concurrency=args.proxy_check_concurrency,
        proxy_timeout=args.proxy_timeout,
        source_cache_mib=args.source_cache_mib,
        source_records=args.source_records,
        max_candidates=args.max_candidates,
        worker_timeout=args.worker_timeout,
        company_attempts=args.company_attempts,
        retry_seconds=args.retry_seconds,
        max_failures=args.max_failures,
        error_cooldown_minutes=args.error_cooldown_minutes,
        max_cooldowns=args.max_cooldowns,
        reset_hour=args.reset_hour,
        free_keep=args.free_keep,
        allowed_proxy_endpoints=args.allowed_proxy_endpoints,
        proxy_ports=args.proxy_ports,
        paid_file=args.paid_file,
        sources=args.sources or "public",
        provider_set=provider_set,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Multi-provider proxy fleet for browser automation")
    parser.add_argument("--output", type=Path, required=True, help="Output directory for worker data")
    parser.add_argument("--database", type=Path, help="Fleet database path (default: output/fleet.sqlite3)")
    parser.add_argument(
        "--providers",
        default="webshare,brightdata,paid,free",
        help="Comma-separated providers: webshare,brightdata,paid,free (default: all)",
    )
    parser.add_argument(
        "--browsers",
        type=int,
        default=DEFAULT_BROWSERS,
        help=f"Number of browser slots (default: {DEFAULT_BROWSERS}, max 10)",
    )
    parser.add_argument(
        "--budget-webshare",
        type=int,
        default=DEFAULT_BUDGET_WEBSHARE,
        help=f"Daily document budget for Webshare (default: {DEFAULT_BUDGET_WEBSHARE})",
    )
    parser.add_argument(
        "--budget-brightdata",
        type=int,
        default=DEFAULT_BUDGET_BRIGHTDATA,
        help=f"Daily document budget for Bright Data (default: {DEFAULT_BUDGET_BRIGHTDATA})",
    )
    parser.add_argument(
        "--budget-paid",
        type=int,
        default=DEFAULT_BUDGET_PAID,
        help=f"Daily document budget for paid file (default: {DEFAULT_BUDGET_PAID})",
    )
    parser.add_argument(
        "--budget-free",
        type=int,
        default=DEFAULT_BUDGET_FREE,
        help=f"Daily document budget for free proxies (default: {DEFAULT_BUDGET_FREE})",
    )
    parser.add_argument(
        "--route-docs",
        type=int,
        default=DEFAULT_ROUTE_DOCS,
        help=f"Per-route document cap before parking (default: {DEFAULT_ROUTE_DOCS})",
    )
    parser.add_argument(
        "--proxy-ttl", type=int, default=DEFAULT_PROXY_TTL, help=f"Free proxy TTL in seconds (default: {DEFAULT_PROXY_TTL})"
    )
    parser.add_argument(
        "--proxy-retry-seconds",
        type=int,
        default=DEFAULT_PROXY_RETRY_SECONDS,
        help=f"Cooldown for failed routes in seconds (default: {DEFAULT_PROXY_RETRY_SECONDS})",
    )
    parser.add_argument(
        "--route-failures",
        type=int,
        default=DEFAULT_ROUTE_FAILURES,
        help=f"Max failures before parking a route (default: {DEFAULT_ROUTE_FAILURES})",
    )
    parser.add_argument(
        "--catalog-seconds",
        type=int,
        default=DEFAULT_CATALOG_SECONDS,
        help=f"Free proxy catalog refresh interval (default: {DEFAULT_CATALOG_SECONDS})",
    )
    parser.add_argument(
        "--proxy-rate-per-minute",
        type=int,
        default=DEFAULT_PROXY_RATE_PER_MINUTE,
        help=f"Free proxy verification rate per minute (default: {DEFAULT_PROXY_RATE_PER_MINUTE})",
    )
    parser.add_argument(
        "--proxy-check-concurrency",
        type=int,
        default=DEFAULT_PROXY_CHECK_CONCURRENCY,
        help=f"Concurrent free proxy verifications (default: {DEFAULT_PROXY_CHECK_CONCURRENCY})",
    )
    parser.add_argument(
        "--proxy-timeout",
        type=float,
        default=DEFAULT_PROXY_TIMEOUT,
        help=f"Free proxy verification timeout (default: {DEFAULT_PROXY_TIMEOUT})",
    )
    parser.add_argument(
        "--source-cache-mib",
        type=int,
        default=DEFAULT_SOURCE_CACHE_MIB,
        help=f"Source cache size limit in MiB (default: {DEFAULT_SOURCE_CACHE_MIB})",
    )
    parser.add_argument(
        "--source-records",
        type=int,
        default=DEFAULT_SOURCE_RECORDS,
        help=f"Max records per source (default: {DEFAULT_SOURCE_RECORDS})",
    )
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=DEFAULT_MAX_CANDIDATES,
        help=f"Max free candidates to queue (default: {DEFAULT_MAX_CANDIDATES})",
    )
    parser.add_argument(
        "--worker-timeout",
        type=int,
        default=DEFAULT_WORKER_TIMEOUT,
        help=f"Worker inactivity timeout in seconds (default: {DEFAULT_WORKER_TIMEOUT})",
    )
    parser.add_argument(
        "--company-attempts",
        type=int,
        default=DEFAULT_COMPANY_ATTEMPTS,
        help=f"Attempts per company (default: {DEFAULT_COMPANY_ATTEMPTS})",
    )
    parser.add_argument(
        "--retry-seconds",
        type=int,
        default=DEFAULT_RETRY_SECONDS,
        help=f"Retry delay for companies in seconds (default: {DEFAULT_RETRY_SECONDS})",
    )
    parser.add_argument(
        "--max-failures",
        type=int,
        default=DEFAULT_MAX_FAILURES,
        help=f"Consecutive proxy failures before fleet cooldown (default: {DEFAULT_MAX_FAILURES})",
    )
    parser.add_argument(
        "--error-cooldown-minutes",
        type=int,
        default=DEFAULT_ERROR_COOLDOWN_MINUTES,
        help=f"Fleet cooldown minutes after max failures (default: {DEFAULT_ERROR_COOLDOWN_MINUTES})",
    )
    parser.add_argument(
        "--max-cooldowns",
        type=int,
        default=DEFAULT_MAX_COOLDOWNS,
        help=f"Max fleet cooldowns before halt (default: {DEFAULT_MAX_COOLDOWNS})",
    )
    parser.add_argument(
        "--reset-hour",
        type=int,
        default=DEFAULT_RESET_HOUR,
        help=f"Daily quota reset hour in UTC (default: {DEFAULT_RESET_HOUR})",
    )
    parser.add_argument(
        "--free-keep",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_FREE_KEEP,
        help="Park free routes until next quota instead of deleting (default: True)",
    )
    parser.add_argument("--allowed-proxy-endpoints", default="", help="Extra host:port exceptions allowed for free proxies")
    parser.add_argument("--proxy-ports", default="80,443,8080,8000,3128,1080", help="Allowed proxy ports for free proxies")
    parser.add_argument("--paid-file", type=Path, default=Path("data/paid-proxies.txt"), help="Path to paid proxies file")
    parser.add_argument("--sources", default="public", help="Comma-separated free proxy sources (default: public)")
    parser.add_argument("--run-once", action="store_true", help="Run one lease per browser and exit (for testing)")
    parser.add_argument("--demo", action="store_true", help="Run with the built-in demo worker (no real scraping)")
    parser.add_argument("--status", action="store_true", help="Print route and budget counts, then exit")
    return parser


def route_proxy_options(route: dict[str, Any]) -> dict[str, Any]:
    """Build proxy settings for a browser from a leased route.

    Bright Data routes carry their own CA bundle; Webshare/paid use the URL as-is.
    """
    proxy_url = route.get("proxy_url") or route.get("endpoint")
    options: dict[str, Any] = {"proxy_url": proxy_url}
    if route.get("ca_file"):
        options["ca_file"] = route["ca_file"]
    if route.get("expected_ip"):
        options["expected_ip"] = route["expected_ip"]
    return options


async def run_fleet(
    args: argparse.Namespace,
    worker_factory: Callable[[int, dict[str, Any]], Awaitable[LeaseResult]],
    session_minter: Callable[[dict[str, Any]], Awaitable[tuple[str, str | None]]] | None = None,
) -> None:
    """Main fleet entry point. worker_factory(slot, route) -> LeaseResult."""
    root = args.output
    root.mkdir(parents=True, exist_ok=True)
    fleet_args = build_fleet_args(args, root)
    queue = FleetQueue(fleet_args.database, fleet_args.run_id)

    if getattr(args, "status", False):
        print(json.dumps(queue.counts(), indent=2, sort_keys=True))
        queue.close()
        return

    queue.recover()
    changed = asyncio.Event()
    stop = asyncio.Event()

    # Seed paid routes (idempotent; free routes arrive from the producer).
    seeded = await seed_routes(fleet_args, queue, has_minter=session_minter is not None)
    print(f"🚀 Fleet seeded: {seeded}; browsers={fleet_args.browsers}; providers={sorted(fleet_args.provider_set)}")
    if "free" in fleet_args.provider_set and not public_collection_allowed():
        print("⚠️ Free proxies disabled: set ALLOW_PUBLIC_PROXY_COLLECTION=1 to discover public candidates")

    # Start producer for free proxies only when they are part of the fleet.
    producer = FleetProducer(fleet_args, root, queue, changed, stop)
    producer_task = asyncio.create_task(producer.run()) if "free" in fleet_args.provider_set else None

    # Start one dispatcher task per browser slot.
    dispatcher = FleetDispatcher(fleet_args, queue, worker_factory, changed, stop, session_minter)
    dispatchers = [
        asyncio.create_task(dispatcher.consume(slot, run_once=args.run_once)) for slot in range(fleet_args.browsers)
    ]

    # Handle signals.
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    try:
        await asyncio.gather(*dispatchers)
    finally:
        stop.set()
        if producer_task is not None:
            producer_task.cancel()
            await asyncio.gather(producer_task, *dispatchers, return_exceptions=True)
        else:
            await asyncio.gather(*dispatchers, return_exceptions=True)
        queue.close()
        print(f"🏁 Fleet stopped. Total documents: {dispatcher.total_documents}")


# Example worker factory for testing
async def example_worker(slot: int, route: dict[str, Any]) -> LeaseResult:
    """Example worker that simulates work."""
    print(f"🖥️ Browser {slot + 1} leased route {route['endpoint']} ({route['provider']})")
    await asyncio.sleep(2)  # Simulate work
    return LeaseResult(
        route=route, documents=1, tokens=1, outcome="ok", completed={"test-company"}, attempted={"test-company"}
    )


async def main() -> None:
    """Run the fleet CLI (``--demo``) or explain how to plug in a real worker."""
    parser = build_parser()
    args = parser.parse_args()
    try:
        if getattr(args, "status", False):
            await run_fleet(args, example_worker)
            return
        if not getattr(args, "demo", False):
            parser.error(
                "This CLI only demonstrates the fleet. Import src.proxy.fleet.run_fleet and pass your "
                "worker_factory(slot, route) to drive real browsers, or add --demo to try it out."
            )
        # The demo performs one lease per browser so it terminates on its own.
        args.run_once = True
        await run_fleet(args, example_worker)
    except (ValueError, OSError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    asyncio.run(main())
