"""Mixed-provider RES fleet: drain paid routes to their limit, park until tomorrow.

Every browser slot leases one route from any configured provider: Webshare,
Bright Data, a static paid list or the public free catalog. A route is drained
until its per-route document cap, the provider's daily budget or the RES
per-IP daily limit. Paid routes that reach a limit are parked until the next
quota reset (next day); free routes are deleted once dead or exhausted, unless
asked to keep them until the next reset. State lives in fleet-runtime.sqlite3
next to the shared tracking ledger, so a restart resumes routes and jobs.
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import ipaddress
import json
import math
import os
import re
import shutil
import signal
import sqlite3
import time
import uuid
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

from scripts import res_free_pilot as pilot
from src.proxy.candidate_filter import filter_candidate_ips
from src.proxy.reputation import (
    ReputationDenied,
    check_ip,
    public_collection_allowed,
    reputation_mode,
    screen_proxy,
    validate_ip,
)
from src.proxy.sources.catalog import load_public_catalog
from src.proxy.sources.free_proxy import load_free_proxy_candidates
from src.proxy.sources.paid import load_paid_candidates
from src.proxy.sources.registry import resolve_sources
from src.proxy.sources.webshare import load_webshare_candidates
from src.sites.res import benchmark
from src.sites.res.fleet_quota import quota_day, rollover
from src.sites.res.free_stream import (
    BatchResult,
    BrowserWorker,
    endpoint_allowed,
    endpoint_exceptions,
    probe_error_category,
    proxy_ports,
    require_socks_support,
)
from src.sites.res.open_data import select_archive, select_companies
from src.sites.res.stream_notifications import StreamNotifications
from src.sites.res.tracking import TrackingStore, import_run
from src.utils.atomic import atomic_write_json
from src.utils.error_details import safe_message

TERMINAL = {"downloaded", "not_found", "no_cve_found"}
PARK_OUTCOMES = {"daily_limit", "budget", "provider_budget"}
PROVIDERS = ("webshare", "brightdata", "paid", "free")


def next_reset(reset_hour: int, now: datetime | None = None) -> float:
    """First local-UTC moment at or after the next daily quota reset."""
    now = now or datetime.now(UTC)
    target = now.replace(hour=reset_hour, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target.timestamp()


def current_day(reset_hour: int = 0) -> str:
    return quota_day(reset_hour)


class FleetQueue:
    """Routes, provider budgets and the durable company queue.

    ``free_jobs``/``free_proxies`` keep the exact FreeQueue schema so the
    unchanged continuous worker child can lease companies by owner.
    """

    def __init__(self, path: Path, run_id: str):
        self.run_id = run_id
        self.database = sqlite3.connect(path, timeout=30)
        self.database.row_factory = sqlite3.Row
        self.database.execute("PRAGMA journal_mode=WAL")
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
            CREATE TABLE IF NOT EXISTS fleet_clock (id INTEGER PRIMARY KEY CHECK(id=1),day TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS fleet_document_claims (
                token TEXT PRIMARY KEY,owner TEXT NOT NULL,day TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS fleet_document_claims_owner ON fleet_document_claims(owner);
            CREATE TABLE IF NOT EXISTS fleet_subnets (
                subnet TEXT PRIMARY KEY,timeout_streak INTEGER NOT NULL DEFAULT 0,
                cooldown_until REAL NOT NULL DEFAULT 0,pauses INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS free_jobs (
                run_id TEXT,company_id TEXT,payload TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,next_at REAL NOT NULL DEFAULT 0,
                owner TEXT,error TEXT,PRIMARY KEY(run_id,company_id));
            CREATE TABLE IF NOT EXISTS free_proxies (
                endpoint TEXT PRIMARY KEY,source TEXT,state TEXT NOT NULL DEFAULT 'pending',
                exit_ip TEXT,verified_at REAL,expires_at REAL NOT NULL DEFAULT 0,
                next_at REAL NOT NULL DEFAULT 0,owner TEXT,error TEXT,seen_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS free_subnets (
                subnet TEXT PRIMARY KEY,timeout_streak INTEGER NOT NULL DEFAULT 0,
                cooldown_until REAL NOT NULL DEFAULT 0,pauses INTEGER NOT NULL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS free_jobs_owner ON free_jobs(run_id,owner,state);
            """
        )
        job_columns = {row[1] for row in self.database.execute("PRAGMA table_info(free_jobs)")}
        if "selection_order" not in job_columns:
            self.database.execute("ALTER TABLE free_jobs ADD COLUMN selection_order INTEGER NOT NULL DEFAULT 0")
        self.database.execute(
            "CREATE INDEX IF NOT EXISTS free_jobs_dispatch ON "
            "free_jobs(run_id,state,attempts,next_at,selection_order,company_id)"
        )
        with self.database:
            self.database.execute("INSERT OR IGNORE INTO fleet_clock(id,day) VALUES(1,?)", (current_day(),))

    def close(self) -> None:
        self.database.close()

    def recover(self) -> None:
        """Return interrupted leases to the pool; the next start reassigns them."""
        with self.database:
            self.database.execute(
                "UPDATE free_jobs SET state='pending',owner=NULL WHERE run_id=? AND state='leased'", (self.run_id,)
            )
            self.database.execute(
                "UPDATE fleet_routes SET state=CASE WHEN provider='free' THEN 'pending' ELSE 'ready' END,"
                "owner=NULL,next_at=0 WHERE state IN ('leased','probing')"
            )
            self.database.execute("DELETE FROM free_proxies")
            self.database.execute("DELETE FROM fleet_document_claims")

    def rollover(self, day: str) -> None:
        with self.database:
            self.database.execute("BEGIN IMMEDIATE")
            rollover(self.database, day)

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
        route_id: str | None = None,
    ) -> None:
        with self.database:
            self.database.execute(
                "INSERT INTO fleet_routes(route_id,provider,endpoint,proxy_url,label,expected_ip,ca_file,"
                "state,source,seen_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(route_id) DO UPDATE SET "
                "proxy_url=COALESCE(excluded.proxy_url,fleet_routes.proxy_url),label=COALESCE(excluded.label,fleet_routes.label),"
                "expected_ip=COALESCE(excluded.expected_ip,fleet_routes.expected_ip),ca_file=COALESCE(excluded.ca_file,fleet_routes.ca_file),"
                "source=COALESCE(excluded.source,fleet_routes.source),seen_at=excluded.seen_at,"
                "state=fleet_routes.state",
                (
                    route_id or f"{provider}:{endpoint}",
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
        with self.database:
            self.database.execute(
                "UPDATE fleet_routes SET proxy_url=?,expected_ip=?,exit_ip=? WHERE route_id=?",
                (proxy_url, expected_ip, expected_ip, route_id),
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
                "AND (provider<>'free' OR expires_at=0 OR expires_at>?) "
                "ORDER BY documents ASC,last_success DESC,route_id LIMIT 1",
                (provider, route_docs, now, now, now),
            ).fetchone()
            if not row:
                continue
            with self.database:
                self.database.execute("BEGIN IMMEDIATE")
                cursor = self.database.execute(
                    "UPDATE fleet_routes SET state='leased',owner=?,next_at=0 WHERE route_id=? AND state='ready'",
                    (owner, row["route_id"]),
                )
                if cursor.rowcount != 1:
                    self.database.execute("ROLLBACK")
                    continue
                self.database.execute(
                    "INSERT OR REPLACE INTO free_proxies(endpoint,source,state,exit_ip,verified_at,expires_at,"
                    "next_at,owner,error,seen_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (row["route_id"], provider, "leased", row["exit_ip"], row["verified_at"], 0, 0, owner, None, now),
                )
                self.database.execute("COMMIT")
            return {**dict(row), "owner": owner, "state": "leased"}
        return None

    def finish_route(
        self,
        route: dict[str, Any],
        *,
        documents: int = 0,
        tokens: int = 0,
        outcome: str = "ok",
        error: str | None = None,
        ttl: float = 600,
        retry_seconds: float = 300,
        reset_at: float = 0.0,
        keep_free: bool = False,
        max_failures: int = 3,
        route_docs: int = 95,
        day: str | None = None,
    ) -> str:
        """Apply one lease result; returns the route's next state."""
        now = time.time()
        route_id, provider = route["route_id"], route["provider"]
        documents = max(0, int(documents))
        tokens = max(0, int(tokens))
        with self.database:
            self.database.execute("BEGIN IMMEDIATE")
            live = self.database.execute(
                "SELECT * FROM fleet_routes WHERE route_id=? AND owner=? AND state='leased'", (route_id, route["owner"])
            ).fetchone()
            if live is None:
                raise ValueError("Cannot finish a stale fleet lease")
            metered = self.database.execute(
                "SELECT COUNT(*) FROM fleet_document_claims WHERE owner=?", (route["owner"],)
            ).fetchone()[0]
            documents = max(0, documents - metered)
            day = day or current_day()
            if documents:
                self.database.execute(
                    "INSERT INTO fleet_budgets(provider,day,documents) VALUES(?,?,?) "
                    "ON CONFLICT(provider,day) DO UPDATE SET documents=documents+excluded.documents",
                    (provider, day, documents),
                )
            total = live["documents"] + documents
            if total >= route_docs and outcome not in PARK_OUTCOMES:
                outcome = "budget"
            self.database.execute("DELETE FROM fleet_document_claims WHERE owner=?", (route["owner"],))
            self.database.execute("DELETE FROM free_proxies WHERE owner=?", (route["owner"],))
            if outcome in PARK_OUTCOMES:
                if provider == "free" and not keep_free and outcome != "provider_budget":
                    self.database.execute("DELETE FROM fleet_routes WHERE route_id=?", (route_id,))
                    return "retired"
                self.database.execute(
                    "UPDATE fleet_routes SET state='parked',owner=NULL,parked_until=?,documents=?,tokens=tokens+?,"
                    "last_error=?,"
                    "proxy_url=CASE WHEN provider='brightdata' THEN NULL ELSE proxy_url END,"
                    "expected_ip=CASE WHEN provider='brightdata' THEN NULL ELSE expected_ip END,"
                    "exit_ip=CASE WHEN provider='brightdata' THEN NULL ELSE exit_ip END,next_at=0 WHERE route_id=?",
                    (reset_at, total, tokens, error, route_id),
                )
                return "parked"
            if outcome == "dead":
                if provider == "free":
                    if keep_free:
                        self.database.execute(
                            "UPDATE fleet_routes SET state='parked',owner=NULL,parked_until=?,documents=?,tokens=tokens+?,"
                            "last_error=?,next_at=0 WHERE route_id=?",
                            (reset_at, total, tokens, error, route_id),
                        )
                        return "parked"
                    self.database.execute("DELETE FROM fleet_routes WHERE route_id=?", (route_id,))
                    return "retired"
                failures = int(route["failures"]) + 1
                if failures >= max_failures:
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
                    "UPDATE fleet_routes SET state='cooldown',owner=NULL,next_at=?,documents=?,tokens=tokens+?,"
                    "last_error=? WHERE route_id=?",
                    (now + retry_seconds, total, tokens, error, route_id),
                )
                return "cooldown"
            if provider == "free":
                self.database.execute(
                    "UPDATE fleet_routes SET state='ready',owner=NULL,documents=?,tokens=tokens+?,"
                    "exit_ip=COALESCE(?,exit_ip),"
                    "last_success=?,verified_at=COALESCE(verified_at,?),expires_at=?,next_at=0,last_error=NULL "
                    "WHERE route_id=?",
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
        """Park every ready route of an over-budget paid provider until tomorrow."""
        with self.database:
            cursor = self.database.execute(
                "UPDATE fleet_routes SET state='parked',owner=NULL,parked_until=?,next_at=0 "
                "WHERE provider=? AND state IN ('ready','pending','cooldown')",
                (reset_at, provider),
            )
            return cursor.rowcount

    def wake_parked(self, now: float) -> int:
        """Reopen parked routes at reset: free ones re-probe, paid ones resume ready."""
        with self.database:
            rows = self.database.execute(
                "SELECT route_id FROM fleet_routes WHERE state='parked' AND parked_until>0 AND parked_until<=?", (now,)
            ).fetchall()
            woken = 0
            for row in rows:
                try:
                    self.database.execute(
                        "UPDATE fleet_routes SET state=CASE WHEN provider='free' THEN 'pending' ELSE 'ready' END,"
                        "parked_until=0,documents=0,failures=0,next_at=0 WHERE route_id=?",
                        (row[0],),
                    )
                except sqlite3.IntegrityError:
                    # Another live route still owns this exit; retry on maintenance.
                    continue
                woken += 1
            return woken

    def expire_free(self, now: float) -> int:
        """Return verified free routes with an expired TTL to the probe queue."""
        with self.database:
            cursor = self.database.execute(
                "UPDATE fleet_routes SET state='pending',owner=NULL,next_at=0 "
                "WHERE provider='free' AND state='ready' AND expires_at>0 AND expires_at<=?",
                (now,),
            )
            return cursor.rowcount

    def wake_cooldown(self, now: float) -> int:
        """Paid cooldowns promote themselves; free ones wait for a fresh probe."""
        with self.database:
            cursor = self.database.execute(
                "UPDATE fleet_routes SET state='ready',next_at=0 WHERE state='cooldown' AND provider<>'free' AND next_at<=?",
                (now,),
            )
            return cursor.rowcount

    def finish_probe(
        self,
        route: dict[str, Any],
        exit_ip: str | None = None,
        *,
        error: str | None = None,
        ttl: float = 600,
        retry_seconds: float = 300,
    ) -> bool:
        now = time.time()
        with self.database:
            if (
                not error
                and self.database.execute(
                    "SELECT 1 FROM fleet_routes WHERE exit_ip=? AND route_id<>? AND state IN ('ready','leased')",
                    (exit_ip, route["route_id"]),
                ).fetchone()
            ):
                error = "Duplicate proxy exit"
            if error:
                self.database.execute(
                    "UPDATE fleet_routes SET state='cooldown',owner=NULL,next_at=?,last_error=? WHERE route_id=?",
                    (now + retry_seconds, error, route["route_id"]),
                )
                return False
            self.database.execute(
                "UPDATE fleet_routes SET state='ready',owner=NULL,exit_ip=?,verified_at=?,expires_at=?,"
                "next_at=0,last_error=NULL WHERE route_id=? AND state='probing'",
                (exit_ip, now, now + ttl, route["route_id"]),
            )
            return True

    # ----------------------------------------------------------------- jobs

    def add_jobs(self, companies: list[dict[str, Any]], *, retry_errors: bool = False) -> None:
        with self.database:
            self.database.executemany(
                "INSERT OR IGNORE INTO free_jobs(run_id,company_id,payload,selection_order) VALUES(?,?,?,?)",
                (
                    (self.run_id, company["company_id"], json.dumps(company), index)
                    for index, company in enumerate(companies)
                ),
            )
            if retry_errors:
                self.database.execute(
                    "UPDATE free_jobs SET state='pending',attempts=0,next_at=0,error=NULL WHERE run_id=? AND state='error'",
                    (self.run_id,),
                )

    def reconcile_jobs(self, complete: set[str]) -> None:
        with self.database:
            self.database.executemany(
                "UPDATE free_jobs SET state='done',owner=NULL,error=NULL WHERE run_id=? AND company_id=?",
                ((self.run_id, identifier) for identifier in complete),
            )
            self.database.executemany(
                "UPDATE free_jobs SET state='pending',owner=NULL,next_at=0 WHERE run_id=? AND company_id=?",
                ((self.run_id, identifier) for identifier in self.completed_company_ids() - complete),
            )

    def claim_first_job(self, owner: str) -> dict[str, Any] | None:
        """Lease exactly one company; the child worker claims the rest itself."""
        with self.database:
            self.database.execute("BEGIN IMMEDIATE")
            if not self.database.execute("SELECT 1 FROM free_proxies WHERE owner=? AND state='leased'", (owner,)).fetchone():
                return None
            row = self.database.execute(
                "SELECT * FROM free_jobs WHERE run_id=? AND state='pending' AND next_at<=? "
                "ORDER BY attempts,next_at,selection_order,company_id LIMIT 1",
                (self.run_id, time.time()),
            ).fetchone()
            if not row:
                return None
            self.database.execute(
                "UPDATE free_jobs SET state='leased',owner=? WHERE run_id=? AND company_id=?",
                (owner, self.run_id, row["company_id"]),
            )
        return json.loads(row["payload"])

    def owned_companies(self, owner: str) -> list[dict[str, Any]]:
        return [
            json.loads(row[0])
            for row in self.database.execute(
                "SELECT payload FROM free_jobs WHERE run_id=? AND owner=? AND state='leased'", (self.run_id, owner)
            )
        ]

    def finish_job(
        self,
        company_id: str,
        owner: str,
        *,
        complete: bool,
        attempted: bool,
        limit: int,
        retry: float,
        error: str | None = None,
    ) -> None:
        with self.database:
            row = self.database.execute(
                "SELECT attempts FROM free_jobs WHERE run_id=? AND company_id=? AND owner=?",
                (self.run_id, company_id, owner),
            ).fetchone()
            if not row:
                return
            attempts = row["attempts"] + int(attempted)
            state = "done" if complete else "error" if attempts >= limit else "pending"
            self.database.execute(
                "UPDATE free_jobs SET state=?,attempts=?,next_at=?,owner=NULL,error=? "
                "WHERE run_id=? AND company_id=? AND owner=?",
                (
                    state,
                    attempts,
                    time.time() + (retry if attempted and not complete else 0),
                    error,
                    self.run_id,
                    company_id,
                    owner,
                ),
            )

    def completed_company_ids(self) -> set[str]:
        return {
            row[0]
            for row in self.database.execute(
                "SELECT company_id FROM free_jobs WHERE run_id=? AND state='done'", (self.run_id,)
            )
        }

    def has_unfinished_jobs(self) -> bool:
        return (
            self.database.execute(
                "SELECT 1 FROM free_jobs WHERE run_id=? AND state IN ('pending','leased') LIMIT 1", (self.run_id,)
            ).fetchone()
            is not None
        )

    def counts(self) -> dict[str, dict[str, int]]:
        return {
            "jobs": {
                row[0]: row[1]
                for row in self.database.execute(
                    "SELECT state,COUNT(*) FROM free_jobs WHERE run_id=? GROUP BY state", (self.run_id,)
                )
            },
            "routes": {
                row[0]: row[1] for row in self.database.execute("SELECT state,COUNT(*) FROM fleet_routes GROUP BY state")
            },
            # Leased routes are mirrored into free_proxies; keep the shared
            # StreamNotifications summary schema compatible with free-stream.
            "proxies": {
                row[0]: row[1] for row in self.database.execute("SELECT state,COUNT(*) FROM free_proxies GROUP BY state")
            },
            "budgets": {
                row[0]: row[1]
                for row in self.database.execute(
                    "SELECT provider,documents FROM fleet_budgets WHERE day=(SELECT day FROM fleet_clock WHERE id=1)"
                )
            },
        }


class FleetProducer:
    """Discovers and verifies public free candidates for the mixed fleet pool."""

    def __init__(self, args, root: Path, queue: FleetQueue, changed: asyncio.Event, stop: asyncio.Event):
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
        self.notifications: StreamNotifications | None = None
        self.next_catalog = 0.0
        self.next_probe = 0.0
        self.rate_lock = asyncio.Lock()

    async def refresh(self) -> None:
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
        ipv4_rows: list[tuple[str, str, str]] = []
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
            ipv4_rows.append((endpoint, source, str(address)))
        # Screen blocklists and threat feeds once, off the event loop, before probing.
        screened, rejected = await asyncio.to_thread(filter_candidate_ips, ipv4_rows)
        candidates = [
            (endpoint, source) for endpoint, source in screened if endpoint_allowed(endpoint, self.exceptions, self.ports)
        ]
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
            screened=len(screened),
            screened_rejected=rejected,
            port_eligible=len(candidates),
            selected_candidates=len(limited),
            queue_inserted=inserted,
        )
        self.stats["catalog_refreshes"] += 1
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
            headers={"User-Agent": pilot.USER_AGENT if hasattr(pilot, "USER_AGENT") else "Mozilla/5.0"},
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
            for url, marker in ((pilot.FORM_URL, b"ddlTipoBusqueda"), (pilot.RES_URL, b"txtCve")):
                if marker not in await get(url):
                    raise ValueError("RES form missing")
            after = str(ipaddress.ip_address(json.loads(await get("https://api.ipify.org?format=json", 65536))["ip"]))
            if after != exit_ip:
                raise ValueError("Proxy exit changed during preflight")
            return exit_ip

    async def scan(self) -> None:
        while not self.stop.is_set():
            if (
                self.args.budget_free
                and self.queue.provider_spent("free", current_day(self.args.reset_hour)) >= self.args.budget_free
            ):
                self.queue.park_provider("free", next_reset(self.args.reset_hour))
                with suppress(TimeoutError):
                    await asyncio.wait_for(self.stop.wait(), 0.5)
                continue
            candidate = self.queue.database.execute(
                "SELECT * FROM fleet_routes WHERE provider='free' AND state IN ('pending','cooldown') AND next_at<=? "
                "ORDER BY last_success DESC,next_at,route_id LIMIT 1",
                (time.time(),),
            ).fetchone()
            if not candidate:
                await asyncio.sleep(0.5)
                continue
            with self.queue.database:
                self.queue.database.execute("BEGIN IMMEDIATE")
                cursor = self.queue.database.execute(
                    "UPDATE fleet_routes SET state='probing' WHERE route_id=? AND state IN ('pending','cooldown')",
                    (candidate["route_id"],),
                )
                if cursor.rowcount != 1:
                    self.queue.database.execute("ROLLBACK")
                    continue
                self.queue.database.execute("COMMIT")
            row = dict(candidate)
            try:
                # One shared verification rate across all scanners, without holding a DB lock.
                async with self.rate_lock:
                    delay = max(0.0, self.next_probe - time.monotonic())
                    self.next_probe = time.monotonic() + delay + 60 / self.args.proxy_rate_per_minute
                if delay:
                    await asyncio.sleep(delay)
                exit_ip = await self.verify(row["endpoint"])
                if self.queue.finish_probe(row, exit_ip, ttl=self.args.proxy_ttl):
                    self.stats["ready"] += 1
                    self.stats["recent_ready"] = [*self.stats["recent_ready"][-4:], exit_ip]
                    if self.notifications:
                        self.notifications.activity(
                            "proxy_ready",
                            f"✅ RES fleet: бесплатный прокси допущен\nEndpoint: {row['endpoint']}; выходной IP: {exit_ip}",
                        )
                    self.changed.set()
                    if (
                        self.args.budget_free
                        and self.queue.provider_spent("free", current_day(self.args.reset_hour)) >= self.args.budget_free
                    ):
                        self.queue.park_provider("free", next_reset(self.args.reset_hour))
                else:
                    self.stats["denied_or_failed"] += 1
            except asyncio.CancelledError:
                self.queue.finish_probe(row, error="probe interrupted")
                raise
            except Exception as error:
                category = probe_error_category(error)
                self.stats["failure_reasons"][category] = self.stats["failure_reasons"].get(category, 0) + 1
                self.queue.finish_probe(
                    row, error=f"{category}: {safe_message(error)}", retry_seconds=self.args.proxy_retry_seconds
                )
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

    async def run(self) -> None:
        require_socks_support()
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
                await asyncio.sleep(0.5)
        finally:
            for scanner in scanners:
                scanner.cancel()
            await asyncio.gather(*scanners, return_exceptions=True)


class FleetBrowserWorker(BrowserWorker):
    """BrowserWorker adapted to fleet routes: leases carry provider credentials."""

    def __init__(self, args, root: Path, store: TrackingStore, queue: FleetQueue):
        super().__init__(args, root, store)
        self.queue = queue

    async def __call__(self, slot: int, route: dict[str, Any], companies: list[dict[str, Any]], owner: str) -> BatchResult:
        if route["provider"] == "brightdata" and not route["proxy_url"]:
            route = await self.mint_brightdata_session(route)
        directory = self.root / "workers" / f"w{slot:02d}"
        directory.mkdir(parents=True, exist_ok=True)
        self.store.restore_worker(directory, companies)
        benchmark.write_database(directory / "queue.sqlite3", companies)
        (directory / "proxy.txt").write_text((route["proxy_url"] or route["endpoint"]) + "\n")
        prior = {str(path) for path in directory.glob("report-*.json")}
        command = benchmark.child_command(self.args, directory, len(companies))

        def option(name: str, value: str) -> None:
            command[command.index(name) + 1] = value

        option("--proxy-provider", "residential")
        option("--proxy-limit", "1")
        option("--company-attempts", "1")
        option("--max-failures", "1")
        option("--error-cooldown-minutes", "0")
        option("--max-cooldowns", "0")
        option("--solves-per-proxy", "0")
        command.extend(
            [
                "--proxy-file",
                str(directory / "proxy.txt"),
                "--site-limit-scope",
                "proxy",
                "--stream-db",
                str(self.args.output / "fleet-runtime.sqlite3"),
                "--stream-owner",
                owner,
                "--stream-company-attempts",
                str(self.args.company_attempts),
                "--stream-network-retries",
                str(self.args.session_network_retries),
                "--fleet-route-docs",
                str(self.args.route_docs),
                "--fleet-provider-budget",
                str(getattr(self.args, f"budget_{route['provider']}")),
                "--fleet-reset-hour",
                str(self.args.reset_hour),
            ]
        )
        if route["expected_ip"]:
            command.extend(["--expected-proxy-ip", route["expected_ip"]])
        if route["ca_file"]:
            command.extend(["--proxy-ca-file", route["ca_file"]])
        return await self.run_child(slot, route, companies, owner, directory, prior, command)

    async def mint_brightdata_session(self, route: dict[str, Any]) -> dict[str, Any]:
        from scripts.res_audio_pilot import brightdata_proxy

        try:
            settings, expected, _ = await asyncio.to_thread(brightdata_proxy, self.args.output)
        except Exception as error:
            raise pilot.SearchIssue(
                "proxy_failed", f"Bright Data session mint failed ({safe_message(error) or type(error).__name__})"
            ) from None
        parts = urlsplit(settings["server"])
        url = (
            parts.scheme
            + "://"
            + quote(settings["username"], safe="")
            + ":"
            + quote(settings["password"], safe="")
            + "@"
            + parts.netloc
        )
        self.queue.set_route_session(route["route_id"], url, expected)
        return {**route, "proxy_url": url, "expected_ip": expected}

    async def run_child(self, slot, route, companies, owner, directory, prior, command) -> BatchResult:
        """Same lifecycle as the free-stream worker, with fleet route bookkeeping."""
        batch_started_at = time.time()
        log = directory / (owner + ".log")
        result = BatchResult()
        process = None
        self.active[slot] = {
            "slot": slot,
            "endpoint": route["endpoint"],
            "exit_ip": route["expected_ip"],
            "owner": owner,
            "directory": str(directory),
            "prior_reports": list(prior),
            "log": str(log),
            "ready": False,
        }
        try:
            with log.open("w") as output:
                process = await asyncio.create_subprocess_exec(
                    *command,
                    stdout=output,
                    stderr=asyncio.subprocess.STDOUT,
                    start_new_session=True,
                    pass_fds=(self.args.runtime_lock_fd,),
                )
                self.active[slot]["pid"] = process.pid
                if getattr(self, "notifications", None):
                    self.notifications.activity(
                        "browser_started",
                        f"🖥 RES fleet: запущен слот {slot + 1}\n"
                        f"Провайдер: {route['provider']}; прокси: {route['endpoint']}\n"
                        "Непрерывная сессия: компании по одной, без лимита числа; подготовка браузера",
                    )
                try:
                    waiter = asyncio.create_task(process.wait())
                    last_activity = time.monotonic()
                    seen_report_time = 0
                    try:
                        while not waiter.done():
                            report_time = max(
                                (
                                    path.stat().st_mtime_ns
                                    for path in directory.glob("report-*.json")
                                    if str(path) not in prior
                                ),
                                default=0,
                            )
                            if report_time > seen_report_time:
                                seen_report_time = report_time
                                last_activity = time.monotonic()
                            if time.monotonic() - last_activity >= self.args.worker_timeout:
                                raise TimeoutError
                            with suppress(TimeoutError):
                                await asyncio.wait_for(asyncio.shield(waiter), 1)
                    finally:
                        if not waiter.done():
                            waiter.cancel()
                        await asyncio.gather(waiter, return_exceptions=True)
                except TimeoutError:
                    result.error = "Browser worker inactivity timeout"
                except asyncio.CancelledError:
                    result.error = "Dispatcher stopping"
                finally:
                    await stop_child(process)
        except Exception as error:
            result.error = safe_message(error)
        finally:
            if process is not None and process.returncode is None:
                await stop_child(process)
            shutil.rmtree(directory / "profile", ignore_errors=True)
            self.active.pop(slot, None)
        state = pilot.load_state(directory / "state.json")
        new_reports = [benchmark.read_json(path, {}) for path in directory.glob("report-*.json") if str(path) not in prior]
        latest = {row["company_id"]: row for report in new_reports for row in report.get("companies", [])}
        result.attempted = {attempt["company_id"] for report in new_reports for attempt in report.get("attempts", [])}
        result.attempted.update(
            row[0]
            for row in self.store.db.execute(
                "SELECT company_id FROM attempts WHERE run_id=? AND worker=? AND started_at>=?",
                (self.args.run_id, directory.name, batch_started_at),
            )
        )
        with self.store.db:
            self.store.db.execute(
                "UPDATE attempts SET status='interrupted',finished_at=? WHERE run_id=? AND worker=? AND "
                "started_at>=? AND status='running'",
                (time.time(), self.args.run_id, directory.name, batch_started_at),
            )
        result.new_pdfs = sum(
            report.get("totals", {}).get("downloaded", report.get("progress", {}).get("downloaded", 0))
            for report in new_reports
        )
        result.completed.update(identifier for identifier, row in latest.items() if row.get("status") in TERMINAL)
        result.terminal_errors.update(
            identifier for identifier, row in latest.items() if row.get("status") == "ambiguous_match"
        )
        for company in companies:
            cached = pilot.cached_company_result(company, state, self.args, directory)
            row = cached or latest.get(company["company_id"], {})
            if row.get("status") in TERMINAL:
                result.completed.add(company["company_id"])
                self.store.company_status(company["company_id"], row["status"])
            elif row.get("status") == "ambiguous_match":
                result.terminal_errors.add(company["company_id"])
            if row.get("error"):
                result.error = safe_message(row["error"])
        result.quota_companies = {
            attempt["company_id"]
            for report in new_reports
            for attempt in report.get("attempts", [])
            if attempt["result"].get("status") in {"daily_limit", "fleet_route_limit", "fleet_provider_limit"}
        }
        result.quota_companies.update(
            row[0]
            for row in self.store.db.execute(
                "SELECT company_id FROM attempts WHERE run_id=? AND worker=? AND started_at>=? "
                "AND status IN ('daily_limit','fleet_route_limit','fleet_provider_limit')",
                (self.args.run_id, directory.name, batch_started_at),
            )
        )
        result.quota_limited = any(
            report.get("stop_reason", {}).get("code") == "daily_document_limit" for report in new_reports
        )
        result.fleet_limit = next(
            (
                report.get("stop_reason", {}).get("code")
                for report in new_reports
                if report.get("stop_reason", {}).get("code") in {"fleet_route_limit", "fleet_provider_limit"}
            ),
            None,
        )
        result.retry_connection = not result.attempted and any(
            proxy.get("status") == "proxy_failed"
            and not proxy.get("http_ip")
            and any(word in proxy.get("error", "") for word in ("ConnectTimeout", "ConnectError", "ReadTimeout"))
            and not any(word in proxy.get("error", "").lower() for word in ("ssl", "certificate", "reputation"))
            for report in new_reports
            for proxy in report.get("proxies", [])
        )
        explicit_reason = next(
            (
                report.get("stop_reason", {}).get("error")
                for report in reversed(new_reports)
                if report.get("stop_reason", {}).get("error")
            ),
            None,
        )
        if explicit_reason and any(report.get("stop_reason", {}).get("code") != "queue_drained" for report in new_reports):
            result.error = safe_message(explicit_reason)
        if not result.error and len(result.completed) < len(companies):
            result.error = next(
                (
                    safe_message(proxy["error"])
                    for report in new_reports
                    for proxy in report.get("proxies", [])
                    if proxy.get("error")
                ),
                "Сессия завершена; незавершённые компании вернутся в очередь",
            )
        if getattr(self, "notifications", None):
            self.notifications.activity(
                "browser_finished",
                f"🔁 RES fleet: сессия слота {slot + 1} завершена\n"
                f"Провайдер: {route['provider']}; компаний: {len(result.completed)}; новых PDF: {result.new_pdfs}\n"
                + (
                    "Причина: лимит RES; маршрут уйдёт до завтра"
                    if result.quota_limited
                    else f"Причина: {result.error}"
                    if result.error
                    else "Порция обработана; маршрут готов к следующей аренде"
                ),
            )
        return result


async def stop_child(process: asyncio.subprocess.Process) -> None:
    """A replacement is never started until the previous worker has exited."""
    if process is None:
        return
    if process.returncode is None:
        with suppress(ProcessLookupError):
            process.send_signal(signal.SIGINT)
        try:
            await asyncio.wait_for(process.wait(), 45)
        except TimeoutError:
            process.kill()
            await process.wait()
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    await asyncio.sleep(0.1)
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)


class FleetDispatcher:
    """Each consumer serially owns one browser slot and one fleet route."""

    def __init__(
        self,
        args,
        queue: FleetQueue,
        worker: FleetBrowserWorker,
        store: TrackingStore,
        changed: asyncio.Event,
        stop: asyncio.Event,
    ):
        if not 1 <= args.browsers <= 10:
            raise ValueError("Fleet dispatcher supports at most ten browsers")
        self.args, self.queue, self.worker, self.store, self.changed, self.stop = args, queue, worker, store, changed, stop
        self.provider_order = [provider.strip() for provider in args.providers.split(",") if provider.strip() in PROVIDERS]
        self.budgets = {
            "webshare": args.budget_webshare,
            "brightdata": args.budget_brightdata,
            "paid": args.budget_paid,
            "free": args.budget_free,
        }
        self.active: dict[int, str] = {}
        self.proxy_failures = 0
        self.total_new_pdfs = 0
        self.site_limits: list[dict[str, Any]] = []
        self.day = current_day(args.reset_hour)
        self.reset_at = next_reset(args.reset_hour)

    def rollover_day(self) -> None:
        self.day = current_day(self.args.reset_hour)
        self.reset_at = next_reset(self.args.reset_hour)
        self.queue.rollover(self.day)

    def route_outcome(self, result: BatchResult, provider: str | None = None) -> str:
        if getattr(result, "fleet_limit", None):
            return "provider_budget" if result.fleet_limit == "fleet_provider_limit" else "budget"
        if result.quota_limited:
            return "daily_limit"
        error = (result.error or "").lower()
        if provider == "free" and (
            result.retry_connection
            or any(
                word in error
                for word in ("connection", "connecterror", "connecttimeout", "readtimeout", "exit changed", "ip_mismatch")
            )
        ):
            return "dead"
        if result.retry_connection:
            return "retry"
        if error and any(word in error for word in ("auth", "reputation", "denied", "ip_blacklisted")):
            return "dead"
        if result.error:
            return "cooldown"
        return "ok"

    async def consume(self, slot: int) -> None:
        while not self.stop.is_set():
            control = self.store.cooldown_state()
            if control["halted"]:
                self.stop.set()
                return
            if control["active"]:
                with suppress(TimeoutError):
                    await asyncio.wait_for(self.stop.wait(), 0.5)
                continue
            self.rollover_day()
            owner = uuid.uuid4().hex
            route = self.queue.claim_route(owner, self.provider_order, self.budgets, self.day, self.args.route_docs)
            if not route:
                with suppress(TimeoutError):
                    await asyncio.wait_for(self.changed.wait(), 0.5)
                self.changed.clear()
                continue
            self.active[slot] = route["route_id"]
            job = self.queue.claim_first_job(owner)
            if not job:
                self.queue.finish_route(
                    route,
                    outcome="ok",
                    ttl=self.args.proxy_ttl,
                    reset_at=self.reset_at,
                    keep_free=self.args.free_keep,
                    max_failures=self.args.route_failures,
                    route_docs=self.args.route_docs,
                    day=self.day,
                )
                self.changed.set()
                self.active.pop(slot, None)
                await asyncio.sleep(0.5)
                continue
            result = BatchResult(error="Worker interrupted before processing")
            try:
                result = await self.worker(slot, route, [job], owner)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                result.error = safe_message(error)
            finally:
                outcome = self.route_outcome(result, route["provider"])
                self.rollover_day()
                state = self.queue.finish_route(
                    route,
                    documents=result.new_pdfs,
                    tokens=len(result.attempted),
                    outcome=outcome,
                    error=result.error,
                    ttl=self.args.proxy_ttl,
                    retry_seconds=self.args.proxy_retry_seconds,
                    reset_at=self.reset_at,
                    keep_free=self.args.free_keep,
                    max_failures=self.args.route_failures,
                    route_docs=self.args.route_docs,
                    day=self.day,
                )
                for company in self.queue.owned_companies(owner):
                    identifier = company["company_id"]
                    self.queue.finish_job(
                        identifier,
                        owner,
                        complete=identifier in result.completed,
                        attempted=identifier in result.attempted and identifier not in result.quota_companies,
                        limit=1 if identifier in result.terminal_errors else self.args.company_attempts,
                        retry=self.args.retry_seconds,
                        error=result.error,
                    )
                # finish_route turns an "ok" result into "budget" when the per-route
                # document cap is reached; mirror that here so the parking is recorded.
                limit_reached = outcome in PARK_OUTCOMES or (outcome == "ok" and state == "parked")
                if limit_reached:
                    self.site_limits.append(
                        {
                            "route": route["route_id"],
                            "provider": route["provider"],
                            "exit_ip": route["exit_ip"],
                            "parked_until": self.reset_at,
                            "state": state,
                            "retired": state == "retired",
                        }
                    )
                elif state == "retired":
                    self.site_limits.append(
                        {"route": route["route_id"], "provider": "free", "exit_ip": route["exit_ip"], "retired": True}
                    )
                if limit_reached and route["provider"] != "free" and self.worker.notifications:
                    self.worker.notifications.activity(
                        "site_limit",
                        f"🅿️ RES fleet: платный маршрут {route['endpoint']} выкачан до лимита\n"
                        f"Документов сегодня: {route['documents'] + result.new_pdfs}; пауза до "
                        f"{datetime.fromtimestamp(self.reset_at, UTC).strftime('%Y-%m-%d %H:%M')} UTC",
                    )
                self.active.pop(slot, None)
                self.changed.set()
            self.total_new_pdfs += result.new_pdfs
            budget = self.budgets.get(route["provider"], 0)
            if budget and self.queue.provider_spent(route["provider"], self.day) >= budget:
                parked = self.queue.park_provider(route["provider"], self.reset_at)
                if parked and self.worker.notifications:
                    self.worker.notifications.activity(
                        "site_limit", f"🅿️ RES fleet: бюджет {route['provider']} исчерпан; {parked} маршрут(ов) до завтра"
                    )
            self.proxy_failures = 0 if result.completed or outcome in PARK_OUTCOMES else self.proxy_failures + 1
            if self.proxy_failures >= self.args.max_failures and self.args.error_cooldown_minutes > 0:
                self.store.request_cooldown(self.args.error_cooldown_minutes * 60, self.args.max_cooldowns or None)
                self.proxy_failures = 0


async def seed_routes(args, queue: FleetQueue) -> dict[str, int]:
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
            try:
                expected = str(ipaddress.ip_address(parts.hostname))
            except ValueError:
                expected = None
            queue.add_route(
                "webshare",
                f"{parts.hostname}:{parts.port}",
                proxy_url=candidate.url,
                expected_ip=expected,
                label=f"webshare:{parts.hostname}:{parts.port}",
                source="webshare",
                state="ready",
            )
        seeded["webshare"] = len(candidates)
    if "brightdata" in args.provider_set:
        server = os.getenv("BRIGHTDATA_PROXY_SERVER", "http://brd.superproxy.io:44445")
        parts = urlsplit(server)
        if parts.scheme not in {"http", "https"} or not parts.hostname or not parts.port or parts.username or parts.password:
            raise ValueError("Bright Data server must be an HTTP(S) proxy URL with a hostname and port")
        if not os.getenv("BRIGHTDATA_PROXY_USERNAME", "").strip() or not os.getenv("BRIGHTDATA_PROXY_PASSWORD", "").strip():
            raise ValueError("Bright Data provider requires BRIGHTDATA_PROXY_USERNAME and BRIGHTDATA_PROXY_PASSWORD")
        sessions = args.brightdata_sessions or args.browsers
        with queue.database:
            queue.database.execute(
                "UPDATE fleet_routes SET route_id=?,state=CASE WHEN state='pending' THEN 'ready' ELSE state END "
                "WHERE route_id=? AND NOT EXISTS (SELECT 1 FROM fleet_routes WHERE route_id=?)",
                (f"brightdata:{parts.netloc}:slot-1", f"brightdata:{parts.netloc}", f"brightdata:{parts.netloc}:slot-1"),
            )
        for index in range(sessions):
            queue.add_route(
                "brightdata",
                parts.netloc,
                label=f"brightdata:{index + 1}",
                ca_file="docker/brightdata-proxy-ca.crt",
                source="brightdata",
                state="ready",
                route_id=f"brightdata:{parts.netloc}:slot-{index + 1}",
            )
        seeded["brightdata"] = sessions
    if "paid" in args.provider_set:
        paid = Path(args.paid_file)
        if not await asyncio.to_thread(paid.exists):
            raise ValueError(f"Paid provider file not found: {paid}")
        candidates = await asyncio.to_thread(load_paid_candidates, paid)
        for candidate in candidates:
            parts = urlsplit(candidate.url)
            if not parts.hostname or not parts.port:
                continue
            try:
                expected = str(ipaddress.ip_address(parts.hostname))
            except ValueError:
                expected = None
            queue.add_route(
                "paid",
                f"{parts.hostname}:{parts.port}",
                proxy_url=candidate.url,
                expected_ip=expected,
                label=f"paid:{parts.hostname}:{parts.port}",
                source="paid",
                state="ready",
            )
        seeded["paid"] = len(candidates)
    return seeded


def attempt_counts(store: TrackingStore, started_at: float) -> dict[str, int]:
    statuses = dict(
        store.db.execute(
            "SELECT status,COUNT(*) FROM attempts WHERE run_id=? AND started_at>=? GROUP BY status",
            (store.run_id, started_at),
        ).fetchall()
    )
    return {"attempts": sum(statuses.values()), "captcha_attempts": statuses.get("captcha_required", 0)}


def progress_message(snapshot: dict[str, Any]) -> str:
    counts = snapshot["counts"]
    routes = counts["routes"]
    budgets = counts["budgets"]
    budget_text = "; ".join(f"{provider}: {spent}" for provider, spent in budgets.items()) or "none"
    return (
        f"📊 RES fleet {snapshot['run_id']}\n"
        f"Браузеров: {snapshot['active_browsers']}/{snapshot['browser_limit']}; компаний завершено: "
        f"{counts['jobs'].get('done', 0)}/{sum(counts['jobs'].values())}\n"
        f"Новых PDF: {snapshot['new_pdfs']}; маршрутов: готово {routes.get('ready', 0)}, "
        f"в работе {routes.get('leased', 0)}, до завтра {routes.get('parked', 0)}\n"
        f"Расход документов сегодня: {budget_text}\n"
        f"Прошло: {snapshot['elapsed_seconds'] / 60:.1f} мин"
    )


async def execute(args, root: Path) -> int:
    store = TrackingStore(args.tracking_db or args.output / "tracking.sqlite3", args.run_id)
    queue = FleetQueue(args.output / "fleet-runtime.sqlite3", args.run_id)
    stop, changed = asyncio.Event(), asyncio.Event()
    producer = FleetProducer(args, root, queue, changed, stop) if "free" in args.provider_set else None
    worker = FleetBrowserWorker(args, root, store, queue)
    notifications = StreamNotifications(args, root)
    worker.notifications = notifications
    if producer is not None:
        producer.notifications = notifications
    dispatcher = FleetDispatcher(args, queue, worker, store, changed, stop)
    started = time.monotonic()
    invocation_started_at = time.time()
    children: list[asyncio.Task] = []
    interrupted = False
    summary: dict[str, Any] = {}
    fatal_error = None
    initial_pdf_count = 0

    def pdf_count() -> int:
        return store.db.execute(
            "SELECT COUNT(DISTINCT f.cve) FROM document_files f JOIN documents d USING(cve) JOIN "
            "company_documents cd USING(cve) JOIN run_companies rc USING(company_id) WHERE rc.run_id=?",
            (args.run_id,),
        ).fetchone()[0]

    await notifications.start()
    notifications.publish("preparing", f"🚀 RES fleet {args.run_id}: подготовка выборки и платных маршрутов")
    try:
        config = {"year": args.year, "month": args.month, "limit": args.limit}
        if args.archive_from_year:
            config["archive_from_year"] = args.archive_from_year
        selection = benchmark.read_json(root / "selection.json")
        if selection and any(selection["config"].get(key) != value for key, value in config.items()):
            raise ValueError("Selection settings changed; choose a new --run-id")
        if not selection:
            selection = (
                await asyncio.to_thread(select_archive, args.archive_from_year, args.limit)
                if args.archive_from_year
                else (await asyncio.to_thread(select_companies, args.year, args.month, args.limit))
            )
            selection["config"] = config
            atomic_write_json(root / "selection.json", selection)
        companies = selection["companies"]
        import_run(store, root, max_documents=args.max_documents, selection=selection)
        queue.recover()
        queue.add_jobs(companies, retry_errors=args.retry_errors)
        recovery = root / "recovery"
        known = {
            row[0]
            for row in store.db.execute(
                "SELECT c.company_id FROM run_companies rc JOIN companies c USING(company_id) "
                "WHERE rc.run_id=? AND c.search_status NOT IN ('pending','searching')",
                (args.run_id,),
            )
        }
        known_companies = [company for company in companies if company["company_id"] in known]
        store.restore_worker(recovery, known_companies)
        recovered = pilot.load_state(recovery / "state.json")
        complete = {
            company["company_id"]
            for company in known_companies
            if (cached := pilot.cached_company_result(company, recovered, args, recovery)) and cached["status"] in TERMINAL
        }
        queue.reconcile_jobs(complete)
        initial_pdf_count = pdf_count()
        store.prepare_cooldown(enabled=args.error_cooldown_minutes > 0)
        dispatcher.rollover_day()
        queue.wake_parked(time.time())
        seeded = await seed_routes(args, queue) if queue.has_unfinished_jobs() else {}
        counts = queue.counts()
        if counts["jobs"].get("pending", 0) and "free" in args.provider_set and not public_collection_allowed():
            raise ValueError("Fleet free provider requires ALLOW_PUBLIC_PROXY_COLLECTION=1")
        store.run_status("running")
        notifications.publish(
            "started",
            f"🚀 RES fleet {args.run_id} — запуск\n"
            f"Выборка: {selection.get('newest_period', f'{args.year}-{args.month:02d}')}, компаний: {len(companies)}\n"
            f"Провайдеры: {', '.join(dispatcher.provider_order)}; seeded: {seeded or 'нет'}\n"
            f"Браузеров: {args.browsers}; лимит документов на маршрут: {args.route_docs}; сброс квоты: "
            f"{args.reset_hour:02d}:00 UTC\n"
            f"Проверка репутации: {reputation_mode()}; сводка каждые {args.telegram_minutes:g} мин",
        )
        if counts["jobs"].get("pending", 0):
            children = [asyncio.create_task(producer.run())] if producer else []
            children += [asyncio.create_task(dispatcher.consume(slot)) for slot in range(args.browsers)]
            children.append(asyncio.create_task(wake_routes(queue, stop)))
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        next_checkpoint = 0.0
        last_work = time.monotonic()
        last_completed = counts["jobs"].get("done", 0)
        ready_notified: set[str] = set()
        site_limit_cursor = 0
        while children and not stop.is_set():
            if not dispatcher.active and not queue.has_unfinished_jobs():
                break
            for task in children:
                if task.done() and not task.cancelled() and (failure := task.exception()):
                    raise RuntimeError(f"Fleet background task failed: {safe_message(failure)}") from failure
            control = store.cooldown_state()
            if control["halted"]:
                break
            if dispatcher.active:
                last_work = time.monotonic()
            if args.idle_minutes and time.monotonic() - last_work > args.idle_minutes * 60:
                summary["stop_reason"] = "no_working_routes_timeout"
                break
            for active in list(worker.active.values()):
                current = await asyncio.to_thread(
                    lambda directory=active["directory"], prior=active["prior_reports"]: [
                        path for path in Path(directory).glob("report-*.json") if str(path) not in prior
                    ]
                )
                if current and active["slot"] in dispatcher.active:
                    report = benchmark.read_json(sorted(current)[-1], {})
                    active["ready"] = any(
                        proxy.get("http_ip") and proxy.get("http_ip") == proxy.get("browser_ip")
                        for proxy in report.get("proxies", [])
                    )
                    if active["ready"] and active["owner"] not in ready_notified:
                        ready_notified.add(active["owner"])
                        notifications.activity(
                            "browser_ready",
                            f"🌐 RES fleet: браузер слота {active['slot'] + 1} готов\n"
                            f"Выходной IP: {active['exit_ip']}; HTTP и браузер совпадают",
                        )
            if len(dispatcher.site_limits) > site_limit_cursor:
                for event in dispatcher.site_limits[site_limit_cursor:]:
                    notifications.activity(
                        "site_limit",
                        (
                            f"🗑 RES fleet: бесплатный маршрут {event['route']} исчерпан и удалён"
                            if event.get("retired")
                            else f"⏸ RES fleet: маршрут {event['route']} достиг лимита; до завтра "
                            f"{datetime.fromtimestamp(event['parked_until'], UTC).strftime('%H:%M')} UTC"
                        ),
                    )
                site_limit_cursor = len(dispatcher.site_limits)
            if time.monotonic() >= next_checkpoint:
                counts = queue.counts()
                if counts["jobs"].get("done", 0) > last_completed:
                    last_work = time.monotonic()
                    last_completed = counts["jobs"]["done"]
                total_pdfs = pdf_count()
                summary.update(
                    run_id=args.run_id,
                    state="cooldown" if store.cooldown_state()["active"] else "running",
                    elapsed_seconds=round(time.monotonic() - started, 2),
                    counts=counts,
                    active_browsers=len(dispatcher.active),
                    browser_limit=args.browsers,
                    ready_browsers=sum(bool(active.get("ready")) for active in worker.active.values()),
                    new_pdfs=max(0, total_pdfs - initial_pdf_count),
                    producer=producer.stats if producer else {"catalog_unique": 0, "checks": 0, "ready": 0},
                    site_limit_pauses=dispatcher.site_limits,
                    total_pdfs=total_pdfs,
                    companies_with_pdf=store.db.execute(
                        "SELECT COUNT(DISTINCT cd.company_id) FROM company_documents cd JOIN "
                        "run_companies rc USING(company_id) JOIN document_files f USING(cve) WHERE rc.run_id=?",
                        (args.run_id,),
                    ).fetchone()[0],
                )
                summary.update(attempt_counts(store, invocation_started_at))
                atomic_write_json(root / "progress.json", summary)
                print(
                    f"RES fleet: jobs={counts['jobs']}; routes={counts['routes']}; "
                    f"browsers={len(dispatcher.active)}/{args.browsers}",
                    flush=True,
                )
                next_checkpoint = time.monotonic() + args.checkpoint_seconds
            notifications.update(summary)
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), 0.5)
        interrupted = stop.is_set() and not store.cooldown_state()["halted"]
    except (asyncio.CancelledError, KeyboardInterrupt):
        interrupted = True
        raise
    except Exception as error:
        fatal_error = safe_message(error)
        raise
    finally:
        stop.set()
        for task in children:
            task.cancel()
        if children:
            await asyncio.gather(*children, return_exceptions=True)
        for signum in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_running_loop().remove_signal_handler(signum)
        counts = queue.counts()
        summary.update(
            run_id=args.run_id,
            counts=counts,
            active_browsers=0,
            browser_limit=args.browsers,
            elapsed_seconds=round(time.monotonic() - started, 2),
            new_pdfs=max(0, pdf_count() - initial_pdf_count),
            producer=producer.stats if producer else {},
            site_limit_pauses=dispatcher.site_limits,
        )
        summary.update(attempt_counts(store, invocation_started_at))
        summary["state"] = (
            "failed"
            if fatal_error
            else "interrupted"
            if interrupted
            else "finished"
            if not counts["jobs"].get("pending", 0) and not counts["jobs"].get("error", 0)
            else "partial"
        )
        summary["companies"] = store.rows()
        summary["tracking_database"] = str(store.path)
        store.run_status(summary["state"])
        summary["ready_browsers"] = 0
        summary["total_pdfs"] = pdf_count()
        summary["companies_with_pdf"] = len({row["company_id"] for row in store.rows() if row.get("pdfs_saved", 0)})
        await notifications.finish(
            notifications.format_summary(summary, "🏁 Итог RES fleet")
            + (f"\nПричина ошибки: {fatal_error}" if fatal_error else "")
            + f"\nОтчёт: {root / 'summary.json'}"
        )
        summary["telegram"] = dict(notifications.state)
        atomic_write_json(root / "summary.json", summary)
        atomic_write_json(root / "progress.json", {key: value for key, value in summary.items() if key != "companies"})
        queue.close()
        store.close()
    return 130 if interrupted else 0 if summary["state"] == "finished" else 2


async def wake_routes(queue: FleetQueue, stop: asyncio.Event) -> None:
    """Promote parked routes at quota reset, expire stale free probes and cool down paid routes."""
    while not stop.is_set():
        now = time.time()
        queue.wake_parked(now)
        queue.wake_cooldown(now)
        queue.expire_free(now)
        with suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), 10)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-from-year", type=int, default=0)
    parser.add_argument("--telegram-events", choices=("summary", "details"), default="summary")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--month", type=int, default=1)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--browsers", type=int, default=int(os.getenv("RES_BROWSERS", "5")))
    parser.add_argument("--session-network-retries", type=int, choices=(0, 1, 2), default=2)
    parser.add_argument(
        "--providers",
        default=os.getenv("RES_FLEET_PROVIDERS", "webshare,brightdata,free"),
        help=f"Comma subset of {', '.join(PROVIDERS)}; free needs public collection allowed",
    )
    parser.add_argument("--paid-file", default=os.getenv("PROXY_PAID_FILE", "data/paid-proxies.txt"))
    parser.add_argument(
        "--route-docs", type=int, default=int(os.getenv("RES_FLEET_ROUTE_DOCS", "95")), help="Docs per route per day"
    )
    parser.add_argument("--budget-webshare", type=int, default=int(os.getenv("RES_FLEET_BUDGET_WEBSHARE", "0")))
    parser.add_argument("--budget-brightdata", type=int, default=int(os.getenv("RES_FLEET_BUDGET_BRIGHTDATA", "0")))
    parser.add_argument("--budget-paid", type=int, default=int(os.getenv("RES_FLEET_BUDGET_PAID", "0")))
    parser.add_argument("--budget-free", type=int, default=int(os.getenv("RES_FLEET_BUDGET_FREE", "0")))
    parser.add_argument(
        "--reset-hour",
        type=int,
        default=int(os.getenv("RES_FLEET_RESET_HOUR", "0")),
        help="UTC hour of the daily quota reset",
    )
    parser.add_argument("--free-keep", action="store_true", default=os.getenv("RES_FLEET_FREE_KEEP", "") == "1")
    parser.add_argument("--brightdata-sessions", type=int, default=int(os.getenv("RES_FLEET_BRIGHTDATA_SESSIONS", "0")))
    parser.add_argument("--route-failures", type=int, default=int(os.getenv("RES_FLEET_ROUTE_FAILURES", "3")))
    parser.add_argument("--sources", default="all")
    parser.add_argument("--max-candidates", type=int, default=0)
    parser.add_argument("--source-records", type=int, default=0)
    parser.add_argument("--source-cache-mib", type=int, default=128)
    parser.add_argument("--catalog-seconds", type=float, default=600)
    parser.add_argument("--proxy-check-concurrency", type=int, default=12)
    parser.add_argument("--proxy-rate-per-minute", type=float, default=120)
    parser.add_argument("--proxy-timeout", type=float, default=15)
    parser.add_argument("--proxy-ttl", type=float, default=600)
    parser.add_argument("--connection-retries", type=int, default=2)
    parser.add_argument("--connection-retry-delay", type=float, default=30)
    parser.add_argument("--proxy-retry-seconds", type=float, default=300)
    parser.add_argument("--retry-seconds", type=float, default=30)
    parser.add_argument("--ready-capacity", type=int, default=20)
    parser.add_argument("--proxy-ports", default=os.getenv("RES_FREE_PROXY_PORTS", "80,443"))
    parser.add_argument("--allowed-proxy-endpoints", default=os.getenv("RES_FREE_ALLOWED_PROXY_ENDPOINTS", ""))
    parser.add_argument("--worker-timeout", type=float, default=1800)
    parser.add_argument("--idle-minutes", type=float, default=0)
    parser.add_argument("--tokens-per-proxy", type=int, default=20)
    parser.add_argument("--max-documents", type=int, default=0)
    parser.add_argument("--company-attempts", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=40)
    parser.add_argument("--captcha-timeout", type=float, default=130)
    parser.add_argument("--audio-model", default="base")
    parser.add_argument("--delay", type=float, default=3)
    parser.add_argument("--max-failures", type=int, default=20)
    parser.add_argument("--error-cooldown-minutes", type=float, default=10)
    parser.add_argument("--max-cooldowns", type=int, default=0)
    parser.add_argument("--site-limit-hours", type=float, default=24)
    parser.add_argument("--checkpoint-seconds", type=float, default=15)
    parser.add_argument("--telegram-minutes", type=float, default=30)
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--retry-errors", action="store_true", default=os.getenv("RES_RETRY_ERRORS", "0") == "1")
    parser.add_argument("--run-id", default="res-fleet-2025-01-100")
    parser.add_argument("--output", type=Path, default=Path("data/res-benchmark"))
    parser.add_argument("--tracking-db", type=Path)
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}", args.run_id):
        parser.error("Invalid run id")
    args.provider_set = {provider.strip() for provider in args.providers.split(",") if provider.strip()}
    unknown = args.provider_set - set(PROVIDERS)
    if unknown or not args.provider_set:
        parser.error(f"providers must be a subset of {', '.join(PROVIDERS)}")
    if not 1 <= args.browsers <= 10 or not 1 <= args.month <= 12:
        parser.error("browsers: 1..10; month: 1..12")
    if args.limit < 0 if args.archive_from_year else not 1 <= args.limit <= 10000:
        parser.error("limit out of range")
    if (
        not 0 <= args.reset_hour <= 23
        or not 1 <= args.route_docs <= 100
        or min(args.route_failures, args.connection_retries) < 0
        or args.route_failures < 1
        or args.brightdata_sessions < 0
        or args.max_candidates < 0
        or args.source_records < 0
        or args.max_documents < 0
        or args.max_cooldowns < 0
        or min(args.budget_webshare, args.budget_brightdata, args.budget_paid, args.budget_free) < 0
    ):
        parser.error("Invalid fleet limits or budgets")
    timing = (
        args.catalog_seconds,
        args.proxy_rate_per_minute,
        args.proxy_timeout,
        args.proxy_ttl,
        args.connection_retry_delay,
        args.site_limit_hours,
        args.proxy_retry_seconds,
        args.worker_timeout,
        args.timeout,
        args.captcha_timeout,
        args.checkpoint_seconds,
        args.telegram_minutes,
    )
    if any(not math.isfinite(value) or value <= 0 for value in timing) or any(
        not math.isfinite(value) or value < 0
        for value in (args.idle_minutes, args.retry_seconds, args.delay, args.error_cooldown_minutes)
    ):
        parser.error("Invalid timing bounds")
    resolve_sources(args.sources)
    proxy_ports(args.proxy_ports)
    endpoint_exceptions(args.allowed_proxy_endpoints)
    args.refresh = False
    args.proxy_provider = "residential"
    args.proxy_sessions = 1
    args.runtime_lock_fd = -1
    return args


def main(argv=None):
    from dotenv import load_dotenv

    for name in (".env.res", ".env.camoufox", ".env"):
        load_dotenv(name)
    args = parse_args(argv)
    args.output.mkdir(parents=True, exist_ok=True)
    root = args.output / args.run_id
    root.mkdir(exist_ok=True)
    with (args.output / ".fleet-dispatcher.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("A fleet dispatcher or its browser workers already hold the runtime lock", flush=True)
            return 1
        args.runtime_lock_fd = lock.fileno()
        try:
            return asyncio.run(execute(args, root))
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("RES fleet dispatcher interrupted; routes and progress saved", flush=True)
            return 130
        except Exception as error:
            print("RES fleet dispatcher stopped: " + safe_message(error), flush=True)
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
