"""Durable jobs and exclusive proxy leases for the bounded free-RES dispatcher."""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any


class FreeQueue:
    """One scheduler owns a flock; transactions keep state recoverable after exit."""

    def __init__(self, path: Path, run_id: str):
        self.run_id = run_id
        self.database = sqlite3.connect(path, timeout=30)
        self.database.row_factory = sqlite3.Row
        self.database.execute("PRAGMA journal_mode=WAL")
        self.database.executescript("""
            CREATE TABLE IF NOT EXISTS free_jobs (
                run_id TEXT,company_id TEXT,payload TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,next_at REAL NOT NULL DEFAULT 0,
                owner TEXT,error TEXT,PRIMARY KEY(run_id,company_id));
            CREATE TABLE IF NOT EXISTS free_proxies (
                endpoint TEXT PRIMARY KEY,source TEXT,state TEXT NOT NULL DEFAULT 'pending',
                exit_ip TEXT,verified_at REAL,expires_at REAL NOT NULL DEFAULT 0,
                next_at REAL NOT NULL DEFAULT 0,owner TEXT,error TEXT,seen_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS free_exit_cooldowns (
                exit_ip TEXT PRIMARY KEY,until_at REAL NOT NULL,reason TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS free_proxies_work ON free_proxies(state,next_at);
            CREATE UNIQUE INDEX IF NOT EXISTS free_proxy_unique_exit ON free_proxies(exit_ip)
                WHERE state IN ('ready','leased');
        """)

    def close(self) -> None:
        self.database.close()

    def recover(self) -> None:
        """Called only after acquiring the shared runtime lock, including child holders."""
        with self.database:
            self.database.execute(
                "UPDATE free_jobs SET state='pending',owner=NULL WHERE run_id=? AND state='leased'", (self.run_id,)
            )
            self.database.execute(
                "UPDATE free_proxies SET state='pending',owner=NULL,next_at=0 WHERE state IN ('leased','probing')"
            )

    def add_jobs(self, companies: Iterable[dict[str, Any]], *, retry_errors: bool = False) -> None:
        with self.database:
            self.database.executemany(
                "INSERT OR IGNORE INTO free_jobs(run_id,company_id,payload) VALUES(?,?,?)",
                ((self.run_id, company["company_id"], json.dumps(company)) for company in companies),
            )
            if retry_errors:
                self.database.execute(
                    "UPDATE free_jobs SET state='pending',attempts=0,next_at=0,error=NULL WHERE run_id=? AND state='error'",
                    (self.run_id,),
                )

    def mark_complete(self, company_id: str) -> None:
        with self.database:
            self.database.execute(
                "UPDATE free_jobs SET state='done',owner=NULL,error=NULL WHERE run_id=? AND company_id=?",
                (self.run_id, company_id),
            )

    def reset_job(self, company_id: str) -> None:
        with self.database:
            self.database.execute(
                "UPDATE free_jobs SET state='pending',owner=NULL,next_at=0 WHERE run_id=? AND company_id=?",
                (self.run_id, company_id),
            )

    def add_candidates(self, candidates: Iterable[tuple[str, str]], *, capacity: int) -> int:
        """Insert candidates; capacity=0 preserves all admitted endpoints."""
        if capacity < 0:
            raise ValueError("Candidate capacity must be nonnegative")
        now = time.time()
        count = self.database.execute("SELECT COUNT(*) FROM free_proxies").fetchone()[0]
        remaining = max(0, capacity - count)
        inserted = 0
        with self.database:
            for endpoint, source in candidates:
                existing = self.database.execute("SELECT 1 FROM free_proxies WHERE endpoint=?", (endpoint,)).fetchone()
                if existing:
                    self.database.execute(
                        "UPDATE free_proxies SET seen_at=?,source=? WHERE endpoint=?", (now, source, endpoint)
                    )
                elif capacity == 0 or inserted < remaining:
                    self.database.execute(
                        "INSERT INTO free_proxies(endpoint,source,seen_at) VALUES(?,?,?)", (endpoint, source, now)
                    )
                    inserted += 1
        return inserted

    def retain_candidates(self, endpoints: Iterable[str]) -> int:
        """Drop inactive candidates outside the newly screened shortlist; preserve live leases."""
        with self.database:
            self.database.execute("CREATE TEMP TABLE IF NOT EXISTS retained_candidates(endpoint TEXT PRIMARY KEY)")
            self.database.execute("DELETE FROM retained_candidates")
            self.database.executemany(
                "INSERT OR IGNORE INTO retained_candidates VALUES(?)", ((value,) for value in endpoints)
            )
            return self.database.execute(
                "DELETE FROM free_proxies WHERE state NOT IN ('leased','probing') AND NOT EXISTS "
                "(SELECT 1 FROM retained_candidates r WHERE r.endpoint=free_proxies.endpoint)"
            ).rowcount

    def claim_probe(self, *, fresh_for: float) -> dict[str, Any] | None:
        now = time.time()
        with self.database:
            self.database.execute("BEGIN IMMEDIATE")
            self.database.execute(
                "UPDATE free_proxies SET state='pending',next_at=0 WHERE state='ready' AND expires_at<=?", (now,)
            )
            row = self.database.execute(
                "SELECT * FROM free_proxies WHERE state IN ('pending','cooldown') "
                "AND next_at<=? AND seen_at>=? ORDER BY next_at,seen_at DESC LIMIT 1",
                (now, now - fresh_for),
            ).fetchone()
            if row:
                self.database.execute("UPDATE free_proxies SET state='probing' WHERE endpoint=?", (row["endpoint"],))
        return dict(row) if row else None

    def finish_probe(
        self, endpoint: str, *, exit_ip: str | None = None, ttl: float = 600, retry: float = 300, error: str | None = None
    ) -> bool:
        now = time.time()
        if exit_ip:
            limited = self.database.execute(
                "SELECT until_at FROM free_exit_cooldowns WHERE exit_ip=? AND until_at>?", (exit_ip, now)
            ).fetchone()
            if limited:
                with self.database:
                    self.database.execute(
                        "UPDATE free_proxies SET state='cooldown',exit_ip=?,next_at=?,error='site_limit_exit' WHERE "
                        "endpoint=?",
                        (exit_ip, limited["until_at"], endpoint),
                    )
                return False
        try:
            with self.database:
                self.database.execute(
                    "UPDATE free_proxies SET state=?,exit_ip=?,verified_at=?,expires_at=?,next_at=?,error=? WHERE "
                    "endpoint=?",
                    (
                        "ready" if exit_ip else "cooldown",
                        exit_ip,
                        now if exit_ip else None,
                        now + ttl if exit_ip else 0,
                        now + retry,
                        error,
                        endpoint,
                    ),
                )
            return bool(exit_ip)
        except sqlite3.IntegrityError:
            with self.database:
                self.database.execute(
                    "UPDATE free_proxies SET state='cooldown',exit_ip=NULL,next_at=?,error='duplicate_exit' WHERE "
                    "endpoint=?",
                    (now + retry, endpoint),
                )
            return False

    def claim_batch(self, owner: str, count: int) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
        now = time.time()
        with self.database:
            self.database.execute("BEGIN IMMEDIATE")
            self.database.execute(
                "UPDATE free_proxies SET state='pending',next_at=0 WHERE state='ready' AND expires_at<=?", (now,)
            )
            proxy = self.database.execute(
                "SELECT * FROM free_proxies WHERE state='ready' AND expires_at>? ORDER BY verified_at DESC LIMIT 1", (now,)
            ).fetchone()
            jobs = self.database.execute(
                "SELECT * FROM free_jobs WHERE run_id=? AND state='pending' AND next_at<=? ORDER BY "
                "attempts,next_at,company_id LIMIT ?",
                (self.run_id, now, count),
            ).fetchall()
            if not proxy or not jobs:
                return None
            self.database.execute(
                "UPDATE free_proxies SET state='leased',owner=? WHERE endpoint=?", (owner, proxy["endpoint"])
            )
            self.database.executemany(
                "UPDATE free_jobs SET state='leased',owner=? WHERE run_id=? AND company_id=?",
                ((owner, self.run_id, row["company_id"]) for row in jobs),
            )
        return {**dict(proxy), "owner": owner}, [json.loads(row["payload"]) for row in jobs]

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
                "UPDATE free_jobs SET state=?,attempts=?,next_at=?,owner=NULL,error=? WHERE run_id=? AND "
                "company_id=? AND owner=?",
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

    def release_proxy(self, endpoint: str, owner: str, retry: float, error: str | None = None) -> None:
        with self.database:
            self.database.execute(
                "UPDATE free_proxies SET state='cooldown',owner=NULL,next_at=?,error=? WHERE endpoint=? AND owner=?",
                (time.time() + retry, error, endpoint, owner),
            )

    def quarantine_proxy(self, endpoint: str, owner: str, seconds: float, error: str | None = None) -> None:
        """Persist a route/exit cooldown across catalog refreshes and restarts."""
        with self.database:
            row = self.database.execute(
                "SELECT exit_ip FROM free_proxies WHERE endpoint=? AND owner=?", (endpoint, owner)
            ).fetchone()
            if not row:
                return
            until_at = time.time() + seconds
            if row["exit_ip"]:
                self.database.execute(
                    "INSERT INTO free_exit_cooldowns VALUES (?,?,'site_document_limit') ON CONFLICT(exit_ip) DO "
                    "UPDATE SET until_at=MAX(until_at,excluded.until_at)",
                    (row["exit_ip"], until_at),
                )
                self.database.execute(
                    "UPDATE free_proxies SET "
                    "state='cooldown',owner=NULL,expires_at=0,next_at=MAX(next_at,?),error=? WHERE exit_ip=?",
                    (until_at, error, row["exit_ip"]),
                )
            else:
                self.database.execute(
                    "UPDATE free_proxies SET state='cooldown',owner=NULL,next_at=?,error=? WHERE endpoint=?",
                    (until_at, error, endpoint),
                )

    def counts(self) -> dict[str, dict[str, int]]:
        return {
            "jobs": {
                row[0]: row[1]
                for row in self.database.execute(
                    "SELECT state,COUNT(*) FROM free_jobs WHERE run_id=? GROUP BY state", (self.run_id,)
                )
            },
            "proxies": {
                row[0]: row[1] for row in self.database.execute("SELECT state,COUNT(*) FROM free_proxies GROUP BY state")
            },
        }

    def prune(self, *, unseen_for: float) -> None:
        with self.database:
            self.database.execute(
                "DELETE FROM free_proxies WHERE state IN ('pending','cooldown') AND seen_at<?", (time.time() - unseen_for,)
            )
