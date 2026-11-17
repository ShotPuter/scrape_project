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
            CREATE TABLE IF NOT EXISTS free_subnets (
                subnet TEXT PRIMARY KEY, timeout_streak INTEGER NOT NULL DEFAULT 0,
                cooldown_until REAL NOT NULL DEFAULT 0, pauses INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS free_connection_retries (
                endpoint TEXT PRIMARY KEY, attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL, created_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS free_exit_cooldowns (
                exit_ip TEXT PRIMARY KEY,until_at REAL NOT NULL,reason TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS free_proxies_work ON free_proxies(state,next_at);
            CREATE UNIQUE INDEX IF NOT EXISTS free_proxy_unique_exit ON free_proxies(exit_ip)
                WHERE state IN ('ready','leased');
        """)
        columns = {row[1] for row in self.database.execute("PRAGMA table_info(free_proxies)")}
        with self.database:
            for name, declaration in (
                ("subnet", "TEXT"),
                ("last_success", "REAL NOT NULL DEFAULT 0"),
                ("catalog_active", "INTEGER NOT NULL DEFAULT 1"),
            ):
                if name not in columns:
                    self.database.execute(f"ALTER TABLE free_proxies ADD COLUMN {name} {declaration}")
            self.database.execute("UPDATE free_proxies SET last_success=verified_at WHERE verified_at>last_success")
            self.database.execute("UPDATE free_proxies SET subnet=" + self.subnet_sql() + " WHERE subnet IS NULL")
            self.database.execute("CREATE INDEX IF NOT EXISTS free_proxies_subnet_active ON free_proxies(state,subnet)")
            self.database.execute(
                "CREATE INDEX IF NOT EXISTS free_proxies_priority ON "
                "free_proxies(state,last_success DESC,next_at,seen_at DESC)"
            )
            self.database.execute(
                "CREATE INDEX IF NOT EXISTS free_proxies_new_priority ON "
                "free_proxies(state,catalog_active,last_success,next_at,seen_at DESC)"
            )

    @staticmethod
    def subnet_sql() -> str:
        """Numeric IPv4 endpoints only; no custom SQLite function needed by other tools."""
        authority = "substr(endpoint,instr(endpoint,'://')+3)"
        host = f"substr({authority},1,instr({authority},':')-1)"
        return f"rtrim({host},'0123456789') || '0/24'"

    def close(self) -> None:
        self.database.close()

    def recover(self) -> None:
        """Called only after acquiring the shared runtime lock, including child holders."""
        with self.database:
            self.database.execute(
                "UPDATE free_jobs SET state='pending',owner=NULL WHERE run_id=? AND state='leased'", (self.run_id,)
            )
            self.database.execute(
                "UPDATE free_connection_retries SET attempts=MAX(0,attempts-1) WHERE endpoint IN "
                "(SELECT endpoint FROM free_proxies WHERE state='retry_probe')"
            )
            self.database.execute("UPDATE free_proxies SET state='retry_wait',owner=NULL WHERE state='retry_probe'")
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
                        "UPDATE free_proxies SET seen_at=?,source=?,catalog_active=1 WHERE endpoint=?",
                        (now, source, endpoint),
                    )
                elif capacity == 0 or inserted < remaining:
                    self.database.execute(
                        "INSERT INTO free_proxies(endpoint,source,seen_at) VALUES(?,?,?)", (endpoint, source, now)
                    )
                    self.database.execute(
                        "UPDATE free_proxies SET subnet=" + self.subnet_sql() + " WHERE endpoint=?", (endpoint,)
                    )
                    inserted += 1
        return inserted

    def retain_candidates(self, endpoints: Iterable[str]) -> int:
        """Deactivate missing candidates without deleting addresses, history or live leases."""
        with self.database:
            self.database.execute("CREATE TEMP TABLE IF NOT EXISTS retained_candidates(endpoint TEXT PRIMARY KEY)")
            self.database.execute("DELETE FROM retained_candidates")
            self.database.executemany(
                "INSERT OR IGNORE INTO retained_candidates VALUES(?)", ((value,) for value in endpoints)
            )
            return self.database.execute(
                "UPDATE free_proxies SET catalog_active=0 WHERE catalog_active=1 "
                "AND state NOT IN ('leased','probing','retry_wait','retry_probe') AND NOT EXISTS "
                "(SELECT 1 FROM retained_candidates r WHERE r.endpoint=free_proxies.endpoint)"
            ).rowcount

    def schedule_connection_retry(
        self, endpoint: str, owner: str, *, attempts: int, delay: float, error: str | None = None
    ) -> bool:
        """Queue a previously verified, exclusively leased route for bounded revalidation."""
        if attempts <= 0:
            return False
        with self.database:
            self.database.execute("BEGIN IMMEDIATE")
            row = self.database.execute(
                "SELECT * FROM free_proxies WHERE endpoint=? AND owner=? AND state='leased' AND verified_at IS NOT NULL",
                (endpoint, owner),
            ).fetchone()
            if row is None:
                return False
            prior = self.database.execute(
                "SELECT attempts,max_attempts FROM free_connection_retries WHERE endpoint=?", (endpoint,)
            ).fetchone()
            if prior and prior["attempts"] >= prior["max_attempts"]:
                return False
            self.database.execute(
                "INSERT OR IGNORE INTO free_connection_retries VALUES (?,0,?,?)", (endpoint, attempts, time.time())
            )
            self.database.execute(
                "UPDATE free_proxies SET state='retry_wait',owner=NULL,next_at=?,error=? WHERE endpoint=?",
                (time.time() + delay, error, endpoint),
            )
        return True

    def claim_probe(self, *, fresh_for: float) -> dict[str, Any] | None:
        now = time.time()
        with self.database:
            self.database.execute("BEGIN IMMEDIATE")
            self.database.execute(
                "UPDATE free_proxies SET state='pending',next_at=0 WHERE state='ready' AND expires_at<=?", (now,)
            )
            busy = [
                row[0]
                for row in self.database.execute(
                    "SELECT subnet FROM free_proxies WHERE state IN ('probing','retry_probe') "
                    "GROUP BY subnet HAVING COUNT(*)>=2"
                )
            ]
            subnet_gate = " AND NOT EXISTS (SELECT 1 FROM free_subnets n WHERE n.subnet=p.subnet AND n.cooldown_until>?)" + (
                " AND p.subnet NOT IN (" + ",".join("?" for _ in busy) + ")" if busy else ""
            )
            row = self.database.execute(
                "SELECT p.* FROM free_proxies p JOIN free_connection_retries r USING(endpoint) "
                "WHERE p.state='retry_wait' AND p.next_at<=? AND r.attempts<r.max_attempts "
                + subnet_gate
                + " ORDER BY p.next_at LIMIT 1",
                (now, now, *busy),
            ).fetchone()
            if row:
                self.database.execute("UPDATE free_proxies SET state='retry_probe' WHERE endpoint=?", (row["endpoint"],))
                self.database.execute(
                    "UPDATE free_connection_retries SET attempts=attempts+1 WHERE endpoint=?", (row["endpoint"],)
                )
                return {**dict(row), "connection_retry": True}
            row = None
            for known in (True, False):
                condition = "p.last_success>0" if known else "p.last_success=0 AND p.catalog_active=1 AND p.seen_at>=?"
                parameters = (now,) if known else (now, now - fresh_for)
                order = "p.last_success DESC,p.next_at,p.seen_at DESC" if known else "p.next_at,p.seen_at DESC"
                # Query each state separately so SQLite can stop at the first indexed
                # match instead of sorting the entire million-address pool for IN(...).
                choices = []
                for state in ("pending", "cooldown"):
                    match = self.database.execute(
                        "SELECT p.* FROM free_proxies p WHERE p.state=? "
                        "AND p.next_at<=? AND " + condition + subnet_gate + " ORDER BY " + order + " LIMIT 1",
                        (state, *parameters, now, *busy),
                    ).fetchone()
                    if match:
                        choices.append(match)
                if choices:
                    row = min(
                        choices,
                        key=lambda value: (-value["last_success"] if known else 0, value["next_at"], -value["seen_at"]),
                    )
                    break
            if row:
                self.database.execute("UPDATE free_proxies SET state='probing' WHERE endpoint=?", (row["endpoint"],))
        return dict(row) if row else None

    def finish_probe(
        self,
        endpoint: str,
        *,
        exit_ip: str | None = None,
        ttl: float = 600,
        retry: float = 300,
        error: str | None = None,
        retryable: bool = False,
        connection_retry_delay: float = 30,
    ) -> bool:
        now = time.time()
        if error != "probe interrupted":
            self.record_subnet_outcome(endpoint, success=bool(exit_ip), error=error)
        recovery = self.database.execute(
            "SELECT r.attempts,r.max_attempts FROM free_connection_retries r JOIN free_proxies p USING(endpoint) "
            "WHERE r.endpoint=? AND p.state='retry_probe'",
            (endpoint,),
        ).fetchone()
        if recovery and not exit_ip:
            interrupted = error == "probe interrupted"
            with self.database:
                if interrupted:
                    self.database.execute(
                        "UPDATE free_connection_retries SET attempts=MAX(0,attempts-1) WHERE endpoint=?", (endpoint,)
                    )
                if interrupted or (retryable and recovery["attempts"] < recovery["max_attempts"]):
                    self.database.execute(
                        "UPDATE free_proxies SET state='retry_wait',next_at=?,error=? WHERE endpoint=?",
                        (now + connection_retry_delay, error, endpoint),
                    )
                    return False
                self.database.execute("DELETE FROM free_connection_retries WHERE endpoint=?", (endpoint,))
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
                    "UPDATE free_proxies SET state=?,exit_ip=?,verified_at=?,expires_at=?,next_at=?,error=?,"
                    "last_success=CASE WHEN ? IS NOT NULL THEN ? ELSE last_success END WHERE "
                    "endpoint=?",
                    (
                        "ready" if exit_ip else "cooldown",
                        exit_ip,
                        now if exit_ip else None,
                        now + ttl if exit_ip else 0,
                        now + retry,
                        error,
                        exit_ip,
                        now,
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

    def record_subnet_outcome(self, endpoint: str, *, success: bool, error: str | None) -> None:
        """Five consecutive timeout completions pause a /24 for ten minutes; no deletions."""
        row = self.database.execute("SELECT subnet FROM free_proxies WHERE endpoint=?", (endpoint,)).fetchone()
        if not row:
            return
        subnet = row[0]
        timeout = bool(error and (error == "timeout" or error.startswith("timeout:")))
        if not success and (not error or error.startswith("reputation_denied:")):
            return
        now = time.time()
        with self.database:
            self.database.execute("INSERT OR IGNORE INTO free_subnets(subnet) VALUES(?)", (subnet,))
            self.database.execute(
                "UPDATE free_subnets SET timeout_streak=CASE WHEN ? THEN "
                "CASE WHEN cooldown_until>0 AND cooldown_until<=? THEN 1 ELSE timeout_streak+1 END ELSE 0 END "
                ",cooldown_until=CASE WHEN cooldown_until<=? THEN 0 ELSE cooldown_until END WHERE subnet=?",
                (int(timeout), now, now, subnet),
            )
            self.database.execute(
                "UPDATE free_subnets SET cooldown_until=?,pauses=pauses+1 "
                "WHERE subnet=? AND timeout_streak>=5 AND cooldown_until<=?",
                (now + 600, subnet, now),
            )

    def subnet_status(self) -> dict[str, int]:
        row = self.database.execute(
            "SELECT COALESCE(SUM(cooldown_until>?),0),COALESCE(SUM(pauses),0) FROM free_subnets", (time.time(),)
        ).fetchone()
        return {"paused": row[0], "pause_events": row[1]}

    def claim_batch(self, owner: str, count: int) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
        now = time.time()
        with self.database:
            self.database.execute("BEGIN IMMEDIATE")
            self.database.execute(
                "UPDATE free_proxies SET state='pending',next_at=0 WHERE state='ready' AND expires_at<=?", (now,)
            )
            proxy = self.database.execute(
                "SELECT p.* FROM free_proxies p WHERE p.state='ready' AND p.expires_at>? "
                "AND NOT EXISTS (SELECT 1 FROM free_subnets n WHERE n.subnet=p.subnet AND n.cooldown_until>?) "
                "ORDER BY p.verified_at DESC LIMIT 1",
                (now, now),
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
                "DELETE FROM free_connection_retries WHERE endpoint=? AND EXISTS "
                "(SELECT 1 FROM free_proxies WHERE endpoint=? AND owner=?)",
                (endpoint, endpoint, owner),
            )
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
                "UPDATE free_proxies SET catalog_active=0 WHERE state IN ('pending','cooldown') AND seen_at<?",
                (time.time() - unseen_for,),
            )
