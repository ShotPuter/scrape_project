"""Durable day barriers and CVE leases, separate from the legacy company queue."""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import timedelta
from pathlib import Path

from src.sites.dequienes.source import validate_cve
from src.sites.res.bulk import santiago_today
from src.sites.res.fleet import FleetQueue


class DailyQueue(FleetQueue):
    def __init__(self, path: Path, run_id: str, *, initialize: bool = True):
        if not initialize:
            self.path, self.run_id = path, run_id
            self.database = sqlite3.connect(path, timeout=30)
            self.database.row_factory = sqlite3.Row
            return
        super().__init__(path, run_id)
        self.database.executescript("""
            CREATE TABLE IF NOT EXISTS fleet_days (
                run_id TEXT,day TEXT,kind TEXT,state TEXT NOT NULL DEFAULT 'discovering',
                PRIMARY KEY(run_id,day,kind));
            CREATE TABLE IF NOT EXISTS fleet_day_jobs (
                run_id TEXT,company_id TEXT,payload TEXT NOT NULL,day TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,
                next_at REAL NOT NULL DEFAULT 0,owner TEXT,error TEXT,
                PRIMARY KEY(run_id,company_id));
            CREATE INDEX IF NOT EXISTS fleet_day_jobs_dispatch ON fleet_day_jobs(run_id,state,next_at,company_id);
            CREATE INDEX IF NOT EXISTS fleet_day_jobs_day ON fleet_day_jobs(run_id,day,state);
            CREATE INDEX IF NOT EXISTS fleet_day_jobs_owner ON fleet_day_jobs(run_id,owner,state);
            CREATE TABLE IF NOT EXISTS fleet_daily_control (
                run_id TEXT PRIMARY KEY,archive_current INTEGER NOT NULL DEFAULT 0);
        """)
        with self.database:
            self.database.execute("INSERT OR IGNORE INTO fleet_daily_control(run_id) VALUES(?)", (run_id,))

    def recover(self):
        super().recover()
        with self.database:
            self.database.execute(
                "UPDATE fleet_day_jobs SET state='pending',owner=NULL WHERE run_id=? AND state IN ('leased','recovering')",
                (self.run_id,),
            )

    def schedule_day(self, start, end, kind):
        """Keep a single day open; never advance past leased, retrying or missing CVEs."""
        current = self.database.execute(
            "SELECT day FROM fleet_days WHERE run_id=? AND kind=? AND state<>'done' ORDER BY day DESC LIMIT 1",
            (self.run_id, kind),
        ).fetchone()
        if current:
            unfinished = self.database.execute(
                "SELECT 1 FROM fleet_day_jobs WHERE run_id=? AND day=? AND state<>'done' LIMIT 1", (self.run_id, current[0])
            ).fetchone()
            if unfinished:
                return current[0]
            with self.database:
                self.database.execute(
                    "UPDATE fleet_days SET state='done' WHERE run_id=? AND day=? AND kind=?", (self.run_id, current[0], kind)
                )
        completed = {
            r[0]
            for r in self.database.execute(
                "SELECT day FROM fleet_days WHERE run_id=? AND kind=? AND state='done'", (self.run_id, kind)
            )
        }
        day = min(end or santiago_today() - timedelta(days=1), santiago_today() - timedelta(days=1))
        while day >= start and day.isoformat() in completed:
            day -= timedelta(days=1)
        if day < start:
            with self.database:
                self.database.execute("UPDATE fleet_daily_control SET archive_current=1 WHERE run_id=?", (self.run_id,))
            return None
        key = day.isoformat()
        task_id = f"date:{kind}:{key}"
        payload = {
            "company_id": task_id,
            "company_name": f"RES {key}",
            "rut": "",
            "task": "discover",
            "day": key,
            "kind": kind,
        }
        with self.database:
            self.database.execute(
                "INSERT OR IGNORE INTO fleet_days(run_id,day,kind) VALUES(?,?,?)", (self.run_id, key, kind)
            )
            self.database.execute(
                "INSERT OR IGNORE INTO fleet_day_jobs(run_id,company_id,payload,day) VALUES(?,?,?,?)",
                (self.run_id, task_id, json.dumps(payload), key),
            )
            self.database.execute("UPDATE fleet_daily_control SET archive_current=0 WHERE run_id=?", (self.run_id,))
        return key

    def expand_day(self, day, kind, actions, store):
        rows = []
        seen = set()
        for action in actions:
            if not action.get("cve"):
                continue
            cve = validate_cve(action["cve"])
            if cve in seen:
                continue
            seen.add(cve)
            company = store.db.execute("SELECT company_id FROM companies WHERE rut=?", (action["rut"],)).fetchone()
            task_id = f"pdf:{kind}:{day}:{cve}"
            payload = {
                "company_id": task_id,
                "company_name": action["company_name"],
                "rut": action["rut"],
                "subject_company_id": company[0],
                "task": "document",
                "day": day,
                "kind": kind,
                "cve": cve,
            }
            # Validate bytes, not just a downloaded flag: corrupt/missing files remain queued.
            state = "done" if store.existing_file(cve) else "pending"
            rows.append((self.run_id, task_id, json.dumps(payload), day, state))
        for offset in range(0, len(rows), 128):
            with self.database:
                self.database.executemany(
                    "INSERT INTO fleet_day_jobs(run_id,company_id,payload,day,state) VALUES(?,?,?,?,?) "
                    "ON CONFLICT(run_id,company_id) DO UPDATE SET state='done',owner=NULL,error=NULL "
                    "WHERE excluded.state='done' AND fleet_day_jobs.state='pending'",
                    rows[offset : offset + 128],
                )
        with self.database:
            self.database.execute(
                "UPDATE fleet_days SET state='draining' WHERE run_id=? AND day=? AND kind=?", (self.run_id, day, kind)
            )

    def archive_current(self):
        row = self.database.execute(
            "SELECT archive_current FROM fleet_daily_control WHERE run_id=?", (self.run_id,)
        ).fetchone()
        return bool(row and row[0])

    def has_available_job(self):
        return (
            self.database.execute(
                "SELECT 1 FROM fleet_day_jobs WHERE run_id=? AND state='pending' AND next_at<=? "
                "AND day=(SELECT MAX(day) FROM fleet_days WHERE run_id=? AND state<>'done') LIMIT 1",
                (self.run_id, time.time(), self.run_id),
            ).fetchone()
            is not None
        )

    def claim_first_job(self, owner):
        with self.database:
            self.database.execute("BEGIN IMMEDIATE")
            if not self.database.execute("SELECT 1 FROM free_proxies WHERE owner=? AND state='leased'", (owner,)).fetchone():
                return None
            row = self.database.execute(
                "SELECT payload FROM fleet_day_jobs WHERE run_id=? AND owner=? AND state='leased' LIMIT 1",
                (self.run_id, owner),
            ).fetchone()
            if row:
                return json.loads(row[0])
            row = self.database.execute(
                "SELECT company_id,payload FROM fleet_day_jobs WHERE run_id=? AND state='pending' AND next_at<=? "
                "AND day=(SELECT MAX(day) FROM fleet_days WHERE run_id=? AND state<>'done') "
                "ORDER BY next_at,company_id LIMIT 1",
                (self.run_id, time.time(), self.run_id),
            ).fetchone()
            if row:
                self.database.execute(
                    "UPDATE fleet_day_jobs SET state='leased',owner=? WHERE run_id=? AND company_id=?",
                    (owner, self.run_id, row[0]),
                )
                return json.loads(row[1])
        return None

    def owned_companies(self, owner):
        return [
            json.loads(r[0])
            for r in self.database.execute(
                "SELECT payload FROM fleet_day_jobs WHERE run_id=? AND owner=? AND state='leased'", (self.run_id, owner)
            )
        ]

    def finish_job(self, company_id, owner, *, complete, attempted, limit, retry, error=None):
        # A failed PDF must not silently complete its day after an attempt budget.
        with self.database:
            self.database.execute(
                "UPDATE fleet_day_jobs SET state=?,owner=NULL,attempts=attempts+?,next_at=?,error=? "
                "WHERE run_id=? AND company_id=? AND owner=? AND state='leased'",
                (
                    "done" if complete else "pending",
                    int(attempted),
                    time.time() + (retry if not complete else 0),
                    error,
                    self.run_id,
                    company_id,
                    owner,
                ),
            )

    def has_unfinished_jobs(self):
        return (
            self.database.execute(
                "SELECT 1 FROM fleet_day_jobs WHERE run_id=? AND state IN ('pending','leased','recovering') LIMIT 1",
                (self.run_id,),
            ).fetchone()
            is not None
        )

    def counts(self):
        # Avoid reading the million-row legacy company queue for daily progress.
        return {
            "jobs": dict(
                self.database.execute(
                    "SELECT state,COUNT(*) FROM fleet_day_jobs WHERE run_id=? GROUP BY state", (self.run_id,)
                )
            ),
            "routes": dict(self.database.execute("SELECT state,COUNT(*) FROM fleet_routes GROUP BY state")),
            "proxies": dict(self.database.execute("SELECT state,COUNT(*) FROM free_proxies GROUP BY state")),
            "budgets": dict(
                self.database.execute(
                    "SELECT provider,documents FROM fleet_budgets WHERE day=(SELECT day FROM fleet_clock WHERE id=1)"
                )
            ),
        }

    def days_done(self):
        return self.database.execute(
            "SELECT COUNT(*) FROM fleet_days WHERE run_id=? AND state='done'", (self.run_id,)
        ).fetchone()[0]
