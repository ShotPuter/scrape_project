"""Document admission shared by the dispatcher and its browser processes."""

from __future__ import annotations

import sqlite3
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

from src.sites.res.source import SearchIssue


def quota_day(reset_hour: int = 0, now: datetime | None = None) -> str:
    return ((now or datetime.now(UTC)) - timedelta(hours=reset_hour)).strftime("%Y-%m-%d")


def rollover(database: sqlite3.Connection, day: str) -> None:
    """Caller holds a write transaction, including when a child crosses midnight."""
    previous = database.execute("SELECT day FROM fleet_clock WHERE id=1").fetchone()
    if previous and previous[0] != day:
        database.execute("UPDATE fleet_routes SET documents=0,failures=0")
    database.execute("INSERT OR REPLACE INTO fleet_clock(id,day) VALUES(1,?)", (day,))


class FleetQuota:
    """Reserve before document I/O; concurrent workers cannot overspend a budget.

    A process killed during document I/O leaves its reservation charged until
    reset, since RES may already have generated the document. Known failures
    release their reservation. Local PDF reuse never enters this guard.
    """

    def __init__(self, database: sqlite3.Connection, owner: str, route_docs: int, budget: int, reset_hour: int):
        self.database, self.owner = database, owner
        self.route_docs, self.budget, self.reset_hour = route_docs, budget, reset_hour

    @contextmanager
    def document(self):
        day = quota_day(self.reset_hour)
        token = uuid.uuid4().hex
        with self.database:
            self.database.execute("BEGIN IMMEDIATE")
            rollover(self.database, day)
            route = self.database.execute(
                "SELECT route_id,provider,documents FROM fleet_routes WHERE owner=? AND state='leased'", (self.owner,)
            ).fetchone()
            if route is None:
                raise SearchIssue("proxy_exhausted", "Fleet route lease is no longer active")
            route_id, provider, documents = route
            spent = self.database.execute(
                "SELECT documents FROM fleet_budgets WHERE provider=? AND day=?", (provider, day)
            ).fetchone()
            if self.budget and spent and spent[0] >= self.budget:
                raise SearchIssue("fleet_provider_limit", "Fleet provider daily budget reached")
            if documents >= self.route_docs:
                raise SearchIssue("fleet_route_limit", "Fleet route daily document cap reached")
            self.database.execute("UPDATE fleet_routes SET documents=documents+1 WHERE route_id=?", (route_id,))
            self.database.execute(
                "INSERT INTO fleet_budgets(provider,day,documents) VALUES(?,?,1) "
                "ON CONFLICT(provider,day) DO UPDATE SET documents=documents+1",
                (provider, day),
            )
            self.database.execute(
                "INSERT INTO fleet_document_claims(token,owner,day) VALUES(?,?,?)", (token, self.owner, day)
            )
        try:
            yield
        except BaseException:
            with self.database:
                self.database.execute("BEGIN IMMEDIATE")
                clock = self.database.execute("SELECT day FROM fleet_clock WHERE id=1").fetchone()
                if clock and clock[0] == day:
                    self.database.execute(
                        "UPDATE fleet_routes SET documents=MAX(0,documents-1) WHERE route_id=?", (route_id,)
                    )
                self.database.execute(
                    "UPDATE fleet_budgets SET documents=MAX(0,documents-1) WHERE provider=? AND day=?", (provider, day)
                )
                self.database.execute("DELETE FROM fleet_document_claims WHERE token=?", (token,))
            raise
