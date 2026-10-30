"""SQLite persistence for proxies verified by the standalone collector."""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import aiosqlite

from src.utils.helpers import StorageError

logger = logging.getLogger(__name__)

SQLITE_BUSY_TIMEOUT_MS = 5000

CREATE_VERIFIED_PROXIES = """
CREATE TABLE IF NOT EXISTS verified_proxies (
    proxy_url TEXT NOT NULL,
    user_agent TEXT NOT NULL,
    transport TEXT NOT NULL,
    latency_ms REAL NOT NULL DEFAULT 0,
    checked_at TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    protocol TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT '',
    country TEXT,
    city TEXT,
    anonymity TEXT,
    uptime_percent REAL,
    source_latency_ms REAL,
    last_checked TEXT,
    https_supported INTEGER,
    successful_requests INTEGER NOT NULL DEFAULT 0,
    failed_requests INTEGER NOT NULL DEFAULT 0,
    success_rate REAL NOT NULL DEFAULT 0,
    average_latency_ms REAL NOT NULL DEFAULT 0,
    last_success TEXT,
    last_failure TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    error_types TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (proxy_url, user_agent, transport)
);
"""

CREATE_VERIFIED_PROXIES_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_verified_proxies_active ON verified_proxies(is_active, checked_at)"
)

# Columns added after the first collector version.  Existing databases are
# upgraded in place; SQLite cannot add multiple columns in one ALTER TABLE.
EXTRA_COLUMNS = (
    ("protocol", "TEXT NOT NULL DEFAULT ''"),
    ("source", "TEXT NOT NULL DEFAULT ''"),
    ("country", "TEXT"),
    ("city", "TEXT"),
    ("anonymity", "TEXT"),
    ("uptime_percent", "REAL"),
    ("source_latency_ms", "REAL"),
    ("last_checked", "TEXT"),
    ("https_supported", "INTEGER"),
    ("successful_requests", "INTEGER NOT NULL DEFAULT 0"),
    ("failed_requests", "INTEGER NOT NULL DEFAULT 0"),
    ("success_rate", "REAL NOT NULL DEFAULT 0"),
    ("average_latency_ms", "REAL NOT NULL DEFAULT 0"),
    ("last_success", "TEXT"),
    ("last_failure", "TEXT"),
    ("consecutive_failures", "INTEGER NOT NULL DEFAULT 0"),
    ("error_types", "TEXT NOT NULL DEFAULT '{}'"),
)

UPSERT_VERIFIED_PROXY = """
INSERT INTO verified_proxies (
    proxy_url, user_agent, transport, latency_ms, checked_at, is_active,
    protocol, source, country, city, anonymity, uptime_percent, source_latency_ms,
    last_checked, https_supported, successful_requests, failed_requests, success_rate,
    average_latency_ms, last_success, last_failure, consecutive_failures, error_types
) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(proxy_url, user_agent, transport) DO UPDATE SET
    latency_ms = excluded.latency_ms,
    checked_at = excluded.checked_at,
    is_active = 1,
    protocol = excluded.protocol,
    source = excluded.source,
    country = excluded.country,
    city = excluded.city,
    anonymity = excluded.anonymity,
    uptime_percent = excluded.uptime_percent,
    source_latency_ms = excluded.source_latency_ms,
    last_checked = excluded.last_checked,
    https_supported = excluded.https_supported,
    successful_requests = excluded.successful_requests,
    failed_requests = excluded.failed_requests,
    success_rate = excluded.success_rate,
    average_latency_ms = excluded.average_latency_ms,
    last_success = excluded.last_success,
    last_failure = excluded.last_failure,
    consecutive_failures = excluded.consecutive_failures,
    error_types = excluded.error_types
"""


@dataclass(frozen=True, slots=True)
class VerifiedProxy:
    url: str
    user_agent: str
    transport: str
    latency_ms: float = 0.0
    checked_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    protocol: str = ""
    source: str = ""
    country: str | None = None
    city: str | None = None
    anonymity: str | None = None
    uptime_percent: float | None = None
    source_latency_ms: float | None = None
    last_checked: str | None = None
    https_supported: bool | None = None
    successful_requests: int = 0
    failed_requests: int = 0
    success_rate: float = 0.0
    average_latency_ms: float = 0.0
    last_success: str | None = None
    last_failure: str | None = None
    consecutive_failures: int = 0
    error_types: dict[str, int] = field(default_factory=dict)


class ProxyDatabase:
    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: aiosqlite.Connection | None = None

    async def __aenter__(self) -> ProxyDatabase:
        try:
            await self.connect()
            await self.init_db()
        except BaseException:
            await self.close()
            raise
        return self

    async def __aexit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        await self.close()

    async def connect(self) -> None:
        if self._conn is not None:
            return
        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is None:
            return
        await self._conn.close()
        self._conn = None

    async def init_db(self) -> None:
        conn = await self._ensure_connection()
        try:
            await conn.execute(CREATE_VERIFIED_PROXIES)
            await self._ensure_columns(conn)
            await conn.execute(CREATE_VERIFIED_PROXIES_INDEX)
            await conn.commit()
        except (aiosqlite.Error, OSError) as exc:
            logger.error("Failed to initialize proxy database: %s", exc)
            raise StorageError(f"Proxy database initialization error: {exc}") from exc

    async def _ensure_columns(self, conn: aiosqlite.Connection) -> None:
        async with conn.execute("PRAGMA table_info(verified_proxies)") as cursor:
            rows = await cursor.fetchall()
        existing = {row[1] for row in rows}
        for name, declaration in EXTRA_COLUMNS:
            if name not in existing:
                await conn.execute(f"ALTER TABLE verified_proxies ADD COLUMN {name} {declaration}")

    async def upsert_many(self, proxies: Sequence[VerifiedProxy]) -> None:
        """Publish a verified batch without deleting other available proxies."""
        if not proxies:
            return
        conn = await self._ensure_connection()
        try:
            await conn.executemany(UPSERT_VERIFIED_PROXY, [_proxy_values(proxy) for proxy in proxies])
            await conn.commit()
        except (aiosqlite.Error, OSError) as exc:
            await conn.rollback()
            raise StorageError(f"Proxy database publication error: {exc}") from exc

    async def replace_all(self, proxies: Sequence[VerifiedProxy]) -> None:
        """Replace the database contents with freshly verified proxies.

        Old rows are physically deleted inside the same transaction, so the
        database contains only the latest successful scan.  The file is
        compacted afterwards to remove deleted pages.  Passing an empty
        sequence clears the table; the collector avoids calling this method
        when a scan produced no verified proxies.
        """
        conn = await self._ensure_connection()
        try:
            await conn.execute("DELETE FROM verified_proxies")
            await conn.executemany(UPSERT_VERIFIED_PROXY, [_proxy_values(proxy) for proxy in proxies])
            await conn.commit()
            logger.info("Replaced proxy database with %d verified proxy/transport combinations", len(proxies))
            await _compact_database(conn)
        except (aiosqlite.Error, OSError) as exc:
            await conn.rollback()
            raise StorageError(f"Proxy database save error: {exc}") from exc

    async def list_active(self, limit: int | None = None, max_age_hours: float | None = None) -> list[VerifiedProxy]:
        conn = await self._ensure_connection()
        query = """
        SELECT proxy_url, user_agent, transport, latency_ms, checked_at,
               protocol, source, country, city, anonymity, uptime_percent,
               source_latency_ms, last_checked, https_supported,
               successful_requests, failed_requests, success_rate,
               average_latency_ms, last_success, last_failure,
               consecutive_failures, error_types
        FROM verified_proxies
        WHERE is_active = 1
        """
        params: list[object] = []
        if max_age_hours is not None and max_age_hours > 0:
            cutoff = datetime.now(UTC) - timedelta(hours=max_age_hours)
            query += " AND checked_at >= ?"
            params.append(cutoff.isoformat())
        query += (
            " ORDER BY CASE WHEN source = 'webshare' THEN 0 WHEN source = 'paid' THEN 1 ELSE 2 END,"
            " latency_ms ASC, checked_at DESC"
        )

        async with conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()

        proxies: list[VerifiedProxy] = []
        seen: set[str] = set()
        for row in rows:
            if row["proxy_url"] in seen:
                continue
            seen.add(row["proxy_url"])
            proxies.append(_proxy_from_row(row))
            if limit is not None and len(proxies) >= limit:
                break
        return proxies

    async def _ensure_connection(self) -> aiosqlite.Connection:
        if self._conn is None:
            await self.connect()
        if self._conn is None:
            raise StorageError("Proxy database connection is not available")
        return self._conn


def _proxy_values(proxy: VerifiedProxy) -> tuple[object, ...]:
    """Serialize fields in the same order as UPSERT_VERIFIED_PROXY."""
    https_supported = None if proxy.https_supported is None else int(proxy.https_supported)
    return (
        proxy.url,
        proxy.user_agent,
        proxy.transport,
        proxy.latency_ms,
        proxy.checked_at,
        proxy.protocol,
        proxy.source,
        proxy.country,
        proxy.city,
        proxy.anonymity,
        proxy.uptime_percent,
        proxy.source_latency_ms,
        proxy.last_checked,
        https_supported,
        proxy.successful_requests,
        proxy.failed_requests,
        proxy.success_rate,
        proxy.average_latency_ms,
        proxy.last_success,
        proxy.last_failure,
        proxy.consecutive_failures,
        json.dumps(proxy.error_types, sort_keys=True),
    )


def _proxy_from_row(row: aiosqlite.Row) -> VerifiedProxy:
    try:
        error_types = json.loads(row["error_types"] or "{}")
    except (TypeError, ValueError):
        error_types = {}
    if not isinstance(error_types, dict):
        error_types = {}
    https_supported = row["https_supported"]
    return VerifiedProxy(
        url=row["proxy_url"],
        user_agent=row["user_agent"],
        transport=row["transport"],
        latency_ms=row["latency_ms"],
        checked_at=row["checked_at"],
        protocol=row["protocol"] or "",
        source=row["source"] or "",
        country=row["country"],
        city=row["city"],
        anonymity=row["anonymity"],
        uptime_percent=row["uptime_percent"],
        source_latency_ms=row["source_latency_ms"],
        last_checked=row["last_checked"],
        https_supported=None if https_supported is None else bool(https_supported),
        successful_requests=row["successful_requests"] or 0,
        failed_requests=row["failed_requests"] or 0,
        success_rate=row["success_rate"] or 0.0,
        average_latency_ms=row["average_latency_ms"] or row["latency_ms"] or 0.0,
        last_success=row["last_success"],
        last_failure=row["last_failure"],
        consecutive_failures=row["consecutive_failures"] or 0,
        error_types={str(key): int(value) for key, value in error_types.items()},
    )


async def _compact_database(conn: aiosqlite.Connection) -> None:
    """Reclaim deleted pages without failing an already committed refresh."""
    try:
        await conn.execute("VACUUM")
        await conn.commit()
    except (aiosqlite.Error, OSError) as exc:
        logger.warning("Proxy database VACUUM failed: %s", exc)
