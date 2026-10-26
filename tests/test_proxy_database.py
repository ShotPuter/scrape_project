"""Verified proxy database tests."""

import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from src.storage.proxy_database import ProxyDatabase, VerifiedProxy


@pytest.mark.asyncio
async def test_replace_all_deletes_old_records(tmp_path):
    db_path = tmp_path / "proxies.db"
    async with ProxyDatabase(db_path) as database:
        await database.replace_all([VerifiedProxy(url="http://old.example:8080", user_agent="UA-OLD", transport="aiohttp")])
        await database.replace_all(
            [VerifiedProxy(url="http://new.example:8080", user_agent="UA-NEW", transport="curl_cffi", latency_ms=12.5)]
        )

        active = await database.list_active()

    assert len(active) == 1
    assert active[0].url == "http://new.example:8080"
    assert active[0].user_agent == "UA-NEW"
    assert active[0].transport == "curl_cffi"
    assert active[0].latency_ms == 12.5

    with sqlite3.connect(db_path) as connection:
        total_rows = connection.execute("SELECT COUNT(*) FROM verified_proxies").fetchone()[0]
    assert total_rows == 1


@pytest.mark.asyncio
async def test_list_active_keeps_fastest_transport_per_proxy(tmp_path):
    async with ProxyDatabase(tmp_path / "proxies.db") as database:
        await database.replace_all(
            [
                VerifiedProxy(url="http://proxy.example:8080", user_agent="UA-CURL", transport="curl_cffi", latency_ms=40.0),
                VerifiedProxy(
                    url="http://proxy.example:8080", user_agent="UA-AIOHTTP", transport="aiohttp", latency_ms=10.0
                ),
                VerifiedProxy(url="http://proxy2.example:8080", user_agent="UA-2", transport="aiohttp", latency_ms=30.0),
            ]
        )

        active = await database.list_active(limit=1)

    assert len(active) == 1
    assert active[0].url == "http://proxy.example:8080"
    assert active[0].transport == "aiohttp"
    assert active[0].user_agent == "UA-AIOHTTP"


@pytest.mark.asyncio
async def test_list_active_can_filter_by_verification_age(tmp_path):
    old = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    async with ProxyDatabase(tmp_path / "proxies.db") as database:
        await database.replace_all(
            [
                VerifiedProxy(url="http://old.example:8080", user_agent="UA-OLD", transport="aiohttp", checked_at=old),
                VerifiedProxy(url="http://fresh.example:8080", user_agent="UA-NEW", transport="aiohttp"),
            ]
        )

        active = await database.list_active(max_age_hours=1)
        unfiltered = await database.list_active(max_age_hours=0)

    assert [proxy.url for proxy in active] == ["http://fresh.example:8080"]
    assert len(unfiltered) == 2


@pytest.mark.asyncio
async def test_metadata_and_statistics_round_trip(tmp_path):
    db_path = tmp_path / "proxies.db"
    proxy = VerifiedProxy(
        url="socks5://proxy.example:1080", user_agent="UA", transport="curl_cffi",
        latency_ms=123.4, protocol="socks5", source="hproxy",
        country="US", city="Ashburn", anonymity="elite",
        uptime_percent=99.5, source_latency_ms=200.0, last_checked="2026-10-23T12:00:00+00:00",
        https_supported=True, successful_requests=2, failed_requests=1,
        success_rate=0.6667, average_latency_ms=123.4, last_success="2026-10-23T12:00:01+00:00",
        last_failure="2026-10-23T12:00:02+00:00", consecutive_failures=0, error_types={"http_429": 1},
    )

    async with ProxyDatabase(db_path) as database:
        await database.replace_all([proxy])
        active = await database.list_active()

    assert len(active) == 1
    stored = active[0]
    assert stored.protocol == "socks5"
    assert stored.source == "hproxy"
    assert stored.country == "US"
    assert stored.city == "Ashburn"
    assert stored.anonymity == "elite"
    assert stored.uptime_percent == 99.5
    assert stored.source_latency_ms == 200.0
    assert stored.last_checked == "2026-10-23T12:00:00+00:00"
    assert stored.https_supported is True
    assert stored.successful_requests == 2
    assert stored.failed_requests == 1
    assert stored.success_rate == 0.6667
    assert stored.average_latency_ms == 123.4
    assert stored.last_success == "2026-10-23T12:00:01+00:00"
    assert stored.last_failure == "2026-10-23T12:00:02+00:00"
    assert stored.consecutive_failures == 0
    assert stored.error_types == {"http_429": 1}


@pytest.mark.asyncio
async def test_existing_database_is_migrated_in_place(tmp_path):
    db_path = tmp_path / "proxies.db"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE verified_proxies (
                proxy_url TEXT NOT NULL,
                user_agent TEXT NOT NULL,
                transport TEXT NOT NULL,
                latency_ms REAL NOT NULL DEFAULT 0,
                checked_at TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (proxy_url, user_agent, transport)
            )
            """
        )
        connection.execute(
            "INSERT INTO verified_proxies (proxy_url, user_agent, transport, latency_ms, checked_at) VALUES (?, ?, ?, ?, ?)",
            ("http://old.example:8080", "UA", "aiohttp", 10.0, datetime.now(UTC).isoformat()),
        )

    async with ProxyDatabase(db_path) as database:
        active = await database.list_active()
        assert active[0].source == ""
        await database.replace_all(
            [
                VerifiedProxy(
                    url="http://new.example:8080", user_agent="UA", transport="aiohttp",
                    source="proxyscrape", successful_requests=3, success_rate=1.0,
                    error_types={"http_503": 1},
                )
            ]
        )

    with sqlite3.connect(db_path) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(verified_proxies)").fetchall()}
    assert {"source", "successful_requests", "error_types", "average_latency_ms"} <= columns
