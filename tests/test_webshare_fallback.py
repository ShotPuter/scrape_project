"""Webshare publication precedes all free traffic; live capacity opens reserves."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from src.proxy import collector
from src.proxy.checker import CheckStats, Stage1Result, Stage2Result
from src.proxy.config import CollectorConfig
from src.proxy.health import pool_health_path, read_webshare_health
from src.proxy.pool import ProxyEndpoint, ProxyPool
from src.proxy.repository import ProxyDatabase, VerifiedProxy
from src.proxy.sources.catalog import CatalogLoadResult, ProxyCandidate
from src.services import proxy_collector
from src.services.proxy_bootstrap import count_fresh_proxies
from src.services.seprec_proxy_probe import seprec_target_probe


def health(database, count, *, age=0, status="running"):
    pool_health_path(database).write_text(
        json.dumps(
            {
                "updated_at": (datetime.now(UTC) - timedelta(seconds=age)).isoformat(),
                "status": status,
                "webshare_usable": count,
            }
        )
    )


async def wait_for_count(database, count):
    async with asyncio.timeout(2):
        while read_webshare_health(database) != count:  # noqa: ASYNC110 - observe the published cross-process snapshot
            await asyncio.sleep(0.005)


@pytest.mark.asyncio
@pytest.mark.parametrize("private_count", [0, 10, 11, 20])
async def test_free_sources_only_start_after_verified_private_pool_reaches_threshold(tmp_path, monkeypatch, private_count):
    db = tmp_path / "proxies.db"
    calls = []
    private = [ProxyCandidate(f"http://private-{i}:80", "http", "webshare") for i in range(private_count)]

    async def inventory(*args, **kwargs):
        calls.append("inventory")
        return private

    async def public(*args, **kwargs):
        async with ProxyDatabase(db) as database:
            records = await database.list_active()
        assert sum(record.source == "webshare" for record in records) == private_count
        assert len([call for call in calls if call.startswith("verified:")]) == private_count
        calls.append("public")
        return CatalogLoadResult([ProxyCandidate("http://public:80", "http", "monosans")])

    async def stage1(url, **kwargs):
        return Stage1Result(True, 1)

    async def stage2(url, **kwargs):
        calls.append(f"verified:{url}")
        return Stage2Result(True, CheckStats(successful_requests=1, total_attempts=1))

    monkeypatch.setattr(collector, "load_webshare_candidates", inventory)
    monkeypatch.setattr(collector, "load_public_catalog", public)
    monkeypatch.setattr(collector, "verify_stage1", stage1)
    monkeypatch.setattr(collector, "verify_stage2", stage2)
    report = await collector.collect(
        config=CollectorConfig(webshare_api_token="token"),
        target=seprec_target_probe("https://target.example/api"),
        csv_path=tmp_path / "unused.csv",
        db_path=db,
        sources="monosans",
        requests_per_second=0,
    )
    assert ("public" in calls) == (private_count <= 10)
    assert report["free_sources_enabled"] == (private_count <= 10)
    async with ProxyDatabase(db) as database:
        records = await database.list_active()
    assert sum(record.source == "webshare" for record in records) == private_count
    assert sum(record.source == "monosans" for record in records) == (private_count <= 10)


@pytest.mark.asyncio
async def test_live_pool_drop_starts_only_free_scan_preserving_private_receipts(tmp_path, monkeypatch):
    db = tmp_path / "proxies.db"
    async with ProxyDatabase(db) as database:
        await database.replace_all(
            [VerifiedProxy(f"http://private-{i}:80", "UA", "aiohttp", source="webshare") for i in range(20)]
        )
    health(db, 10)

    async def public(*args, **kwargs):
        return CatalogLoadResult([ProxyCandidate("http://public:80", "http", "monosans")])

    monkeypatch.setattr(
        collector,
        "load_webshare_candidates",
        AsyncMock(side_effect=AssertionError("Do not refetch Webshare before starting reserve")),
    )
    monkeypatch.setattr(collector, "load_public_catalog", public)
    monkeypatch.setattr(collector, "verify_stage1", AsyncMock(return_value=Stage1Result(True, 1)))
    monkeypatch.setattr(
        collector,
        "verify_stage2",
        AsyncMock(return_value=Stage2Result(True, CheckStats(successful_requests=1, total_attempts=1))),
    )
    report = await collector.collect(
        config=CollectorConfig(webshare_api_token="token"),
        target=seprec_target_probe("https://target.example/api"),
        csv_path=tmp_path / "unused.csv",
        db_path=db,
        sources="monosans",
        requests_per_second=0,
        _fallback_only=True,
    )
    assert report["webshare_usable_proxies"] == 10
    assert report["free_sources_enabled"]
    async with ProxyDatabase(db) as database:
        records = await database.list_active()
    assert len(records) == 21


@pytest.mark.asyncio
async def test_pool_health_counts_failures_and_quarantine_but_not_busy_slots(tmp_path):
    db = tmp_path / "proxies.db"
    pool = ProxyPool([], lambda **kwargs: AsyncMock(), health_path=pool_health_path(db))
    endpoints = [ProxyEndpoint(f"http://user:password@private-{i}:80", source="webshare") for i in range(11)]
    sessions = [AsyncMock() for _ in endpoints]
    for endpoint, session in zip(endpoints, sessions, strict=True):
        pool.add_session(endpoint, session)
    try:
        await wait_for_count(db, 11)
        async with pool.lease(1):
            assert read_webshare_health(db) == 11
        pool.discard(sessions[0])
        await wait_for_count(db, 10)
        pool.handle_rate_limit(endpoints[1].url, 150)
        await wait_for_count(db, 9)
        assert "user" not in pool_health_path(db).read_text()
        assert "password" not in pool_health_path(db).read_text()
    finally:
        await pool.close()
    assert json.loads(pool_health_path(db).read_text())["status"] == "stopped"
    assert read_webshare_health(db) is None


@pytest.mark.parametrize(
    "count,age,status,expected",
    [
        (10, 0, "running", 10),
        (10, 120, "running", None),
        (10, 0, "stopped", None),
        (-1, 0, "running", None),
        (True, 0, "running", None),
    ],
)
def test_health_ignores_stale_stopped_or_invalid_snapshots(tmp_path, count, age, status, expected):
    db = tmp_path / "proxies.db"
    health(db, count, age=age, status=status)
    assert read_webshare_health(db) == expected


@pytest.mark.asyncio
async def test_watch_wakes_for_live_threshold_without_full_refresh_delay(tmp_path, monkeypatch):
    db = tmp_path / "proxies.db"
    health(db, 10)
    monkeypatch.setattr(proxy_collector.settings, "webshare_api_token", "token")
    assert await asyncio.wait_for(proxy_collector.wait_for_next_scan(0.01, db, {"free_sources_enabled": False}), 1)
    health(db, 11)
    assert not await proxy_collector.wait_for_next_scan(0.01, db, {"free_sources_enabled": False})


@pytest.mark.asyncio
async def test_startup_waits_for_webshare_even_when_old_public_database_exists(tmp_path):
    db = tmp_path / "proxies.db"
    report = tmp_path / "proxy-report.json"
    async with ProxyDatabase(db) as database:
        await database.replace_all([VerifiedProxy("http://public:80", "UA", "aiohttp", source="monosans")])
    options = {"max_age_hours": 24, "preferred_source": "webshare", "fallback_report": report}
    assert await count_fresh_proxies(db, **options) == 0
    report.write_text('{"free_sources_enabled": true}')
    assert await count_fresh_proxies(db, **options) == 1
    async with ProxyDatabase(db) as database:
        await database.upsert_many([VerifiedProxy("http://private:80", "UA", "aiohttp", source="webshare")])
    assert await count_fresh_proxies(db, **options) == 1


@pytest.mark.asyncio
async def test_reserve_is_saved_before_webshare_recheck(tmp_path, monkeypatch):
    calls = []

    async def scan(**kwargs):
        calls.append(kwargs)
        count = 2000 if kwargs['sources'] == 'monosans' else (10 if len(calls) == 1 else 20)
        return {'verified_proxies': count, 'verified_combinations': count, 'database_replaced': True}

    monkeypatch.setattr(collector, '_collect', scan)
    report = await collector.collect(
        config=CollectorConfig(webshare_api_token='token'),
        target=seprec_target_probe('https://target.example/api'),
        csv_path=tmp_path / 'unused.csv', db_path=tmp_path / 'proxies.db', sources='monosans',
    )
    assert [call['sources'] for call in calls] == ['webshare', 'monosans', 'webshare']
    assert calls[1]['_verified_target'] == 2000
    assert calls[1]['_preserve_sources'] == ('webshare',)
    assert calls[2]['_preserve_sources'] == ('monosans',)
    assert report['webshare_recheck']['verified_proxies'] == 20
