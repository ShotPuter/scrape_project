"""Live database snapshots, session draining and independent collector lifecycle."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock

import pytest

from src import cli
from src.client.base_client import HttpClient
from src.client.config import ClientConfig
from src.proxy.config import ProxyConfig
from src.proxy.pool import ProxyEndpoint, ProxyPool, WarmupSpec
from src.proxy.repository import ProxyDatabase, VerifiedProxy
from src.services import proxy_collector


async def wait_for(predicate):
    async with asyncio.timeout(2):
        while not predicate():  # noqa: ASYNC110 - observe background state through the public effects
            await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_snapshot_adds_new_proxies_above_replenish_threshold():
    first, second = AsyncMock(), AsyncMock()
    second.post.return_value.status_code = 200
    factory = Mock(return_value=second)
    pool = ProxyPool(["http://first:80"], factory, replenish_threshold=1)
    pool.configure_warmup(WarmupSpec("https://target.example/health"))
    pool.add_session("http://first:80", first)
    try:
        pool.update_candidates([ProxyEndpoint("http://first:80"), ProxyEndpoint("http://second:80")])
        await wait_for(lambda: len(pool.sessions) == 2)
        assert factory.call_count == 1
        pool.update_candidates([ProxyEndpoint("http://first:80"), ProxyEndpoint("http://second:80")])
        await asyncio.sleep(0)
        assert factory.call_count == 1
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_removed_proxy_drains_existing_lease_before_closing():
    session = AsyncMock()
    pool = ProxyPool(["http://first:80"], Mock())
    pool.add_session("http://first:80", session)
    try:
        async with pool.lease(1) as leased:
            pool.update_candidates([])
            assert leased is session
            assert pool.sessions == []
            session.close.assert_not_awaited()
        session.close.assert_awaited_once()
    finally:
        await pool.close()
    session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_dead_proxy_only_revives_after_new_verification():
    now = datetime.now(UTC)
    endpoint = ProxyEndpoint("http://first:80", checked_at=now.isoformat())
    session = AsyncMock()
    session.post.return_value.status_code = 200
    factory = Mock(return_value=session)
    pool = ProxyPool([endpoint], factory)
    pool.configure_warmup(WarmupSpec("https://target.example/health"))
    pool.record_dead(endpoint)
    try:
        pool.update_candidates([endpoint])
        pool.update_candidates([])
        pool.update_candidates([endpoint])
        factory.assert_not_called()
        fresh = replace(endpoint, checked_at=(now + timedelta(seconds=1)).isoformat())
        pool.update_candidates([fresh])
        await wait_for(lambda: len(pool.sessions) == 1)
        factory.assert_called_once()
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_removed_inflight_probe_cannot_reenter_rotation():
    started, finish = asyncio.Event(), asyncio.Event()
    session = AsyncMock()

    async def probe(*args, **kwargs):
        started.set()
        await finish.wait()
        return Mock(status_code=200)

    session.post.side_effect = probe
    pool = ProxyPool(["http://first:80"], lambda **kwargs: session)
    task = pool.start_warmup(WarmupSpec("https://target.example/health"))
    try:
        await started.wait()
        pool.update_candidates([])
        finish.set()
        await task
        assert pool.sessions == []
        session.close.assert_awaited_once()
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_client_picks_up_database_after_starting_direct_and_stops_watcher(tmp_path):
    db_path = tmp_path / "proxies.db"
    direct, proxied = AsyncMock(), AsyncMock()
    client = HttpClient(
        ClientConfig(
            base_url="https://target.example",
            proxy=ProxyConfig(db_path=db_path, check_path="health", warmup=False, min_ready=1, db_refresh_seconds=0.01),
        )
    )
    client._session_factory.create = Mock(side_effect=lambda proxy=None, **kwargs: proxied if proxy else direct)
    try:
        await client.open()
        assert client._proxy_pool is None
        watcher = client._database_watch_task
        assert watcher is not None
        async with ProxyDatabase(db_path) as database:
            await database.replace_all([VerifiedProxy("http://fresh:80", "UA", "aiohttp")])
        await wait_for(lambda: client._proxy_pool is not None)
        assert client._proxy_pool.sessions == [proxied]
        async with ProxyDatabase(db_path) as database:
            await database.replace_all([])
        await wait_for(lambda: not client._proxy_pool.sessions)
    finally:
        await client.close()
    assert watcher.done()
    assert client._database_watch_task is None
    direct.close.assert_awaited_once()
    proxied.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_database_refresh_error_preserves_pool_and_watcher_recovers(tmp_path, monkeypatch):
    db_path = tmp_path / "proxies.db"
    async with ProxyDatabase(db_path) as database:
        await database.replace_all([VerifiedProxy("http://fresh:80", "UA", "aiohttp")])
    session = AsyncMock()
    client = HttpClient(
        ClientConfig(
            base_url="https://target.example",
            proxy=ProxyConfig(db_path=db_path, check_path="health", warmup=False, db_refresh_seconds=0.01),
        )
    )
    client._session_factory.create = Mock(return_value=session)
    calls = 0
    refresh = client._refresh_proxy_database

    async def flaky_refresh():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("database temporarily unavailable")
        await refresh()

    monkeypatch.setattr(client, "_refresh_proxy_database", flaky_refresh)
    try:
        await client.open()
        original = client._proxy_pool
        await wait_for(lambda: calls >= 2)
        assert client._proxy_pool is original
        assert client._proxy_pool.sessions == [session]
        assert not client._database_watch_task.done()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_direct_mode_does_not_start_database_watcher(tmp_path):
    client = HttpClient(
        ClientConfig(
            base_url="https://target.example",
            proxy=ProxyConfig(db_path=tmp_path / "unused.db", check_path="health", mode="direct", db_refresh_seconds=0.01),
        )
    )
    client._session_factory.create = Mock(return_value=AsyncMock())
    try:
        await client.open()
        assert client._database_watch_task is None
        assert not (tmp_path / "unused.db").exists()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_watch_collection_recovers_from_failed_scan_and_does_not_overlap(monkeypatch):
    calls, sleeps = [], []

    async def collect(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise ValueError("source temporarily unavailable")
        return {"verified_proxies": 1, "database_replaced": True}

    async def sleep(interval):
        sleeps.append(interval)
        if len(sleeps) == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(proxy_collector, "collect", collect)
    monkeypatch.setattr(proxy_collector.asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await proxy_collector.run_collection(watch=True, interval=7, provider="monosans")
    assert calls == [{"provider": "monosans"}, {"provider": "monosans"}]
    assert sleeps == [7, 7]


@pytest.mark.asyncio
async def test_one_shot_collection_preserves_failure_exit(monkeypatch):
    async def collect(**kwargs):
        raise ValueError("no proxies")

    monkeypatch.setattr(proxy_collector, "collect", collect)
    with pytest.raises(ValueError, match="no proxies"):
        await proxy_collector.run_collection(watch=False, interval=7)


@pytest.mark.asyncio
async def test_external_collector_mode_never_spawns_a_child(tmp_path, monkeypatch):
    monkeypatch.setattr(cli.settings, "proxy_mode", "pool")
    monkeypatch.setattr(cli.settings, "proxy_db_path", tmp_path / "proxies.db")
    monkeypatch.setattr(cli.settings, "proxy_autocollect", False)
    monkeypatch.setattr(cli, "count_fresh_proxies", AsyncMock(return_value=0))
    wait = AsyncMock(return_value=0)
    monkeypatch.setattr(cli, "wait_for_proxies", wait)
    spawn = Mock(side_effect=AssertionError("must not spawn an external collector"))
    monkeypatch.setattr(cli, "proxy_collector", spawn)
    run = AsyncMock(return_value=[{"status": "COMPLETED"}])
    monkeypatch.setattr(cli, "_run_scraper", run)
    args = cli.build_parser().parse_args([])
    await cli._run_once(args)
    spawn.assert_not_called()
    assert wait.await_args.args == (tmp_path / "proxies.db", None)
    run.assert_awaited_once_with(args, tmp_path / "proxies.db")
