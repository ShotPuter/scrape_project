"""Request-count and freshness contracts for the reduced verification policy."""

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock

import pytest

from src.client.base_client import HttpClient
from src.client.config import ClientConfig
from src.network.budget import NetworkBudget
from src.network.transport import Response
from src.proxy import checker, collector
from src.proxy.checker import CheckStats, Stage1Result, Stage2Result
from src.proxy.collector import Stage1Success
from src.proxy.config import CollectorConfig, ProxyConfig
from src.proxy.pool import ProxyEndpoint, ProxyPool, WarmupSpec
from src.proxy.probes import ProbeRequest, TargetProbe
from src.proxy.repository import ProxyDatabase, VerifiedProxy
from src.proxy.sources.catalog import ProxyCandidate
from src.services.seprec_proxy_probe import seprec_target_probe

SEARCH_RESPONSE = Response(200, {}, b'{"finalizado":true,"datos":{"total":0,"filas":[]}}')


@pytest.mark.asyncio
@pytest.mark.parametrize("primary_ok", [True, False])
async def test_stage1_stops_at_first_success_without_user_agent_matrix(monkeypatch, primary_ok):
    calls = []

    async def verify(url, **kwargs):
        calls.append((kwargs["transport"], kwargs["user_agent"]))
        return Stage1Result(primary_ok or kwargs["transport"] == "curl_cffi", 10)

    monkeypatch.setattr(collector, "verify_stage1", verify)
    successes, failures = await collector._stage1_scan(
        [ProxyCandidate("http://proxy.example:80", "http", "test")],
        config=CollectorConfig(default_headers={"User-Agent": "selected-UA"}),
        concurrency=1,
        transports=["aiohttp", "curl_cffi"],
        check_url=None,
        timeout=1,
        connect_timeout=1,
    )
    assert failures == []
    assert len(successes) == 1
    assert calls == (
        [("aiohttp", "selected-UA")] if primary_ok else [("aiohttp", "selected-UA"), ("curl_cffi", "selected-UA")]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("primary_ok", [True, False])
async def test_stage2_uses_alternate_transport_only_after_failure(monkeypatch, primary_ok):
    calls = []

    async def verify(url, **kwargs):
        calls.append(kwargs)
        ok = primary_ok or kwargs["transport"] == "curl_cffi"
        return Stage2Result(
            ok,
            CheckStats(successful_requests=int(ok), failed_requests=int(not ok), total_attempts=1, average_latency_ms=10),
            "" if ok else "http_403",
        )

    monkeypatch.setattr(collector, "verify_stage2", verify)
    success = Stage1Success(ProxyCandidate("http://proxy.example:80", "http", "test"), "aiohttp", "UA", 5)
    checks = await collector._stage2_scan(
        [success, success],
        config=CollectorConfig(proxy_user_agent_limit=1),
        target=seprec_target_probe("https://target.example/api"),
        concurrency=2,
        timeout=1,
        requests_per_second=0,
        attempts=None,
        retry_delay=0,
        max_retry_delay=0,
    )
    expected = ["aiohttp"] if primary_ok else ["aiohttp", "curl_cffi"]
    assert [call["transport"] for call in calls] == expected
    assert all(call["attempts"] == 1 and call["user_agent"] == "UA" for call in calls)
    record = collector._record_from_check(checks[-1], 1, 0)
    assert record.transport == expected[-1]
    assert checks[-1].stage1.transport == "aiohttp"


@pytest.mark.asyncio
@pytest.mark.parametrize("valid", [True, False])
async def test_seprec_stage2_is_one_real_search_request_by_default(monkeypatch, valid):
    session = AsyncMock()
    session.request.return_value = SEARCH_RESPONSE if valid else Response(503, {})
    monkeypatch.setattr(checker, "_session_for", lambda *args, **kwargs: session)
    result = await checker.verify_stage2(
        "http://proxy.example:80",
        transport="aiohttp",
        user_agent="UA",
        target=seprec_target_probe("https://target.example/api"),
    )
    assert result.ok is valid
    assert result.stats.total_attempts == 1
    session.request.assert_awaited_once()
    assert session.request.await_args.args == ("POST", "https://target.example/api/publicaciones")
    assert session.request.await_args.kwargs["json"]["limite"] == 10
    session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_budget_counts_each_request_in_a_custom_profile(monkeypatch):
    class CountingBudget(NetworkBudget):
        entries = 0

        @asynccontextmanager
        async def slot(self, **kwargs):
            self.entries += 1
            async with super().slot(**kwargs):
                yield

    session = AsyncMock()
    session.request.return_value = Response(200, {}, b'{"ok":true}')
    monkeypatch.setattr(checker, "_session_for", lambda *args, **kwargs: session)
    budget = CountingBudget(1)
    target = TargetProbe(
        tuple(
            ProbeRequest("GET", f"https://target.example/{name}", lambda data: data.get("ok") is True)
            for name in ("first", "second")
        )
    )
    result = await checker.verify_stage2(
        "http://proxy.example:80", transport="aiohttp", user_agent="UA", target=target, budget=budget
    )
    assert result.ok
    assert budget.entries == 2
    assert session.request.await_count == 2


@pytest.mark.asyncio
async def test_recent_database_records_skip_startup_and_reload_probes(tmp_path):
    path = tmp_path / "proxies.db"
    first_record = VerifiedProxy("http://first:80", "UA", "aiohttp")
    second_record = VerifiedProxy("http://second:80", "UA", "aiohttp")
    async with ProxyDatabase(path) as database:
        await database.replace_all([first_record])
    sessions = {first_record.url: AsyncMock(), second_record.url: AsyncMock()}
    client = HttpClient(
        ClientConfig(
            base_url="https://target.example",
            proxy=ProxyConfig(db_path=path, check_path="health", db_refresh_seconds=0, min_ready=20),
        )
    )
    client._session_factory.create = Mock(side_effect=lambda proxy=None, **kwargs: sessions[proxy])
    try:
        await client.open()
        assert client._proxy_pool.sessions == [sessions[first_record.url]]
        async with ProxyDatabase(path) as database:
            await database.replace_all([first_record, second_record])
        await client._refresh_proxy_database()
        assert len(client._proxy_pool.sessions) == 2
        assert client.metrics.counts["verified_reuses"] == 2
        assert client.metrics.counts["wire_requests"] == 0
        for session in sessions.values():
            session.post.assert_not_awaited()
            session.request.assert_not_awaited()
    finally:
        await client.close()
    for session in sessions.values():
        session.close.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("age,reuse_seconds", [(301, 300), (0, 0)])
async def test_expired_or_disabled_reuse_still_performs_readiness(tmp_path, age, reuse_seconds):
    path = tmp_path / "proxies.db"
    checked_at = (datetime.now(UTC) - timedelta(seconds=age)).isoformat()
    async with ProxyDatabase(path) as database:
        await database.replace_all([VerifiedProxy("http://proxy:80", "UA", "aiohttp", checked_at=checked_at)])
    session = AsyncMock()
    session.post.return_value = SEARCH_RESPONSE
    client = HttpClient(
        ClientConfig(
            base_url="https://target.example",
            proxy=ProxyConfig(
                db_path=path, check_path="health", db_refresh_seconds=0, min_ready=1, verified_reuse_seconds=reuse_seconds
            ),
        )
    )
    client._session_factory.create = Mock(return_value=session)
    try:
        await client.open()
        session.post.assert_awaited_once()
        assert client.metrics.counts["verified_reuses"] == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_runtime_failure_cannot_reuse_the_same_database_receipt():
    now = datetime.now(UTC)
    endpoint = ProxyEndpoint("http://proxy:80", checked_at=now.isoformat())
    first, second = AsyncMock(), AsyncMock()
    factory = Mock(side_effect=[first, second])
    pool = ProxyPool([endpoint], factory, max_failures=1, verified_reuse_seconds=300)
    try:
        assert pool.reuse_verified([endpoint]) == 1
        pool.discard(first)
        pool.update_candidates([endpoint])
        assert pool.sessions == []
        assert factory.call_count == 1
        fresh = ProxyEndpoint(endpoint.url, checked_at=(now + timedelta(microseconds=1)).isoformat())
        pool.update_candidates([fresh])
        assert pool.sessions == [second]
        second.post.assert_not_awaited()
    finally:
        await pool.close()


@pytest.mark.parametrize("stamp", ["", "invalid", "2026-01-01T00:00:00", "2999-01-01T00:00:00+00:00"])
def test_invalid_or_future_receipts_are_not_trusted(stamp):
    factory = Mock()
    endpoint = ProxyEndpoint("http://proxy:80", checked_at=stamp)
    pool = ProxyPool([endpoint], factory, verified_reuse_seconds=300)
    assert pool.reuse_verified([endpoint]) == 0
    factory.assert_not_called()


@pytest.mark.asyncio
async def test_fresh_session_does_not_wait_for_old_candidates(tmp_path):
    path = tmp_path / "proxies.db"
    old_stamp = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
    async with ProxyDatabase(path) as database:
        await database.replace_all(
            [
                VerifiedProxy("http://fresh:80", "UA", "aiohttp"),
                VerifiedProxy("http://old:80", "UA", "aiohttp", checked_at=old_stamp),
            ]
        )
    fresh, old = AsyncMock(), AsyncMock()
    probing = asyncio.Event()

    async def slow_probe(*args, **kwargs):
        probing.set()
        await asyncio.Event().wait()

    old.post.side_effect = slow_probe
    client = HttpClient(
        ClientConfig(
            base_url="https://target.example",
            proxy=ProxyConfig(db_path=path, check_path="health", min_ready=20, db_refresh_seconds=0),
        )
    )
    client._session_factory.create = Mock(side_effect=lambda proxy=None, **kwargs: fresh if "fresh" in proxy else old)
    try:
        await asyncio.wait_for(client.open(), 1)
        async with client._proxy_pool.lease(1) as session:
            assert session is fresh
        await asyncio.wait_for(probing.wait(), 1)
        fresh.post.assert_not_awaited()
    finally:
        await client.close()
    fresh.close.assert_awaited_once()
    old.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_new_receipt_supersedes_an_already_running_old_probe():
    old_stamp = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
    old, fresh = AsyncMock(), AsyncMock()
    started, finish = asyncio.Event(), asyncio.Event()

    async def old_probe(*args, **kwargs):
        started.set()
        await finish.wait()
        return SEARCH_RESPONSE

    old.post.side_effect = old_probe
    pool = ProxyPool(
        [ProxyEndpoint("http://proxy:80", checked_at=old_stamp)], Mock(side_effect=[old, fresh]), verified_reuse_seconds=300
    )
    task = pool.start_warmup(WarmupSpec("https://target.example/health"))
    try:
        await asyncio.wait_for(started.wait(), 1)
        pool.update_candidates([ProxyEndpoint("http://proxy:80", checked_at=datetime.now(UTC).isoformat())])
        finish.set()
        await task
        await pool.wait_until_ready()
        assert pool.sessions == [fresh]
        fresh.post.assert_not_awaited()
        old.post.assert_awaited_once()
    finally:
        await pool.close()
