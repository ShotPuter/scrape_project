"""Rate-limit quarantine survives database refreshes and applies to probes too."""

import asyncio
import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest

from src.client.base_client import HttpClient, Route
from src.client.config import ClientConfig
from src.network.budget import NetworkBudget
from src.network.errors import HTTPClientError
from src.network.transport import Response
from src.proxy.pool import ProxyEndpoint, ProxyPool


@pytest.mark.asyncio
async def test_first_429_quarantines_only_the_offending_url_across_refresh():
    first, second = AsyncMock(), AsyncMock()
    pool = ProxyPool([], lambda **kw: AsyncMock(), verified_reuse_seconds=300)
    pool.add_session("http://one:80", first)
    pool.add_session("http://two:80", second)
    pool.budget.pause = Mock()
    try:
        assert pool.handle_rate_limit("http://one:80", 1800) == 150
        assert first not in pool.sessions
        assert second in pool.sessions
        assert pool._rate_limit_until["http://one:80"] > time.monotonic() + 1790
        pool.update_candidates(
            [ProxyEndpoint("http://one:80", checked_at=datetime.now(UTC).isoformat()), ProxyEndpoint("http://two:80")]
        )
        assert all(pool.session_proxies[s] != "http://one:80" for s in pool.sessions)
        pool.budget.pause.assert_called_once_with(150)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_repeated_429_for_distinct_urls_escalates_to_server_pause():
    pool = ProxyPool([], lambda **kw: AsyncMock())
    pool.budget.pause = Mock()
    try:
        assert pool.handle_rate_limit("http://one:80", 1800) == 150
        assert pool.handle_rate_limit("http://one:80", 1800) == 150
        assert pool.handle_rate_limit("http://two:80", 1800) == 150
        assert pool.handle_rate_limit("http://three:80", 1800) == 1800
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_download_429_uses_quarantine_and_short_global_pause():
    client = HttpClient(ClientConfig(base_url="https://example.test"))
    session = AsyncMock()
    pool = ProxyPool([], lambda **kw: AsyncMock(), network_budget=client.budget)
    pool.add_session("http://one:80", session)
    client._proxy_pool = pool
    client.budget.pause = Mock()
    response = Response(429, {"Retry-After": "1800"}, b"{}")
    try:
        with pytest.raises(HTTPClientError) as exc:
            await client._handle_error_response(
                Mock(raw=session), "GET", "https://example.test/publicaciones/1", None, None, response, 429, Route.POOL
            )
        assert exc.value.retry_after == "150"
        assert session not in pool.sessions
        client.budget.pause.assert_called_once_with(150)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_probe_429_uses_same_policy_and_api_delay():
    pool = ProxyPool([], lambda **kw: AsyncMock())
    pool.budget.pause = Mock()
    try:
        pool._pause_if_rate_limited(Response(429, {"Retry-After": "9999"}, b'{"retry_after": 900}'), "http://one:80")
        pool.budget.pause.assert_called_once_with(150)
        assert pool._rate_limit_until["http://one:80"] > time.monotonic() + 890
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_probe_bypasses_pacing_but_not_global_cooldown():
    budget = NetworkBudget(1, 0)
    budget.pause(0.03)
    entered = asyncio.Event()

    async def probe():
        async with budget.slot(apply_rate=False):
            entered.set()

    task = asyncio.create_task(probe())
    await asyncio.sleep(0)
    assert not entered.is_set()
    await asyncio.wait_for(task, 1)
    assert entered.is_set()
