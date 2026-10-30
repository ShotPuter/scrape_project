"""Proxy selection checks without connecting to public proxies.

A few tests inspect pool counters directly because they simulate failures that are
otherwise timing-dependent; user-visible behavior is asserted where practical.
"""

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest

from src.client.base_client import HttpClient
from src.client.config import ClientConfig, ProxyConfig
from src.client.readiness import is_ready_response
from src.network.session_factory import SessionFactory
from src.proxy.pool import ProxyPool, WarmupSpec, dead_proxy_path, load_proxies


def test_text_candidates_respect_validation_deduplication_and_dead_list(tmp_path):
    path = tmp_path / "proxies.txt"
    path.write_text(
        "# comment\n\none:80\nhttp://one:80\ninvalid\nftp://bad:21\nhttp://dead:80\nsocks5://two:1080\nhttp://three:80\n"
    )
    dead_path = dead_proxy_path(path)
    dead_path.write_text(json.dumps({"http://dead:80": datetime.now(UTC).isoformat()}))

    assert load_proxies(path, 2, dead_path=dead_path) == ["http://one:80", "socks5://two:1080"]


def test_csv_candidates_are_valid_unique_and_limited(tmp_path):
    path = tmp_path / "proxies.csv"
    path.write_text(
        "alive,protocol,proxy\nfalse,http,dead:80\ntrue,http,one:80\n"
        "true,http,one:80\ntrue,ftp,bad:80\ntrue,socks5,two:1080\ntrue,http,three:80\n"
    )
    assert load_proxies(path, 2) == ["http://one:80", "socks5://two:1080"]


def test_zero_sample_size_loads_every_candidate(tmp_path):
    path = tmp_path / "proxies.csv"
    path.write_text("alive,protocol,proxy\ntrue,http,one:80\ntrue,socks5,two:1080\n")
    assert load_proxies(path, 0) == ["http://one:80", "socks5://two:1080"]


def test_recently_dead_proxies_are_skipped(tmp_path):
    path = tmp_path / "proxies.csv"
    path.write_text("alive,protocol,proxy\ntrue,http,one:80\ntrue,http,two:80\n")
    dead_path = dead_proxy_path(path)
    dead_path.write_text(json.dumps({"http://one:80": datetime.now(UTC).isoformat()}))
    assert load_proxies(path, dead_path=dead_path) == ["http://two:80"]


class Response:
    def __init__(self, status, data, headers=None):
        self.status_code = status
        self.data = data
        self.headers = headers or {}

    @property
    def content(self):
        return json.dumps(self.data).encode()

    def json(self):
        return self.data


@pytest.mark.asyncio
async def test_only_valid_api_responses_enter_rotation():
    sessions = []
    responses = [
        Response(201, {"finalizado": True, "datos": []}),
        Response(403, {}),
        Response(200, {"finalizado": False}),
        Response(200, {"finalizado": True, "datos": []}),
    ]

    def factory(**kwargs):
        session = AsyncMock()
        session.post.return_value = responses[len(sessions)]
        sessions.append(session)
        return session

    pool = ProxyPool(["a", "b", "c", "d"], factory)
    warmup_task = pool.start_warmup(WarmupSpec("https://example.test/catalog", validator=is_ready_response, concurrency=1))
    await warmup_task
    await pool.wait_until_ready()
    assert pool.checked == 4
    chosen = []
    for _ in range(3):
        async with pool.lease(1) as session:
            chosen.append(session)
    assert chosen == [sessions[0], sessions[3], sessions[0]]
    sessions[1].close.assert_awaited_once()
    sessions[2].close.assert_awaited_once()
    await pool.close()
    sessions[0].close.assert_awaited_once()
    sessions[3].close.assert_awaited_once()


@pytest.mark.asyncio
async def test_empty_pool_fails_without_direct_fallback():
    session = AsyncMock()
    session.post.return_value = Response(200, {"html": "challenge"})
    pool = ProxyPool(["a"], lambda **kwargs: session)
    warmup_task = pool.start_warmup(WarmupSpec("https://example.test/catalog", validator=is_ready_response))
    await warmup_task
    with pytest.raises(RuntimeError, match="None of 1"):
        await pool.wait_until_ready()
    session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_one_broken_proxy_is_cooled_down_without_permanent_exclusion(tmp_path):
    class SocksLikeError(Exception):
        pass

    def factory(**kwargs):
        session = AsyncMock()
        if kwargs["proxy"] == "broken":
            session.post.side_effect = SocksLikeError("proxy refused connection")
        else:
            session.post.return_value = Response(200, {"finalizado": True, "datos": []})
        return session

    dead_path = tmp_path / "proxies.dead.json"
    pool = ProxyPool(["broken", "working"], factory, dead_path=dead_path)
    warmup_task = pool.start_warmup(WarmupSpec("https://example.test/catalog", validator=is_ready_response, concurrency=1))
    await warmup_task
    await pool.wait_until_ready()

    assert pool.checked == 2
    assert len(pool.sessions) == 1
    assert not dead_path.exists()
    await pool.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [{}, {"Retry-After": "2.5"}, {"Retry-After": "invalid"}])
async def test_readiness_429_pauses_requests_and_closes_the_failed_session(headers):
    session = AsyncMock()
    session.post.return_value = Response(429, {}, headers)
    pool = ProxyPool(["http://proxy.test:80"], lambda **kwargs: session)
    pool.budget.pause = Mock()
    try:
        warmup_task = pool.start_warmup(WarmupSpec("https://example.test/catalog", validator=is_ready_response))
        await warmup_task
        with pytest.raises(RuntimeError, match="None of 1"):
            await pool.wait_until_ready()
        pool.budget.pause.assert_called_once_with(150)
        session.close.assert_awaited_once()
        assert pool.metrics.samples[-1]["error_class"] == "HTTP429"
        assert pool.budget.active == 0
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_cookie_jars_are_separate_and_preserve_domain(tmp_path):
    path = tmp_path / "cookies.txt"
    path.write_text("# Netscape HTTP Cookie File\n.example.test\tTRUE\t/\tTRUE\t0\tsession\tvalue\n")
    factory = SessionFactory(
        headers={}, timeout=30, connection_limit=9, impersonate="chrome", cookies_file=path, transport="curl_cffi"
    )
    first, second = factory.make_curl(), factory.make_curl()
    try:
        cookie = next(iter(first.cookies.jar))
        assert cookie.domain == ".example.test"
        first.cookies.set("local", "one", domain="example.test")
        assert second.cookies.get("local") is None
    finally:
        await first.close()
        await second.close()


@pytest.mark.asyncio
async def test_network_failure_removes_proxy_and_retry_uses_another():
    from curl_cffi.requests.exceptions import ConnectionError

    first, second = AsyncMock(), AsyncMock()
    first.request.side_effect = ConnectionError("failed")
    second.request.return_value = Response(200, {"ok": True})
    pool = ProxyPool([], lambda **kwargs: None)
    pool.add_session("http://one:80", first)
    pool.add_session("http://two:80", second)
    client = HttpClient(ClientConfig(base_url="https://example.test"))
    client._proxy_pool = pool
    assert await client.request("GET", "/test") == {"ok": True}
    assert pool.sessions == [second]
    assert pool.retired == []
    first.close.assert_awaited_once()
    assert await client.request("GET", "/test") == {"ok": True}
    assert first.request.await_count == 1
    await client.close()
    first.close.assert_awaited_once()
    second.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_exhausted_proxy_pool_does_not_fall_back_to_direct_connection():
    from curl_cffi.requests.exceptions import ConnectionError

    from src.network.errors import HTTPClientError

    session = AsyncMock()
    session.request.side_effect = ConnectionError("failed")
    pool = ProxyPool([], lambda **kwargs: None)
    pool.add_session("http://one:80", session)
    client = HttpClient(ClientConfig(base_url="https://example.test", proxy=ProxyConfig(mode="always")))
    client._proxy_pool = pool
    with pytest.raises(HTTPClientError, match="pool is empty"):
        await client.request("GET", "/test")
    assert client.metrics.counts["direct_attempts"] == 0
    await client.close()
