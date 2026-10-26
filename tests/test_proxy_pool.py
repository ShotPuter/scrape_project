"""Proxy selection checks without connecting to public proxies."""

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest

from src.client.base_client import HttpClient
from src.client.network import attempt_context
from src.client.proxy_pool import ProxyPool, dead_proxy_path, load_proxies
from src.client.session_factory import SessionFactory


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

    def json(self):
        return self.data


@pytest.mark.asyncio
async def test_warmup_timeout_starts_with_already_ready_sessions():
    class SlowPool:
        sessions = [object()]
        checked = 1

        def start_warmup(self, *args, **kwargs):
            pass

        async def wait_until_ready(self):
            await asyncio.Event().wait()

    client = HttpClient("https://example.test", proxy_warmup_timeout_seconds=0.01)

    await asyncio.wait_for(client._warm_up_pool(SlowPool(), ["http://proxy.test:80"]), 1)


@pytest.mark.asyncio
async def test_warmup_timeout_without_ready_sessions_raises():
    class SlowPool:
        sessions = []
        checked = 1

        def start_warmup(self, *args, **kwargs):
            pass

        async def wait_until_ready(self):
            await asyncio.Event().wait()

    client = HttpClient("https://example.test", proxy_warmup_timeout_seconds=0.01)

    with pytest.raises(RuntimeError, match="No proxy became ready"):
        await asyncio.wait_for(client._warm_up_pool(SlowPool(), ["http://proxy.test:80"]), 1)


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
    await pool.warmup("https://example.test/catalog", {}, concurrency=1)
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
    with pytest.raises(RuntimeError, match="None of 1"):
        await pool.warmup("https://example.test/catalog", {})
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
    await pool.warmup("https://example.test/catalog", {}, concurrency=1)

    assert pool.checked == 2
    assert len(pool.sessions) == 1
    assert not dead_path.exists()
    assert pool._failure_counts["broken"] == 1
    await pool.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("headers", "expected_delay"), [({}, 1), ({"Retry-After": "2.5"}, 2.5), ({"Retry-After": "invalid"}, 1)]
)
async def test_readiness_429_pauses_requests_and_closes_the_failed_session(headers, expected_delay):
    session = AsyncMock()
    session.post.return_value = Response(429, {}, headers)
    pool = ProxyPool(["http://proxy.test:80"], lambda **kwargs: session)
    pool.budget.pause = Mock()
    try:
        with pytest.raises(RuntimeError, match="None of 1"):
            await pool.warmup("https://example.test/catalog", {})
        pool.budget.pause.assert_called_once_with(expected_delay)
        session.close.assert_awaited_once()
        assert pool._failure_counts["http://proxy.test:80"] == 1
        assert pool.metrics.samples[-1]["error_class"] == "HTTP429"
        assert pool.budget.active == 0
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_readiness_failure_restores_context_even_when_session_close_fails():
    session = AsyncMock()
    session.post.side_effect = OSError("connection lost")
    session.close.side_effect = RuntimeError("already closed")
    pool = ProxyPool([], lambda **kwargs: session)
    parent_event = {"request_id": "parent"}
    token = attempt_context.set(parent_event)
    try:
        await pool._check_proxy(
            "http://proxy.test:80", "https://example.test/catalog", {}, asyncio.Semaphore(1), 1, persist_failure=False
        )
        assert attempt_context.get() is parent_event
        assert pool.checked == 1
        assert pool._failure_counts["http://proxy.test:80"] == 1
        assert pool.metrics.samples[-1]["error_class"] == "OSError"
        session.close.assert_awaited_once()
    finally:
        attempt_context.reset(token)
        await pool.close()


@pytest.mark.asyncio
async def test_cancelled_readiness_releases_slots_without_penalizing_the_proxy():
    proxy_url = "http://proxy.test:80"
    started = asyncio.Event()
    restored_contexts = []
    session = AsyncMock()

    async def post(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    session.post.side_effect = post
    pool = ProxyPool([], lambda **kwargs: session)
    pool._scheduled.add(proxy_url)
    parent_event = {"request_id": "parent"}
    token = attempt_context.set(parent_event)

    async def check():
        try:
            await pool._check_proxy(
                proxy_url, "https://example.test/catalog", {}, asyncio.Semaphore(1), 1, persist_failure=False
            )
        finally:
            restored_contexts.append(attempt_context.get())

    task = asyncio.create_task(check())
    try:
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        session.close.assert_awaited_once()
        assert pool.checked == 1
        assert not pool._scheduled
        assert not pool._failure_counts
        assert pool.budget.active == 0
        assert pool._host_in_flight["proxy.test"] == 0
        assert pool.metrics.samples[-1]["error_class"] == "CancelledError"
        assert restored_contexts == [parent_event]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        attempt_context.reset(token)
        await pool.close()


@pytest.mark.asyncio
async def test_duplicate_readiness_keeps_the_original_session_and_closes_the_new_one():
    proxy_url = "http://proxy.test:80"
    original, duplicate = AsyncMock(), AsyncMock()
    duplicate.post.return_value = Response(200, {"finalizado": True, "datos": []})
    pool = ProxyPool([], lambda **kwargs: duplicate)
    pool.add_session(proxy_url, original)
    try:
        await pool._check_proxy(
            proxy_url, "https://example.test/catalog", {}, asyncio.Semaphore(1), 1, persist_failure=False
        )
        assert pool.sessions == [original]
        original.close.assert_not_awaited()
        duplicate.close.assert_awaited_once()
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
    pool.sessions = [first, second]
    client = HttpClient("https://example.test", retry_backoff_factor=0, retry_jitter_seconds=0)
    client._proxy_pool = pool
    assert await client._request("GET", "/test") == {"ok": True}
    assert pool.sessions == [second]
    assert pool.retired == []
    first.close.assert_awaited_once()
    assert await client._request("GET", "/test") == {"ok": True}
    assert first.request.await_count == 1
    await client.close()
    first.close.assert_awaited_once()
    second.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_exhausted_proxy_pool_does_not_fall_back_to_direct_connection():
    from curl_cffi.requests.exceptions import ConnectionError

    from src.client.errors import HTTPClientError

    session = AsyncMock()
    session.request.side_effect = ConnectionError("failed")
    pool = ProxyPool([], lambda **kwargs: None)
    pool.sessions = [session]
    client = HttpClient("https://example.test", proxy_mode="always", retry_backoff_factor=0, retry_jitter_seconds=0)
    client._proxy_pool = pool
    with pytest.raises(HTTPClientError, match="pool is empty"):
        await client._request("GET", "/test")
    assert client._session is None
    await client.close()
