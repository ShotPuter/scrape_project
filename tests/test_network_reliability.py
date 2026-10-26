"""Regression checks for the audit findings. All networking is loopback-only."""

import asyncio
import time
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from test_client import local_api

from src.client.base_client import HttpClient, Route
from src.client.errors import HTTPClientError
from src.client.network import NetworkBudget, TimeoutPolicy, proxy_id
from src.client.proxy_pool import ProxyEndpoint, ProxyPool


class Response:
    status_code = 200
    headers = {}

    def json(self):
        return {"finalizado": True, "datos": []}


@pytest.mark.asyncio
async def test_incomplete_read_retries_another_proxy_and_closes_retired():
    first, second = AsyncMock(), AsyncMock()
    first.request.side_effect = asyncio.IncompleteReadError(b"", 2)
    second.request.return_value = Response()
    pool = ProxyPool([], lambda **kw: None)
    pool.add_session("http://one:80", first)
    pool.add_session("http://two:80", second)
    client = HttpClient("https://example.test", retry_backoff_factor=0, retry_jitter_seconds=0)
    client._proxy_pool = pool
    try:
        assert await client._request("GET", "publicaciones/1") == Response().json()
        assert client.retries_made == 1
        first.close.assert_awaited_once()
        assert not pool.retired
        assert pool._failure_counts["http://one:80"] == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_cooldown_wakes_empty_pool_and_readiness_preserves_failures():
    replacement = AsyncMock()
    replacement.post.return_value = Response()
    pool = ProxyPool(["http://one:80"], lambda **kw: replacement, failure_cooldown_seconds=0.02, replenish_threshold=1)
    pool.configure_warmup("https://example.test/catalog", {}, 1, 0.1)
    original = AsyncMock()
    pool.add_session("http://one:80", original)
    pool.discard(original)
    try:
        async with pool.lease(1) as session:
            assert session is replacement
            assert pool._failure_counts["http://one:80"] == 1
            pool.record_runtime_success(session)
        assert "http://one:80" not in pool._failure_counts
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_leases_limit_same_ip_and_release_after_cancellation():
    pool = ProxyPool([], lambda **kw: None)
    one, two = AsyncMock(), AsyncMock()
    pool.add_session("http://same-host:80", one)
    pool.add_session("http://same-host:81", two)
    waiting_started = asyncio.Event()

    async def waiting():
        waiting_started.set()
        async with pool.lease(1):
            pytest.fail("same host must not have another active lease")

    try:
        async with pool.lease(1):
            task = asyncio.create_task(waiting())
            await waiting_started.wait()
            await asyncio.sleep(0.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert pool._host_in_flight["same-host"] == 1
        assert pool._host_in_flight["same-host"] == 0
        async with pool.lease(0.1):
            assert sum(pool.in_flight.values()) == 1
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_retired_session_closes_only_after_last_lease():
    session = AsyncMock()
    pool = ProxyPool([], lambda **kw: None)
    pool.add_session("http://one:80", session)
    async with pool.lease(0.1):
        pool.discard(session)
        session.close.assert_not_awaited()
    session.close.assert_awaited_once()
    assert not pool.retired
    await pool.close()
    session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_probes_share_global_budget_with_downloads():
    session = AsyncMock()
    session.post.return_value = Response()
    budget = NetworkBudget(1)
    pool = ProxyPool(["http://one:80"], lambda **kw: session, network_budget=budget)
    try:
        async with budget.slot():
            task = pool.start_warmup("https://example.test/catalog", {}, 2, 0.2)
            await asyncio.sleep(0.02)
            session.post.assert_not_awaited()
            assert budget.active == 1
        await task
        session.post.assert_awaited_once()
        assert budget.active == 0
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_rate_and_origin_pause_apply_to_all_slots():
    budget = NetworkBudget(3, requests_per_second=100)
    starts = []

    async def request():
        async with budget.slot():
            starts.append(time.monotonic())

    budget.pause(0.02)
    started = time.monotonic()
    await asyncio.gather(*(request() for _ in range(3)))
    assert starts[0] - started >= 0.015
    assert starts[-1] - starts[0] >= 0.015
    assert budget.active == 0


@pytest.mark.asyncio
async def test_document_total_is_separate_from_api_timeout():
    async def slow(request):
        await asyncio.sleep(0.06)
        return web.json_response({"ok": True})

    app = web.Application()
    app.router.add_get("/api/publicaciones/1", slow)
    app.router.add_post("/api/publicaciones", slow)
    async with (
        local_api(app) as base_url,
        HttpClient(
            base_url,
            retry_attempts=1,
            api_timeout_policy=TimeoutPolicy(total=0.02),
            detail_timeout_policy=TimeoutPolicy(total=0.3),
        ) as client,
    ):
        assert await client._request("GET", "publicaciones/1") == {"ok": True}
        with pytest.raises(HTTPClientError):
            await client._request("POST", "publicaciones")
        assert client.metrics.samples[-1]["timeout_kind"] == "total"


@pytest.mark.asyncio
async def test_read_inactivity_is_identified_separately():
    async def slow_body(request):
        response = web.StreamResponse(headers={"Content-Type": "application/json"})
        await response.prepare(request)
        await response.write(b'{"ok":')
        await asyncio.sleep(0.1)
        return response

    app = web.Application()
    app.router.add_get("/api/publicaciones/1", slow_body)
    async with (
        local_api(app) as base_url,
        HttpClient(base_url, retry_attempts=1, detail_timeout_policy=TimeoutPolicy(total=0.5, sock_read=0.02)) as client,
    ):
        with pytest.raises(HTTPClientError):
            await client._request("GET", "publicaciones/1")
        event = client.metrics.samples[-1]
        assert event["timeout_kind"] == "read_inactivity"
        assert event["error_stage"] == "body_read"


@pytest.mark.asyncio
async def test_traces_measure_full_body_and_decode_without_credentials(caplog):
    async def handler(request):
        return web.json_response({"ok": True})

    app = web.Application()
    app.router.add_get("/api/publicaciones/1", handler)
    async with local_api(app) as base_url, HttpClient(base_url) as client:
        await client._request("GET", "publicaciones/1")
        event = client.metrics.samples[-1]
        assert event["response_bytes"] > 0
        assert event["connection_setup_ms"] >= 0
        assert event["first_body_chunk_ms"] >= event["response_headers_ms"]
        assert event["body_read_ms"] >= 0
        assert event["json_decode_ms"] >= 0
    assert "secret" not in proxy_id("http://user:secret@host:80")


@pytest.mark.asyncio
async def test_no_warmup_still_allows_runtime_recovery(tmp_path):
    from unittest.mock import Mock

    source = tmp_path / "proxies.csv"
    source.write_text("alive,protocol,proxy\ntrue,http,proxy.test:80\n")
    first, second = AsyncMock(), AsyncMock()
    first.request.side_effect = asyncio.IncompleteReadError(b"", 2)
    second.post.return_value = second.request.return_value = Response()
    client = HttpClient(
        "https://example.test",
        proxy_file=source,
        proxy_check_path="catalog",
        proxy_warmup=False,
        proxy_min_ready=1,
        proxy_replenish_threshold=1,
        proxy_failure_cooldown_seconds=0.01,
        retry_backoff_factor=0,
        retry_jitter_seconds=0,
    )
    client._session_factory.create = Mock(side_effect=[first, second])
    try:
        assert await client._request("GET", "test") == Response().json()
        second.post.assert_awaited_once()
        assert client.retries_made == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_runtime_failure_threshold_survives_multiple_readiness_successes(tmp_path):
    dead_path = tmp_path / "proxies.dead.json"
    pool = ProxyPool(["http://one:80"], lambda **kw: None, max_failures=3, dead_path=dead_path)
    try:
        for number in range(3):
            session = AsyncMock()
            assert pool.add_session("http://one:80", session)
            pool.discard(session)
            await asyncio.sleep(0)
            assert pool._failure_counts["http://one:80"] == number + 1
        assert "http://one:80" in pool._permanent_dead
        assert not pool.add_session("http://one:80", AsyncMock())
    finally:
        await pool.close()
    assert not dead_path.exists()


@pytest.mark.asyncio
async def test_proxy_credentials_are_not_logged(caplog):
    import logging

    from curl_cffi.requests.exceptions import ConnectionError

    secret_url = "http://private-user:private-password@proxy.test:80"
    failed = AsyncMock()
    failed.request.side_effect = ConnectionError(f"Failed to connect to {secret_url}")
    pool = ProxyPool([], lambda **kw: None)
    pool.add_session(secret_url, failed)
    client = HttpClient("https://example.test", retry_attempts=1)
    client._proxy_pool = pool
    with caplog.at_level(logging.DEBUG):
        try:
            with pytest.raises(HTTPClientError):
                await client._request("GET", "test")
        finally:
            await client.close()
    assert "private-password" not in caplog.text
    assert "private-user" not in caplog.text
    assert "proxy.test" not in caplog.text


@pytest.mark.asyncio
async def test_global_slot_wait_is_outside_request_timeout():
    client = HttpClient("https://example.test", connection_limit=1, api_timeout_policy=TimeoutPolicy(total=0.01))
    client._session = AsyncMock()
    client._session.request.return_value = Response()
    try:
        async with client.budget.slot():
            task = asyncio.create_task(client._request("GET", "test"))
            await asyncio.sleep(0.04)
            assert not task.done()
        await task
        assert client.metrics.samples[-1]["global_slot_wait_ms"] >= 30
    finally:
        await client.close()


class FakePool:
    """Minimal pool interface used by the direct-first fallback routing tests."""

    def __init__(self, session):
        self.session = session

    @asynccontextmanager
    async def lease(self, timeout, avoid=None):  # noqa: ASYNC109 - mirrors ProxyPool.lease
        yield self.session

    sessions: list = [object()]

    def record_runtime_success(self, session) -> None:
        pass

    def latency_for(self, session) -> float:
        return 0.0

    def discard(self, session) -> bool:
        return False

    async def close(self) -> None:
        pass


class FakeResponse:
    def __init__(self, status_code=200, headers=None, payload=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._payload = payload if payload is not None else {"finalizado": True, "datos": []}

    def json(self):
        return self._payload


def fallback_client(direct, proxied, **overrides) -> HttpClient:
    client = HttpClient(
        "https://example.test",
        proxy_mode="fallback",
        retry_attempts=overrides.pop("attempts", 3),
        retry_backoff_factor=0,
        retry_jitter_seconds=0,
        **overrides,
    )
    client._session = direct
    client._proxy_pool = FakePool(proxied)
    return client


class EmptyPool(FakePool):
    sessions: list = []


class UnusablePool(FakePool):
    sessions: list = []

    @asynccontextmanager
    async def lease(self, timeout, avoid=None):  # noqa: ASYNC109 - mirrors ProxyPool.lease
        raise HTTPClientError("All proxies have failed; working proxy pool is empty")
        yield


@pytest.mark.asyncio
async def test_fallback_prefers_direct_requests():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.return_value = FakeResponse()
    client = fallback_client(direct, proxied)
    try:
        assert await client._request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
        assert direct.request.await_count == 1
        assert proxied.request.await_count == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_fallback_routes_to_proxies_after_direct_403():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.return_value = FakeResponse(403)
    proxied.request.return_value = FakeResponse()
    client = fallback_client(direct, proxied, attempts=2)
    try:
        assert await client._request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
        assert client.retries_made == 1
        assert direct.request.await_count == 1
        assert proxied.request.await_count == 1
        assert client._direct_blocked_until > time.monotonic()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_fallback_recovers_direct_after_the_cooldown_expires():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.side_effect = [FakeResponse(403), FakeResponse()]
    proxied.request.return_value = FakeResponse()
    client = fallback_client(direct, proxied, attempts=3)
    try:
        await client._request("GET", "publicaciones/1")
        assert client._direct_blocked_until > time.monotonic()
        client._direct_blocked_until = time.monotonic() - 1
        assert await client._request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
        assert direct.request.await_count == 2
        assert proxied.request.await_count == 1
        assert client._direct_blocked_until == 0.0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_direct_429_cools_the_route_without_pausing_the_pool():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.return_value = FakeResponse(429, headers={"Retry-After": "0.3"})
    proxied.request.return_value = FakeResponse()
    client = fallback_client(direct, proxied, attempts=2)
    try:
        assert await client._request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
        assert client.retries_made == 1
        assert client.budget._paused_until <= time.monotonic()
        assert client._direct_blocked_until > time.monotonic()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_cloudflare_challenge_on_direct_is_retried_via_proxies():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.return_value = FakeResponse(403, headers={"cf-mitigated": "challenge"})
    proxied.request.return_value = FakeResponse()
    client = fallback_client(direct, proxied, attempts=2)
    try:
        assert await client._request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
        assert direct.request.await_count == 1
        assert proxied.request.await_count == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_cloudflare_challenge_stays_final_without_a_pool():
    direct = AsyncMock()
    direct.request.return_value = FakeResponse(403, headers={"cf-mitigated": "challenge"})
    client = HttpClient("https://example.test", retry_attempts=2, retry_backoff_factor=0, retry_jitter_seconds=0)
    client._session = direct
    with pytest.raises(HTTPClientError) as exc_info:
        await client._request("GET", "publicaciones/1")
    assert not exc_info.value.retryable
    assert client.retries_made == 0


@pytest.mark.asyncio
async def test_three_direct_network_failures_cool_the_route_down():
    direct, proxied = AsyncMock(), AsyncMock()
    client = fallback_client(direct, proxied, attempts=4)
    # First three attempts fail fast on direct (connect-level stage), the fourth
    # goes through the pool once the direct route has cooled down.
    direct.request.side_effect = [TimeoutError(), TimeoutError(), TimeoutError(), None]
    proxied.request.return_value = FakeResponse()
    try:
        assert await client._request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
        assert direct.request.await_count == 3
        assert proxied.request.await_count == 1
        assert client.retries_made == 3
        assert client._direct_blocked_until > time.monotonic()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_mix_share_routes_share_of_attempts_through_pool():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.return_value = FakeResponse()
    proxied.request.return_value = FakeResponse()
    client = fallback_client(direct, proxied, proxy_mix_share=1)
    try:
        assert client._route() is Route.POOL
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_zero_mix_share_keeps_all_attempts_direct():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.return_value = FakeResponse()
    client = fallback_client(direct, proxied)
    try:
        assert client._route() is Route.DIRECT
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_inflight_direct_success_does_not_cancel_an_active_block():
    direct, proxied = AsyncMock(), AsyncMock()
    client = fallback_client(direct, proxied)
    try:
        client._direct_blocked_until = time.monotonic() + 60
        client._direct_block_kind = "origin"
        client._note_direct_success()
        assert client._direct_blocked_until > time.monotonic()
        assert client._route() is Route.POOL
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_pool_mode_prefers_the_proxy_pool():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.return_value = FakeResponse()
    proxied.request.return_value = FakeResponse()
    client = HttpClient("https://example.test", proxy_mode="pool", retry_backoff_factor=0, retry_jitter_seconds=0)
    client._session = direct
    client._proxy_pool = FakePool(proxied)
    try:
        assert client._route() is Route.POOL
        await client._request("GET", "publicaciones/1")
        assert proxied.request.await_count == 1
        assert direct.request.await_count == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_pool_mode_uses_direct_when_the_pool_is_empty():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.return_value = FakeResponse()
    client = HttpClient("https://example.test", proxy_mode="pool", retry_backoff_factor=0, retry_jitter_seconds=0)
    client._session = direct
    client._proxy_pool = UnusablePool(proxied)
    try:
        assert client._route() is Route.POOL
        assert await client._request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
        assert direct.request.await_count == 1
        assert client.metrics.counts["direct_attempts"] == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_fallback_blocked_with_empty_pool_does_not_use_direct():
    direct, proxied = AsyncMock(), AsyncMock()
    client = fallback_client(direct, proxied, attempts=1)
    client._proxy_pool = UnusablePool(proxied)
    client._direct_blocked_until = time.monotonic() + 60
    client._direct_block_kind = "origin"
    try:
        with pytest.raises(HTTPClientError):
            await client._request("GET", "publicaciones/1")
        assert direct.request.await_count == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_network_block_with_empty_pool_keeps_trying_direct():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.side_effect = TimeoutError()
    client = HttpClient(
        "https://example.test", proxy_mode="fallback", retry_attempts=3, retry_backoff_factor=0, retry_jitter_seconds=0
    )
    client._session = direct
    client._proxy_pool = EmptyPool(proxied)
    try:
        with pytest.raises(HTTPClientError):
            await client._request("GET", "publicaciones/1")
        assert proxied.request.await_count == 0
        assert direct.request.await_count == 3
        assert client._direct_block_kind == "network"
    finally:
        await client.close()


def test_slow_proxy_gets_a_larger_timeout_policy():
    session = object()
    pool = ProxyPool([], lambda **kw: None)
    pool.add_session(ProxyEndpoint("http://slow:80", latency_ms=10_000), session)
    client = HttpClient("https://example.test")
    client._proxy_pool = pool
    base = TimeoutPolicy(total=30, connect=10, sock_connect=10, sock_read=30)

    scaled = client._timeout_policy_for(session, base)

    assert scaled.total == 60
    assert scaled.connect == 15
    assert scaled.sock_connect == 15
    assert scaled.sock_read == 30


def test_fast_proxy_keeps_the_base_timeout_policy():
    session = object()
    pool = ProxyPool([], lambda **kw: None)
    pool.add_session(ProxyEndpoint("http://fast:80", latency_ms=1_000), session)
    client = HttpClient("https://example.test")
    client._proxy_pool = pool
    base = TimeoutPolicy(total=30, connect=10, sock_connect=10, sock_read=30)

    assert client._timeout_policy_for(session, base) is base
