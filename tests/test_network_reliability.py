"""Regression checks for the audit findings. All networking is loopback-only.

Private-state setup is used only to inject deterministic routing and retry failures;
assertions about request outcomes go through the public client surface.
"""

import asyncio
import json
import time
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from test_client import local_api

from src.client.base_client import HttpClient
from src.client.config import ClientConfig, ProxyConfig, RetryConfig, TimeoutConfig
from src.network.budget import NetworkBudget, TimeoutPolicy, proxy_id
from src.network.errors import HTTPClientError
from src.proxy.pool import ProxyPool, WarmupSpec

NO_WAIT = RetryConfig(backoff_factor=0.0, jitter_seconds=0.0)


class Response:
    status_code = 200
    headers = {}
    content = json.dumps({"finalizado": True, "datos": []}).encode()

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
    client = HttpClient(ClientConfig(base_url="https://example.test", retry=NO_WAIT))
    client._proxy_pool = pool
    try:
        assert await client.request("GET", "publicaciones/1") == Response().json()
        assert client.retries_made == 1
        first.close.assert_awaited_once()
        assert not pool.retired
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_cooldown_wakes_empty_pool_and_readiness_preserves_failures():
    replacement = AsyncMock()
    replacement.post.return_value = Response()
    pool = ProxyPool(["http://one:80"], lambda **kw: replacement, failure_cooldown_seconds=0.02, replenish_threshold=1)
    pool.configure_warmup(WarmupSpec("https://example.test/catalog", {}, 1, 0.1))
    original = AsyncMock()
    pool.add_session("http://one:80", original)
    pool.discard(original)
    try:
        async with pool.lease(1) as session:
            assert session is replacement
            pool.record_runtime_success(session)
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
            task = pool.start_warmup(WarmupSpec("https://example.test/catalog", {}, 2, 0.2))
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
            ClientConfig(
                base_url=base_url,
                retry=RetryConfig(attempts=1, backoff_factor=0.0, jitter_seconds=0.0),
                timeouts=TimeoutConfig(api=TimeoutPolicy(total=0.02), detail=TimeoutPolicy(total=0.3)),
            )
        ) as client,
    ):
        assert await client.request("GET", "publicaciones/1", endpoint_kind="detail") == {"ok": True}
        with pytest.raises(HTTPClientError):
            await client.request("POST", "publicaciones")
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
        HttpClient(
            ClientConfig(
                base_url=base_url,
                retry=RetryConfig(attempts=1, backoff_factor=0.0, jitter_seconds=0.0),
                timeouts=TimeoutConfig(detail=TimeoutPolicy(total=0.5, sock_read=0.02)),
            )
        ) as client,
    ):
        with pytest.raises(HTTPClientError):
            await client.request("GET", "publicaciones/1", endpoint_kind="detail")
        event = client.metrics.samples[-1]
        assert event["timeout_kind"] == "read_inactivity"
        assert event["error_stage"] == "body_read"


@pytest.mark.asyncio
async def test_traces_measure_full_body_and_decode_without_credentials(caplog):
    async def handler(request):
        return web.json_response({"ok": True})

    app = web.Application()
    app.router.add_get("/api/publicaciones/1", handler)
    async with local_api(app) as base_url, HttpClient(ClientConfig(base_url=base_url, retry=NO_WAIT)) as client:
        await client.request("GET", "publicaciones/1")
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
        ClientConfig(
            base_url="https://example.test",
            retry=NO_WAIT,
            proxy=ProxyConfig(
                file=source,
                check_path="catalog",
                warmup=False,
                min_ready=1,
                replenish_threshold=1,
                failure_cooldown_seconds=0.01,
            ),
        )
    )
    client._session_factory.create = Mock(side_effect=[first, second])
    try:
        assert await client.request("GET", "test") == Response().json()
        second.post.assert_awaited_once()
        assert client.retries_made == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_runtime_failure_threshold_survives_multiple_readiness_successes(tmp_path):
    dead_path = tmp_path / "proxies.dead.json"
    pool = ProxyPool(["http://one:80"], lambda **kw: None, max_failures=3, dead_path=dead_path)
    try:
        for _ in range(3):
            session = AsyncMock()
            assert pool.add_session("http://one:80", session)
            pool.discard(session)
            await asyncio.sleep(0)
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
    client = HttpClient(
        ClientConfig(base_url="https://example.test", retry=RetryConfig(attempts=1, backoff_factor=0.0, jitter_seconds=0.0))
    )
    client._proxy_pool = pool
    with caplog.at_level(logging.DEBUG):
        try:
            with pytest.raises(HTTPClientError):
                await client.request("GET", "test")
        finally:
            await client.close()
    assert "private-password" not in caplog.text
    assert "private-user" not in caplog.text
    assert "proxy.test" not in caplog.text


@pytest.mark.asyncio
async def test_global_slot_wait_is_outside_request_timeout():
    client = HttpClient(
        ClientConfig(
            base_url="https://example.test",
            retry=NO_WAIT,
            connection_limit=1,
            timeouts=TimeoutConfig(api=TimeoutPolicy(total=0.01)),
        )
    )
    client._session = AsyncMock()
    client._session.request.return_value = Response()
    try:
        async with client.budget.slot():
            task = asyncio.create_task(client.request("GET", "test"))
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

    @property
    def content(self):
        return json.dumps(self._payload).encode()

    def json(self):
        return self._payload


def fallback_client(
    direct, proxied, *, attempts: int = 3, mix_share: float = 0, direct_block_cooldown_seconds: float = 300
) -> HttpClient:
    client = HttpClient(
        ClientConfig(
            base_url="https://example.test",
            retry=RetryConfig(attempts=attempts, backoff_factor=0.0, jitter_seconds=0.0),
            proxy=ProxyConfig(
                mode="fallback", mix_share=mix_share, direct_block_cooldown_seconds=direct_block_cooldown_seconds
            ),
        )
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
        assert await client.request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
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
        assert await client.request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
        assert client.retries_made == 1
        assert direct.request.await_count == 1
        assert proxied.request.await_count == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_fallback_recovers_direct_after_the_cooldown_expires():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.side_effect = [FakeResponse(403), FakeResponse()]
    proxied.request.return_value = FakeResponse()
    client = fallback_client(direct, proxied, attempts=3, direct_block_cooldown_seconds=0.01)
    try:
        await client.request("GET", "publicaciones/1")
        await asyncio.sleep(0.02)
        assert await client.request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
        assert direct.request.await_count == 2
        assert proxied.request.await_count == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_direct_429_cools_the_route_without_pausing_the_pool():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.return_value = FakeResponse(429, headers={"Retry-After": "0.3"})
    proxied.request.return_value = FakeResponse()
    client = fallback_client(direct, proxied, attempts=2)
    try:
        assert await client.request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
        assert client.retries_made == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_cloudflare_challenge_on_direct_is_retried_via_proxies():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.return_value = FakeResponse(403, headers={"cf-mitigated": "challenge"})
    proxied.request.return_value = FakeResponse()
    client = fallback_client(direct, proxied, attempts=2)
    try:
        assert await client.request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
        assert direct.request.await_count == 1
        assert proxied.request.await_count == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_cloudflare_challenge_stays_final_without_a_pool():
    direct = AsyncMock()
    direct.request.return_value = FakeResponse(403, headers={"cf-mitigated": "challenge"})
    client = HttpClient(
        ClientConfig(base_url="https://example.test", retry=RetryConfig(attempts=2, backoff_factor=0.0, jitter_seconds=0.0))
    )
    client._session = direct
    with pytest.raises(HTTPClientError) as exc_info:
        await client.request("GET", "publicaciones/1")
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
        assert await client.request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
        assert direct.request.await_count == 3
        assert proxied.request.await_count == 1
        assert client.retries_made == 3
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_mix_share_routes_share_of_attempts_through_pool():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.return_value = FakeResponse()
    proxied.request.return_value = FakeResponse()
    client = fallback_client(direct, proxied, mix_share=1)
    try:
        assert await client.request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
        assert proxied.request.await_count == 1
        assert direct.request.await_count == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_zero_mix_share_keeps_all_attempts_direct():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.return_value = FakeResponse()
    client = fallback_client(direct, proxied)
    try:
        assert await client.request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
        assert direct.request.await_count == 1
        assert proxied.request.await_count == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_pool_mode_prefers_the_proxy_pool():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.return_value = FakeResponse()
    proxied.request.return_value = FakeResponse()
    client = HttpClient(ClientConfig(base_url="https://example.test", retry=NO_WAIT, proxy=ProxyConfig(mode="pool")))
    client._session = direct
    client._proxy_pool = FakePool(proxied)
    try:
        await client.request("GET", "publicaciones/1")
        assert proxied.request.await_count == 1
        assert direct.request.await_count == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_pool_mode_uses_direct_when_the_pool_is_empty():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.return_value = FakeResponse()
    client = HttpClient(ClientConfig(base_url="https://example.test", retry=NO_WAIT, proxy=ProxyConfig(mode="pool")))
    client._session = direct
    client._proxy_pool = UnusablePool(proxied)
    try:
        assert await client.request("GET", "publicaciones/1") == {"finalizado": True, "datos": []}
        assert direct.request.await_count == 1
        assert client.metrics.counts["direct_attempts"] == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_network_block_with_empty_pool_keeps_trying_direct():
    direct, proxied = AsyncMock(), AsyncMock()
    direct.request.side_effect = TimeoutError()
    client = HttpClient(
        ClientConfig(
            base_url="https://example.test",
            retry=RetryConfig(attempts=3, backoff_factor=0.0, jitter_seconds=0.0),
            proxy=ProxyConfig(mode="fallback"),
        )
    )
    client._session = direct
    client._proxy_pool = EmptyPool(proxied)
    try:
        with pytest.raises(HTTPClientError):
            await client.request("GET", "publicaciones/1")
        assert proxied.request.await_count == 0
        assert direct.request.await_count == 3
    finally:
        await client.close()
