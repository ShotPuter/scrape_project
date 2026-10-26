"""Stage-1 and stage-2 verifier tests."""

import json

import pytest
from aiohttp import ClientProxyConnectionError, ConnectionTimeoutError
from curl_cffi.requests.exceptions import ConnectionError as CurlConnectionError, Timeout as CurlTimeout

from src.client.network import NetworkBudget
from src.services import proxy_verifier
from src.services.proxy_verifier import (
    _AttemptOutcome,
    classify_exception,
    classify_status,
    retry_delay_for,
    should_retry,
    verify_stage1,
    verify_stage2,
)


def test_status_classification():
    assert classify_status(403) == "http_403"
    assert classify_status(429) == "http_429"
    assert classify_status(500) == "http_5xx"
    assert classify_status(502) == "http_5xx"
    assert classify_status(404) == "http_404"


def test_exception_classification():
    assert classify_exception(ConnectionTimeoutError()) == "connect_timeout"
    assert classify_exception(TimeoutError()) == "read_timeout"
    assert classify_exception(ConnectionResetError()) == "connection_reset"
    assert classify_exception(json.JSONDecodeError("bad", "", 0)) == "invalid_json"
    assert classify_exception(ClientProxyConnectionError(None, OSError("proxy"))) == "proxy_connection_error"
    assert classify_exception(CurlTimeout("Connection timed out after 5000 milliseconds")) == "connect_timeout"
    assert classify_exception(CurlConnectionError("CONNECT tunnel failed, response 502")) == "proxy_connection_error"


def test_retry_policy_distinguishes_error_classes():
    assert should_retry("http_503", 1, 3) is True
    assert should_retry("http_429", 2, 3) is True
    assert should_retry("http_403", 1, 3) is True
    assert should_retry("http_403", 2, 3) is False
    assert should_retry("ssl_error", 1, 3) is False
    assert retry_delay_for("http_429", 1, base_delay=0.5, retry_after=2, max_delay=5) == 2
    assert retry_delay_for("http_503", 2, base_delay=0.5, retry_after=None, max_delay=5) == 1.0


class _FakeSession:
    async def close(self):
        return None


class _FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


@pytest.mark.asyncio
async def test_stage1_requires_json_with_ip(monkeypatch):
    monkeypatch.setattr(proxy_verifier, "_session_for", lambda *args, **kwargs: _FakeSession())

    async def fake_request(*args, **kwargs):
        return _FakeResponse(200, {"ip": "203.0.113.10"})

    monkeypatch.setattr(proxy_verifier, "_request", fake_request)
    result = await verify_stage1(
        "http://proxy.example:8080", transport="aiohttp", user_agent="UA",
        timeout=1, connect_timeout=1, budget=NetworkBudget(1),
    )
    assert result.ok is True
    assert result.error_type == ""


@pytest.mark.asyncio
async def test_stage1_rejects_invalid_json(monkeypatch):
    monkeypatch.setattr(proxy_verifier, "_session_for", lambda *args, **kwargs: _FakeSession())

    async def fake_request(*args, **kwargs):
        return _FakeResponse(200, ValueError("not json"))

    monkeypatch.setattr(proxy_verifier, "_request", fake_request)
    result = await verify_stage1(
        "http://proxy.example:8080", transport="aiohttp", user_agent="UA",
        timeout=1, connect_timeout=1, budget=NetworkBudget(1),
    )
    assert result.ok is False
    assert result.error_type == "invalid_json"


@pytest.mark.asyncio
async def test_stage2_retries_transient_error_and_records_stats(monkeypatch):
    outcomes = [_AttemptOutcome(False, 5.0, "http_503", "upstream 503"), _AttemptOutcome(True, 10.0)]

    monkeypatch.setattr(proxy_verifier, "_session_for", lambda *args, **kwargs: _FakeSession())

    async def fake_attempt(*args, **kwargs):
        return outcomes.pop(0)

    monkeypatch.setattr(proxy_verifier, "_target_attempt", fake_attempt)
    result = await verify_stage2(
        "http://proxy.example:8080", transport="aiohttp", user_agent="UA",
        attempts=3, timeout=1, connect_timeout=1,
        retry_delay=0, max_retry_delay=0, budget=NetworkBudget(1),
    )

    assert result.ok is True
    assert result.stats.total_attempts == 2
    assert result.stats.successful_requests == 1
    assert result.stats.failed_requests == 1
    assert result.stats.success_rate == 0.5
    assert result.stats.average_latency_ms == 10.0
    assert result.stats.error_types == {"http_503": 1}
    assert result.stats.consecutive_failures == 0


@pytest.mark.asyncio
async def test_stage2_403_gets_only_one_retry(monkeypatch):
    outcomes = [
        _AttemptOutcome(False, 1.0, "http_403", "forbidden"),
        _AttemptOutcome(False, 1.0, "http_403", "forbidden"),
        _AttemptOutcome(True, 1.0),
    ]

    monkeypatch.setattr(proxy_verifier, "_session_for", lambda *args, **kwargs: _FakeSession())

    async def fake_attempt(*args, **kwargs):
        return outcomes.pop(0)

    monkeypatch.setattr(proxy_verifier, "_target_attempt", fake_attempt)
    result = await verify_stage2(
        "http://proxy.example:8080", transport="aiohttp", user_agent="UA",
        attempts=3, timeout=1, connect_timeout=1,
        retry_delay=0, max_retry_delay=0, budget=NetworkBudget(1),
    )

    assert result.ok is False
    assert result.stats.total_attempts == 2
    assert result.stats.error_types == {"http_403": 2}
