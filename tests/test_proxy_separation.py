"""Contract tests for proxy independence and caller-supplied target checks."""

import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from config import Settings
from src.client.seprec_client import SeprecClient
from src.network.transport import Response
from src.proxy import checker
from src.proxy.config import CollectorConfig
from src.proxy.pool import ProxyPool, WarmupSpec
from src.proxy.probes import ProbeRequest, TargetProbe
from src.services.proxy_collector import collector_config_from_settings
from src.services.seprec_proxy_probe import seprec_target_probe


def test_proxy_core_imports_without_scraper_configuration(tmp_path):
    env = {
        **os.environ,
        "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
        "MAX_CONCURRENT_REQUESTS": "invalid-if-application-settings-are-loaded",
    }
    code = """
import sys
import src.proxy.checker
import src.proxy.collector
import src.proxy.config
import src.proxy.pool
import src.proxy.repository
import src.proxy.sources.catalog
import src.proxy.sources.free_proxy
import src.proxy.sources.iplocate
assert 'config' not in sys.modules
assert 'src.models.publication' not in sys.modules
assert 'src.client.seprec_client' not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=10, check=False
    )
    assert result.returncode == 0, result.stderr
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("second_response_ok", [True, False])
async def test_checker_executes_custom_target_profile_and_closes_session(monkeypatch, second_response_ok):
    session = AsyncMock()
    session.request.side_effect = [
        Response(200, {}, b'{"ready":true}'),
        Response(200, {}, b'{"count":1}' if second_response_ok else b'{"unexpected":true}'),
    ]
    session_options = []

    def factory(*args, **kwargs):
        session_options.append(kwargs)
        return session

    monkeypatch.setattr(checker, "_session_for", factory)
    target = TargetProbe(
        (
            ProbeRequest("GET", "https://other.example/health", lambda data: data.get("ready") is True),
            ProbeRequest("POST", "https://other.example/search", lambda data: "count" in data, {"limit": 1}),
        )
    )
    result = await checker.verify_stage2(
        "http://proxy.example:80",
        transport="aiohttp",
        user_agent="custom-UA",
        target=target,
        attempts=1,
        headers={"X-Application": "other"},
        impersonate="firefox",
    )
    assert result.ok is second_response_ok
    assert result.error_type == ("" if second_response_ok else "unexpected_response")
    assert [(call.args, call.kwargs) for call in session.request.await_args_list] == [
        (("GET", "https://other.example/health"), {"json": None}),
        (("POST", "https://other.example/search"), {"json": {"limit": 1}}),
    ]
    assert session_options[0]["headers"] == {"X-Application": "other"}
    assert session_options[0]["impersonate"] == "firefox"
    session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_failed_probe_does_not_execute_remaining_requests(monkeypatch):
    session = AsyncMock()
    session.request.return_value = Response(403, {})
    monkeypatch.setattr(checker, "_session_for", lambda *args, **kwargs: session)
    target = TargetProbe(
        (
            ProbeRequest("GET", "https://other.example/first", lambda data: True),
            ProbeRequest("GET", "https://other.example/second", lambda data: True),
        )
    )
    result = await checker.verify_stage2(
        "http://proxy.example:80", transport="aiohttp", user_agent="UA", target=target, attempts=1
    )
    assert result.error_type == "http_403"
    session.request.assert_awaited_once()
    session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_pool_accepts_application_specific_readiness_validator():
    session = AsyncMock()
    session.post.return_value = Response(200, {}, b'{"service":"other"}')
    pool = ProxyPool(["http://proxy.example:80"], lambda **kwargs: session)
    try:
        await pool.start_warmup(
            WarmupSpec("https://other.example/health", validator=lambda response: response.json().get("service") == "other")
        )
        await pool.wait_until_ready()
        async with pool.lease(1) as leased:
            assert leased is session
    finally:
        await pool.close()
    session.close.assert_awaited_once()


def test_seprec_adapter_uses_one_search_and_strict_readiness():
    target = seprec_target_probe("https://example.test/api/")
    assert [request.url for request in target.requests] == ["https://example.test/api/publicaciones"]
    assert target.requests[0].payload["limite"] == 10
    assert target.requests[0].validator({"finalizado": True, "datos": {"total": 0, "filas": []}})
    assert not target.requests[0].validator({"finalizado": False, "datos": []})
    config = SeprecClient.config_from_settings(Settings())
    assert config.proxy.check_validator(Response(200, {}, b'{"finalizado":true,"datos":{"total":0,"filas":[]}}'))
    assert not config.proxy.check_validator(Response(200, {}, b'{"finalizado":true,"datos":[]}'))
    assert not config.proxy.check_validator(Response(200, {}, b'{"html":"challenge"}'))


def test_collector_configuration_copies_application_headers():
    settings = Settings(proxy_stage1_timeout=7, default_headers={"User-Agent": "original"})
    config = collector_config_from_settings(settings)
    settings.default_headers["User-Agent"] = "changed"
    assert config.default_headers == {"User-Agent": "original"}
    assert config.proxy_stage1_timeout == 7
    assert CollectorConfig().default_headers == {}


def test_target_profile_cannot_be_empty():
    with pytest.raises(ValueError, match="at least one request"):
        TargetProbe(())


@pytest.mark.parametrize("command", [["main.py"], ["collect_proxies.py"], ["-m", "src.proxy"]])
def test_cli_entry_points_remain_available(command):
    result = subprocess.run(
        [sys.executable, *command, "--help"],
        cwd=Path(__file__).resolve().parents[1],
        env={**os.environ, "LOG_TO_FILE": "false"},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout
    assert ("--category" if command == ["main.py"] else "--provider") in result.stdout
