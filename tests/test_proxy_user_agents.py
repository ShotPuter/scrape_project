"""Bounded User-Agent fallback and persistence of the combination that worked."""

import json
from unittest.mock import Mock

import pytest

from config import Settings
from src.proxy import collector, user_agents
from src.proxy.checker import CheckStats, Stage2Result
from src.proxy.collector import Stage1Success
from src.proxy.config import CollectorConfig
from src.proxy.repository import ProxyDatabase
from src.proxy.sources.catalog import ProxyCandidate
from src.services.proxy_collector import build_parser
from src.services.seprec_proxy_probe import seprec_target_probe


class Generator:
    def __init__(self, values):
        self.values = iter(values)

    @property
    def random(self):
        return next(self.values)


def test_generation_preserves_override_and_filters_duplicates_and_invalid_headers(monkeypatch):
    factory = Mock(return_value=Generator(["explicit-UA", "\ninvalid", "UA-second", "UA-second", "UA-third"]))
    monkeypatch.setattr(user_agents, "_generator", factory)
    values = user_agents.generate_user_agents(limit=3, preferred="explicit-UA", profile="firefox135")
    assert values == ["explicit-UA", "UA-second", "UA-third"]
    factory.assert_called_once_with("Firefox")


def test_duplicate_only_generator_is_bounded(monkeypatch):
    generator = Mock()
    generator.random = "one-UA"
    monkeypatch.setattr(user_agents, "_generator", lambda browser: generator)
    assert user_agents.generate_user_agents(limit=3) == ["one-UA"]


def test_missing_dataset_falls_back_without_failing_collection(monkeypatch):
    factory = Mock(side_effect=RuntimeError("dataset not available"))
    monkeypatch.setattr(user_agents, "_generator", factory)
    assert user_agents.generate_user_agents(limit=3, preferred="configured-UA") == ["configured-UA"]
    assert user_agents.generate_user_agents(limit=3, profile="safari") == [user_agents.FALLBACK_USER_AGENTS["Safari"]]


def test_one_configured_header_does_not_initialize_generator(monkeypatch):
    factory = Mock(side_effect=AssertionError("generation not needed"))
    monkeypatch.setattr(user_agents, "_generator", factory)
    assert user_agents.generate_user_agents(limit=1, preferred="configured-UA") == ["configured-UA"]
    factory.assert_not_called()


def test_real_package_generates_bounded_desktop_chrome_headers():
    values = user_agents.generate_user_agents(limit=3, profile="chrome")
    assert 1 <= len(values) <= 3
    assert len(set(values)) == len(values)
    assert all("Chrome/" in value and "Mobile" not in value for value in values)


@pytest.mark.parametrize("limit", [0, 11])
def test_limits_are_validated_at_configuration_and_generation_boundaries(limit):
    with pytest.raises(ValueError):
        Settings(proxy_user_agent_limit=limit)
    with pytest.raises(ValueError):
        user_agents.generate_user_agents(limit=limit)


async def scan(monkeypatch, verify, limit=3, transports=("aiohttp", "curl_cffi")):
    monkeypatch.setattr(collector, "user_agents", lambda config: ["UA-first", "UA-second", "UA-third"])
    monkeypatch.setattr(collector, "verify_stage2", verify)
    success = Stage1Success(ProxyCandidate("http://proxy.example:80", "http", "test"), "aiohttp", "UA-first", 10)
    return await collector._stage2_scan(
        [success],
        config=CollectorConfig(proxy_user_agent_limit=limit),
        target=seprec_target_probe("https://target.example/api"),
        concurrency=1,
        timeout=1,
        requests_per_second=0,
        attempts=None,
        retry_delay=0,
        max_retry_delay=0,
        transports=transports,
    )


def result(ok=False, error="http_403"):
    return Stage2Result(
        ok, CheckStats(successful_requests=int(ok), failed_requests=int(not ok), total_attempts=1), "" if ok else error
    )


@pytest.mark.asyncio
async def test_successful_replacement_is_selected_and_scan_stops(monkeypatch):
    calls = []

    async def verify(url, **kwargs):
        calls.append((kwargs["transport"], kwargs["user_agent"]))
        return result(ok=kwargs["user_agent"] == "UA-second")

    checks = await scan(monkeypatch, verify)
    assert calls == [("aiohttp", "UA-first"), ("aiohttp", "UA-second")]
    record = collector._record_from_check(checks[-1], 1, 0)
    assert record.user_agent == "UA-second"
    assert record.transport == "aiohttp"


@pytest.mark.asyncio
@pytest.mark.parametrize("error", ["connect_timeout", "read_timeout", "proxy_connection_error", "http_429", "http_5xx"])
async def test_transport_failures_do_not_trigger_user_agent_matrix(monkeypatch, error):
    calls = []

    async def verify(url, **kwargs):
        calls.append((kwargs["transport"], kwargs["user_agent"]))
        return result(error=error)

    await scan(monkeypatch, verify)
    assert calls == [("aiohttp", "UA-first"), ("curl_cffi", "UA-first")]


@pytest.mark.asyncio
async def test_exhausted_user_agent_budget_is_bounded_per_transport(monkeypatch):
    calls = []

    async def verify(url, **kwargs):
        calls.append((kwargs["transport"], kwargs["user_agent"]))
        return result()

    await scan(monkeypatch, verify)
    assert calls == [
        (transport, agent) for transport in ("aiohttp", "curl_cffi") for agent in ("UA-first", "UA-second", "UA-third")
    ]


@pytest.mark.asyncio
async def test_single_user_agent_limit_disables_fallback(monkeypatch):
    calls = []

    async def verify(url, **kwargs):
        calls.append(kwargs["user_agent"])
        return result()

    await scan(monkeypatch, verify, limit=1, transports=("aiohttp",))
    assert calls == ["UA-first"]


@pytest.mark.asyncio
async def test_database_and_report_keep_successful_user_agent(tmp_path, monkeypatch):
    path = tmp_path / "proxies.csv"
    path.write_text("alive,protocol,proxy\ntrue,http,proxy.example:80\n")
    db = tmp_path / "proxies.db"
    report_path = tmp_path / "report.json"

    async def stage1(candidates, **kwargs):
        found = [Stage1Success(candidate, "aiohttp", "UA-first", 10) for candidate in candidates]
        for success in found:
            await kwargs["on_success"](success)
        return found, []

    async def verify(url, **kwargs):
        return result(ok=kwargs["user_agent"] == "UA-second")

    monkeypatch.setattr(collector, "_stage1_scan", stage1)
    monkeypatch.setattr(collector, "verify_stage2", verify)
    monkeypatch.setattr(collector, "user_agents", lambda config: ["UA-first", "UA-second", "UA-third"])
    await collector.collect(
        config=CollectorConfig(),
        target=seprec_target_probe("https://target.example/api"),
        csv_path=path,
        db_path=db,
        provider="iplocate",
        source_url="",
        report_path=report_path,
        requests_per_second=0,
    )
    async with ProxyDatabase(db) as database:
        proxies = await database.list_active()
    assert [proxy.user_agent for proxy in proxies] == ["UA-second"]
    checks = json.loads(report_path.read_text())["stage2"]["results"]
    assert [check["user_agent"] for check in checks] == ["UA-first", "UA-second"]
    assert checks[-1]["stage1_user_agent"] == "UA-first"
    assert checks[-1]["verified"] is True


def test_cli_accepts_user_agent_limit():
    assert build_parser().parse_args(["--user-agent-limit", "5"]).user_agent_limit == 5
