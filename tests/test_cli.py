"""CLI parsing and orchestration tests (no network access)."""

import argparse
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

from src import cli
from src.cli import _execute_scrape, build_parser
from src.storage.proxy_database import ProxyDatabase, VerifiedProxy


def test_defaults_run_one_incremental_pass():
    args = build_parser().parse_args([])

    assert args.year is None
    assert args.category == "all"
    assert args.collect_proxies is False


def test_year_category_and_collect_flag_are_parsed():
    args = build_parser().parse_args(["--year", "2025", "--category", "2"])

    assert args.year == 2025
    assert args.category == "2"
    assert build_parser().parse_args(["--collect-proxies"]).collect_proxies is True


@pytest.mark.parametrize("value", ["1800", "3000", "not-a-year"])
def test_year_is_validated(value):
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--year", value])


@pytest.mark.parametrize("option", ["--proxy-mode", "--proxy-db", "--schedule", "--audit-storage"])
def test_removed_operational_switches_are_rejected(option):
    with pytest.raises(SystemExit):
        build_parser().parse_args([option, "x"])


@pytest.mark.asyncio
async def test_execute_scrape_runs_every_selected_category_with_year_filter():
    calls = []

    class FakeScraper:
        async def run(self, **kwargs):
            calls.append(kwargs)
            return {"status": "COMPLETED", "items_scraped": 1, "items_skipped": 0, "items_failed": 0}

    args = argparse.Namespace(category="all", year=2025)

    reports = await _execute_scrape(FakeScraper(), args)

    assert len(reports) == 4
    assert [call["category"] for call in calls] == ["1", "2", "3", "4"]
    assert all(call["date_filter"] == "5" for call in calls)
    assert all(call["start_date"] == "2025-01-01" for call in calls)
    assert all(call["end_date"] == "2025-12-31" for call in calls)


class FakeProcess:
    pid = 4242

    def __init__(self, returncode=None):
        self.returncode = returncode
        self.terminate = Mock()
        self.kill = Mock()

    async def wait(self):
        if self.returncode is None:
            self.returncode = -15
        return self.returncode


def _fresh_proxy() -> VerifiedProxy:
    return VerifiedProxy("http://proxy.example:80", "UA", "aiohttp")


def _stale_proxy() -> VerifiedProxy:
    checked_at = (datetime.now(UTC) - timedelta(days=2)).isoformat()
    return VerifiedProxy("http://stale.example:80", "UA", "aiohttp", checked_at=checked_at)


@pytest.mark.asyncio
async def test_fresh_database_is_used_without_starting_the_collector(monkeypatch, tmp_path):
    db_path = tmp_path / "proxies.db"
    async with ProxyDatabase(db_path) as database:
        await database.replace_all([_fresh_proxy()])

    events = []

    class FakeScraper:
        def __init__(self, **kwargs):
            assert kwargs["client_config"].proxy.db_path == db_path
            events.append("scraper_open")

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            events.append("scraper_close")

    @asynccontextmanager
    async def unexpected_collector(path):
        pytest.fail("Collector must not run when the database is fresh")
        yield

    async def scrape(scraper, args):
        events.append("scrape")
        return [{"status": "COMPLETED"}]

    monkeypatch.setattr(cli.settings, "proxy_mode", "pool")
    monkeypatch.setattr(cli.settings, "proxy_db_path", db_path)
    monkeypatch.setattr(cli.settings, "proxy_min_ready", 1)
    monkeypatch.setattr(cli.settings, "proxy_db_max_age_hours", 24)
    monkeypatch.setattr(cli, "proxy_collector", unexpected_collector)
    monkeypatch.setattr(cli, "SeprecScraper", FakeScraper)
    monkeypatch.setattr(cli, "_execute_scrape", scrape)

    reports = await cli._run_once(build_parser().parse_args([]))

    assert reports == [{"status": "COMPLETED"}]
    assert events == ["scraper_open", "scrape", "scraper_close"]


@pytest.mark.asyncio
async def test_partial_fresh_database_is_used_without_waiting_for_more(monkeypatch, tmp_path):
    db_path = tmp_path / "proxies.db"
    async with ProxyDatabase(db_path) as database:
        await database.replace_all([_fresh_proxy()])

    class FakeScraper:
        def __init__(self, **kwargs):
            assert kwargs["client_config"].proxy.db_path == db_path

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    @asynccontextmanager
    async def unexpected_collector(path):
        pytest.fail("Collector must not run while any fresh proxies exist")
        yield

    async def scrape(scraper, args):
        return [{"status": "COMPLETED"}]

    monkeypatch.setattr(cli.settings, "proxy_mode", "pool")
    monkeypatch.setattr(cli.settings, "proxy_db_path", db_path)
    monkeypatch.setattr(cli.settings, "proxy_min_ready", 5)
    monkeypatch.setattr(cli.settings, "proxy_db_max_age_hours", 24)
    monkeypatch.setattr(cli, "proxy_collector", unexpected_collector)
    monkeypatch.setattr(cli, "SeprecScraper", FakeScraper)
    monkeypatch.setattr(cli, "_execute_scrape", scrape)

    assert await cli._run_once(build_parser().parse_args([])) == [{"status": "COMPLETED"}]


@pytest.mark.asyncio
async def test_stale_database_starts_bounded_collection_then_falls_back_to_direct(monkeypatch, tmp_path):
    db_path = tmp_path / "proxies.db"
    async with ProxyDatabase(db_path) as database:
        await database.replace_all([_stale_proxy()])

    events = []
    process = FakeProcess(returncode=1)

    @asynccontextmanager
    async def collector(path):
        events.append("collector_open")
        try:
            yield process
        finally:
            events.append("collector_close")

    class FakeScraper:
        def __init__(self, **kwargs):
            assert kwargs["client_config"].proxy.db_path is None
            events.append("scraper_open")

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            events.append("scraper_close")

    async def scrape(scraper, args):
        events.append("scrape")
        return [{"status": "COMPLETED"}]

    monkeypatch.setattr(cli.settings, "proxy_mode", "pool")
    monkeypatch.setattr(cli.settings, "proxy_db_path", db_path)
    monkeypatch.setattr(cli.settings, "proxy_min_ready", 1)
    monkeypatch.setattr(cli.settings, "proxy_db_max_age_hours", 24)
    monkeypatch.setattr(cli.settings, "proxy_startup_timeout_seconds", 0.1)
    monkeypatch.setattr(cli, "proxy_collector", collector)
    monkeypatch.setattr(cli, "SeprecScraper", FakeScraper)
    monkeypatch.setattr(cli, "_execute_scrape", scrape)

    reports = await cli._run_once(build_parser().parse_args([]))

    assert reports == [{"status": "COMPLETED"}]
    assert events == ["collector_open", "scraper_open", "scrape", "scraper_close", "collector_close"]


@pytest.mark.asyncio
async def test_empty_collection_fails_in_always_mode(monkeypatch, tmp_path):
    db_path = tmp_path / "proxies.db"
    process = FakeProcess(returncode=2)

    @asynccontextmanager
    async def collector(path):
        yield process

    monkeypatch.setattr(cli.settings, "proxy_mode", "always")
    monkeypatch.setattr(cli.settings, "proxy_db_path", db_path)
    monkeypatch.setattr(cli.settings, "proxy_min_ready", 1)
    monkeypatch.setattr(cli.settings, "proxy_db_max_age_hours", 24)
    monkeypatch.setattr(cli.settings, "proxy_startup_timeout_seconds", 0.1)
    monkeypatch.setattr(cli, "proxy_collector", collector)

    with pytest.raises(RuntimeError, match="no fresh verified proxies"):
        await cli._run_once(build_parser().parse_args([]))


@pytest.mark.asyncio
async def test_collector_is_terminated_when_startup_timeout_expires(monkeypatch, tmp_path):
    db_path = tmp_path / "proxies.db"
    process = FakeProcess(returncode=None)

    @asynccontextmanager
    async def collector(path):
        try:
            yield process
        finally:
            process.terminate()
            await process.wait()

    class FakeScraper:
        def __init__(self, **kwargs):
            assert kwargs["client_config"].proxy.db_path is None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    async def scrape(scraper, args):
        return [{"status": "COMPLETED"}]

    monkeypatch.setattr(cli.settings, "proxy_mode", "pool")
    monkeypatch.setattr(cli.settings, "proxy_db_path", db_path)
    monkeypatch.setattr(cli.settings, "proxy_min_ready", 1)
    monkeypatch.setattr(cli.settings, "proxy_db_max_age_hours", 24)
    monkeypatch.setattr(cli.settings, "proxy_startup_timeout_seconds", 0.05)
    monkeypatch.setattr(cli.settings, "proxy_collector_stop_timeout_seconds", 0.05)
    monkeypatch.setattr(cli, "proxy_collector", collector)
    monkeypatch.setattr(cli, "SeprecScraper", FakeScraper)
    monkeypatch.setattr(cli, "_execute_scrape", scrape)

    await cli._run_once(build_parser().parse_args([]))

    process.terminate.assert_called_once()
    assert process.returncode == -15


def test_only_category_and_year_options_remain():
    options = {action.dest for action in build_parser()._actions}
    assert options == {"help", "version", "year", "category", "collect_proxies"}


@pytest.mark.asyncio
async def test_collect_command_uses_configured_paths(monkeypatch):
    called = {}

    async def fake_collect(**kwargs):
        called.update(kwargs)
        return {"verified_combinations": 3}

    monkeypatch.setattr("src.services.proxy_collector.collect", fake_collect)

    assert await cli._collect_proxies() == cli.EXIT_SUCCESS
    assert called["db_path"] == cli.settings.proxy_db_path
    assert called["csv_path"] == cli.settings.proxy_list_path
    assert called["report_path"] == cli.settings.proxy_report_path


@pytest.mark.asyncio
async def test_collect_command_reports_failure_when_nothing_verifies(monkeypatch):
    async def fake_collect(**kwargs):
        return {"verified_combinations": 0}

    monkeypatch.setattr("src.services.proxy_collector.collect", fake_collect)

    assert await cli._collect_proxies() == cli.EXIT_FAILURE
