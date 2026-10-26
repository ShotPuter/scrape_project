"""CLI parsing and orchestration tests through the real ``cli.main`` entry point."""

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

from src import cli
from src.cli import build_parser
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


def _seed_database(path, proxies):
    async def seed():
        async with ProxyDatabase(path) as database:
            await database.replace_all(proxies)

    asyncio.run(seed())


def test_main_runs_every_selected_category_with_year_filter(monkeypatch):
    calls = []

    class FakeScraper:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            pass

        async def run(self, **kwargs):
            calls.append(kwargs)
            return {"status": "COMPLETED"}

    monkeypatch.setattr(cli, "SeprecScraper", FakeScraper)
    monkeypatch.setattr(cli.settings, "proxy_mode", "direct")

    assert cli.main(["--year", "2025", "--category", "all"]) == cli.EXIT_SUCCESS
    assert [call["category"] for call in calls] == ["1", "2", "3", "4"]
    assert all(call["date_filter"] == "5" for call in calls)
    assert all(call["start_date"] == "2025-01-01" for call in calls)
    assert all(call["end_date"] == "2025-12-31" for call in calls)


def test_fresh_database_is_used_without_starting_the_collector(monkeypatch, tmp_path):
    db_path = tmp_path / "proxies.db"
    _seed_database(db_path, [_fresh_proxy()])
    events = []

    class FakeScraper:
        def __init__(self, **kwargs):
            assert kwargs["client_config"].proxy.db_path == db_path
            events.append("scraper_open")

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            events.append("scraper_close")

        async def run(self, **_kwargs):
            events.append("scrape")
            return {"status": "COMPLETED"}

    @asynccontextmanager
    async def unexpected_collector(_path):
        pytest.fail("Collector must not run when the database is fresh")
        yield

    monkeypatch.setattr(cli.settings, "proxy_mode", "pool")
    monkeypatch.setattr(cli.settings, "proxy_db_path", db_path)
    monkeypatch.setattr(cli.settings, "proxy_min_ready", 1)
    monkeypatch.setattr(cli.settings, "proxy_db_max_age_hours", 24)
    monkeypatch.setattr(cli, "proxy_collector", unexpected_collector)
    monkeypatch.setattr(cli, "SeprecScraper", FakeScraper)

    assert cli.main(["--category", "1"]) == cli.EXIT_SUCCESS
    assert events == ["scraper_open", "scrape", "scraper_close"]


def test_partial_fresh_database_is_used_without_waiting_for_more(monkeypatch, tmp_path):
    db_path = tmp_path / "proxies.db"
    _seed_database(db_path, [_fresh_proxy()])

    class FakeScraper:
        def __init__(self, **kwargs):
            assert kwargs["client_config"].proxy.db_path == db_path

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            pass

        async def run(self, **_kwargs):
            return {"status": "COMPLETED"}

    @asynccontextmanager
    async def unexpected_collector(_path):
        pytest.fail("Collector must not run while any fresh proxies exist")
        yield

    monkeypatch.setattr(cli.settings, "proxy_mode", "pool")
    monkeypatch.setattr(cli.settings, "proxy_db_path", db_path)
    monkeypatch.setattr(cli.settings, "proxy_min_ready", 5)
    monkeypatch.setattr(cli.settings, "proxy_db_max_age_hours", 24)
    monkeypatch.setattr(cli, "proxy_collector", unexpected_collector)
    monkeypatch.setattr(cli, "SeprecScraper", FakeScraper)

    assert cli.main(["--category", "1"]) == cli.EXIT_SUCCESS


def test_stale_database_starts_bounded_collection_then_falls_back_to_direct(monkeypatch, tmp_path):
    db_path = tmp_path / "proxies.db"
    _seed_database(db_path, [_stale_proxy()])
    events = []
    process = FakeProcess(returncode=1)

    @asynccontextmanager
    async def collector(_path):
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

        async def __aexit__(self, *_args):
            events.append("scraper_close")

        async def run(self, **_kwargs):
            events.append("scrape")
            return {"status": "COMPLETED"}

    monkeypatch.setattr(cli.settings, "proxy_mode", "pool")
    monkeypatch.setattr(cli.settings, "proxy_db_path", db_path)
    monkeypatch.setattr(cli.settings, "proxy_min_ready", 1)
    monkeypatch.setattr(cli.settings, "proxy_db_max_age_hours", 24)
    monkeypatch.setattr(cli.settings, "proxy_startup_timeout_seconds", 0.1)
    monkeypatch.setattr(cli, "proxy_collector", collector)
    monkeypatch.setattr(cli, "SeprecScraper", FakeScraper)

    assert cli.main(["--category", "1"]) == cli.EXIT_SUCCESS
    assert events == ["collector_open", "scraper_open", "scrape", "scraper_close", "collector_close"]


def test_empty_collection_fails_in_always_mode(monkeypatch, tmp_path):
    db_path = tmp_path / "proxies.db"
    process = FakeProcess(returncode=2)

    @asynccontextmanager
    async def collector(_path):
        yield process

    monkeypatch.setattr(cli.settings, "proxy_mode", "always")
    monkeypatch.setattr(cli.settings, "proxy_db_path", db_path)
    monkeypatch.setattr(cli.settings, "proxy_min_ready", 1)
    monkeypatch.setattr(cli.settings, "proxy_db_max_age_hours", 24)
    monkeypatch.setattr(cli.settings, "proxy_startup_timeout_seconds", 0.1)
    monkeypatch.setattr(cli, "proxy_collector", collector)

    assert cli.main(["--category", "1"]) == cli.EXIT_FAILURE


def test_collector_is_terminated_when_startup_timeout_expires(monkeypatch, tmp_path):
    db_path = tmp_path / "proxies.db"
    process = FakeProcess(returncode=None)

    @asynccontextmanager
    async def collector(_path):
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

        async def __aexit__(self, *_args):
            pass

        async def run(self, **_kwargs):
            return {"status": "COMPLETED"}

    monkeypatch.setattr(cli.settings, "proxy_mode", "pool")
    monkeypatch.setattr(cli.settings, "proxy_db_path", db_path)
    monkeypatch.setattr(cli.settings, "proxy_min_ready", 1)
    monkeypatch.setattr(cli.settings, "proxy_db_max_age_hours", 24)
    monkeypatch.setattr(cli.settings, "proxy_startup_timeout_seconds", 0.05)
    monkeypatch.setattr(cli.settings, "proxy_collector_stop_timeout_seconds", 0.05)
    monkeypatch.setattr(cli, "proxy_collector", collector)
    monkeypatch.setattr(cli, "SeprecScraper", FakeScraper)

    assert cli.main(["--category", "1"]) == cli.EXIT_SUCCESS
    process.terminate.assert_called_once()
    assert process.returncode == -15


def test_only_category_and_year_options_remain():
    options = {action.dest for action in build_parser()._actions}
    assert options == {"help", "version", "year", "category", "collect_proxies"}


def test_collect_command_uses_configured_paths(monkeypatch):
    called = {}

    async def fake_collect(**kwargs):
        called.update(kwargs)
        return {"verified_combinations": 3}

    monkeypatch.setattr("src.services.proxy_collector.collect", fake_collect)

    assert cli.main(["--collect-proxies"]) == cli.EXIT_SUCCESS
    assert called["db_path"] == cli.settings.proxy_db_path
    assert called["csv_path"] == cli.settings.proxy_list_path
    assert called["report_path"] == cli.settings.proxy_report_path


def test_collect_command_reports_failure_when_nothing_verifies(monkeypatch):
    async def fake_collect(**_kwargs):
        return {"verified_combinations": 0}

    monkeypatch.setattr("src.services.proxy_collector.collect", fake_collect)

    assert cli.main(["--collect-proxies"]) == cli.EXIT_FAILURE
