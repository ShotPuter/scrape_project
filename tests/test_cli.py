"""CLI parsing and orchestration tests (no network access)."""

import argparse
import sys
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

import src.cli as cli
from src.cli import _execute_scrape, _validate_args, build_parser
from src.storage.proxy_database import ProxyDatabase, VerifiedProxy


def test_defaults_run_one_incremental_pass():
    args = build_parser().parse_args([])

    assert args.year is None
    assert args.category == "all"
    assert args.full is False
    assert args.schedule is False
    assert args.concurrency >= 1
    assert args.proxy_db is None
    assert args.refresh_proxies is True
    assert args.interval_hours > 0


def test_proxy_database_is_parsed():
    args = build_parser().parse_args(["--proxy-db", "data/proxies.db"])

    assert args.proxy_db == Path("data/proxies.db")


def test_proxy_refresh_can_be_disabled():
    assert build_parser().parse_args(["--no-refresh-proxies"]).refresh_proxies is False


@pytest.mark.asyncio
async def test_proxy_refresh_starts_after_scraper_opens_existing_database(monkeypatch, tmp_path):
    db_path = tmp_path / "proxies.db"
    monkeypatch.setattr(cli.settings, "proxy_min_ready", 1)
    async with ProxyDatabase(db_path) as database:
        await database.replace_all([VerifiedProxy("http://proxy.example:80", "UA", "aiohttp")])

    events = []

    class FakeScraper:
        def __init__(self, **kwargs):
            assert kwargs["proxy_db_path"] == db_path

        async def __aenter__(self):
            events.append("scraper_open")
            return self

        async def __aexit__(self, *args):
            events.append("scraper_close")

    class FakeProcess:
        returncode = None

        async def wait(self):
            if self.returncode is None:
                events.append("collector_done")
                self.returncode = 0
            return self.returncode

    async def start_collector(*command):
        assert command[0] == sys.executable
        assert Path(command[1]).name == "collect_proxies.py"
        assert command[2:] == ("--db", str(db_path), "--publish-after", "1")
        events.append("collector_start")
        return FakeProcess()

    async def scrape(scraper, args):
        events.append("scrape")
        return [{"status": "COMPLETED", "items_scraped": 1}]

    monkeypatch.setattr(cli, "SeprecScraper", FakeScraper)
    monkeypatch.setattr(cli, "_execute_scrape", scrape)
    monkeypatch.setattr(cli.asyncio, "create_subprocess_exec", start_collector)

    reports = await cli._run_once(build_parser().parse_args(["--proxy-db", str(db_path)]))

    assert reports[0]["status"] == "COMPLETED"
    assert events == ["scraper_open", "collector_start", "scrape", "collector_done", "scraper_close"]


@pytest.mark.asyncio
async def test_empty_proxy_database_is_filled_before_scraping(monkeypatch, tmp_path):
    db_path = tmp_path / "proxies.db"
    monkeypatch.setattr(cli.settings, "proxy_min_ready", 2)
    monkeypatch.setattr(cli, "PROXY_READY_POLL_SECONDS", 0.01)
    events = []

    class FakeScraper:
        def __init__(self, **kwargs):
            assert kwargs["proxy_db_path"] == db_path

        async def __aenter__(self):
            events.append("scraper_open")
            async with ProxyDatabase(db_path) as database:
                assert len(await database.list_active()) == 2
            return self

        async def __aexit__(self, *args):
            events.append("scraper_close")

    class FakeProcess:
        returncode = None

        async def wait(self):
            if self.returncode is None:
                events.append("collector_done")
                self.returncode = 0
            return self.returncode

    async def start_collector(*command):
        events.append("collector_start")

        async def publish():
            await cli.asyncio.sleep(0.02)
            async with ProxyDatabase(db_path) as database:
                await database.replace_all(
                    [
                        VerifiedProxy("http://first.example:80", "UA", "aiohttp"),
                        VerifiedProxy("http://second.example:80", "UA", "aiohttp"),
                    ]
                )
            events.append("published")

        cli.asyncio.create_task(publish())
        return FakeProcess()

    async def scrape(scraper, args):
        events.append("scrape")
        return [{"status": "COMPLETED"}]

    monkeypatch.setattr(cli, "SeprecScraper", FakeScraper)
    monkeypatch.setattr(cli, "_execute_scrape", scrape)
    monkeypatch.setattr(cli.asyncio, "create_subprocess_exec", start_collector)

    await cli._run_once(build_parser().parse_args(["--proxy-db", str(db_path)]))

    assert events == ["collector_start", "published", "scraper_open", "scrape", "collector_done", "scraper_close"]


@pytest.mark.asyncio
async def test_empty_proxy_database_does_not_scrape_when_collection_fails(monkeypatch, tmp_path):
    db_path = tmp_path / "proxies.db"
    process = argparse.Namespace(returncode=2, wait=AsyncMock(return_value=2))

    async def start_collector(*command):
        return process

    def unexpected_scraper(**kwargs):
        pytest.fail("Scraper started without verified proxies")

    monkeypatch.setattr(cli, "SeprecScraper", unexpected_scraper)
    monkeypatch.setattr(cli.asyncio, "create_subprocess_exec", start_collector)

    with pytest.raises(RuntimeError, match="0/20 verified proxies"):
        await cli._run_once(build_parser().parse_args(["--proxy-db", str(db_path)]))


@pytest.mark.asyncio
async def test_proxy_collector_is_terminated_when_scrape_stops(monkeypatch, tmp_path):
    process = argparse.Namespace(returncode=None, wait=AsyncMock(return_value=-15), terminate=Mock())

    async def start_collector(*command):
        return process

    monkeypatch.setattr(cli.asyncio, "create_subprocess_exec", start_collector)

    with pytest.raises(RuntimeError, match="scrape stopped"):
        async with cli._proxy_collector(tmp_path / "proxies.db"):
            raise RuntimeError("scrape stopped")

    process.terminate.assert_called_once()


def test_year_and_category_are_parsed():
    args = build_parser().parse_args(["--year", "2025", "--category", "2", "--max-items", "7"])

    assert args.year == 2025
    assert args.category == "2"
    assert args.max_items == 7


def test_sub_category_requires_specific_category():
    parser = build_parser()
    args = parser.parse_args(["--sub-category", "1001"])

    with pytest.raises(SystemExit):
        _validate_args(args, parser)


@pytest.mark.parametrize("value", ["1800", "3000", "not-a-year"])
def test_year_is_validated(value):
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--year", value])


@pytest.mark.asyncio
async def test_execute_scrape_respects_item_budget_across_categories():
    calls = []

    class FakeScraper:
        async def run(self, **kwargs):
            calls.append(kwargs)
            return {"status": "COMPLETED", "items_scraped": 1, "items_skipped": 0, "items_failed": 0}

    args = argparse.Namespace(category="all", sub_category="", year=2025, max_items=1, concurrency=2, full=False)

    reports = await _execute_scrape(FakeScraper(), args)

    assert len(reports) == 1
    assert len(calls) == 1
    assert calls[0]["category"] == "1"
    assert calls[0]["max_items"] == 1
    assert calls[0]["date_filter"] == "5"
    assert calls[0]["start_date"] == "2025-01-01"
    assert calls[0]["end_date"] == "2025-12-31"
