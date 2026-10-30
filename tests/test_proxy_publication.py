"""Results reach other processes in repeated batches before stage 2 finishes."""

import asyncio

import pytest

from src.proxy import collector
from src.proxy.checker import CheckStats, Stage1Result, Stage2Result
from src.proxy.collector import Stage1Success
from src.proxy.config import CollectorConfig
from src.proxy.repository import ProxyDatabase, VerifiedProxy
from src.proxy.sources.catalog import ProxyCandidate
from src.services.proxy_bootstrap import count_fresh_proxies
from src.services.proxy_collector import build_parser
from src.services.seprec_proxy_probe import seprec_target_probe


@pytest.mark.asyncio
@pytest.mark.parametrize("publish_after", [None, 2])
async def test_every_batch_is_visible_before_scan_finishes(tmp_path, monkeypatch, publish_after):
    path = tmp_path / "proxies.csv"
    path.write_text("alive,protocol,proxy\n" + "".join(f"true,http,p{i}.example:80\n" for i in range(5)))
    db = tmp_path / "proxies.db"
    async with ProxyDatabase(db) as database:
        await database.replace_all([VerifiedProxy("http://existing.example:80", "UA", "aiohttp")])
    fifth_started, finish = asyncio.Event(), asyncio.Event()

    async def stage1(candidates, **kwargs):
        found = [Stage1Success(candidate, "aiohttp", "UA", 1) for candidate in candidates]
        for success in found:
            await kwargs["on_success"](success)
        return found, []

    async def verify(url, **kwargs):
        if url == "http://p4.example:80":
            fifth_started.set()
            await finish.wait()
        return Stage2Result(True, CheckStats(successful_requests=1, total_attempts=1))

    monkeypatch.setattr(collector, "_stage1_scan", stage1)
    monkeypatch.setattr(collector, "verify_stage2", verify)
    scan = asyncio.create_task(
        collector.collect(
            config=CollectorConfig(proxy_min_ready=2, proxy_user_agent_limit=1),
            target=seprec_target_probe("https://target.example/api"),
            csv_path=path,
            db_path=db,
            provider="iplocate",
            source_url="",
            publish_after=publish_after,
            stage2_concurrency=1,
            requests_per_second=0,
        )
    )
    try:
        await asyncio.wait_for(fifth_started.wait(), 2)
        assert not scan.done()
        async with ProxyDatabase(db) as reader:
            records = {record.url: record for record in await reader.list_active()}
        assert set(records) == {"http://existing.example:80", *(f"http://p{i}.example:80" for i in range(4))}
        first_stamp = records["http://p0.example:80"].checked_at
        assert await count_fresh_proxies(db, max_age_hours=24) == 5
    finally:
        finish.set()
        if not fifth_started.is_set():
            scan.cancel()
        await asyncio.gather(scan, return_exceptions=True)
    report = scan.result()
    assert report["verified_proxies"] == 5
    async with ProxyDatabase(db) as reader:
        final = {record.url: record for record in await reader.list_active()}
    assert len(final) == 5  # completed snapshot replaces the previous scan
    assert final["http://p0.example:80"].checked_at == first_stamp


@pytest.mark.asyncio
async def test_progress_counts_eligible_records_instead_of_only_success_responses(monkeypatch, capsys):
    async def verify(url, **kwargs):
        return Stage2Result(True, CheckStats(successful_requests=1, total_attempts=1))

    monkeypatch.setattr(collector, "verify_stage2", verify)
    await collector._stage2_scan(
        [Stage1Success(ProxyCandidate("http://proxy.example:80", "http", "test"), "aiohttp", "UA", 1)],
        config=CollectorConfig(proxy_user_agent_limit=1),
        target=seprec_target_probe("https://target.example/api"),
        concurrency=1,
        timeout=1,
        requests_per_second=0,
        attempts=1,
        retry_delay=0,
        max_retry_delay=0,
        min_successes=2,
    )
    assert "eligible proxies=0" in capsys.readouterr().out


def test_one_shot_cli_enables_early_publication_without_watch():
    args = build_parser().parse_args([])
    assert not args.watch
    assert args.publish_after >= 1


@pytest.mark.asyncio
async def test_stage2_publishes_while_stage1_is_still_running(tmp_path, monkeypatch):
    path = tmp_path / "proxies.csv"
    path.write_text("alive,protocol,proxy\n" + "".join(f"true,http,p{i}.example:80\n" for i in range(25)))
    db = tmp_path / "proxies.db"
    blocked, release, published = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def stage1(url, **kwargs):
        if url == "http://p24.example:80":
            blocked.set()
            await release.wait()
        return Stage1Result(True, 1)

    async def stage2(url, **kwargs):
        return Stage2Result(True, CheckStats(successful_requests=1, total_attempts=1))

    original = ProxyDatabase.upsert_many

    async def upsert(self, records):
        await original(self, records)
        published.set()

    monkeypatch.setattr(collector, "verify_stage1", stage1)
    monkeypatch.setattr(collector, "verify_stage2", stage2)
    monkeypatch.setattr(ProxyDatabase, "upsert_many", upsert)
    scan = asyncio.create_task(
        collector.collect(
            config=CollectorConfig(proxy_user_agent_limit=1),
            target=seprec_target_probe("https://target.example/api"),
            csv_path=path,
            db_path=db,
            provider="iplocate",
            source_url="",
            publish_after=20,
            concurrency=2,
            stage2_concurrency=2,
            requests_per_second=0,
        )
    )
    try:
        await asyncio.wait_for(blocked.wait(), 5)
        await asyncio.wait_for(published.wait(), 5)
        assert not scan.done()
        async with ProxyDatabase(db) as reader:
            assert len(await reader.list_active()) == 20
    finally:
        release.set()
        await asyncio.gather(scan, return_exceptions=True)
    assert scan.result()["verified_proxies"] == 25


@pytest.mark.asyncio
async def test_cancel_stops_both_streaming_stages(tmp_path, monkeypatch):
    path = tmp_path / "proxies.csv"
    path.write_text("alive,protocol,proxy\ntrue,http,p0.example:80\ntrue,http,p1.example:80\n")
    started = [asyncio.Event(), asyncio.Event()]
    stopped = [asyncio.Event(), asyncio.Event()]

    async def stage1(url, **kwargs):
        if url.endswith("p0.example:80"):
            return Stage1Result(True, 1)
        started[0].set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped[0].set()

    async def stage2(url, **kwargs):
        started[1].set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped[1].set()

    monkeypatch.setattr(collector, "verify_stage1", stage1)
    monkeypatch.setattr(collector, "verify_stage2", stage2)
    scan = asyncio.create_task(
        collector.collect(
            config=CollectorConfig(proxy_user_agent_limit=1),
            target=seprec_target_probe("https://target.example/api"),
            csv_path=path,
            db_path=tmp_path / "proxies.db",
            provider="iplocate",
            source_url="",
            concurrency=1,
            stage2_concurrency=1,
            requests_per_second=0,
        )
    )
    try:
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in started)), 5)
    finally:
        scan.cancel()
        await asyncio.wait_for(asyncio.gather(scan, return_exceptions=True), 5)
    assert all(event.is_set() for event in stopped)
