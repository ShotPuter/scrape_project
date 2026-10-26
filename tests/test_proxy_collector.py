"""Proxy catalog, filtering, collector, and payload validation tests."""

import asyncio
import csv
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from src.client.proxy_catalog import ProxyCandidate, filter_and_limit_candidates, parse_source_payload
from src.client.proxy_pool import dead_proxy_path
from src.services import proxy_collector
from src.services.proxy_collector import (
    Stage1Failure,
    Stage1Success,
    Stage2Check,
    build_parser,
    clear_dead_proxies,
    read_candidates,
)
from src.services.proxy_verifier import CheckStats, Stage2Result, catalog_ready, search_ready
from src.storage.proxy_database import ProxyDatabase, VerifiedProxy


def test_provider_defaults_to_public_sources():
    args = build_parser().parse_args([])
    assert args.provider == "public"


def test_free_proxy_flag_selects_library_provider():
    args = build_parser().parse_args(["--free-proxy"])
    assert args.provider == "free-proxy"


def test_clear_dead_proxies_removes_sidecar(tmp_path):
    db_path = tmp_path / "proxies.db"
    dead_path = dead_proxy_path(db_path)
    dead_path.write_text("{}", encoding="utf-8")

    clear_dead_proxies(db_path)

    assert not dead_path.exists()


def _success_stats(latency_ms: float = 10.0) -> CheckStats:
    stats = CheckStats(
        successful_requests=3, failed_requests=0, total_attempts=3, average_latency_ms=latency_ms, consecutive_failures=0
    )
    return stats


@pytest.mark.asyncio
async def test_collect_replaces_database_and_clears_dead_list(tmp_path, monkeypatch):
    csv_path = tmp_path / "proxies.csv"
    csv_path.write_text("alive,protocol,proxy\ntrue,http,1.1.1.1:8080\n", encoding="utf-8")
    db_path = tmp_path / "proxies.db"
    dead_path = dead_proxy_path(db_path)
    async with ProxyDatabase(db_path) as database:
        await database.replace_all([VerifiedProxy(url="http://old.example:8080", user_agent="UA-OLD", transport="aiohttp")])
    dead_path.write_text("{}", encoding="utf-8")

    async def fake_free_proxy_candidates(**kwargs):
        return ["http://free.example:8080"]

    async def fake_stage1(candidates, **kwargs):
        successes = [
            Stage1Success(candidate, "curl_cffi", "UA-NEW", 5.0)
            for candidate in candidates
            if candidate.source == "free-proxy"
        ]
        return successes, []

    async def fake_stage2(stage1_successes, **kwargs):
        return [Stage2Check(success, Stage2Result(True, _success_stats(10.0))) for success in stage1_successes]

    monkeypatch.setattr(proxy_collector, "load_free_proxy_candidates", fake_free_proxy_candidates)
    monkeypatch.setattr(proxy_collector, "_stage1_scan", fake_stage1)
    monkeypatch.setattr(proxy_collector, "_stage2_scan", fake_stage2)

    report = await proxy_collector.collect(
        csv_path=csv_path, db_path=db_path, source_url="", provider="both", concurrency=1, timeout=1
    )

    assert report["database_replaced"] is True
    assert report["csv_candidates"] == 1
    assert report["free_proxy_candidates"] == 1
    async with ProxyDatabase(db_path) as database:
        active = await database.list_active()
    assert [proxy.url for proxy in active] == ["http://free.example:8080"]
    assert active[0].source == "free-proxy"
    assert active[0].successful_requests == 3
    with sqlite3.connect(db_path) as connection:
        total_rows = connection.execute("SELECT COUNT(*) FROM verified_proxies").fetchone()[0]
    assert total_rows == 1
    assert not dead_path.exists()


@pytest.mark.asyncio
async def test_collect_keeps_existing_database_when_scan_finds_nothing(tmp_path, monkeypatch):
    csv_path = tmp_path / "proxies.csv"
    csv_path.write_text("alive,protocol,proxy\ntrue,http,1.1.1.1:8080\n", encoding="utf-8")
    db_path = tmp_path / "proxies.db"
    dead_path = dead_proxy_path(db_path)
    async with ProxyDatabase(db_path) as database:
        await database.replace_all([VerifiedProxy(url="http://old.example:8080", user_agent="UA-OLD", transport="aiohttp")])
    dead_path.write_text("{}", encoding="utf-8")

    async def fake_free_proxy_candidates(**kwargs):
        return []

    async def fake_stage1(candidates, **kwargs):
        return [], [Stage1Failure(candidate, "connect_timeout", "timed out") for candidate in candidates]

    monkeypatch.setattr(proxy_collector, "load_free_proxy_candidates", fake_free_proxy_candidates)
    monkeypatch.setattr(proxy_collector, "_stage1_scan", fake_stage1)

    report = await proxy_collector.collect(
        csv_path=csv_path, db_path=db_path, source_url="", provider="both", concurrency=1, timeout=1
    )

    assert report["database_replaced"] is False
    assert report["stage1_failed"] == 1
    async with ProxyDatabase(db_path) as database:
        active = await database.list_active()
    assert [proxy.url for proxy in active] == ["http://old.example:8080"]
    assert dead_path.exists()


@pytest.mark.asyncio
async def test_collect_publishes_first_verified_proxies_before_scan_finishes(tmp_path, monkeypatch):
    csv_path = tmp_path / "proxies.csv"
    csv_path.write_text(
        "alive,protocol,proxy\ntrue,http,first.example:80\ntrue,http,second.example:80\ntrue,http,third.example:80\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "proxies.db"
    third_started = asyncio.Event()
    finish_third = asyncio.Event()
    checked = 0

    async def fake_stage1(candidates, **kwargs):
        return [Stage1Success(candidate, "aiohttp", "UA", 5.0) for candidate in candidates], []

    async def fake_verify_stage2(url, **kwargs):
        nonlocal checked
        checked += 1
        if checked == 3:
            third_started.set()
            await finish_third.wait()
        return Stage2Result(True, _success_stats())

    monkeypatch.setattr(proxy_collector, "_stage1_scan", fake_stage1)
    monkeypatch.setattr(proxy_collector, "verify_stage2", fake_verify_stage2)

    scan = asyncio.create_task(
        proxy_collector.collect(
            csv_path=csv_path, db_path=db_path, source_url="",
            provider="iplocate", concurrency=1, stage2_concurrency=1,
            timeout=1, publish_after=2,
        )
    )
    try:
        await asyncio.wait_for(third_started.wait(), timeout=2)
        async with ProxyDatabase(db_path) as database:
            published = await database.list_active()
        assert {proxy.url for proxy in published} == {"http://first.example:80", "http://second.example:80"}
        assert not scan.done()
    finally:
        finish_third.set()

    report = await scan
    assert report["verified_proxies"] == 3
    async with ProxyDatabase(db_path) as database:
        assert len(await database.list_active()) == 3


@pytest.mark.asyncio
async def test_collect_falls_back_to_csv_when_free_proxy_is_missing(tmp_path, monkeypatch):
    csv_path = tmp_path / "proxies.csv"
    csv_path.write_text("alive,protocol,proxy\ntrue,http,1.1.1.1:8080\n", encoding="utf-8")
    db_path = tmp_path / "proxies.db"

    async def missing_free_proxy(**kwargs):
        raise RuntimeError("free-proxy is not installed")

    async def fake_stage1(candidates, **kwargs):
        return [], []

    monkeypatch.setattr(proxy_collector, "load_free_proxy_candidates", missing_free_proxy)
    monkeypatch.setattr(proxy_collector, "_stage1_scan", fake_stage1)

    report = await proxy_collector.collect(
        csv_path=csv_path, db_path=db_path, source_url="", provider="both", concurrency=1, timeout=1
    )

    assert report["free_proxy_candidates"] == 0
    assert report["csv_candidates"] == 1
    assert report["database_replaced"] is False


def test_read_candidates_skips_dead_and_invalid_rows(tmp_path):
    path = tmp_path / "proxies.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["alive", "protocol", "proxy"])
        writer.writeheader()
        writer.writerow({"alive": "true", "protocol": "http", "proxy": "1.2.3.4:8080"})
        writer.writerow({"alive": "false", "protocol": "http", "proxy": "5.6.7.8:8080"})
        writer.writerow({"alive": "true", "protocol": "ftp", "proxy": "9.9.9.9:21"})
        writer.writerow({"alive": "true", "protocol": "http", "proxy": "9.9.9.9:99999"})
        writer.writerow({"alive": "true", "protocol": "socks5", "proxy": "10.0.0.1:1080"})
        writer.writerow({"alive": "true", "protocol": "http", "proxy": "1.2.3.4:8080"})

    assert read_candidates(path) == ["http://1.2.3.4:8080", "socks5://10.0.0.1:1080"]


def test_catalog_payload_requires_finalizado_and_list():
    assert catalog_ready({"finalizado": True, "datos": []}) is True
    assert catalog_ready({"finalizado": False, "datos": []}) is False
    assert catalog_ready({"finalizado": True, "datos": {}}) is False


def test_search_payload_requires_rows_and_total():
    assert search_ready({"finalizado": True, "datos": {"filas": [], "total": 0}}) is True
    assert search_ready({"finalizado": True, "datos": {"filas": []}}) is False
    assert search_ready({"finalizado": False, "datos": {"filas": [], "total": 0}}) is False


def test_parse_proxyscrape_metadata():
    candidates = parse_source_payload(
        [
            {
                "protocol": "socks5",
                "ip": "195.135.255.98",
                "port": 1080,
                "country_code": "LV",
                "city": "Riga",
                "anonymity": "elite",
                "ssl": True,
                "uptime_percent": 72.01,
                "latency_ms": 179.69,
                "last_checked": 1792697992.9,
            }
        ],
        source="proxyscrape",
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.url == "socks5://195.135.255.98:1080"
    assert candidate.protocol == "socks5"
    assert candidate.country == "LV"
    assert candidate.uptime_percent == pytest.approx(72.01)
    assert candidate.latency_ms == pytest.approx(179.69)
    assert candidate.https_supported is True
    assert candidate.last_checked is not None


def test_parse_hproxy_metadata_and_protocol_choice():
    candidates = parse_source_payload(
        [
            {
                "ip": "201.79.50.139",
                "port": 10000,
                "protocols": ["http", "https", "socks4", "socks5"],
                "country_code": "BR",
                "city": "Juazeiro",
                "anonymity": "elite",
                "latency_ms": 200,
                "uptime_pct": 100.0,
                "last_verified_at": "2026-10-23T12:18:49.868313Z",
                "status": "alive",
            }
        ],
        source="hproxy",
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.url == "http://201.79.50.139:10000"
    assert candidate.protocol == "http"
    assert candidate.https_supported is True
    assert candidate.uptime_percent == 100.0


def test_parse_proxifly_missing_metadata_is_kept():
    candidates = parse_source_payload(
        [
            {
                "proxy": "http://1.2.3.4:8080",
                "protocol": "http",
                "ip": "1.2.3.4",
                "port": 8080,
                "https": True,
                "anonymity": "elite",
                "score": 1,
                "geolocation": {"country": "US", "city": "Ashburn"},
            }
        ],
        source="proxifly",
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.country == "US"
    assert candidate.city == "Ashburn"
    assert candidate.metadata_missing is True


def test_fair_allocation_reserves_slots_for_each_source():
    candidates = [
        *[
            ProxyCandidate(
                url=f"http://hproxy-{index}.example:8080", protocol="http", source="hproxy",
                uptime_percent=100, latency_ms=float(index),
            )
            for index in range(10)
        ],
        *[
            ProxyCandidate(
                url=f"http://proxyscrape-{index}.example:8080", protocol="http", source="proxyscrape",
                uptime_percent=100, latency_ms=float(index),
            )
            for index in range(10)
        ],
        *[
            ProxyCandidate(url=f"http://proxifly-{index}.example:8080", protocol="http", source="proxifly")
            for index in range(10)
        ],
    ]

    selection = filter_and_limit_candidates(
        candidates, min_uptime=0, max_latency_ms=1500,
        max_age_minutes=0, preferred_age_minutes=0, require_https=False,
        max_per_source=0, max_total=9,
    )

    assert selection.source_counts == {"hproxy": 3, "proxyscrape": 3, "proxifly": 3}


def test_metadata_filters_drop_explicit_bad_values_but_keep_missing():
    now = datetime.now(UTC)
    candidates = [
        ProxyCandidate(
            url="http://good.example:8080", protocol="http", source="hproxy",
            uptime_percent=99, latency_ms=100, last_checked=now,
        ),
        ProxyCandidate(
            url="http://slow.example:8080", protocol="http", source="hproxy",
            uptime_percent=99, latency_ms=2000, last_checked=now,
        ),
        ProxyCandidate(
            url="http://unstable.example:8080", protocol="http", source="hproxy",
            uptime_percent=50, latency_ms=100, last_checked=now,
        ),
        ProxyCandidate(
            url="http://stale.example:8080", protocol="http", source="hproxy",
            uptime_percent=99, latency_ms=100, last_checked=now - timedelta(days=2),
        ),
        ProxyCandidate(url="http://unknown.example:8080", protocol="http", source="proxifly"),
    ]

    selection = filter_and_limit_candidates(
        candidates, min_uptime=80, max_latency_ms=1500,
        max_age_minutes=1440, preferred_age_minutes=15, require_https=True,
        max_per_source=0, max_total=0, now=now,
    )

    assert [candidate.url for candidate in selection.candidates] == [
        "http://good.example:8080",
        "http://unknown.example:8080",
    ]
    assert selection.rejected["high_latency"] == 1
    assert selection.rejected["low_uptime"] == 1
    assert selection.rejected["stale"] == 1
