"""Provider schemas, protocol preservation and bounded conditional downloads."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from aiohttp import web
from test_client import local_api

from src.network.aiohttp_session import AiohttpSession
from src.network.errors import ResponseTooLargeError
from src.network.transport import Response
from src.proxy.sources import catalog, download
from src.proxy.sources.catalog import filter_and_limit_candidates, parse_source_payload
from src.proxy.sources.download import load_document
from src.proxy.sources.registry import DEFAULT_SOURCES, SOURCE_REGISTRY, resolve_sources
from src.services.proxy_collector import build_parser

NOW = datetime(2026, 10, 29, tzinfo=UTC)


def test_registry_covers_requested_repositories_and_retains_existing_source():
    assert set(SOURCE_REGISTRY) == {
        "proxifly",
        "proxyscrape",
        "monosans",
        "thespeedx",
        "jetkai",
        "iplocate",
        "vpslab",
        "thordata",
        "gfpcom",
        "dpangestuw",
        "rix4uni",
        "relayglass",
        "hproxy",
    }
    assert "jetkai" not in DEFAULT_SOURCES
    assert "gfpcom" not in DEFAULT_SOURCES
    assert resolve_sources(" monosans,relayglass,monosans ") == ["monosans", "relayglass"]
    assert resolve_sources("all") == [*SOURCE_REGISTRY, "free-proxy"]
    with pytest.raises(ValueError):
        resolve_sources("invalid")
    assert build_parser().parse_args(["--sources", "monosans,relayglass"]).sources == "monosans,relayglass"
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--sources", "monosans", "--provider", "relayglass"])


@pytest.mark.parametrize("source", ["thespeedx", "vpslab", "gfpcom", "dpangestuw"])
def test_protocol_shards_do_not_turn_socks_into_http(source):
    for file in SOURCE_REGISTRY[source].files:
        proxies = parse_source_payload("203.0.113.1:1080\n", source=source, default_protocol=file.protocol)
        assert proxies[0].url == f"{file.protocol}://203.0.113.1:1080"


def test_monosans_normalizes_seconds_and_nested_geolocation():
    proxies = parse_source_payload(
        [
            {
                "protocol": "socks5",
                "host": "203.0.113.1",
                "port": 1080,
                "timeout": 0.14,
                "geolocation": {"country": {"iso_code": "DE"}, "city": {"names": {"en": "Berlin"}}},
            }
        ],
        source="monosans",
    )
    assert proxies[0].latency_ms == 140
    assert proxies[0].country == "DE"
    assert proxies[0].city == "Berlin"
    assert proxies[0].protocol == "socks5"


def test_rix4uni_expands_flags_instead_of_guessing_protocol():
    proxies = parse_source_payload(
        [
            {
                "ip": "203.0.113.2",
                "port": "1080",
                "http": "0",
                "socks4": "1",
                "socks5": "1",
                "ssl": "1",
                "delay": 250,
                "lastseen": 60,
                "checks_up": "9",
                "checks_down": "1",
            }
        ],
        source="rix4uni",
        now=NOW,
    )
    assert {proxy.protocol for proxy in proxies} == {"socks4", "socks5"}
    assert all(proxy.uptime_percent == 90 for proxy in proxies)
    assert proxies[0].last_checked == NOW - timedelta(seconds=60)
    assert proxies[0].latency_ms == 250


def test_jetkai_expands_protocols_and_keeps_stale_timestamp():
    proxies = parse_source_payload(
        [
            {
                "ip": "203.0.113.3",
                "port": 8080,
                "protocols": [{"type": "https", "port": 8080, "tls": False}, {"type": "socks5", "port": 1080}],
                "location": {"isocode": "TH", "city": "Bangkok"},
                "lastSuccess": "2023-04-15 02:35:45.0",
            }
        ],
        source="jetkai",
    )
    assert {proxy.url for proxy in proxies} == {"http://203.0.113.3:8080", "socks5://203.0.113.3:1080"}
    assert proxies[0].https_supported is True
    selection = filter_and_limit_candidates(proxies, now=NOW)
    assert selection.candidates == []
    assert selection.rejected["stale"] == 2


def test_thordata_and_relayglass_metadata():
    thordata = parse_source_payload(
        [{"type": "socks5", "ip": "203.0.113.4", "port": 1080, "updated_utc": NOW.isoformat(), "latency_ms": 72.6}],
        source="thordata",
    )[0]
    relayglass = parse_source_payload(
        [{"protocol": "http", "ip": "203.0.113.5", "port": 8080, "https": True, "response_ms": 15, "uptime_pct": 0.5}],
        source="relayglass",
    )[0]
    assert thordata.protocol == "socks5"
    assert thordata.last_checked == NOW
    assert relayglass.latency_ms == 15
    assert relayglass.uptime_percent == 0.5
    assert relayglass.url.startswith("http://")


def test_unsupported_protocols_and_remote_dns_are_preserved():
    proxies = parse_source_payload(
        [
            {"protocol": "vmess", "ip": "203.0.113.6", "port": 443},
            {"protocol": "socks5h", "ip": "203.0.113.7", "port": 1080},
            {"protocol": "socks4a", "ip": "203.0.113.8", "port": 1080},
        ],
        source="other",
    )
    assert [proxy.protocol for proxy in proxies] == ["socks5h", "socks4a"]


@pytest.mark.asyncio
async def test_partial_source_failure_and_cross_source_deduplication(monkeypatch):
    async def fake_fetch(url, *_args):
        if "socks4" in url:
            raise OSError("offline")
        return Response(200, {}, b"203.0.113.9:1080\n203.0.113.10:8080\n")

    monkeypatch.setattr(download, "fetch_bytes", fake_fetch)
    result = await catalog.load_public_catalog(["thespeedx", "dpangestuw"], max_records=1)
    assert len(result.candidates) == 2  # same address; HTTP and SOCKS5 remain distinct
    assert all(proxy.source == "thespeedx+dpangestuw" for proxy in result.candidates)
    assert len(result.errors) == 2
    assert result.source_counts == {"thespeedx": 2, "dpangestuw": 2}


@pytest.mark.asyncio
async def test_source_download_concurrency_is_bounded(monkeypatch):
    active = maximum = 0

    async def fake_fetch(url, *_args):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.01)
        active -= 1
        return Response(200, {}, b"203.0.113.9:1080\n")

    monkeypatch.setattr(download, "fetch_bytes", fake_fetch)
    await catalog.load_public_catalog(["thespeedx", "dpangestuw"], download_concurrency=2)
    assert maximum == 2


def parse_text(body, _fetched_at):
    text = body.decode()
    if text == "bad":
        raise ValueError("invalid source")
    return text.splitlines()


@pytest.mark.asyncio
async def test_cache_revalidation_stale_fallback_and_expiry(tmp_path, monkeypatch):
    responses = [Response(200, {"etag": '"v1"'}, b"proxy:80\n"), Response(304, {})]
    headers_seen = []

    async def fetch(url, timeout, max_bytes, headers):  # noqa: ASYNC109 - matches downloader contract
        headers_seen.append(headers)
        if not responses:
            raise OSError("offline")
        return responses.pop(0)

    monkeypatch.setattr(download, "fetch_bytes", fetch)
    kwargs = {
        "timeout": 1,
        "max_bytes": 1024,
        "refresh_minutes": 0,
        "cache_dir": tmp_path,
        "max_cache_age_hours": 24,
        "network_slots": asyncio.Semaphore(1),
    }
    assert await load_document("https://source.example/list", parse_text, **kwargs) == (["proxy:80"], None)
    assert await load_document("https://source.example/list", parse_text, **kwargs) == (["proxy:80"], None)
    assert headers_seen[-1]["If-None-Match"] == '"v1"'
    items, warning = await load_document("https://source.example/list", parse_text, **kwargs)
    assert items == ["proxy:80"]
    assert "cached" in warning
    with pytest.raises(OSError):
        await load_document("https://source.example/list", parse_text, **{**kwargs, "max_cache_age_hours": 0})


@pytest.mark.asyncio
async def test_cache_interval_avoids_network_and_invalid_payload_keeps_good_cache(tmp_path, monkeypatch):
    fetch = asyncio.Future()
    fetch.set_result(Response(200, {}, b"proxy:80\n"))
    calls = 0

    async def fetch_bytes(*args):
        nonlocal calls
        calls += 1
        return fetch.result()

    monkeypatch.setattr(download, "fetch_bytes", fetch_bytes)
    kwargs = {
        "timeout": 1,
        "max_bytes": 1024,
        "refresh_minutes": 60,
        "cache_dir": tmp_path,
        "max_cache_age_hours": 24,
        "network_slots": asyncio.Semaphore(1),
    }
    await load_document("https://source.example/list", parse_text, **kwargs)
    await load_document("https://source.example/list", parse_text, **kwargs)
    assert calls == 1
    fetch.result().body = b"bad"
    items, warning = await load_document("https://source.example/list", parse_text, **{**kwargs, "refresh_minutes": 0})
    assert items == ["proxy:80"]
    assert warning is not None
    assert next(tmp_path.glob("*.body")).read_bytes() == b"proxy:80\n"


@pytest.mark.asyncio
async def test_aiohttp_limits_decompressed_response_size():
    async def handler(_request):
        response = web.Response(body=b"x" * 10000)
        response.enable_compression()
        return response

    app = web.Application()
    app.router.add_get("/api/list", handler)
    async with local_api(app) as base_url, AiohttpSession() as session:
        with pytest.raises(ResponseTooLargeError):
            await session.get(f"{base_url}/list", max_response_bytes=100)


@pytest.mark.asyncio
async def test_curl_fallback_limits_body_before_retaining_it(monkeypatch):
    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, url, *, timeout, content_callback):  # noqa: ASYNC109 - matches curl interface
            assert content_callback(b"1234") == 4
            assert content_callback(b"5678") == 0
            raise RuntimeError("curl aborted the write callback")

    monkeypatch.setattr(download, "AIOHTTP_AVAILABLE", False)
    monkeypatch.setattr("curl_cffi.requests.AsyncSession", lambda **kwargs: Session())
    with pytest.raises(ResponseTooLargeError):
        await download.fetch_bytes("https://source.example/list", 1, 5, {})


def test_record_limit_is_enforced_inside_expanded_json_rows():
    proxies = parse_source_payload(
        [{"ip": "203.0.113.2", "port": 1080, "http": "1", "socks4": "1", "socks5": "1"}], source="rix4uni", max_candidates=1
    )
    assert len(proxies) == 1
