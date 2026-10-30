"""Webshare authentication, bounded inventory, and private-to-public routing."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from test_client import local_api

from config import Settings
from src.proxy import collector
from src.proxy.checker import CheckStats, Stage1Result, Stage2Result
from src.proxy.config import CollectorConfig
from src.proxy.pool import ProxyEndpoint, ProxyPool
from src.proxy.repository import ProxyDatabase, VerifiedProxy
from src.proxy.sources import webshare
from src.proxy.sources.catalog import CatalogLoadResult, ProxyCandidate, filter_and_limit_candidates
from src.services.seprec_proxy_probe import seprec_target_probe


def inventory_row(**overrides):
    return {
        "proxy_address": "203.0.113.8",
        "port": 8080,
        "username": "private@user",
        "password": "secret:/pass",
        "valid": True,
        **overrides,
    }


@pytest.mark.asyncio
async def test_authenticated_pagination_deduplicates_and_encodes_credentials(monkeypatch):
    calls = []

    async def inventory(request):
        calls.append((request.headers.get("Authorization"), dict(request.query)))
        rows = [inventory_row(), inventory_row(valid=False)]
        if request.query["page"] == "2":
            return web.json_response({"next": None, "results": rows})
        # Pagination URLs are not followed, including their host and query params.
        return web.json_response({"next": "https://untrusted.example/?token=wrong", "results": rows})

    app = web.Application()
    app.router.add_get("/api/list/", inventory)
    async with local_api(app) as base:
        monkeypatch.setattr(webshare, "API_URL", base + "/list/")
        candidates = await webshare.load_webshare_candidates("api-secret")
    assert len(candidates) == 1
    assert candidates[0].url == "http://private%40user:secret%3A%2Fpass@203.0.113.8:8080"
    assert candidates[0].source == "webshare"
    assert calls == [("Token api-secret", {"mode": "direct", "page": str(page), "page_size": "100"}) for page in (1, 2)]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 429, 503, 302])
async def test_provider_errors_and_redirects_do_not_disclose_secrets(monkeypatch, status):
    calls = []

    async def inventory(request):
        calls.append(request.path)
        return web.Response(status=status, text="api-secret private-password", headers={"Location": "/api/leak"})

    async def leak(request):
        pytest.fail("Authenticated redirect must not be followed")

    app = web.Application()
    app.router.add_get("/api/list/", inventory)
    app.router.add_get("/api/leak", leak)
    async with local_api(app) as base:
        monkeypatch.setattr(webshare, "API_URL", base + "/list/")
        with pytest.raises(webshare.WebshareError) as exc:
            await webshare.load_webshare_candidates("api-secret")
    assert str(status) in str(exc.value)
    assert "api-secret" not in str(exc.value)
    assert "private-password" not in str(exc.value)
    assert calls == ["/api/list/"]


@pytest.mark.asyncio
async def test_inventory_record_budget_and_backbone_mode(monkeypatch):
    async def inventory(request):
        assert request.query["mode"] == "backbone"
        return web.json_response({"next": "more", "results": [inventory_row(), inventory_row(username="other")]})

    app = web.Application()
    app.router.add_get("/api/list/", inventory)
    async with local_api(app) as base:
        monkeypatch.setattr(webshare, "API_URL", base + "/list/")
        candidates = await webshare.load_webshare_candidates("key", mode="backbone", max_records=1)
    assert len(candidates) == 1
    assert "@p.webshare.io:80" in candidates[0].url


@pytest.mark.asyncio
async def test_oversized_inventory_fails_without_disclosing_body(monkeypatch):
    async def inventory(request):
        return web.json_response({"next": None, "results": [inventory_row()]})

    app = web.Application()
    app.router.add_get("/api/list/", inventory)
    async with local_api(app) as base:
        monkeypatch.setattr(webshare, "API_URL", base + "/list/")
        with pytest.raises(webshare.WebshareError) as exc:
            await webshare.load_webshare_candidates("api-secret", max_bytes=10)
    assert "api-secret" not in str(exc.value)
    assert "secret:/pass" not in str(exc.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["working", "api_error", "empty", "verification_error"])
async def test_token_auto_enables_webshare_and_keeps_public_reserve(tmp_path, monkeypatch, scenario):
    private = ProxyCandidate("http://user:password@private.example:80", "http", "webshare")
    public = ProxyCandidate("http://public.example:80", "http", "monosans")
    seen = []

    async def load_private(token, **kwargs):
        assert token == "api-secret"
        if scenario == "api_error":
            raise OSError("api-secret must never appear in reports")
        return [] if scenario == "empty" else [private]

    async def load_public(*args, **kwargs):
        return CatalogLoadResult([public], {"monosans": 1})

    async def stage1(url, **kwargs):
        seen.append(url)
        return Stage1Result(True, 1)

    async def stage2(url, **kwargs):
        if scenario == "verification_error" and url == private.url:
            return Stage2Result(False, CheckStats(failed_requests=1, total_attempts=1))
        return Stage2Result(True, CheckStats(successful_requests=1, total_attempts=1))

    monkeypatch.setattr(collector, "load_webshare_candidates", load_private)
    monkeypatch.setattr(collector, "load_public_catalog", load_public)
    monkeypatch.setattr(collector, "verify_stage1", stage1)
    monkeypatch.setattr(collector, "verify_stage2", stage2)
    db = tmp_path / "proxies.db"
    report = await collector.collect(
        config=CollectorConfig(webshare_api_token="api-secret"),
        target=seprec_target_probe("https://target.example/api"),
        sources="monosans",
        csv_path=tmp_path / "unused.csv",
        db_path=db,
        requests_per_second=0,
    )
    assert public.url in seen
    assert (private.url in seen) == (scenario in {"working", "verification_error"})
    assert "api-secret" not in json.dumps(report)
    assert "password@" not in json.dumps(report)
    async with ProxyDatabase(db) as database:
        records = await database.list_active()
    assert {record.source for record in records} == ({"monosans", "webshare"} if scenario == "working" else {"monosans"})


@pytest.mark.asyncio
async def test_public_reserve_activates_after_private_failure_and_private_recovers():
    first, second, reserve, recovered = (AsyncMock() for _ in range(4))
    now = datetime.now(UTC) - timedelta(seconds=2)
    endpoints = [
        ProxyEndpoint("http://private-one:80", checked_at=now.isoformat(), source="webshare"),
        ProxyEndpoint("http://private-two:80", checked_at=now.isoformat(), source="webshare"),
        ProxyEndpoint("http://public:80", source="monosans"),
    ]
    pool = ProxyPool(endpoints, lambda **kwargs: recovered, verified_reuse_seconds=300)
    for endpoint, session in zip(endpoints, (first, second, reserve), strict=True):
        pool.add_session(endpoint, session)
    try:
        async with pool.lease(1) as one:
            assert one in {first, second}
            pool.discard(one)
        async with pool.lease(1) as two:
            assert two in {first, second} and two is not one
            pool.discard(two)
        async with pool.lease(1) as leased:
            assert leased is reserve
        fresh = ProxyEndpoint(endpoints[0].url, checked_at=(now + timedelta(seconds=1)).isoformat(), source="webshare")
        pool.update_candidates([fresh, endpoints[2]])
        async with pool.lease(1) as leased:
            assert leased is recovered
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_busy_private_proxy_does_not_spill_over_to_public():
    private, reserve = AsyncMock(), AsyncMock()
    pool = ProxyPool([], lambda **kwargs: AsyncMock())
    pool.add_session(ProxyEndpoint("http://private:80", source="webshare"), private)
    pool.add_session(ProxyEndpoint("http://public:80", source="monosans"), reserve)
    try:
        async with pool.lease(1) as first:
            assert first is private

            async def next_lease():
                async with pool.lease(1) as next_session:
                    return next_session

            waiting = asyncio.create_task(next_lease())
            await asyncio.sleep(0)
            assert not waiting.done()
        assert await asyncio.wait_for(waiting, 1) is private
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_sample_limit_and_candidate_cap_keep_private_proxies_first(tmp_path):
    async with ProxyDatabase(tmp_path / "proxies.db") as database:
        await database.replace_all(
            [
                VerifiedProxy("http://fast-public:80", "UA", "aiohttp", latency_ms=1, source="monosans"),
                VerifiedProxy("http://private:80", "UA", "aiohttp", latency_ms=1000, source="webshare"),
            ]
        )
        assert (await database.list_active(limit=1))[0].source == "webshare"
    selected = filter_and_limit_candidates(
        [
            ProxyCandidate(
                "http://public:80",
                "http",
                "monosans",
                uptime_percent=100,
                latency_ms=1,
                last_checked=datetime.now(UTC),
                https_supported=True,
            ),
            ProxyCandidate("http://private:80", "http", "webshare"),
        ],
        max_total=1,
    )
    assert selected.candidates[0].source == "webshare"
    assert "api-secret" not in repr(Settings(webshare_api_token="api-secret"))
    assert "api-secret" not in repr(CollectorConfig(webshare_api_token="api-secret"))
