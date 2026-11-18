"""Exercise saved-link consumption without network access."""

import asyncio
from types import SimpleNamespace

import httpx
import pytest

from src.proxy.reputation import ReputationDenied
from src.sites.res import ready_download
from src.sites.res.document_lock import document_lock
from src.sites.res.pdf import persist_pdf
from src.sites.res.ready_download import download_ready
from src.sites.res.tracking import TrackingStore, pending_cves

CVES = ("ACcode0001", "ACcode0002", "ACcode0003", "ACcode0004")
PDF = b"%PDF-1.4\nfixture\n%%EOF"
COMPANY = {"company_id": "company", "company_name": "Example"}
RUN = "ready-test"


async def test_proxy_reputation_denial_prevents_http_transport(monkeypatch):
    monkeypatch.setenv("PROXY_REPUTATION_MODE", "strict")

    def unexpected_transport(**kwargs):
        pytest.fail("A rejected proxy must never reach the HTTP transport")

    monkeypatch.setattr(httpx, "AsyncHTTPTransport", unexpected_transport)
    args = SimpleNamespace(run_id=RUN, limit=1, workers=1, proxy="http://127.0.0.1:8080")
    with pytest.raises(ReputationDenied, match="denied by local network policy"):
        await ready_download.run(args)


def link(cve):
    return (
        "https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve="
        + cve
        + "&codigo="
        + "-".join(["AB"] * 16)
    )


def seed(store):
    store.register_run({}, [COMPANY])
    store.search_result("company", {"cves": CVES})
    for cve in CVES[:3]:
        store.document_status(cve, "verified", record={"link": link(cve)})


async def test_saved_links_skip_verified_pdf_and_return_expired_to_worklist(tmp_path):
    calls = []

    def transport(request):
        assert request.method == "GET"
        cve = request.url.params["cve"]
        calls.append(cve)
        if cve == CVES[2]:
            return httpx.Response(302, headers={"location": "https://example.invalid/"})
        return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF)

    store = TrackingStore(tmp_path / "tracking.sqlite3", RUN)
    try:
        seed(store)
        store.file_result(persist_pdf(tmp_path, CVES[0], PDF, link(CVES[0])), tmp_path)
        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
            result = await download_ready(store, client, interval=0)
            again = await download_ready(store, client, interval=0, audit_cache=True)
        assert calls == [CVES[1], CVES[2]]
        assert (result["inspected"], result["reused"], result["downloaded"], result["expired"]) == (2, 0, 1, 1)
        assert result["pdfs_per_minute"] > 0
        assert again["reused"] == 2 and again["requested"] == 0
        assert [row["cve"] for row in pending_cves(store.path, RUN, 10, needs_link=True)] == list(CVES[2:])
    finally:
        store.close()


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        (httpx.Response(429), "HTTP 429 from RES"),
        (
            httpx.Response(
                200,
                headers={"content-type": "text/html; charset=utf-8"},
                text="Estimado(a) usuario(a): No es posible generar más de 100 documentos diarios.",
            ),
            "RES daily document limit reached",
        ),
    ],
)
async def test_access_denial_stops_following_requests(tmp_path, response, reason):
    store = TrackingStore(tmp_path / "tracking.sqlite3", RUN)
    try:
        seed(store)
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: response)) as client:
            result = await download_ready(store, client, interval=0)
        assert result["requested"] == 1
        assert result["stop_reason"] == reason
    finally:
        store.close()


async def test_shared_lock_prevents_duplicate_get_and_reuses_file(tmp_path):
    def unexpected(_):
        raise AssertionError("Duplicate HTTP request")

    store = TrackingStore(tmp_path / "tracking.sqlite3", RUN)
    try:
        store.register_run({}, [COMPANY])
        cve = CVES[0]
        store.search_result("company", {"cves": [cve]})
        store.document_status(cve, "verified", record={"link": link(cve)})
        async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected)) as client:
            async with document_lock(store.path, cve):
                runner = asyncio.create_task(download_ready(store, client, interval=0))
                await asyncio.sleep(0)
                assert not runner.done()
                store.file_result(persist_pdf(tmp_path, cve, PDF, link(cve)), tmp_path)
            result = await runner
        assert result["reused"] == 1 and result["requested"] == 0
    finally:
        store.close()


async def test_all_drains_more_than_one_page_of_ready_links(tmp_path):
    from src.sites.res.ready_download import request_limit

    cves = [f"ACarchive{index:04d}" for index in range(260)]
    requests = []

    def transport(request):
        requests.append(request.url.params["cve"])
        return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF)

    store = TrackingStore(tmp_path / "tracking.sqlite3", RUN)
    try:
        store.register_run({}, [COMPANY])
        store.search_result("company", {"cves": cves})
        for cve in cves:
            store.document_status(cve, "verified", record={"link": link(cve)})
        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
            result = await download_ready(store, client, limit=request_limit("all"), interval=0)
        assert result["downloaded"] == len(cves)
        assert sorted(requests) == cves
    finally:
        store.close()


async def test_progress_is_reported_while_an_http_request_is_in_flight(tmp_path):
    reported_in_flight = asyncio.Event()
    snapshots = []

    def progress(snapshot):
        snapshots.append(snapshot)
        if snapshot["requested"] == 1 and snapshot["downloaded"] == 0:
            reported_in_flight.set()

    async def transport(request):
        await asyncio.wait_for(reported_in_flight.wait(), timeout=2)
        return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF)

    store = TrackingStore(tmp_path / "tracking.sqlite3", RUN)
    try:
        seed(store)
        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
            result = await download_ready(store, client, limit=1, interval=0, progress=progress, progress_interval=0.01)
        assert any(row["requested"] == 1 and row["downloaded"] == 0 for row in snapshots)
        assert result["downloaded"] == result["requested"] == 1
        assert result["inspected"] >= 1
    finally:
        store.close()


async def test_cache_audit_can_recover_missing_pdf_marked_downloaded(tmp_path):
    store = TrackingStore(tmp_path / "tracking.sqlite3", RUN)
    try:
        store.register_run({}, [COMPANY])
        cve = CVES[0]
        store.search_result("company", {"cves": [cve]})
        saved = persist_pdf(tmp_path, cve, PDF, link(cve))
        store.file_result(saved, tmp_path)
        (tmp_path / saved["local_path"]).unlink()

        def transport(request):
            return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF)

        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
            regular = await download_ready(store, client, interval=0)
            audited = await download_ready(store, client, interval=0, audit_cache=True)
        assert regular["inspected"] == regular["requested"] == 0
        assert audited["requested"] == audited["downloaded"] == 1
        assert store.existing_file(cve)
    finally:
        store.close()
