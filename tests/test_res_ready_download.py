"""Exercise saved-link consumption without network access."""

import asyncio

import httpx

from src.sites.res.document_lock import document_lock
from src.sites.res.pdf import persist_pdf
from src.sites.res.ready_download import download_ready
from src.sites.res.tracking import TrackingStore, pending_cves

CVES = ("ACcode0001", "ACcode0002", "ACcode0003", "ACcode0004")
PDF = b"%PDF-1.4\nfixture\n%%EOF"
COMPANY = {"company_id": "company", "company_name": "Example"}
RUN = "ready-test"


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
            again = await download_ready(store, client, interval=0)
        assert calls == [CVES[1], CVES[2]]
        assert (result["reused"], result["downloaded"], result["expired"]) == (1, 1, 1)
        assert result["pdfs_per_minute"] > 0
        assert again["reused"] == 2 and again["requested"] == 0
        assert [row["cve"] for row in pending_cves(store.path, RUN, 10, needs_link=True)] == list(CVES[2:])
    finally:
        store.close()


async def test_access_denial_stops_following_requests(tmp_path):
    store = TrackingStore(tmp_path / "tracking.sqlite3", RUN)
    try:
        seed(store)
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(429))) as client:
            result = await download_ready(store, client, interval=0)
        assert result["requested"] == 1
        assert result["stop_reason"] == "HTTP 429 from RES"
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
