import asyncio
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.sites.pep import benchmark
from src.sites.pep.browser_diagnostics import LANDING
from src.sites.pep.browser_discovery import BrowserDiscoveryClient
from src.sites.pep.browser_download import DownloadFailure
from src.sites.pep.documents import save_payload
from src.sites.pep.source import safe_url
from src.sites.pep.undetected_download import UndetectedDownloader
from tests.test_pep_benchmark import make_args
from tests.test_pep_nodriver_download import PDF, URL, configure

TABLE = '''<table class="table"><thead><tr><th>Declarante</th><th>Descargar</th>
<th>Fecha de publicación</th></tr></thead><tbody><tr><td>example</td><td>
<form action="/fdci/consultaCiudadana/generarReporteConsultaCiudadana">
<input name="id" value="123"><input name="captchatoken" value=""></form>
</td><td>2024-03-01</td></tr></tbody></table>'''


def browser_for_discovery(html=TABLE):
    return SimpleNamespace(lock=asyncio.Lock(), start=AsyncMock(), navigate=AsyncMock(),
                           session_initialized=False, timeout_ms=40000, failures=0,
                           page=SimpleNamespace(get_content=AsyncMock(return_value=html), select=AsyncMock()))


@pytest.mark.asyncio
async def test_dom_discovery_and_preflight_reuse_browser():
    browser = browser_for_discovery()
    client = BrowserDiscoveryClient(browser)
    await client.preflight(None)
    await client.preflight(None)
    assert browser.session_initialized is True
    browser.navigate.assert_awaited_once_with(LANDING)
    status, docs = await client.discover(URL)
    assert status == "discovered"
    assert docs[0]["id"] == "123" and docs[0]["requires_captcha"] is True
    assert docs[0]["source_page"] == URL
    assert docs[0]["publication_year"] == "2024"
    assert not hasattr(client, "session")
    await client.close()


@pytest.mark.asyncio
async def test_dom_pagination_loop_preserves_first_page():
    html = TABLE + f'<div class="pagination"><a class="nextLink" href="{URL}">next</a></div>'
    browser = browser_for_discovery(html)
    status, docs = await BrowserDiscoveryClient(browser).discover(URL)
    assert status == "discovery_failed_pagination_loop"
    assert len(docs) == 1
    browser.navigate.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(("failure", "expected"), [(DownloadFailure("browser_navigation_http_403"),
                                                    "discovery_failed_http_403"),
                                                   (TimeoutError(), "discovery_failed_browser_timeout")])
async def test_dom_navigation_failure_not_counted_as_empty(failure, expected):
    browser = browser_for_discovery()
    browser.navigate.side_effect = failure
    assert await BrowserDiscoveryClient(browser).discover(URL) == (expected, [])


@pytest.mark.asyncio
async def test_dom_verification_notice_stops_before_table_parsing():
    browser = browser_for_discovery('<div class="alert">Recaptcha did not pass validation</div>')
    client = BrowserDiscoveryClient(browser)
    assert await client.discover(URL) == ("discovery_failed_verification", [])
    assert client.last_failure == {"reason": "site_verification_notice"}
    assert browser.failures == 3
    browser.page.select.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("refused", [False, True])
async def test_browser_only_never_constructs_http_portal_clients(tmp_path, monkeypatch, refused):
    args = make_args(tmp_path)
    args.network, args.browser_engine, args.headed, args.workers = "webshare", "undetected", True, 1
    args.browser_only = True
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setattr(benchmark, "load_webshare_candidates", AsyncMock(return_value=[
        SimpleNamespace(url="http://user:secret@192.0.2.1:8000"),
    ]))

    def forbidden(*args, **kwargs):
        raise AssertionError("Browser-only must not construct an HTTP discovery/downloader")

    monkeypatch.setattr(benchmark.DiscoveryClient, "__init__", forbidden)
    monkeypatch.setattr(benchmark, "DocumentDownloader", forbidden)
    pages = []

    async def start(browser):
        browser.page = SimpleNamespace(get_content=AsyncMock(return_value=TABLE), select=AsyncMock())

    async def navigate(browser, url):
        pages.append(safe_url(url))
        browser.loaded_url = url

    async def download(browser, doc):
        assert browser.session_initialized
        if refused:
            browser.failures = 3
            return {**doc, "status": "browser_rejected_http_302", "rejection_reason": "site_verification_notice"}
        return {**doc, **save_payload(PDF, args.output, doc["id"]), "download_method": "browser_undetected_chromedriver"}

    monkeypatch.setattr(UndetectedDownloader, "observe_egress", AsyncMock(return_value="198.51.100.1"))
    monkeypatch.setattr(UndetectedDownloader, "start", start)
    monkeypatch.setattr(UndetectedDownloader, "navigate", navigate)
    download_mock = AsyncMock(side_effect=download)

    async def wrapped(browser, doc):
        return await download_mock(browser, doc)

    monkeypatch.setattr(UndetectedDownloader, "download", wrapped)
    monkeypatch.setattr(UndetectedDownloader, "close", AsyncMock())
    assert await benchmark.run(args) == int(refused)
    report = json.loads((args.output / "benchmark.json").read_text())
    assert report["source_mode"] == "browser-only"
    assert report["proxy_sessions"][0]["routing"] == "browser_verified"
    assert report["unique_documents_downloaded"] == (0 if refused else 1)
    assert download_mock.await_count == 1
    assert pages[0] == LANDING
    if refused:
        assert report["jobs"] == {"partial": 1, "pending": 1}
    else:
        assert report["completed_links"] == 2
        assert report["new_downloads_this_run"] == 1
        events = [json.loads(line) for line in (args.output / "events.jsonl").read_text().splitlines()]
        finished = [event for event in events if event["event"] == "document_finished"]
        assert finished[-1]["reused_file"] is True
    args.browser_only = False
    with pytest.raises(ValueError, match="new --output"):
        await benchmark.run(args)


@pytest.mark.asyncio
async def test_browser_legacy_link_download_does_not_wait_for_recaptcha(tmp_path, monkeypatch):
    downloader, _, _, button, _ = configure(tmp_path, monkeypatch)
    wait = AsyncMock(side_effect=AssertionError("Legacy link must not wait for grecaptcha"))
    monkeypatch.setattr(downloader, "wait_for_captcha_script", wait)
    target = "https://www.funcionpublica.gov.co/ley-transparencia-web/declaracionServlet?action=download&id=1"
    identifier = "legacy-" + hashlib.sha256(target.encode()).hexdigest()[:24]
    result = await downloader.download({"id": identifier, "source_page": URL,
                                        "requires_captcha": False, "download_url": target})
    assert result["status"] == "downloaded"
    button.mouse_click.assert_awaited_once()
    wait.assert_not_awaited()
