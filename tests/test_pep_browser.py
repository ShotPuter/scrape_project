from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.sites.pep.browser_download import BrowserDownloader, DownloadFailure, click_download, validate_cdp_url


def test_cdp_only_allows_explicit_local_endpoints() -> None:
    assert validate_cdp_url("http://127.0.0.1:9222")
    for url in ("http://example.org:9222", "file:///tmp/chrome", "http://user:secret@localhost:9222"):
        with pytest.raises(ValueError):
            validate_cdp_url(url)


@pytest.mark.asyncio
async def test_attached_context_remains_open(tmp_path) -> None:
    downloader = BrowserDownloader(output=tmp_path, profile=tmp_path / "profile", channel="chrome",
                                   headed=True, cdp_url="http://localhost:9222", timeout_ms=1000, pace=AsyncMock())
    page = downloader.page = SimpleNamespace(close=AsyncMock())
    context = downloader.context = SimpleNamespace(close=AsyncMock())
    playwright = downloader.playwright = SimpleNamespace(stop=AsyncMock())
    await downloader.close()
    page.close.assert_awaited_once()
    context.close.assert_not_awaited()
    playwright.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_owned_native_process_is_stopped(tmp_path) -> None:
    downloader = BrowserDownloader(output=tmp_path, profile=tmp_path / "profile", channel="chrome",
                                   headed=True, cdp_url=None, timeout_ms=1000, pace=AsyncMock(), native=True)
    process = downloader.process = SimpleNamespace(returncode=None, terminate=MagicMock(), wait=AsyncMock())
    context = downloader.context = SimpleNamespace(close=AsyncMock())
    await downloader.close()
    process.terminate.assert_called_once()
    process.wait.assert_awaited_once()
    context.close.assert_not_awaited()
    assert downloader.process is None


@pytest.mark.asyncio
async def test_repeated_rejections_do_not_keep_opening_browser(tmp_path) -> None:
    downloader = BrowserDownloader(output=tmp_path, profile=tmp_path / "profile", channel="chrome",
                                   headed=True, cdp_url=None, timeout_ms=1000, pace=AsyncMock())
    downloader.failures = 3
    downloader.start = AsyncMock()
    result = await downloader.download({"id": "123", "status": "captcha_required"})
    assert result["status"] == "browser_session_blocked"
    downloader.start.assert_not_awaited()


@pytest.mark.asyncio
async def test_select_only_asset_form_and_save_download(tmp_path) -> None:
    url = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?find=test"
    downloader = BrowserDownloader(output=tmp_path, profile=tmp_path / "profile", channel="chrome",
                                   headed=True, cdp_url=None, timeout_ms=1000, pace=AsyncMock())

    async def save_as(path):
        path.write_bytes(b"%PDF-1.4\nexample\n%%EOF")

    download = SimpleNamespace(save_as=AsyncMock(side_effect=save_as), delete=AsyncMock())

    listeners = {}

    async def click(**kwargs):
        listeners["download"](download)

    button = SimpleNamespace(click=AsyncMock(side_effect=click))
    form = SimpleNamespace(locator=MagicMock(return_value=button))
    locator = SimpleNamespace(filter=MagicMock(return_value=form))
    downloader.page = SimpleNamespace(url=url, locator=MagicMock(return_value=locator),
                                      wait_for_function=AsyncMock(),
                                      on=lambda name, handler: listeners.update({name: handler}),
                                      remove_listener=lambda name, handler: listeners.pop(name))
    downloader.start = AsyncMock()
    downloader.loaded_url = url
    result = await downloader.download({"id": "123", "source_page": url, "status": "captcha_required"})
    assert result["status"] == "downloaded"
    assert result["download_method"] == "browser_persistent"
    assert downloader.page.locator.call_args_list[0].args[0] == 'form[action$="/generarReporteConsultaCiudadana"]'
    button.click.assert_awaited_once()


@pytest.mark.asyncio
async def test_302_is_reported_without_waiting_for_download_timeout() -> None:
    listeners = {}

    async def click(**kwargs):
        listeners["response"](SimpleNamespace(
            url="https://www.funcionpublica.gov.co/fdci/consultaCiudadana/generarReporteConsultaCiudadana",
            status=302,
        ))

    page = SimpleNamespace(on=lambda name, handler: listeners.update({name: handler}),
                           remove_listener=lambda name, handler: listeners.pop(name))
    form = SimpleNamespace(locator=lambda selector: SimpleNamespace(click=click))
    with pytest.raises(DownloadFailure, match="browser_rejected_http_302"):
        await click_download(page, form, 1000)
    assert not listeners


@pytest.mark.asyncio
async def test_transient_302_has_only_one_fresh_submission(tmp_path, monkeypatch) -> None:
    downloader = BrowserDownloader(output=tmp_path, profile=tmp_path / "profile", channel="chrome",
                                   headed=False, cdp_url=None, timeout_ms=1000, pace=AsyncMock())
    downloader.download_once = AsyncMock(side_effect=[
        {"id": "123", "status": "browser_rejected_http_302", "browser_seconds": 1.5},
        {"id": "123", "status": "downloaded", "browser_seconds": 2.5},
    ])
    monkeypatch.setattr("src.sites.pep.browser_download.asyncio.sleep", AsyncMock())
    result = await downloader.download({"id": "123"})
    assert result["status"] == "downloaded"
    assert result["browser_attempts"] == 2
    assert result["browser_seconds"] == 4.0
    assert downloader.download_once.await_count == 2


@pytest.mark.asyncio
async def test_redirect_notice_is_classified_without_storing_query_or_tokens(tmp_path) -> None:
    downloader = BrowserDownloader(output=tmp_path, profile=tmp_path / "profile", channel="chrome",
                                   headed=False, cdp_url=None, timeout_ms=1000, pace=AsyncMock())
    downloader.last_redirect = "http://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?private=example"
    notice = SimpleNamespace(all_text_contents=AsyncMock(return_value=["Error de verificación captcha"]))
    downloader.page = SimpleNamespace(
        url="https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index",
        wait_for_url=AsyncMock(),
        locator=lambda selector: notice,
    )
    doc = {}
    await downloader.rejection_details(doc)
    assert doc == {"redirect_path": "/fdci/consultaCiudadana/index", "rejection_reason": "site_verification_notice"}


@pytest.mark.asyncio
async def test_explicit_captcha_refusal_is_not_resubmitted(tmp_path) -> None:
    downloader = BrowserDownloader(output=tmp_path, profile=tmp_path / "profile", channel="chrome",
                                   headed=False, cdp_url=None, timeout_ms=1000, pace=AsyncMock())
    downloader.download_once = AsyncMock(return_value={
        "id": "123", "status": "browser_rejected_http_302", "rejection_reason": "site_verification_notice",
    })
    await downloader.download({"id": "123"})
    downloader.download_once.assert_awaited_once()
