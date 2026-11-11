from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.sites.pep.browser_download import BrowserDownloader

LANDING = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/consultaPEP"


def browser(tmp_path, *, warmup=True):
    return BrowserDownloader(output=tmp_path, profile=tmp_path / "profile", channel="chrome", headed=True,
                             cdp_url=None, timeout_ms=1000, pace=AsyncMock(), native=True, warmup=warmup)


@pytest.mark.asyncio
async def test_browser_landing_is_visited_once_per_owned_session(tmp_path):
    downloader = browser(tmp_path)
    downloader.page = SimpleNamespace(url=LANDING, goto=AsyncMock(return_value=SimpleNamespace(status=200)))
    await downloader.initialize_browser_session()
    await downloader.initialize_browser_session()
    downloader.page.goto.assert_awaited_once_with(LANDING, wait_until="domcontentloaded", timeout=1000)
    downloader.pace.assert_awaited_once()
    assert downloader.session_initialized is True
    assert downloader.warmup_http_status == 200


@pytest.mark.asyncio
async def test_warmup_remains_opt_in_for_other_browser_workflows(tmp_path):
    downloader = browser(tmp_path, warmup=False)
    await downloader.initialize_browser_session()
    downloader.pace.assert_not_awaited()
    assert downloader.page is None


@pytest.mark.asyncio
async def test_forbidden_landing_stops_before_document_submission(tmp_path):
    downloader = browser(tmp_path)
    downloader.start = AsyncMock()
    downloader.page = SimpleNamespace(goto=AsyncMock(return_value=SimpleNamespace(status=403)))
    result = await downloader.download({"id": "123", "source_page": LANDING})
    assert result["status"] == "browser_warmup_http_403"
    assert result["failure_stage"] == "browser_session_initialize"
    assert result["browser_warmup_http_status"] == 403
    assert downloader.failures == 3
    downloader.page.goto.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_resets_browser_session_initialization(tmp_path):
    downloader = browser(tmp_path)
    downloader.session_initialized = True
    downloader.warmup_http_status = 200
    await downloader.close()
    assert downloader.session_initialized is False
    assert downloader.warmup_http_status is None
