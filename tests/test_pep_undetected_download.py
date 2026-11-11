import asyncio
import sys
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.sites.pep.undetected_download import UndetectedDownloader
from tests.test_pep_nodriver_download import URL, configure


def make_downloader(tmp_path):
    return UndetectedDownloader(output=tmp_path, profile=tmp_path / "profile", channel="chrome", headed=True,
                                cdp_url=None, timeout_ms=40000, pace=AsyncMock(), headless=False)


def test_uc_launch_retains_tls_sandbox_and_proxy(tmp_path, monkeypatch):
    class Options:
        def __init__(self):
            self.arguments = []
            self.capabilities = {}

        def add_argument(self, argument):
            self.arguments.append(argument)

        def set_capability(self, name, value):
            self.capabilities[name] = value

    def chrome(**kwargs):
        options = kwargs["options"]
        options.add_argument("--no-sandbox")  # UC's unconditional default is filtered.
        return SimpleNamespace(options=options, kwargs=kwargs)

    monkeypatch.setitem(sys.modules, "undetected_chromedriver", SimpleNamespace(ChromeOptions=Options, Chrome=chrome))
    monkeypatch.setattr("src.sites.pep.undetected_download.shutil.which", lambda _: "/usr/bin/google-chrome")
    monkeypatch.setattr("src.sites.pep.undetected_download.subprocess.run",
                        lambda *a, **kw: SimpleNamespace(stdout="Google Chrome 154.0.8037.97"))
    driver = make_downloader(tmp_path).create_driver(["--proxy-server=http://127.0.0.1:9000", "--disable-quic"])
    assert driver.kwargs["version_main"] == 154
    assert driver.kwargs["no_sandbox"] is False
    assert driver.options.capabilities["acceptInsecureCerts"] is False
    assert "--no-sandbox" not in driver.options.arguments
    assert "--disable-quic" in driver.options.arguments
    assert "--window-size=1920,1080" in driver.options.arguments
    assert "--start-maximized" in driver.options.arguments
    assert "--proxy-server=http://127.0.0.1:9000" in driver.options.arguments
    assert not any("ignore-certificate" in arg for arg in driver.options.arguments)


@pytest.mark.asyncio
async def test_uc_owns_browser_and_cdp_only_attaches(tmp_path, monkeypatch):
    import nodriver

    browser = MagicMock()
    browser.aclose = AsyncMock()
    driver = SimpleNamespace(options=SimpleNamespace(debugger_address="127.0.0.1:9000"), quit=MagicMock())
    downloader = make_downloader(tmp_path)
    monkeypatch.setattr(downloader, "create_driver", lambda arguments: driver)
    attach = AsyncMock(return_value=browser)
    monkeypatch.setattr(nodriver, "start", attach)
    downloader.browser = await downloader.launch_browser([])
    assert attach.call_args.kwargs["port"] == 9000
    assert attach.call_args.kwargs["host"] == "127.0.0.1"
    await downloader.close()
    browser.aclose.assert_awaited_once()
    driver.quit.assert_called_once()
    assert downloader.driver is downloader.browser is None


@pytest.mark.asyncio
async def test_cancelled_launch_is_reaped(tmp_path, monkeypatch):
    downloader = make_downloader(tmp_path)
    started = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    driver = SimpleNamespace(quit=MagicMock())

    def create(arguments):
        loop.call_soon_threadsafe(started.set)
        assert release.wait(timeout=5)
        return driver

    monkeypatch.setattr(downloader, "create_driver", create)
    task = asyncio.create_task(downloader.launch_browser([]))
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        await downloader.close()
    driver.quit.assert_called_once()


@pytest.mark.asyncio
async def test_uc_uses_existing_native_form_and_download_validation(tmp_path, monkeypatch):
    downloader, browser, _, button, _ = configure(tmp_path, monkeypatch, downloader_class=UndetectedDownloader)
    monkeypatch.setattr(downloader, "launch_browser", AsyncMock(return_value=browser))
    result = await downloader.download({"id": "123", "source_page": URL})
    assert result["status"] == "downloaded"
    assert result["download_method"] == "browser_undetected_chromedriver"
    button.mouse_click.assert_awaited_once()


@pytest.mark.asyncio
async def test_uc_settles_before_native_click(tmp_path, monkeypatch):
    downloader, browser, _, button, _ = configure(tmp_path, monkeypatch, downloader_class=UndetectedDownloader)
    monkeypatch.setattr(downloader, "launch_browser", AsyncMock(return_value=browser))
    settle = AsyncMock(return_value=6.25)
    monkeypatch.setattr(downloader, "settle_page", settle)
    click = button.mouse_click.side_effect

    async def checked_click():
        settle.assert_awaited_once()
        await click()

    button.mouse_click.side_effect = checked_click
    result = await downloader.download({"id": "123", "source_page": URL})
    assert result["status"] == "downloaded"
    assert result["browser_settle_seconds"] == 6.25
    assert result["browser_page_age_at_click_seconds"] >= 0


@pytest.mark.asyncio
async def test_uc_does_not_retry_verification_refusal(tmp_path, monkeypatch):
    downloader, browser, _, button, _ = configure(tmp_path, monkeypatch, refused=True,
                                               downloader_class=UndetectedDownloader)
    monkeypatch.setattr(downloader, "launch_browser", AsyncMock(return_value=browser))
    first = await downloader.download({"id": "123", "source_page": URL})
    second = await downloader.download({"id": "456", "source_page": URL})
    assert first["rejection_reason"] == "site_verification_notice"
    assert second["status"] == "browser_session_blocked"
    button.mouse_click.assert_awaited_once()
