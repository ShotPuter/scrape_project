import json
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.sites.pep.browser_diagnostics import LANDING, nodriver_value
from src.sites.pep.nodriver_download import NodriverDownloader
from src.sites.pep.source import DOWNLOAD_PATH

URL = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?find=example"
PDF = b"%PDF-1.4\nexample\n%%EOF"


def configure(tmp_path, monkeypatch, *, refused=False, landing_status=200, downloader_class=NodriverDownloader,
              **downloader_options):
    handlers, navigation = {}, []
    cdp = SimpleNamespace(
        network=SimpleNamespace(ResponseReceived=type("ResponseReceived", (), {}),
                                RequestWillBeSent=type("RequestWillBeSent", (), {}), enable=lambda: None),
        page=SimpleNamespace(DomContentEventFired=type("DomContentEventFired", (), {}), enable=lambda: None,
                             navigate=lambda url: ("navigate", url)),
    )
    page = SimpleNamespace(add_handler=lambda kind, callback: handlers.update({kind: callback}),
                           get_content=AsyncMock(return_value="<html></html>"))

    async def response(url, status):
        await handlers[cdp.network.ResponseReceived](SimpleNamespace(response=SimpleNamespace(url=url, status=status)))

    async def send(command):
        if command:
            _, url = command
            navigation.append(url)
            page.url = url
            await response(url, landing_status if url == LANDING else 200)
            await handlers[cdp.page.DomContentEventFired](None)

    async def evaluate(expression, **kwargs):
        if "getBoundingClientRect" in expression:
            assert button.scroll_into_view.await_count > 0
            return json.dumps({"in_view": True, "receives_pointer": True, "x": 120, "y": 50,
                               "width": 1920, "height": 1080})
        return json.dumps(page.url) if "location.href" in expression else "true"

    async def set_path(path):
        page.download_dir = path

    async def click():
        await response(f"https://www.funcionpublica.gov.co{DOWNLOAD_PATH}", 302 if refused else 200)
        if refused:
            page.get_content.return_value = '<div class="alert">Recaptcha did not pass validation</div>'
        else:
            (page.download_dir / "response.pdf").write_bytes(PDF)

    button = SimpleNamespace(mouse_click=AsyncMock(side_effect=click), scroll_into_view=AsyncMock())
    page.send = AsyncMock(side_effect=send)
    page.evaluate = AsyncMock(side_effect=evaluate)
    page.set_download_path = AsyncMock(side_effect=set_path)
    page.select = AsyncMock(return_value=button)
    process = SimpleNamespace(wait=AsyncMock(), kill=MagicMock())
    browser = SimpleNamespace(main_tab=page, _process=process, stop=MagicMock())
    start = AsyncMock(return_value=browser)
    monkeypatch.setitem(sys.modules, "nodriver", SimpleNamespace(start=start, cdp=cdp))
    monkeypatch.setattr("src.sites.pep.nodriver_download.os.geteuid", lambda: 1000)
    monkeypatch.setattr("src.sites.pep.nodriver_download.shutil.which", lambda name: "/usr/bin/google-chrome")
    downloader = downloader_class(output=tmp_path, profile=tmp_path / "profile", channel="chrome", headed=True,
                                    cdp_url=None, timeout_ms=1000, pace=AsyncMock(), headless=False,
                                     warmup=True, retry_transient_redirect=False, **downloader_options)
    return downloader, browser, start, button, navigation


@pytest.mark.asyncio
async def test_two_documents_reuse_one_browser_page_and_profile(tmp_path, monkeypatch):
    downloader, browser, start, button, navigation = configure(tmp_path, monkeypatch)
    first = await downloader.download({"id": "123", "source_page": URL})
    second = await downloader.download({"id": "456", "source_page": URL})
    assert first["status"] == second["status"] == "downloaded"
    assert first["download_method"] == "browser_nodriver"
    assert first["browser_http_status"] == 200
    assert len(first["files"]) == 1
    assert (tmp_path / first["local_path"]).read_bytes() == PDF
    start.assert_awaited_once()
    assert navigation == [LANDING, URL]
    assert button.mouse_click.await_count == 2
    assert button.scroll_into_view.await_count == 2
    await downloader.close()
    browser.stop.assert_called_once()
    browser._process.wait.assert_awaited_once()


@pytest.mark.asyncio
async def test_explicit_refusal_never_resubmits_next_document(tmp_path, monkeypatch):
    downloader, _, _, button, _ = configure(tmp_path, monkeypatch, refused=True)
    first = await downloader.download({"id": "123", "source_page": URL})
    second = await downloader.download({"id": "456", "source_page": URL})
    assert first["status"] == "browser_rejected_http_302"
    assert first["rejection_reason"] == "site_verification_notice"
    assert second["status"] == "browser_session_blocked"
    assert second["browser_attempts"] == 0
    button.mouse_click.assert_awaited_once()
    await downloader.close()


@pytest.mark.asyncio
async def test_forbidden_landing_stops_before_consultation(tmp_path, monkeypatch):
    downloader, _, _, button, navigation = configure(tmp_path, monkeypatch, landing_status=403)
    result = await downloader.download({"id": "123", "source_page": URL})
    assert result["status"] == "browser_navigation_http_403"
    assert downloader.failures == 3
    assert navigation == [LANDING]
    button.mouse_click.assert_not_awaited()
    await downloader.close()


@pytest.mark.asyncio
async def test_falsey_remote_javascript_is_not_mistaken_for_ready():
    page = SimpleNamespace(evaluate=AsyncMock(return_value="false"))
    assert await nodriver_value(page, "false") is False


@pytest.mark.asyncio
async def test_pointer_blocked_button_is_not_clicked_or_reported_as_captcha(tmp_path, monkeypatch):
    downloader, browser, _, button, _ = configure(tmp_path, monkeypatch)
    original = browser.main_tab.evaluate.side_effect

    async def evaluate(expression, **kwargs):
        if "getBoundingClientRect" in expression:
            return json.dumps({"in_view": True, "receives_pointer": False})
        return await original(expression, **kwargs)

    browser.main_tab.evaluate.side_effect = evaluate
    result = await downloader.download({"id": "123", "source_page": URL})
    assert result["status"] == "browser_button_not_actionable"
    assert result["failure_stage"] == "button_visibility"
    assert "rejection_reason" not in result
    button.mouse_click.assert_not_awaited()
    await downloader.close()


@pytest.mark.asyncio
async def test_proxy_launch_uses_only_loopback_and_closes_on_start_failure(tmp_path, monkeypatch):
    from src.sites.pep.proxy_tunnel import ProxyTunnel

    downloader, _, start, _, _ = configure(tmp_path, monkeypatch)
    downloader.tunnel = ProxyTunnel({"server": "http://127.0.0.1:1", "username": "secret-user", "password": "secret"})
    start.side_effect = OSError("startup failed")
    try:
        with pytest.raises(OSError):
            await downloader.start()
        assert downloader.tunnel.server
        arguments = start.call_args.kwargs["browser_args"]
        assert any(arg.startswith("--proxy-server=http://127.0.0.1:") for arg in arguments)
        assert "--disable-quic" in arguments
        assert "secret" not in repr(start.call_args)
    finally:
        await downloader.close()
    assert downloader.tunnel.server is None


@pytest.mark.asyncio
async def test_egress_mismatch_prevents_source_navigation(tmp_path, monkeypatch):
    downloader, browser, _, _, navigation = configure(tmp_path, monkeypatch)
    browser.main_tab.evaluate.return_value = json.dumps('{"ip":"192.0.2.2"}')
    browser.main_tab.evaluate.side_effect = None
    with pytest.raises(ValueError, match="egress mismatch"):
        await downloader.verify_egress("192.0.2.1")
    assert navigation == ["https://api.ipify.org?format=json"]
    await downloader.close()
