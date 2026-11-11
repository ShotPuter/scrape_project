import json
from unittest.mock import AsyncMock

import pytest

from src.sites.pep.browser_diagnostics import browser_environment
from src.sites.pep.nodriver_download import NodriverDownloader


def make_downloader(tmp_path, **kwargs):
    return NodriverDownloader(output=tmp_path, profile=tmp_path / "profile", channel="chrome", headed=True,
                              cdp_url=None, timeout_ms=40000, pace=AsyncMock(), headless=False, **kwargs)


@pytest.mark.parametrize(("minimum", "maximum"), [(-1, 2), (8, 4), (0, 16), (float("nan"), 4), (1, float("inf"))])
def test_invalid_settle_range(tmp_path, minimum, maximum):
    with pytest.raises(ValueError, match="page-settle"):
        make_downloader(tmp_path, page_settle_min=minimum, page_settle_max=maximum)


@pytest.mark.asyncio
async def test_default_dwell_does_not_sleep_or_scroll(tmp_path, monkeypatch):
    downloader = make_downloader(tmp_path)
    sleep = AsyncMock()
    monkeypatch.setattr("src.sites.pep.nodriver_download.asyncio.sleep", sleep)
    assert await downloader.settle_page() == 0
    sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_bounded_dwell_scrolls_and_restores_position(tmp_path, monkeypatch):
    downloader = make_downloader(tmp_path, page_settle_min=4, page_settle_max=8, page_settle_scroll=True)
    downloader.page = AsyncMock()
    downloader.page.evaluate.return_value = json.dumps({"x": 10, "y": 200})
    sleep = AsyncMock()
    monkeypatch.setattr("src.sites.pep.nodriver_download.asyncio.sleep", sleep)
    monkeypatch.setattr("src.sites.pep.nodriver_download.random.uniform", lambda low, high: 6.0)
    assert await downloader.settle_page() == 6
    assert [call.args[0] for call in sleep.await_args_list] == [3, 3]
    commands = [call.args[0] for call in downloader.page.evaluate.await_args_list]
    assert "scrollBy" in commands[1]
    assert commands[2] == "window.scrollTo(10,200)"
    assert not any("grecaptcha" in command for command in commands)


@pytest.mark.asyncio
async def test_dwell_only_does_not_need_page_commands(tmp_path, monkeypatch):
    downloader = make_downloader(tmp_path, page_settle_min=4, page_settle_max=4)
    sleep = AsyncMock()
    monkeypatch.setattr("src.sites.pep.nodriver_download.asyncio.sleep", sleep)
    assert await downloader.settle_page() == 4
    sleep.assert_awaited_once_with(4)


@pytest.mark.asyncio
@pytest.mark.parametrize(("renderer", "software"), [("llvmpipe (LLVM)", True), ("ANGLE SwiftShader", True),
                                                    ("Mesa Intel UHD Graphics", False), (None, None)])
async def test_environment_distinguishes_mesa_from_software_rendering(renderer, software):
    page = AsyncMock()
    page.evaluate.return_value = json.dumps({
        "platform": "Linux x86_64", "webgl": {"renderer": renderer}, "cookie": "must-not-persist",
    })
    result = await browser_environment(page)
    assert result["software_renderer_detected"] is software
    assert result["platform"] == "Linux x86_64"
    assert "cookie" not in result


@pytest.mark.asyncio
async def test_environment_report_is_local_private_and_contains_no_page_text(tmp_path, monkeypatch):
    downloader = make_downloader(tmp_path)
    downloader.page = AsyncMock()
    downloader.page.evaluate.return_value = json.dumps({"webgl": {"renderer": "SwiftShader"}})
    monkeypatch.setattr(downloader, "start", AsyncMock())
    path = await downloader.save_environment()
    assert path.name == "browser-environment-profile.json"
    assert path.stat().st_mode & 0o777 == 0o600
    assert json.loads(path.read_text())["software_renderer_detected"] is True
    expression = downloader.page.evaluate.call_args.args[0]
    assert "document.body" not in expression
    assert "document.cookie" not in expression
    assert "fetch(" not in expression
