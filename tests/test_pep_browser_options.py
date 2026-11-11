import json
import random
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.sites.pep.browser_input import bezier_path, click_pointer, move_pointer
from src.sites.pep.browser_options import BrowserEnvironmentError, BrowserOptions
from tests.test_pep_browser_experiment import make_downloader
from tests.test_pep_nodriver_download import URL, configure


@pytest.mark.parametrize("kwargs", [{"render_mode": "fake-gpu"}, {"browser_language": "es-CO\r\nX: value"},
                                    {"browser_timezone": "not/a/timezone"}])
def test_options_reject_invalid_configuration(kwargs):
    with pytest.raises(ValueError):
        BrowserOptions(**kwargs)


def test_swiftshader_does_not_disable_security():
    options = BrowserOptions(render_mode="swiftshader", browser_language="es-CO", browser_timezone="America/Los_Angeles")
    assert options.arguments() == ["--use-gl=angle", "--use-angle=swiftshader", "--enable-webgl", "--lang=es-CO"]
    assert options.accept_language == "es-CO,es;q=0.9"
    assert "--no-sandbox" not in options.arguments()
    assert not any("unsafe" in argument for argument in options.arguments())


def test_profile_language_update_preserves_other_preferences(tmp_path):
    target = tmp_path / "Default" / "Preferences"
    target.parent.mkdir()
    target.write_text(json.dumps({"other": {"keep": 123}, "intl": {"selected_languages": "keep"}}))
    BrowserOptions(browser_language="es-CO").prepare_profile(tmp_path)
    result = json.loads(target.read_text())
    assert result["other"] == {"keep": 123}
    assert result["intl"] == {"selected_languages": "keep", "accept_languages": "es-CO,es"}


def test_bezier_path_is_bounded_reaches_target_and_slows_near_target():
    start, end = (20, 20), (800, 600)
    path = bezier_path(start, end, steps=20, width=1920, height=1080, rng=random.Random(0))
    assert len(path) == 20
    assert path[-1] == end
    assert all(0 <= x < 1920 and 0 <= y < 1080 for x, y in path)
    assert len(set(path)) == 20
    assert abs(path[1][0] - path[0][0]) > abs(path[-1][0] - path[-2][0])


@pytest.mark.parametrize("end", [(-1, 30), (1920, 30), (1, float("nan"))])
def test_invalid_pointer_target(end):
    with pytest.raises(ValueError):
        bezier_path((0, 0), end, steps=20, width=1920, height=1080)


@pytest.mark.asyncio
async def test_cdp_moves_then_presses_and_releases_without_dom_flash(monkeypatch):
    page = SimpleNamespace(send=AsyncMock())
    monkeypatch.setattr("src.sites.pep.browser_input.asyncio.sleep", AsyncMock())
    count = await move_pointer(page, (20, 20), (100, 100), width=1920, height=1080)
    await click_pointer(page, 100, 100)
    commands = [next(call.args[0]) for call in page.send.await_args_list]
    assert 15 <= count <= 25
    assert [cmd["params"]["type"] for cmd in commands] == ["mouseMoved"] * count + ["mousePressed", "mouseReleased"]
    assert all(cmd["method"] == "Input.dispatchMouseEvent" for cmd in commands)
    assert commands[-1]["params"]["buttons"] == 0


@pytest.mark.asyncio
async def test_motion_download_uses_cdp_path_not_nodriver_flash(tmp_path, monkeypatch):
    downloader, _, _, button, _ = configure(tmp_path, monkeypatch, browser_options=BrowserOptions(mouse_motion=True))
    move = AsyncMock(return_value=20)

    async def submit(*args):
        await button.mouse_click.side_effect()

    click = AsyncMock(side_effect=submit)
    monkeypatch.setattr("src.sites.pep.nodriver_download.move_pointer", move)
    monkeypatch.setattr("src.sites.pep.nodriver_download.click_pointer", click)
    result = await downloader.download({"id": "123", "source_page": URL})
    assert result["status"] == "downloaded"
    assert result["browser_mouse_moves"] == 20
    move.assert_awaited_once()
    click.assert_awaited_once()
    button.mouse_click.assert_not_awaited()


@pytest.mark.asyncio
async def test_motion_does_not_click_if_button_moved(tmp_path, monkeypatch):
    downloader, _, _, button, _ = configure(tmp_path, monkeypatch, browser_options=BrowserOptions(mouse_motion=True))
    initial = {"in_view": True, "receives_pointer": True, "x": 120, "y": 50, "width": 1920, "height": 1080}
    monkeypatch.setattr(downloader, "button_geometry", AsyncMock(side_effect=[initial, {**initial, "x": 150}]))
    monkeypatch.setattr("src.sites.pep.nodriver_download.move_pointer", AsyncMock(return_value=20))
    click = AsyncMock()
    monkeypatch.setattr("src.sites.pep.nodriver_download.click_pointer", click)
    result = await downloader.download({"id": "123", "source_page": URL})
    assert result["status"] == "browser_button_moved_before_click"
    click.assert_not_awaited()
    button.mouse_click.assert_not_awaited()


@pytest.mark.asyncio
async def test_required_webgl_failure_saves_diagnostic_before_raising(tmp_path, monkeypatch):
    downloader = make_downloader(tmp_path, browser_options=BrowserOptions(require_webgl=True))
    downloader.page = SimpleNamespace(evaluate=AsyncMock(return_value=json.dumps({"webgl": {"available": False}})))
    monkeypatch.setattr(downloader, "start", AsyncMock())
    with pytest.raises(BrowserEnvironmentError, match="WebGL is unavailable"):
        await downloader.save_environment()
    assert (tmp_path / "browser-environment-profile.json").is_file()


@pytest.mark.asyncio
async def test_locale_is_applied_before_navigation(tmp_path, monkeypatch):
    downloader, browser, start, _, _ = configure(tmp_path, monkeypatch, browser_options=BrowserOptions(
        browser_language="es-CO", browser_timezone="America/Los_Angeles",
    ))
    import nodriver

    nodriver.cdp.network.Headers = dict
    nodriver.cdp.network.set_extra_http_headers = lambda headers: ("headers", headers)
    nodriver.cdp.emulation = SimpleNamespace(
        set_locale_override=lambda locale: ("locale", locale),
        set_timezone_override=lambda timezone: ("timezone", timezone),
    )
    browser.main_tab.send = AsyncMock()
    await downloader.start()
    commands = [call.args[0] for call in browser.main_tab.send.await_args_list]
    assert ("headers", {"Accept-Language": "es-CO,es;q=0.9"}) in commands
    assert ("locale", "es_CO") in commands
    assert ("timezone", "America/Los_Angeles") in commands
    assert "--lang=es-CO" in start.call_args.kwargs["browser_args"]
    preferences = json.loads((downloader.profile / "Default" / "Preferences").read_text())
    assert preferences["intl"]["accept_languages"] == "es-CO,es"
