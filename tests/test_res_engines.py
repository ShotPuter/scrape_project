import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from src.sites.dequienes.navigation import navigate
from src.sites.res.engines import launch_context


@pytest.mark.parametrize("engine", ["camoufox", "patchright"])
async def test_modified_proxy_ca_is_rejected_before_browser_launch(tmp_path, monkeypatch, engine):
    certificate = tmp_path / "untrusted.pem"
    certificate.write_text("modified certificate")
    monkeypatch.setattr("src.sites.res.engines.ssl.PEM_cert_to_DER_cert", lambda _: b"modified")
    with pytest.raises(ValueError, match="CA fingerprint mismatch"):
        async with launch_context(tmp_path / "profile", engine=engine, proxy_ca=certificate):
            pytest.fail("Untrusted certificate must not reach browser startup")


@pytest.mark.parametrize("fail", [False, True])
async def test_owned_display_proxy_and_context_are_closed_on_success_and_launch_failure(tmp_path, monkeypatch, fail):
    closed = []
    options = {}
    original_display = os.environ.get("DISPLAY")

    class Display:
        returncode = None

        async def wait(self):
            closed.append("display-wait")
            self.returncode = 0

        def terminate(self):
            closed.append("display")

        stdout = SimpleNamespace(readline=lambda: asyncio.sleep(0, result=b"93\n"))

    async def subprocess(*args, **kwargs):
        return Display()

    class Tunnel:
        def __init__(self, settings, **kwargs):
            assert settings["password"] == "secret"

        async def start(self):
            return "http://127.0.0.1:12345"

        async def close(self):
            closed.append("tunnel")

    async def launch(profile, **kwargs):
        options.update(kwargs)
        if fail:
            raise RuntimeError("launch failed")

        async def close():
            closed.append("context")

        return SimpleNamespace(close=close)

    @asynccontextmanager
    async def playwright():
        try:
            yield SimpleNamespace(chromium=SimpleNamespace(launch_persistent_context=launch))
        finally:
            closed.append("driver")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", subprocess)
    monkeypatch.setattr("src.sites.pep.proxy_tunnel.ProxyTunnel", Tunnel)
    monkeypatch.setitem(sys.modules, "patchright.async_api", SimpleNamespace(async_playwright=playwright))
    try:
        async with launch_context(tmp_path, proxy={"password": "secret"}, pdf=True):
            assert not fail
    except RuntimeError:
        assert fail
    assert "tunnel" in closed and "display" in closed and "driver" in closed
    assert ("context" in closed) is not fail
    assert os.environ.get("DISPLAY") == original_display
    assert options["env"]["DISPLAY"] == ":93"
    assert options["proxy"] == {"server": "http://127.0.0.1:12345"}
    assert options["chromium_sandbox"] is True
    assert not options["headless"]
    assert "secret" not in str(options)
    prefs = json.loads((tmp_path / "Default" / "Preferences").read_text())
    assert prefs["plugins"]["always_open_pdf_externally"] is True


async def test_challenge_checkbox_and_response_replacement():
    listeners = []
    clicked = []
    url = "https://dequienes.cl/"

    def response(status, text):
        return SimpleNamespace(status=status, url=url, text=lambda: asyncio.sleep(0, result=text))

    async def click(**kwargs):
        clicked.append(True)
        for listener in listeners:
            listener(response(200, "<h1>DeQuienes</h1>"))

    checkbox = SimpleNamespace(is_visible=lambda **k: asyncio.sleep(0, result=True), click=click)
    frame = SimpleNamespace(
        url="https://challenges.cloudflare.com/widget", locator=lambda s: SimpleNamespace(first=checkbox)
    )

    class Page:
        frames = [frame]

        def __init__(self):
            self.url = url

        def on(self, event, callback):
            listeners.append(callback)

        def remove_listener(self, event, callback):
            listeners.remove(callback)

        async def goto(self, *args, **kwargs):
            return response(403, "Just a moment")

    r = await navigate(Page(), url, timeout_ms=2000)
    assert r.status == 200 and clicked == [True]
    assert not listeners
