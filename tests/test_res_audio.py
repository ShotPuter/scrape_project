from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.sites.res.audio import LocalAudioSolver, validate_audio_url
from src.sites.res.source import SearchIssue


@pytest.mark.parametrize(
    "url",
    [
        "http://www.google.com/recaptcha/api2/payload",
        "https://evil.test/recaptcha/audio",
        "https://www.google.com.evil.test/recaptcha/audio",
        "https://user:secret@www.google.com/recaptcha/audio",
        "https://www.google.com:444/recaptcha/audio",
        "https://www.google.com/other/audio",
    ],
)
def test_audio_rejects_unexpected_origins(url):
    with pytest.raises(SearchIssue):
        validate_audio_url(url)


def test_brightdata_preflight_rejects_modified_ca_before_network(tmp_path, monkeypatch):
    from scripts import res_audio_pilot

    monkeypatch.setattr(
        res_audio_pilot,
        "dotenv_values",
        lambda _: {
            "BRIGHTDATA_PROXY_USERNAME": "test-user",
            "BRIGHTDATA_PROXY_PASSWORD": "test-password",
            "BRIGHTDATA_PROXY_SERVER": "http://brd.superproxy.io:44445",
        },
    )
    monkeypatch.setattr(res_audio_pilot.ssl, "PEM_cert_to_DER_cert", lambda _: b"modified-certificate")
    execute = AsyncMock()
    monkeypatch.setattr(res_audio_pilot.subprocess, "run", execute)
    with pytest.raises(ValueError, match="fingerprint mismatch"):
        res_audio_pilot.brightdata_proxy(tmp_path)
    execute.assert_not_called()


async def test_google_declines_audio_without_model_or_network_call(monkeypatch):
    monkeypatch.setattr("src.sites.res.audio.asyncio.sleep", AsyncMock())
    anchor = SimpleNamespace(click=AsyncMock(), get_attribute=AsyncMock(return_value="false"))
    button = SimpleNamespace(
        click=AsyncMock(), is_visible=AsyncMock(return_value=True), is_enabled=AsyncMock(return_value=True)
    )
    blocked = SimpleNamespace(count=AsyncMock(return_value=1), is_visible=AsyncMock(return_value=True))
    frame = SimpleNamespace(
        url="https://www.google.com/recaptcha/api2/bframe?test=1",
        locator=lambda s: button if s == "#recaptcha-audio-button" else blocked,
        evaluate=AsyncMock(),
    )
    page = SimpleNamespace(frames=[frame], frame_locator=lambda s: SimpleNamespace(locator=lambda s: anchor))
    events = []
    solver = LocalAudioSolver(None, events=events)
    with pytest.raises(SearchIssue, match="declined"):
        await solver(page)
    assert events == [{"round": 1, "status": "audio_blocked"}]
    frame.evaluate.assert_not_awaited()


def test_cdp_recovery_requires_completed_matching_valid_pdf(tmp_path):
    from scripts.res_audio_pilot import ChromeDownloads

    observer = ChromeDownloads(tmp_path)
    guid = "09e56608-d71b-42fc-9096-753572ff07b4"
    url = "https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve=ACuKDHhqsvAd"
    observer.started({"guid": guid, "url": url})
    (tmp_path / guid).write_bytes(b"%PDF-1.4\n%%EOF\n")
    assert observer.recover("ACuKDHhqsvAd", url, tmp_path) is None
    observer.progress({"guid": guid, "state": "completed"})
    assert observer.recover("ACuKDHhqsvAd", url + "different", tmp_path) is None
    assert observer.recover("ACuKDHhqsvAd", None, tmp_path) is None
    result = observer.recover("ACuKDHhqsvAd", url, tmp_path)
    assert result["status"] == "downloaded"
    (tmp_path / guid).write_bytes(b"<html>Error</html>")
    with pytest.raises(ValueError, match="not a complete PDF"):
        observer.recover("ACuKDHhqsvAd", url, tmp_path)
    with pytest.raises(ValueError):
        observer.started({"guid": "../outside", "url": url})


async def test_closed_challenge_is_not_accepted_without_checked_anchor(monkeypatch):
    monkeypatch.setattr("src.sites.res.audio.asyncio.sleep", AsyncMock())
    anchor = SimpleNamespace(click=AsyncMock(), get_attribute=AsyncMock(return_value="false"))
    page = SimpleNamespace(frames=[], frame_locator=lambda s: SimpleNamespace(locator=lambda s: anchor))
    with pytest.raises(SearchIssue, match="not offered"):
        await LocalAudioSolver(None)(page)


@pytest.mark.parametrize("denied", [False, True])
async def test_audio_is_fetched_in_browser_frame_and_requires_checked_anchor(monkeypatch, denied):
    monkeypatch.setattr("src.sites.res.audio.asyncio.sleep", AsyncMock())

    async def inference(function, *args):
        return function(*args)

    monkeypatch.setattr("src.sites.res.audio.asyncio.to_thread", inference)
    anchor = SimpleNamespace(click=AsyncMock(), get_attribute=AsyncMock(return_value="false"))
    button = SimpleNamespace(
        click=AsyncMock(), is_visible=AsyncMock(return_value=True), is_enabled=AsyncMock(return_value=True)
    )

    async def accept(**kwargs):
        anchor.get_attribute.return_value = "true"

    verify = SimpleNamespace(click=accept)
    audio_url = "https://www.google.com/recaptcha/api2/payload/audio.mp3?test=1"
    audio = SimpleNamespace(wait_for=AsyncMock(), get_attribute=AsyncMock(return_value=audio_url))
    field = SimpleNamespace(fill=AsyncMock())
    elements = {
        "#recaptcha-audio-button": button,
        ".rc-doscaptcha-header-text": SimpleNamespace(count=AsyncMock(return_value=0)),
        "#audio-source": audio,
        "#audio-response": field,
        "#recaptcha-verify-button": verify,
    }
    frame = SimpleNamespace(
        url="https://www.google.com/recaptcha/api2/bframe",
        locator=elements.__getitem__,
        evaluate=AsyncMock(return_value="YXVkaW8="),
    )
    page = SimpleNamespace(frames=[frame], frame_locator=lambda s: SimpleNamespace(locator=lambda s: anchor))

    def transcribe(stream, **kwargs):
        assert stream.read() == b"audio"
        return iter([SimpleNamespace(text="not have network access" if denied else " hello world ")]), None

    events = []
    solver = LocalAudioSolver(SimpleNamespace(transcribe=transcribe), events=events)
    if denied:
        with pytest.raises(SearchIssue, match="network-access denial"):
            await solver(page)
        field.fill.assert_not_awaited()
        assert events[-1]["status"] == "audio_network_denied"
        return
    await solver(page)
    assert frame.evaluate.await_args.args[1] == audio_url
    field.fill.assert_awaited_once_with("hello world", timeout=5000)
    assert [event["status"] for event in events] == ["audio_received", "transcribed", "checkbox_accepted"]


async def test_late_silent_acceptance_does_not_click_disabled_audio(monkeypatch):
    monkeypatch.setattr("src.sites.res.audio.asyncio.sleep", AsyncMock())
    anchor = SimpleNamespace(click=AsyncMock(), get_attribute=AsyncMock(side_effect=["false", "false", "true"]))
    button = SimpleNamespace(
        click=AsyncMock(), is_visible=AsyncMock(return_value=True), is_enabled=AsyncMock(return_value=False)
    )
    frame = SimpleNamespace(url="https://www.google.com/recaptcha/api2/bframe", locator=lambda s: button)
    page = SimpleNamespace(frames=[frame], frame_locator=lambda s: SimpleNamespace(locator=lambda s: anchor))
    events = []
    await LocalAudioSolver(None, events=events)(page)
    assert events == [{"status": "checkbox_accepted"}]
    button.click.assert_not_awaited()
