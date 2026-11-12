"""Experimental local audio recognition; browser traffic keeps its proxy session."""

from __future__ import annotations

import asyncio
import base64
import io
from time import monotonic
from urllib.parse import urlsplit

from src.sites.res.source import SearchIssue


def validate_audio_url(url):
    parts = urlsplit(url)
    if (
        parts.scheme != "https"
        or parts.hostname not in {"www.google.com", "www.recaptcha.net"}
        or parts.port not in (None, 443)
        or parts.username
        or parts.password
        or not parts.path.startswith("/recaptcha/")
    ):
        raise SearchIssue("captcha_required", "Unexpected CAPTCHA audio URL")
    return url


class LocalAudioSolver:
    """Opt-in pilot, bounded to three rounds; model inference runs outside the event loop."""

    def __init__(self, model, *, rounds=3, events=None):
        self.model = model
        self.rounds = rounds
        self.events = events if events is not None else []

    async def __call__(self, page, *, timeout_ms=60000, captcha_seconds=120, click_anchor=True):
        async with asyncio.timeout(captcha_seconds):
            await self.solve(page, timeout_ms=timeout_ms, click_anchor=click_anchor)

    async def solve(self, page, *, timeout_ms, click_anchor=True):
        anchor = page.frame_locator('iframe[title="reCAPTCHA"]').locator("#recaptcha-anchor")
        if click_anchor:
            await anchor.click(timeout=min(timeout_ms, 15000))
            await asyncio.sleep(2)
        if await anchor.get_attribute("aria-checked") == "true":
            self.events.append({"status": "checkbox_accepted"})
            return
        challenge = None
        for _ in range(20):
            if await anchor.get_attribute("aria-checked") == "true":
                self.events.append({"status": "checkbox_accepted"})
                return
            for frame in page.frames:
                parts = urlsplit(frame.url)
                if (
                    parts.hostname in {"www.google.com", "www.recaptcha.net"}
                    and parts.path.endswith("/bframe")
                    and await frame.locator("#recaptcha-audio-button").is_visible()
                ):
                    challenge = frame
                    break
            if challenge:
                break
            await asyncio.sleep(0.5)
        if challenge is None:
            raise SearchIssue("captcha_required", "Audio challenge was not offered")
        button = challenge.locator("#recaptcha-audio-button")
        for _ in range(40):
            if await anchor.get_attribute("aria-checked") == "true":
                self.events.append({"status": "checkbox_accepted"})
                return
            if await button.is_enabled():
                break
            await asyncio.sleep(0.25)
        else:
            self.events.append({"status": "audio_unavailable"})
            raise SearchIssue("captcha_required", "Audio challenge button remained disabled")
        try:
            await button.click(timeout=10000)
        except Exception as exc:
            if await anchor.get_attribute("aria-checked") == "true":
                self.events.append({"status": "checkbox_accepted"})
                return
            raise SearchIssue("captcha_required", "Audio challenge button was unavailable") from exc
        for number in range(1, self.rounds + 1):
            await asyncio.sleep(1)
            blocked = challenge.locator(".rc-doscaptcha-header-text")
            if await blocked.count() and await blocked.is_visible():
                self.events.append({"round": number, "status": "audio_blocked"})
                raise SearchIssue("captcha_required", "Google declined the audio challenge")
            audio = challenge.locator("#audio-source")
            try:
                await audio.wait_for(state="attached", timeout=12000)
            except Exception as exc:
                raise SearchIssue("captcha_required", "Audio source was not available") from exc
            url = validate_audio_url(await audio.get_attribute("src"))
            # Fetch inside the actual Google frame: browser cookies, TLS and proxy are preserved.
            # No requests.get(), direct connection or separate HTTP cookie session.
            payload = await challenge.evaluate(
                """async url => {
                const r = await fetch(url, {credentials:'include', redirect:'error',
                    signal:AbortSignal.timeout(20000)});
                if (!r.ok) throw new Error('Audio HTTP ' + r.status);
                const blob = await r.blob();
                if (!blob.size || blob.size > 2097152)
                    throw new Error('Audio size invalid: ' + blob.size + ' bytes; type=' + blob.type);
                return await new Promise((resolve, reject) => {
                    const reader = new FileReader();
                    reader.onerror = () => reject(new Error('Could not read CAPTCHA audio'));
                    reader.onload = () => resolve(String(reader.result).split(',')[1]);
                    reader.readAsDataURL(blob);
                });
            }""",
                url,
            )
            data = base64.b64decode(payload, validate=True)
            self.events.append({"round": number, "status": "audio_received", "size_bytes": len(data)})
            started = monotonic()
            text = await asyncio.to_thread(self.transcribe, data)
            self.events.append(
                {
                    "round": number,
                    "status": "transcribed" if text else "empty_transcript",
                    "seconds": round(monotonic() - started, 2),
                }
            )
            if not text:
                raise SearchIssue("captcha_required", "Local audio recognition returned no text")
            if any(phrase in text.casefold() for phrase in ("network access", "automated queries", "computer or network")):
                self.events.append({"round": number, "status": "audio_network_denied"})
                raise SearchIssue("captcha_required", "Google returned an audio network-access denial")
            await challenge.locator("#audio-response").fill(text, timeout=5000)
            await challenge.locator("#recaptcha-verify-button").click(timeout=5000)
            for _ in range(12):
                await asyncio.sleep(0.5)
                if await anchor.get_attribute("aria-checked") == "true":
                    self.events.append({"round": number, "status": "checkbox_accepted"})
                    return
            if number < self.rounds:
                await challenge.locator("#recaptcha-reload-button").click(timeout=5000)
        raise SearchIssue("captcha_required", "Audio attempt limit reached")

    def transcribe(self, data):
        segments, _ = self.model.transcribe(io.BytesIO(data), beam_size=5)
        return " ".join(segment.text.strip() for segment in segments).strip()
