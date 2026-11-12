"""Experimental local-VLM reCAPTCHA grid solver over an Ollama-compatible endpoint.

Per-tile binary questioning is used instead of asking for cell indices: small local
models answer "does this tile contain X" far more reliably than "return JSON indices".
Accuracy is still low, so this runs only as one stage of the local cascade and every
round is measured. No API keys, no registrations, no external solver services.
"""

from __future__ import annotations

import asyncio
import base64
import json
from time import monotonic
from urllib.parse import urlsplit

import httpx

from src.sites.res.source import SearchIssue


def bframe(page):
    """The challenge frame, matched by URL shape so a stale frame cannot be picked."""
    for frame in page.frames:
        parts = urlsplit(frame.url)
        if parts.hostname in {"www.google.com", "www.recaptcha.net"} and parts.path.endswith("/bframe"):
            return frame
    return None


def instruction_object(text: str) -> str:
    """Extract the requested object from a possibly multi-line challenge instruction."""
    cleaned = " ".join(text.split())
    lowered = cleaned.casefold()
    value = cleaned
    for marker in (" con ", " with ", " с "):
        if marker in lowered:
            value = cleaned[lowered.rindex(marker) + len(marker) :]
            break
    for stop in (
        " si no hay",
        " if there are none",
        " haz clic",
        " haga clic",
        " click ",
        " omitir",
        " skip",
        " если их нет",
        " пропустить",
    ):
        index = value.casefold().find(stop)
        if index > 0:
            value = value[:index]
    return value.strip(" .:")


async def challenge_heading(frame, timeout_ms=12000) -> str:
    """Enterprise markup varies by locale; poll every known instruction container."""
    selectors = (
        ".rc-imageselect-desc-no-canonical",
        ".rc-imageselect-instructions",
        ".rc-imageselect-desc",
        "#rc-imageselect-instructions",
        ".rc-imageselect-desc-wrapper",
    )
    deadline = monotonic() + timeout_ms / 1000
    while monotonic() < deadline:
        for selector in selectors:
            locator = frame.locator(selector).first
            try:
                if await locator.count() and await locator.is_visible():
                    text = (await locator.inner_text(timeout=2000)).strip()
                    if text:
                        return text
            except Exception:
                continue
        await asyncio.sleep(0.25)
    try:
        return (await frame.locator("body").inner_text(timeout=3000)).strip()
    except Exception:
        return ""


def tiles(frame):
    """Grid cells in reading order; the layout is 3x3 or 4x4 depending on the challenge."""
    return frame.locator("#rc-imageselect-target td.rc-imageselect-tile")


def rejects(text: str) -> bool:
    value = text.strip().casefold().strip(" .!¡¿?")
    if value.startswith(("no", "false", "nope")):
        return True
    return not value.startswith(("si", "sí", "yes", "correct", "true"))


class LocalGridSolver:
    """Ask a local VLM about each tile, click the positives, submit, repeat on failure."""

    def __init__(self, model, *, endpoint="http://127.0.0.1:11434", rounds=2, events=None, timeout=60.0):
        self.model = model
        self.endpoint = endpoint.rstrip("/")
        self.rounds = rounds
        self.events = events if events is not None else []
        self.timeout = timeout

    async def ask(self, client, image: bytes, target: str) -> bool:
        prompt = (
            f"Look at this single cropped CAPTCHA tile. Does it clearly show {target}? "
            "Answer with exactly one word: yes or no."
        )
        payload = {
            "model": self.model,
            "prompt": prompt,
            "images": [base64.b64encode(image).decode()],
            "stream": False,
            "think": False,
            "options": {"temperature": 0, "num_predict": 4},
        }
        response = await client.post(f"{self.endpoint}/api/generate", json=payload, timeout=self.timeout)
        response.raise_for_status()
        answer = json.loads(response.text).get("response", "")
        return not rejects(answer)

    async def solve(self, page, *, deadline_seconds=180) -> bool:
        started = monotonic()
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for round_number in range(1, self.rounds + 1):
                frame = bframe(page)
                if frame is None:
                    self.events.append({"round": round_number, "status": "no_bframe"})
                    return False
                heading = await challenge_heading(frame)
                target = instruction_object(heading) or "the requested object"
                cells = tiles(frame)
                count = await cells.count()
                for _ in range(40):
                    if count:
                        break
                    await page.wait_for_timeout(250)
                    count = await cells.count()
                if count == 0:
                    self.events.append({"round": round_number, "status": "no_tiles"})
                    return False
                selected = 0
                for index in range(count):
                    if monotonic() - started > deadline_seconds:
                        self.events.append({"round": round_number, "status": "deadline", "tiles": count})
                        return False
                    cell = cells.nth(index)
                    try:
                        image = await cell.screenshot(timeout=10000)
                    except Exception:
                        continue
                    if await self.ask(client, image, target):
                        try:
                            await cell.click(timeout=5000)
                            selected += 1
                        except Exception:
                            pass
                self.events.append(
                    {"round": round_number, "status": "submitted", "tiles": count, "selected": selected, "target": target}
                )
                try:
                    await frame.locator("#recaptcha-verify-button").click(timeout=10000)
                except Exception as exc:
                    raise SearchIssue("captcha_required", "VLM could not submit the grid") from exc
                for _ in range(40):
                    if await self.accepted(page):
                        self.events.append({"round": round_number, "status": "accepted"})
                        return True
                    await page.wait_for_timeout(250)
        self.events.append({"status": "exhausted"})
        return False

    @staticmethod
    async def accepted(page) -> bool:
        anchor = page.frame_locator('iframe[title="reCAPTCHA"]').locator("#recaptcha-anchor")
        try:
            return await anchor.get_attribute("aria-checked") == "true"
        except Exception:
            return False
