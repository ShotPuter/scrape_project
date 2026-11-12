"""Navigate public DeQuienes pages and interact with visible challenge checkboxes."""

from __future__ import annotations

import asyncio
from time import monotonic
from urllib.parse import urlsplit


async def navigate(page, url, *, timeout_ms=60000, on_progress=None):
    from src.sites.dequienes.hybrid import is_challenge

    expected = urlsplit(url)
    if expected.scheme != "https" or expected.hostname != "dequienes.cl":
        raise ValueError("Unexpected public navigation URL")
    responses = []

    def observe(response):
        parts = urlsplit(response.url)
        if parts.scheme == "https" and parts.hostname == expected.hostname and parts.path == expected.path:
            responses.append(response)

    page.on("response", observe)
    try:
        initial = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        actual = urlsplit(page.url)
        if actual.scheme != "https" or actual.hostname != expected.hostname or actual.path != expected.path:
            raise ValueError("Unexpected public navigation redirect")
        if initial is not None and initial.status == 200 and not is_challenge(await initial.text()):
            return initial
        deadline = monotonic() + timeout_ms / 1000
        clicked = False
        while monotonic() < deadline:
            parts = urlsplit(page.url)
            if parts.scheme != "https" or parts.hostname != "dequienes.cl":
                raise ValueError("Unexpected public navigation redirect")
            if responses and responses[-1].status == 200:
                latest = responses[-1]
                if not is_challenge(await latest.text()):
                    return latest
            if on_progress:
                on_progress("automatic_browser_check", last_http_status=responses[-1].status if responses else None)
            if not clicked:
                for frame in page.frames:
                    if urlsplit(frame.url).hostname != "challenges.cloudflare.com":
                        continue
                    for selector in ('input[type="checkbox"]', '[role="checkbox"]'):
                        checkbox = frame.locator(selector).first
                        try:
                            if await checkbox.is_visible(timeout=1000):
                                await checkbox.click(timeout=min(3000, max(1, int((deadline - monotonic()) * 1000))))
                                clicked = True
                                break
                        except Exception:
                            pass  # Frame replacement during a challenge reload; retry within the deadline.
                    if clicked:
                        break
            await asyncio.sleep(min(1, max(0, deadline - monotonic())))
        return responses[-1] if responses else initial
    finally:
        page.remove_listener("response", observe)
