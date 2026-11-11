"""Shared bounded browser diagnostics; no tokens, cookies or response bodies in logs."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

from src.sites.pep.source import DOWNLOAD_PATH, HOST, RESULT_PATH

LANDING_PATH = "/fdci/consultaCiudadana/consultaPEP"
LANDING = f"https://{HOST}{LANDING_PATH}"
READY = "typeof grecaptcha !== 'undefined' && typeof grecaptcha.execute === 'function'"

# No navigation, cookies, storage, page text, tokens, or third-party fingerprint service.
ENVIRONMENT = """(() => {
    const canvas = document.createElement('canvas');
    let webgl = {available:false, vendor:null, renderer:null};
    try {
        const gl = canvas.getContext('webgl') || canvas.getContext('experimental-webgl');
        if (gl) {
            const ext = gl.getExtension('WEBGL_debug_renderer_info');
            webgl = {available:true,
                     vendor:String(gl.getParameter(ext ? ext.UNMASKED_VENDOR_WEBGL : gl.VENDOR)),
                     renderer:String(gl.getParameter(ext ? ext.UNMASKED_RENDERER_WEBGL : gl.RENDERER))};
            gl.getExtension('WEBGL_lose_context')?.loseContext();
        }
    } catch (_) {}
    return {user_agent:navigator.userAgent, platform:navigator.platform,
            languages:Array.from(navigator.languages), webdriver:navigator.webdriver,
            timezone:Intl.DateTimeFormat().resolvedOptions().timeZone, locale:Intl.DateTimeFormat().resolvedOptions().locale,
            user_agent_data:navigator.userAgentData ? {
                brands:navigator.userAgentData.brands, platform:navigator.userAgentData.platform,
                mobile:navigator.userAgentData.mobile} : null,
            screen:{width:screen.width,height:screen.height,avail_width:screen.availWidth,avail_height:screen.availHeight},
            window:{outer_width:outerWidth,outer_height:outerHeight,inner_width:innerWidth,inner_height:innerHeight,
                    device_pixel_ratio:devicePixelRatio}, webgl:webgl};
})()"""


async def browser_environment(page) -> dict:
    result = await nodriver_value(page, ENVIRONMENT)
    if not isinstance(result, dict):
        raise ValueError("Unexpected browser environment result")
    environment = {key: result.get(key) for key in (
        "user_agent", "platform", "languages", "webdriver", "user_agent_data", "screen", "window", "webgl",
        "timezone", "locale",
    )}
    renderer = str((environment.get("webgl") or {}).get("renderer") or "").lower()
    environment["software_renderer_detected"] = any(
        marker in renderer for marker in ("swiftshader", "llvmpipe", "softpipe", "software rasterizer")
    ) if renderer else None
    environment["score_note"] = "Local browser observations only; reCAPTCHA score is not available to this client."
    return environment


class NetworkState:
    def __init__(self):
        self.statuses: dict[str, int] = {}

    def response(self, url: str, status: int | float) -> None:
        parsed = urlsplit(url)
        if parsed.hostname == HOST and parsed.path in {LANDING_PATH, RESULT_PATH, DOWNLOAD_PATH}:
            self.statuses[parsed.path] = int(status)


def notice_reason(html: str) -> str | None:
    messages = BeautifulSoup(html, "html.parser").select('.alert, .message, .errors, [role="alert"]')
    text = " ".join(item.get_text(" ", strip=True) for item in messages).lower()
    if any(word in text for word in ("captcha", "robot", "verificaci", "verification")):
        return "site_verification_notice"
    if any(word in text for word in ("demasiad", "too many", "límite", "limite")):
        return "site_limit_notice"
    return None


def completed_file(directory: Path) -> Path | None:
    files = [path for path in directory.iterdir() if path.is_file() and not path.is_symlink()]
    if any(path.suffix == ".crdownload" for path in files):
        return None
    candidates = [path for path in files if path.suffix.lower() in {".zip", ".pdf"}]
    if len(candidates) > 1:
        raise ValueError("Multiple downloads from one submission")
    return candidates[0] if candidates else None


async def nodriver_value(page, expression: str):
    # nodriver 0.50 returns RemoteObject for falsey values. Decode a JSON string.
    value = await page.evaluate(f"JSON.stringify({expression})", return_by_value=True)
    if not isinstance(value, str):
        raise ValueError("Unexpected JavaScript result type")
    return json.loads(value)
