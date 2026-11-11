"""Persistent, serialized nodriver downloader with the same contract as native CDP."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import math
import os
import random
import shutil
import tempfile
import time
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from src.sites.pep.browser_diagnostics import (
    LANDING,
    LANDING_PATH,
    READY,
    NetworkState,
    browser_environment,
    completed_file,
    nodriver_value,
    notice_reason,
)
from src.sites.pep.browser_download import BrowserDownloader, DownloadFailure
from src.sites.pep.browser_input import click_pointer, move_pointer
from src.sites.pep.browser_options import BrowserEnvironmentError, BrowserOptions
from src.sites.pep.documents import MAX_PDF_BYTES, save_payload
from src.sites.pep.proxy_tunnel import ProxyTunnel
from src.sites.pep.source import DOWNLOAD_PATH, LEGACY_PATH, RESULT_PATH, safe_url
from src.utils.atomic import atomic_write_json


class NodriverDownloader(BrowserDownloader):
    """One owned Chrome/page/profile and fixed optional proxy per session."""

    ADAPTER_REVISION = "nodriver-visible-native-click-v2"
    DOWNLOAD_METHOD = "browser_nodriver"

    def __init__(self, *, ca_bundle: Path | None = None, page_settle_min: float = 0,
                 page_settle_max: float = 0, page_settle_scroll: bool = False,
                 browser_options: BrowserOptions | None = None, **kwargs):
        super().__init__(**kwargs)
        self.browser_options = browser_options or BrowserOptions()
        self.pointer_position = None
        if (not math.isfinite(page_settle_min) or not math.isfinite(page_settle_max)
                or not 0 <= page_settle_min <= page_settle_max <= 15):
            raise ValueError("page-settle range must satisfy 0 <= min <= max <= 15 seconds")
        if page_settle_scroll and page_settle_max == 0:
            raise ValueError("page-settle-scroll requires a positive page-settle range")
        self.ca_bundle = ca_bundle
        self.page_settle_min = page_settle_min
        self.page_settle_max = page_settle_max
        self.page_settle_scroll = page_settle_scroll
        self.loaded_at = None
        if self.cdp_url or self.headless:
            raise ValueError("nodriver benchmark requires an owned headed browser (use Xvfb)")
        self.tunnel = ProxyTunnel(self.proxy, timeout=max(30, self.timeout_ms / 1000)) if self.proxy else None
        self.browser = None
        self.state = NetworkState()
        self.dom_ready = asyncio.Event()
        self.response_received = asyncio.Event()

    async def start(self) -> None:
        if self.browser is not None:
            return
        if os.geteuid() == 0 or not shutil.which("google-chrome"):
            raise ValueError("Installed Chrome and a non-root user are required")
        from nodriver import cdp

        self.profile.mkdir(parents=True, exist_ok=True, mode=0o700)
        await asyncio.to_thread(self.browser_options.prepare_profile, self.profile)
        ca_bundle = self.ca_bundle or self.output / "ca-bundle.pem"
        if ca_bundle.is_file():
            from src.sites.pep.tls import install_chrome_intermediate

            await asyncio.to_thread(install_chrome_intermediate, ca_bundle)
        arguments = self.browser_options.arguments()
        if self.tunnel:
            endpoint = await self.tunnel.start()
            arguments.extend([f"--proxy-server={endpoint}", "--proxy-bypass-list=<-loopback>", "--disable-quic",
                              "--force-webrtc-ip-handling-policy=disable_non_proxied_udp"])
        self.browser = await self.launch_browser(arguments)
        self.process = self.browser._process
        self.page = self.browser.main_tab

        async def response(event):
            self.state.response(event.response.url, event.response.status)
            if urlsplit(event.response.url).path == DOWNLOAD_PATH:
                self.last_form_status = int(event.response.status)
            self.response_received.set()

        async def request(event):
            if event.redirect_response:
                self.state.response(event.redirect_response.url, event.redirect_response.status)
                if urlsplit(event.redirect_response.url).path == DOWNLOAD_PATH:
                    self.last_form_status = int(event.redirect_response.status)
                self.response_received.set()

        async def ready(event):
            self.dom_ready.set()

        self.page.add_handler(cdp.network.ResponseReceived, response)
        self.page.add_handler(cdp.network.RequestWillBeSent, request)
        self.page.add_handler(cdp.page.DomContentEventFired, ready)
        await self.page.send(cdp.network.enable())
        await self.page.send(cdp.page.enable())
        if self.browser_options.accept_language:
            await self.page.send(cdp.network.set_extra_http_headers(cdp.network.Headers({
                "Accept-Language": self.browser_options.accept_language,
            })))
            await self.page.send(cdp.emulation.set_locale_override(self.browser_options.browser_language.replace("-", "_")))
        if self.browser_options.browser_timezone:
            await self.page.send(cdp.emulation.set_timezone_override(self.browser_options.browser_timezone))

    async def launch_browser(self, arguments: list[str]):
        import nodriver as uc

        return await uc.start(
            user_data_dir=str(self.profile.resolve()), headless=False,
            browser_executable_path=shutil.which("google-chrome"), host="127.0.0.1", sandbox=True,
            browser_args=arguments,
        )

    async def verify_egress(self, expected: str) -> None:
        """Require the browser and HTTP client to observe the same public IP."""
        expected = ipaddress.ip_address(expected)
        if ipaddress.ip_address(await self.observe_egress()) != expected:
            raise ValueError("Browser/HTTP proxy egress mismatch")

    async def observe_egress(self) -> str:
        """Observe Chrome's route without opening an independent HTTP session."""
        from nodriver import cdp

        await self.start()
        async with asyncio.timeout(self.timeout_ms / 1000):
            self.dom_ready.clear()
            await self.page.send(cdp.page.navigate("https://api.ipify.org?format=json"))
            await self.dom_ready.wait()
            text = await nodriver_value(self.page, "document.body.innerText")
            return str(ipaddress.ip_address(json.loads(text)["ip"]))

    async def navigate(self, url: str) -> None:
        from nodriver import cdp

        url = safe_url(url)
        path = urlsplit(url).path
        self.state.statuses.pop(path, None)
        self.dom_ready.clear()
        await self.pace()
        # Keep Network/Page domains on this session; Tab.get() re-attaches it.
        async with asyncio.timeout(self.timeout_ms / 1000):
            await self.page.send(cdp.page.navigate(url))
            await self.dom_ready.wait()
            while path not in self.state.statuses:
                self.response_received.clear()
                await self.response_received.wait()
        status = self.state.statuses[path]
        if path == LANDING_PATH:
            self.warmup_http_status = status
        if status != 200:
            raise DownloadFailure(f"browser_navigation_http_{status}")
        if urlsplit(safe_url(await nodriver_value(self.page, "location.href"))).path != path:
            raise DownloadFailure("browser_unexpected_redirect")
        self.loaded_url = url
        self.loaded_at = time.monotonic()

    async def save_environment(self) -> Path:
        await self.start()
        async with asyncio.timeout(self.timeout_ms / 1000):
            environment = await browser_environment(self.page)
        path = self.output / f"browser-environment-{self.profile.name}.json"
        await asyncio.to_thread(atomic_write_json, path, environment)
        if self.browser_options.require_webgl and not (environment.get("webgl") or {}).get("available"):
            raise BrowserEnvironmentError("WebGL is unavailable; refusing source submissions (see browser-environment JSON)")
        return path

    async def button_geometry(self, selector: str) -> dict:
        return await nodriver_value(self.page, """(() => {
            const el = document.querySelector(SELECTOR);
            if (!el) return {in_view:false, receives_pointer:false};
            const r = el.getBoundingClientRect(), x = r.x+r.width/2, y = r.y+r.height/2;
            const hit = document.elementFromPoint(x,y);
            return {x:x,y:y,width:innerWidth,height:innerHeight,
                    in_view:r.width>0 && r.height>0 && x>=0 && y>=0 && x<innerWidth && y<innerHeight,
                    receives_pointer:hit===el || el.contains(hit)};
        })()""".replace("SELECTOR", json.dumps(selector)))

    async def settle_page(self) -> float:
        """Optional bounded dwell/scroll experiment; never requests a CAPTCHA token."""
        if self.page_settle_max == 0:
            return 0.0
        duration = random.uniform(self.page_settle_min, self.page_settle_max)
        if self.page_settle_scroll:
            # Preserve scroll position, including when a page has no scroll range.
            position = await nodriver_value(self.page, "({x:scrollX,y:scrollY})")
            await self.page.evaluate("window.scrollBy({top:160,behavior:'instant'})")
            await asyncio.sleep(duration / 2)
            await self.page.evaluate(f"window.scrollTo({json.dumps(position['x'])},{json.dumps(position['y'])})")
            await asyncio.sleep(duration / 2)
        else:
            await asyncio.sleep(duration)
        return duration

    async def wait_for_captcha_script(self) -> None:
        async with asyncio.timeout(self.timeout_ms / 1000):
            while not await nodriver_value(self.page, READY):  # noqa: ASYNC110 - remote JS state
                await asyncio.sleep(0.1)

    async def download_once(self, document: dict) -> dict:
        doc = dict(document)
        for key in ("failure_stage", "error_type", "rejection_reason", "redirect_path", "browser_http_status",
                    "browser_history", "browser_seconds", "browser_warmup_http_status", "download_method",
                    "browser_settle_seconds", "browser_page_age_at_click_seconds", "browser_mouse_moves"):
            doc.pop(key, None)
        async with self.lock:
            if self.failures >= 3:
                return {**doc, "status": "browser_session_blocked", "failure_stage": "browser_session"}
            started = time.monotonic()
            self.last_form_status = None
            try:
                # Also bound commands that hang before the per-navigation/download deadlines.
                async with asyncio.timeout(self.timeout_ms * 3 / 1000):
                    doc["failure_stage"] = "browser_connect"
                    await self.start()
                    if not self.session_initialized:
                        doc["failure_stage"] = "browser_session_initialize"
                        await self.navigate(LANDING)
                        self.session_initialized = True
                    url = safe_url(doc["source_page"])
                    if urlsplit(url).path != RESULT_PATH:
                        raise ValueError("Unexpected consultation path")
                    if self.loaded_url != url:
                        doc["failure_stage"] = "page_navigation"
                        await self.navigate(url)
                    if doc.get("requires_captcha", True):
                        doc["failure_stage"] = "captcha_script_ready"
                        await self.wait_for_captcha_script()
                    doc["failure_stage"] = "page_settle"
                    doc["browser_settle_seconds"] = round(await self.settle_page(), 3)
                    doc_id = str(doc["id"])
                    if doc_id.isdecimal():
                        selector = f'form[action$="{DOWNLOAD_PATH}"]:has(input[name="id"][value="{doc_id}"]) button'
                    else:
                        target = safe_url(doc.get("download_url", ""))
                        parts = urlsplit(target)
                        if (parts.path != LEGACY_PATH or parse_qs(parts.query).get("action") != ["download"]
                                or doc_id != "legacy-" + hashlib.sha256(target.encode()).hexdigest()[:24]):
                            raise ValueError("Invalid legacy document link")
                        relative = parts.path + "?" + parts.query
                        selector = ",".join(f"a[href={json.dumps(href)}]" for href in (
                            relative, target, target.replace("https://", "http://", 1),
                        ))
                    button = await self.page.select(selector, timeout=self.timeout_ms / 1000)
                    doc["failure_stage"] = "button_visibility"
                    # nodriver.mouse_click() does NOT scroll automatically. Later
                    # table rows otherwise receive off-viewport clicks and no POST.
                    await button.scroll_into_view()
                    geometry = await self.button_geometry(selector)
                    doc["browser_button_in_view"] = geometry["in_view"]
                    doc["browser_button_receives_pointer"] = geometry["receives_pointer"]
                    if not geometry["in_view"] or not geometry["receives_pointer"]:
                        raise DownloadFailure("browser_button_not_actionable")
                    with tempfile.TemporaryDirectory(prefix="nodriver-", dir=self.output) as temp:
                        directory = Path(temp)
                        await self.page.set_download_path(directory)
                        self.state.statuses.pop(DOWNLOAD_PATH, None)
                        if self.browser_options.mouse_motion:
                            doc["failure_stage"] = "pointer_movement"
                            start = self.pointer_position or (geometry["width"] / 3, geometry["height"] / 3)
                            end = (geometry["x"], geometry["y"])
                            doc["browser_mouse_moves"] = await move_pointer(
                                self.page, start, end, width=geometry["width"], height=geometry["height"],
                            )
                            self.pointer_position = end
                        await self.pace()
                        if self.browser_options.mouse_motion:
                            current = await self.button_geometry(selector)
                            if (not current["in_view"] or not current["receives_pointer"]
                                    or math.hypot(current["x"] - geometry["x"], current["y"] - geometry["y"]) > 2):
                                raise DownloadFailure("browser_button_moved_before_click")
                        doc["failure_stage"] = "download_response"
                        if self.loaded_at is not None:
                            doc["browser_page_age_at_click_seconds"] = round(time.monotonic() - self.loaded_at, 3)
                        if self.browser_options.mouse_motion:
                            await click_pointer(self.page, *end)
                        else:
                            await button.mouse_click()
                        async with asyncio.timeout(self.timeout_ms / 1000):
                            while True:
                                target = await asyncio.to_thread(completed_file, directory)
                                if target:
                                    doc["failure_stage"] = "download_validation"
                                    if (await asyncio.to_thread(target.stat)).st_size > MAX_PDF_BYTES:
                                        raise ValueError("Download exceeds size limit")
                                    data = await asyncio.to_thread(target.read_bytes)
                                    doc.update(await asyncio.to_thread(save_payload, data, self.output, doc_id))
                                    doc["download_method"] = self.DOWNLOAD_METHOD
                                    doc.pop("failure_stage", None)
                                    self.failures = 0
                                    return doc
                                reason = notice_reason(await self.page.get_content())
                                if reason:
                                    doc["rejection_reason"] = reason
                                    raise DownloadFailure(f"browser_rejected_http_{self.last_form_status or 'unknown'}")
                                if self.last_form_status and self.last_form_status >= 400:
                                    raise DownloadFailure(f"browser_rejected_http_{self.last_form_status}")
                                await asyncio.sleep(0.25)
            except DownloadFailure as exc:
                doc["status"] = str(exc)
                self.failures += 1
                if (doc.get("rejection_reason") in {"site_verification_notice", "site_limit_notice"}
                        or doc["status"].endswith(("_http_403", "_http_429"))):
                    self.failures = 3
                self.loaded_url = None
            except TimeoutError:
                doc.update(status="browser_download_timeout", error_type="TimeoutError")
                self.failures += 1
                self.loaded_url = None
            except Exception as exc:
                doc.update(status="browser_failed_tls" if "ERR_CERT_" in str(exc) else "browser_download_failed",
                           error_type=type(exc).__name__)
                self.failures += 1
                if doc.get("failure_stage") == "browser_connect" and isinstance(exc, (ImportError, ValueError, OSError)):
                    self.failures = 3
                self.loaded_url = None
            finally:
                doc["browser_seconds"] = round(time.monotonic() - started, 3)
                if self.warmup_http_status is not None:
                    doc["browser_warmup_http_status"] = self.warmup_http_status
                if self.last_form_status is not None:
                    doc["browser_http_status"] = self.last_form_status
            return doc

    async def close(self) -> None:
        try:
            if self.browser:
                self.browser.stop()
                if self.process:
                    try:
                        await asyncio.wait_for(self.process.wait(), timeout=5)
                    except TimeoutError:
                        self.process.kill()
                        await self.process.wait()
        finally:
            if self.tunnel:
                await self.tunnel.close()
            self.browser = None
            self.process = None
            self.page = None
            self.loaded_url = None
            self.loaded_at = None
            self.pointer_position = None
            self.session_initialized = False
            self.warmup_http_status = None
