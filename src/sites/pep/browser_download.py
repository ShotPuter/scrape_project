"""Minimal native-browser fallback for downloads protected by site JavaScript."""

from __future__ import annotations

import asyncio
import re
import shutil
import socket
import tempfile
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from urllib.parse import unquote, urlsplit

from src.sites.pep.documents import MAX_PDF_BYTES, save_payload
from src.sites.pep.source import DOWNLOAD_PATH, RESULT_PATH, safe_url


class DownloadFailure(ValueError):
    """Explicit site refusal or timeout, without waiting out a known rejection."""


async def click_download(page, form, timeout_ms: int):
    async def submit() -> None:
        await form.locator("button").click(timeout=timeout_ms, no_wait_after=True)

    return await wait_for_download(page, submit, timeout_ms)


async def wait_for_download(page, submit: Callable[[], Awaitable[None]], timeout_ms: int):
    """Race a submitted download against an explicit HTTP refusal."""
    loop = asyncio.get_running_loop()
    downloaded = loop.create_future()
    rejected = loop.create_future()

    def on_download(download):
        if not downloaded.done():
            downloaded.set_result(download)

    def on_response(response):
        if urlsplit(response.url).path == DOWNLOAD_PATH and response.status >= 300 and not rejected.done():
            rejected.set_result(response.status)

    page.on("download", on_download)
    page.on("response", on_response)
    try:
        async with asyncio.timeout(timeout_ms / 1000):
            await submit()
            done, _ = await asyncio.wait((downloaded, rejected), return_when=asyncio.FIRST_COMPLETED)
            if downloaded in done:
                return downloaded.result()
            raise DownloadFailure(f"browser_rejected_http_{rejected.result()}")
    except TimeoutError as exc:
        raise DownloadFailure("browser_download_timeout") from exc
    finally:
        page.remove_listener("download", on_download)
        page.remove_listener("response", on_response)
        downloaded.cancel()
        rejected.cancel()


def validate_cdp_url(url: str) -> str:
    parsed = urlsplit(url)
    if (parsed.scheme not in {"http", "ws"} or parsed.hostname not in {"localhost", "127.0.0.1", "::1"}
            or parsed.username or parsed.password):
        raise ValueError("CDP must use a local debugging endpoint without credentials")
    return url


def proxy_settings(url: str) -> dict:
    parsed = urlsplit(url)
    if parsed.scheme != "http" or not parsed.hostname or not parsed.port:
        raise ValueError("Expected a Webshare HTTP proxy")
    host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
    return {"server": f"http://{host}:{parsed.port}",
            "username": unquote(parsed.username or ""), "password": unquote(parsed.password or "")}


class BrowserDownloader:
    """One persistent browser page, serialized and reused across documents.

    No token forging, fingerprint patches, stealth plugins or CAPTCHA solvers.
    A connected browser remains user-owned and is never shut down by this class.
    """

    ADAPTER_REVISION = "native-cdp-v1"

    def __init__(self, *, output: Path, profile: Path, channel: str | None, headed: bool,
                  cdp_url: str | None, timeout_ms: int, pace, native: bool = False, headless: bool = False,
                  proxy: str | None = None, warmup: bool = False, retry_transient_redirect: bool = True) -> None:
        self.output = output
        self.profile = profile
        self.channel = channel
        self.headed = headed
        self.cdp_url = validate_cdp_url(cdp_url) if cdp_url else None
        self.timeout_ms = timeout_ms
        self.pace = pace
        self.lock = asyncio.Lock()
        self.playwright = None
        self.context = None
        self.page = None
        self.loaded_url = None
        self.failures = 0
        self.last_form_status = None
        self.last_redirect = None
        self.native = native
        self.process = None
        self.headless = headless
        self.warmup = warmup
        self.retry_transient_redirect = retry_transient_redirect
        self.session_initialized = False
        self.warmup_http_status = None
        self.browser_errors: tuple[type[Exception], ...] = ()
        self.proxy = proxy_settings(proxy) if proxy else None
        if self.proxy and self.cdp_url:
            raise ValueError("Use a dedicated proxy profile, not an attached browser")

    async def launch_native(self) -> str:
        executable = shutil.which("microsoft-edge" if self.channel == "msedge" else "google-chrome")
        if not executable:
            raise ValueError("Install Chrome or supply --cdp-url for an existing browser")
        self.profile.mkdir(parents=True, exist_ok=True, mode=0o700)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        command = [
            executable, f"--remote-debugging-port={port}", "--remote-debugging-address=127.0.0.1",
            f"--user-data-dir={self.profile.resolve()}", "--no-first-run", "--no-default-browser-check",
        ]
        if self.headless:
            command.append("--headless=new")
            # The portal returns 403 solely for the literal HeadlessChrome UA.
            # Keep one stable UA matching the installed browser, not rotating identities.
            version_process = await asyncio.create_subprocess_exec(
                executable, "--version", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            version_output, _ = await version_process.communicate()
            version = re.search(rb"(\d+)\.\d+\.\d+\.\d+", version_output)
            if version:
                major = version.group(1).decode()
                command.append(
                    "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    f"(KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36"
                )
        self.process = await asyncio.create_subprocess_exec(
            *command, "about:blank",
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        return f"http://127.0.0.1:{port}"

    async def start(self) -> None:
        if self.page is not None:
            return
        if self.playwright is not None:
            await self.close()
        from playwright.async_api import Error as BrowserError, async_playwright

        self.playwright = await async_playwright().start()
        if self.cdp_url or (self.native and not self.proxy):
            endpoint = self.cdp_url or await self.launch_native()
            deadline = asyncio.get_running_loop().time() + self.timeout_ms / 1000
            while True:
                try:
                    browser = await self.playwright.chromium.connect_over_cdp(endpoint, timeout=1000)
                    break
                except BrowserError:
                    if not self.native or asyncio.get_running_loop().time() >= deadline:
                        raise
                    if self.process.returncode is not None:
                        raise ValueError("Native Chrome exited before CDP became available") from None
                    await asyncio.sleep(0.2)
            if not browser.contexts:
                raise ValueError("Connected browser has no existing context")
            self.context = browser.contexts[0]
        else:
            self.profile.mkdir(parents=True, exist_ok=True, mode=0o700)
            extra = {}
            if self.proxy:
                # Playwright's supported proxy authentication avoids putting credentials
                # in Chrome's process command line or writing an auth extension.
                executable = shutil.which("microsoft-edge" if self.channel == "msedge" else "google-chrome")
                if not executable:
                    raise ValueError("Installed Chrome is required for the proxy test")
                version_process = await asyncio.create_subprocess_exec(
                    executable, "--version", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
                )
                stdout, _ = await version_process.communicate()
                version = re.search(rb"(\d+)\.\d+\.\d+\.\d+", stdout)
                if version:
                    extra["user_agent"] = (
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        f"(KHTML, like Gecko) Chrome/{version.group(1).decode()}.0.0.0 Safari/537.36"
                    )
            self.context = await self.playwright.chromium.launch_persistent_context(
                str(self.profile.resolve()), channel=self.channel, headless=self.headless or not self.headed,
                accept_downloads=True, locale="es-CO", proxy=self.proxy, **extra,
            )
        self.page = await self.context.new_page()
        self.page.on("response", self.observe_response)

    def observe_response(self, response) -> None:
        if urlsplit(response.url).path.endswith("/generarReporteConsultaCiudadana"):
            self.last_form_status = response.status
            self.last_redirect = getattr(response, "headers", {}).get("location")

    async def initialize_browser_session(self) -> None:
        """Match the tested landing-page flow in this browser, not the HTTP cookie jar."""
        if not self.warmup or self.session_initialized:
            return
        await self.pace()
        response = await self.page.goto(
            "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/consultaPEP",
            wait_until="domcontentloaded", timeout=self.timeout_ms,
        )
        self.warmup_http_status = response.status if response else None
        if not response or response.status != 200:
            raise DownloadFailure(f"browser_warmup_http_{self.warmup_http_status or 'unknown'}")
        if urlsplit(safe_url(self.page.url)).path != "/fdci/consultaCiudadana/consultaPEP":
            raise DownloadFailure("browser_warmup_unexpected_redirect")
        self.session_initialized = True

    async def wait_for_captcha_script(self) -> None:
        await self.page.wait_for_function(
            "typeof grecaptcha !== 'undefined' && typeof grecaptcha.execute === 'function'",
            timeout=self.timeout_ms,
        )

    async def rejection_details(self, doc: dict) -> None:
        """Finish the native redirect and classify the site's notice, without tokens."""
        from playwright.async_api import Error as BrowserError

        if not self.last_redirect:
            return
        try:
            target = urlsplit(safe_url(self.last_redirect, self.page.url))
            doc["redirect_path"] = target.path  # Do not persist query strings or cookies.
            await self.page.wait_for_url(
                lambda url: urlsplit(url).path == target.path and urlsplit(url).query == target.query,
                wait_until="domcontentloaded", timeout=min(self.timeout_ms, 5000),
            )
            messages = await self.page.locator('.alert, .message, .errors, [role="alert"]').all_text_contents()
            text = " ".join(messages).lower()
            if any(word in text for word in ("captcha", "robot", "verificaci", "verification")):
                doc["rejection_reason"] = "site_verification_notice"
            elif any(word in text for word in ("demasiad", "too many", "límite", "limite")):
                doc["rejection_reason"] = "site_limit_notice"
            elif text.strip():
                doc["rejection_reason"] = "site_notice_unclassified"
            else:
                doc["rejection_reason"] = "no_site_notice"
        except (BrowserError, ValueError, *self.browser_errors):
            doc["rejection_reason"] = "redirect_inspection_failed"

    async def close(self) -> None:
        try:
            try:
                if self.page:
                    await self.page.close()
            finally:
                if self.context and (self.proxy or not (self.cdp_url or self.native)):
                    await self.context.close()
        finally:
            try:
                if self.playwright:
                    await self.playwright.stop()
            finally:
                if self.process and self.process.returncode is None:
                    self.process.terminate()
                    try:
                        await asyncio.wait_for(self.process.wait(), timeout=5)
                    except TimeoutError:
                        self.process.kill()
                        await self.process.wait()
                self.process = None
                self.page = None
                self.context = None
                self.playwright = None
                self.loaded_url = None
                self.session_initialized = False
                self.warmup_http_status = None

    async def download(self, document: dict) -> dict:
        diagnostic_fields = ("status", "failure_stage", "error_type", "rejection_reason",
                             "browser_http_status", "browser_seconds")
        result = await self.download_once(document)
        history = [{key: result[key] for key in diagnostic_fields if key in result}]
        result["browser_attempts"] = 0 if result["status"] == "browser_session_blocked" else 1
        # A fresh site's own submission can succeed after a transient 302.
        # Retry at most once; repeated rejection still trips the session breaker.
        if (self.retry_transient_redirect and result["status"] == "browser_rejected_http_302" and self.failures < 3
                and result.get("rejection_reason") != "site_verification_notice"):
            elapsed = result.get("browser_seconds", 0)
            await asyncio.sleep(2)
            result = await self.download_once(result)
            history.append({key: result[key] for key in diagnostic_fields if key in result})
            result["browser_seconds"] = round(elapsed + result.get("browser_seconds", 0), 3)
            result["browser_attempts"] = 2
        result["browser_history"] = history
        return result

    async def download_once(self, document: dict) -> dict:
        from playwright.async_api import Error as BrowserError

        doc = dict(document)
        for key in ("failure_stage", "error_type", "rejection_reason", "redirect_path", "browser_http_status",
                    "native_exit_code", "browser_history", "browser_warmup_http_status"):
            doc.pop(key, None)
        async with self.lock:
            started = time.perf_counter()
            if self.failures >= 3:
                doc["status"] = "browser_session_blocked"
                doc["failure_stage"] = "browser_session"
                return doc
            self.last_form_status = None
            self.last_redirect = None
            try:
                doc["failure_stage"] = "browser_connect"
                await self.start()
                doc["failure_stage"] = "browser_session_initialize"
                await self.initialize_browser_session()
                url = safe_url(doc["source_page"])
                if self.loaded_url != url or urlsplit(self.page.url).path != RESULT_PATH:
                    doc["failure_stage"] = "page_navigation"
                    await self.pace()
                    response = await self.page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                    if not response or response.status != 200:
                        doc["status"] = f"browser_failed_http_{response.status if response else 'unknown'}"
                        if response:
                            doc["browser_http_status"] = response.status
                        self.failures = 3 if response and response.status in {403, 429} else self.failures + 1
                        return doc
                    if urlsplit(safe_url(self.page.url)).path != RESULT_PATH:
                        raise ValueError("Unexpected browser redirect")
                    self.loaded_url = url
                doc["failure_stage"] = "captcha_script_ready"
                await self.wait_for_captcha_script()
                doc_id = str(doc["id"])
                if not doc_id.isdecimal():
                    raise ValueError("Invalid document identifier")
                form = self.page.locator('form[action$="/generarReporteConsultaCiudadana"]').filter(
                    has=self.page.locator(f'input[name="id"][value="{doc_id}"]')
                )
                await self.pace()
                self.last_form_status = None
                self.last_redirect = None
                doc["failure_stage"] = "download_response"
                download = await click_download(self.page, form, self.timeout_ms)
                doc["failure_stage"] = "download_transfer"
                # save_as also works with a remotely connected browser, unlike path().
                with tempfile.TemporaryDirectory(prefix="pep-", dir=self.output) as temp:
                    target = Path(temp) / "response.bin"
                    await download.save_as(target)
                    if (await asyncio.to_thread(target.stat)).st_size > MAX_PDF_BYTES:
                        raise ValueError("Browser download exceeds size limit")
                    data = await asyncio.to_thread(target.read_bytes)
                    doc.update(await asyncio.to_thread(save_payload, data, self.output, doc_id))
                await download.delete()
                doc["download_method"] = ("browser_proxy" if self.proxy else
                                          "browser_cdp" if (self.cdp_url or self.native) else "browser_persistent")
                doc.pop("failure_stage", None)
                doc.pop("error_type", None)
                self.failures = 0
            except DownloadFailure as exc:
                doc["status"] = str(exc)
                if self.last_form_status == 302:
                    await self.rejection_details(doc)
                if (doc.get("rejection_reason") in {"site_verification_notice", "site_limit_notice"}
                        or doc["status"].endswith(("_http_403", "_http_429"))):
                    self.failures = 3
                else:
                    self.failures += 1
                self.loaded_url = None
            except (BrowserError, TimeoutError, *self.browser_errors) as exc:
                doc["error_type"] = type(exc).__name__
                doc["status"] = "browser_failed_tls" if "ERR_CERT_" in str(exc) else "browser_download_failed"
                if self.last_form_status and self.last_form_status != 200:
                    doc["status"] = f"browser_rejected_http_{self.last_form_status}"
                self.failures += 1
                self.loaded_url = None
            except (ValueError, OSError, KeyError) as exc:
                doc["error_type"] = type(exc).__name__
                if self.process and self.process.returncode is not None:
                    doc["native_exit_code"] = self.process.returncode
                doc["status"] = "browser_download_failed"
                self.failures += 1
                self.loaded_url = None
            finally:
                if self.warmup_http_status is not None:
                    doc["browser_warmup_http_status"] = self.warmup_http_status
                if self.last_form_status is not None:
                    doc["browser_http_status"] = self.last_form_status
                doc["browser_seconds"] = round(time.perf_counter() - started, 3)
            return doc
