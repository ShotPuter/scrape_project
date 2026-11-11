"""One automatic Camoufox bootstrap, then bounded asynchronous HTTP discovery."""

from __future__ import annotations

import asyncio
import re
import xml.etree.ElementTree as ET
from contextlib import suppress
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

from src.utils.atomic import atomic_write_json, atomic_write_text
from src.utils.error_details import exception_details, failure_state, safe_message

MAX_RESPONSE_BYTES = 10 * 1024 * 1024


class BootstrapFailure(ValueError):
    def __init__(self, reason, *, state="failed"):
        super().__init__(reason)
        self.state = state


class HTTPFailure(ValueError):
    def __init__(self, status: int, *, retry_after: float = 0):
        super().__init__(f"HTTP {status} from public archive")
        self.retry_after = retry_after


def retry_seconds(value: str) -> float:
    try:
        if value.isdecimal():
            return float(value)
        return max(0, (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds())
    except (ValueError, TypeError, OverflowError):
        return 0


def is_challenge(text: str) -> bool:
    return bool(
        re.search(r"<title[^>]*>\s*(?:just a moment|un momento|один момент)", text, re.I)
        or "/cdn-cgi/challenge-platform/" in text
        or "cf-chl-" in text
    )


class HybridClient:
    def __init__(
        self, context, stage, request_gate, *, session_factory=None, proxy_url=None, on_progress=None, diagnostics_dir=None
    ):
        self.context, self.stage, self.request_gate = context, stage, request_gate
        self.session_factory = session_factory
        self.proxy_url = proxy_url
        self.on_progress, self.diagnostics_dir = on_progress, diagnostics_dir
        self.failure_saved = False
        self.page = None
        self.session = None
        self.sessions = []
        self.generation = 0
        self.refresh_lock = asyncio.Lock()
        self.metrics = {"browser_navigations": 0, "session_refreshes": 0, "http_requests": 0, "http_successes": 0}

    async def __aenter__(self):
        self.page = await self.context.new_page()
        try:
            await self._bootstrap()
        except BaseException as exc:
            await self.capture_failure(exc)
            await self.close()
            raise
        return self

    async def __aexit__(self, *args):
        if args and args[1] is not None:
            await self.capture_failure(args[1])
        await self.close()

    def progress(self, phase, **details):
        self.metrics.update(phase=phase, **details)
        if self.on_progress is not None:
            self.on_progress(dict(self.metrics))

    async def capture_failure(self, exc):
        if self.failure_saved:
            return
        self.failure_saved = True
        details = {**exception_details(exc), "state": failure_state(exc), "phase": self.metrics.get("phase")}
        self.metrics["failure"] = details
        if self.diagnostics_dir is not None:
            try:
                # The HTML/screenshot stay in the ignored data directory, never in Telegram.
                self.diagnostics_dir.mkdir(parents=True, exist_ok=True)
                atomic_write_json(self.diagnostics_dir / "failure.json", {**self.metrics, "failure": details})
                if details["state"] != "interrupted" and self.page is not None:
                    try:
                        html = await asyncio.wait_for(self.page.content(), timeout=3)
                        atomic_write_text(self.diagnostics_dir / "page.html", html[: 1024 * 1024])
                    except Exception:
                        pass
                    with suppress(Exception):
                        await self.page.screenshot(path=str(self.diagnostics_dir / "page.png"), timeout=5000)
                self.metrics["diagnostics"] = str(self.diagnostics_dir)
            except OSError:
                pass
        self.progress(self.metrics.get("phase", "bootstrap"))

    async def close(self):
        for session in self.sessions:
            await session.close()
        if self.page:
            await self.page.close()

    async def _navigate(self, url):
        await self.request_gate()
        self.metrics["browser_navigations"] += 1
        self.progress("browser_navigation", last_url=safe_message(url), last_http_status=None)
        response = await self.page.goto(url, wait_until="domcontentloaded", timeout=self.stage.timeout_seconds * 1000)
        self.progress("browser_response", last_http_status=response.status if response else 0)
        return response

    async def _document(self, url):
        # Capture an automatic reload that can finish before goto() returns from the challenge page.
        arrived = []

        def ready(candidate):
            parts, expected = urlsplit(candidate.url), urlsplit(url)
            return (
                candidate.status == 200
                and parts.scheme == "https"
                and parts.hostname == expected.hostname
                and parts.path == expected.path
            )

        def observe(candidate):
            if ready(candidate):
                arrived.append(candidate)

        self.page.on("response", observe)
        try:
            response = await self._navigate(url)
            if response and response.status in {403, 503}:
                self.progress("automatic_browser_check")
                print("Hybrid: waiting for the automatic browser check to finish.", flush=True)
                if arrived:
                    response = arrived[-1]
                else:
                    try:
                        async with self.page.expect_response(ready, timeout=self.stage.timeout_seconds * 1000) as event:
                            pass
                        response = await event.value
                    except Exception:
                        return None
            self.progress(
                "document_received", last_http_status=response.status if response else self.metrics.get("last_http_status")
            )
            return response
        finally:
            self.page.remove_listener("response", observe)

    async def _index_ready(self):
        from src.sites.dequienes.companies import INDEX, parse_sitemap

        response = await self._document(INDEX)
        if not response or response.status != 200:
            return False
        parts = urlsplit(getattr(response, "url", self.page.url))
        if parts.scheme != "https" or parts.hostname != "dequienes.cl" or parts.path != urlsplit(INDEX).path:
            raise ValueError("Unexpected bootstrap redirect")
        try:
            text = await response.text()
            self.metrics["challenge_detected"] = is_challenge(text)
            if self.metrics["challenge_detected"]:
                self.progress("sitemap_challenge", challenge_detected=True)
                return False
            parse_sitemap(text, index=True)
        except (ValueError, ET.ParseError):
            self.progress("sitemap_format_invalid")
            return False
        return True

    async def _bootstrap(self):
        from src.sites.dequienes.companies import BASE

        if not await self._index_ready():
            # Homepage content/inputs are irrelevant to sitemap access; only the normal session matters.
            response = await self._document(BASE + "/")
            parts = urlsplit(self.page.url)
            if parts.scheme != "https" or parts.hostname != "dequienes.cl":
                raise ValueError("Unexpected bootstrap redirect")
            if not response or response.status != 200:
                status = response.status if response else self.metrics.get("last_http_status")
                raise BootstrapFailure(
                    f"DeQuienes homepage session check did not complete (HTTP {status}); operator review required",
                    state="blocked" if status in {403, 429, 503} or self.metrics.get("challenge_detected") else "failed",
                )
            if not await self._index_ready():
                status = self.metrics.get("last_http_status")
                raise BootstrapFailure(
                    f"DeQuienes sitemap unavailable after browser session preparation (HTTP {status})",
                    state="blocked" if status in {403, 429, 503} or self.metrics.get("challenge_detected") else "failed",
                )
        self.progress("reading_browser_identity")
        user_agent = await self._user_agent()
        if self.session_factory is None:
            from curl_cffi.requests import AsyncSession

            factory = AsyncSession
        else:
            factory = self.session_factory
        self.progress("creating_http_session")
        session = factory(
            impersonate="firefox",
            headers={"User-Agent": user_agent, "Accept-Language": "es-CL,es;q=0.9"},
            timeout=self.stage.timeout_seconds,
            max_clients=self.stage.tabs_per_browser,
            trust_env=False,
            verify=True,
            **({"proxy": self.proxy_url} if self.proxy_url is not None else {}),
        )
        self.sessions.append(session)
        self.progress("copying_session_cookies")
        # Session credentials stay in memory and are only sent to validated archive URLs.
        for cookie in await self.context.cookies(["https://dequienes.cl/"]):
            if cookie["domain"].lstrip(".") == "dequienes.cl":
                session.cookies.set(cookie["name"], cookie["value"], domain=cookie["domain"], path=cookie["path"])
        self.session = session
        self.generation += 1
        self.progress("session_ready")
        print("Hybrid: browser session ready; discovery now uses asynchronous HTTP.", flush=True)

    async def _user_agent(self):
        # A successful response can arrive before Cloudflare commits the new document.
        async with asyncio.timeout(self.stage.timeout_seconds):
            while True:
                try:
                    await self.page.wait_for_load_state("domcontentloaded", timeout=self.stage.timeout_seconds * 1000)
                    return await self.page.evaluate("navigator.userAgent")
                except Exception as exc:
                    message = str(exc).lower()
                    if "execution context was destroyed" not in message and "because of a navigation" not in message:
                        raise
                    await asyncio.sleep(0.1)

    async def _refresh(self, generation):
        async with self.refresh_lock:
            if generation != self.generation:
                return
            if self.metrics["session_refreshes"] >= self.stage.session_refreshes:
                raise ValueError("HTTP 403: automatic session refresh limit reached")
            self.metrics["session_refreshes"] += 1
            print("Hybrid: refreshing browser session automatically.", flush=True)
            await self._bootstrap()

    async def get_html(self, url: str) -> str:
        from src.sites.dequienes.companies import source_url

        source_url(url)
        for attempt in range(2):
            await self.request_gate()
            self.progress("http_request", last_url=safe_message(url), last_http_status=None)
            generation, session = self.generation, self.session
            self.metrics["http_requests"] += 1
            try:
                # Stream so a missing/incorrect Content-Length cannot cause an unbounded body read.
                async with session.stream("GET", url, allow_redirects=False) as response:
                    status = response.status_code
                    self.progress("http_response", last_http_status=status)
                    if status == 429:
                        raise HTTPFailure(status, retry_after=retry_seconds(response.headers.get("Retry-After", "")))
                    if status not in {200, 403}:
                        raise HTTPFailure(status)
                    source_url(str(response.url))
                    data = bytearray()
                    async for chunk in response.aiter_content():
                        if len(data) + len(chunk) > MAX_RESPONSE_BYTES:
                            raise ValueError("Archive response exceeds 10 MiB")
                        data.extend(chunk)
                    text = bytes(data).decode("utf-8-sig", errors="replace")
            except ValueError:
                raise
            except Exception as exc:
                raise ValueError(f"HTTP transport failed: {type(exc).__name__}") from exc
            if status == 403 or is_challenge(text):
                if attempt:
                    raise HTTPFailure(403)
                await self._refresh(generation)
                continue
            self.metrics["http_successes"] += 1
            return text
        raise HTTPFailure(403)
