"""One automatic Camoufox bootstrap, then bounded asynchronous HTTP discovery."""

from __future__ import annotations

import asyncio
import re
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

MAX_RESPONSE_BYTES = 10 * 1024 * 1024


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
    def __init__(self, context, stage, request_gate, *, session_factory=None, proxy_url=None):
        self.context, self.stage, self.request_gate = context, stage, request_gate
        self.session_factory = session_factory
        self.proxy_url = proxy_url
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
        except BaseException:
            await self.close()
            raise
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        for session in self.sessions:
            await session.close()
        if self.page:
            await self.page.close()

    async def _navigate(self, url):
        await self.request_gate()
        self.metrics["browser_navigations"] += 1
        return await self.page.goto(url, wait_until="domcontentloaded", timeout=self.stage.timeout_seconds * 1000)

    async def _index_ready(self):
        from src.sites.dequienes.companies import INDEX, parse_sitemap

        response = await self._navigate(INDEX)
        if response and response.status in {403, 503}:
            print("Hybrid: waiting for the automatic browser check to finish.", flush=True)

            def ready(candidate):
                parts = urlsplit(candidate.url)
                return (
                    candidate.status == 200
                    and parts.scheme == "https"
                    and parts.hostname == "dequienes.cl"
                    and parts.path == "/diario-oficial/sitemap-index.xml"
                )

            try:
                # Keep the challenge page open instead of interrupting it with another navigation.
                async with self.page.expect_response(ready, timeout=self.stage.timeout_seconds * 1000) as event:
                    pass
                response = await event.value
            except Exception:
                return False
        if not response or response.status != 200:
            return False
        parts = urlsplit(getattr(response, "url", self.page.url))
        if parts.scheme != "https" or parts.hostname != "dequienes.cl" or parts.path != urlsplit(INDEX).path:
            raise ValueError("Unexpected bootstrap redirect")
        try:
            parse_sitemap(await response.text(), index=True)
        except (ValueError, ET.ParseError):
            return False
        return True

    async def _bootstrap(self):
        from src.sites.dequienes.companies import BASE

        if not await self._index_ready():
            await self._navigate(BASE + "/")
            await self.page.locator("input[type=search]").wait_for(
                state="visible", timeout=self.stage.timeout_seconds * 1000
            )
            parts = urlsplit(self.page.url)
            if parts.scheme != "https" or parts.hostname != "dequienes.cl":
                raise ValueError("Unexpected bootstrap redirect")
            if not await self._index_ready():
                raise ValueError("HTTP 403: automatic browser session bootstrap did not complete")
        user_agent = await self._user_agent()
        if self.session_factory is None:
            from curl_cffi.requests import AsyncSession

            factory = AsyncSession
        else:
            factory = self.session_factory
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
        # Session credentials stay in memory and are only sent to validated archive URLs.
        for cookie in await self.context.cookies(["https://dequienes.cl/"]):
            if cookie["domain"].lstrip(".") == "dequienes.cl":
                session.cookies.set(cookie["name"], cookie["value"], domain=cookie["domain"], path=cookie["path"])
        self.session = session
        self.generation += 1
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
            generation, session = self.generation, self.session
            self.metrics["http_requests"] += 1
            try:
                # Stream so a missing/incorrect Content-Length cannot cause an unbounded body read.
                async with session.stream("GET", url, allow_redirects=False) as response:
                    status = response.status_code
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
