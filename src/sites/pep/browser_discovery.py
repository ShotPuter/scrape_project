"""Discover declaration metadata from the owned Chrome DOM, never HTTP APIs."""

from __future__ import annotations

import asyncio
from urllib.parse import urlsplit

from src.sites.pep.browser_diagnostics import LANDING, notice_reason
from src.sites.pep.browser_download import DownloadFailure
from src.sites.pep.source import RESULT_PATH, parse_results, safe_url


class BrowserDiscoveryClient:
    def __init__(self, browser):
        self.browser = browser
        self.last_failure: dict = {}

    async def preflight(self, output) -> None:
        async with self.browser.lock:
            await self.browser.start()
            if not self.browser.session_initialized:
                await self.browser.navigate(LANDING)
                self.browser.session_initialized = True

    async def discover(self, url: str) -> tuple[str, list[dict]]:
        self.last_failure = {}
        documents: dict[str, dict] = {}
        visited: set[str] = set()
        try:
            async with self.browser.lock:
                while url:
                    url = safe_url(url)
                    if urlsplit(url).path != RESULT_PATH:
                        raise ValueError("Unexpected consultation path")
                    if url in visited:
                        return "discovery_failed_pagination_loop", list(documents.values())
                    visited.add(url)
                    await self.browser.navigate(url)
                    html = await self.browser.page.get_content()
                    reason = notice_reason(html)
                    if reason:
                        self.last_failure = {"reason": reason}
                        self.browser.failures = 3
                        return "discovery_failed_verification", list(documents.values())
                    await self.browser.page.select("table.table tbody", timeout=self.browser.timeout_ms / 1000)
                    html = await self.browser.page.get_content()
                    reason = notice_reason(html)
                    if reason:
                        self.last_failure = {"reason": reason}
                        self.browser.failures = 3
                        return "discovery_failed_verification", list(documents.values())
                    entries, next_url = await asyncio.to_thread(parse_results, html, url)
                    for doc in entries:
                        doc.update(source_page=url, status="discovered", local_path=None, s3_uri=None,
                                   publication_year=doc["publication_date"][:4] or None)
                        documents.setdefault(doc["id"], doc)
                    url = next_url
            return "discovered", list(documents.values())
        except DownloadFailure as exc:
            self.last_failure = {"error_type": type(exc).__name__}
            message = str(exc)
            prefix = "browser_navigation_http_"
            if message.startswith(prefix) and message[len(prefix):].isdecimal():
                return f"discovery_failed_http_{message[len(prefix):]}", list(documents.values())
            return "discovery_failed_redirect", list(documents.values())
        except TimeoutError:
            self.last_failure = {"error_type": "TimeoutError"}
            return "discovery_failed_browser_timeout", list(documents.values())
        except ValueError as exc:
            self.last_failure = {"error_type": type(exc).__name__}
            return "discovery_failed_schema", list(documents.values())

    async def close(self) -> None:
        # The benchmark worker owns and closes the shared browser.
        pass
