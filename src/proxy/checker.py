"""Asynchronous health verification of free proxies against the SEPREC API."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from dataclasses import dataclass

import aiohttp

SEPREC_CHECK_URL = "https://servicios.seprec.gob.bo/api/publicaciones/catalogos/lista"
DEFAULT_CHECK_TIMEOUT_SECONDS = 6.0
DEFAULT_CHECK_CONCURRENCY = 20

CHECK_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://gacetadecomercio.gob.bo",
    "Referer": "https://gacetadecomercio.gob.bo/",
}


@dataclass(frozen=True)
class VerifiedProxy:
    """Represents the verification result of a single proxy address."""

    url: str
    is_working: bool
    latency_ms: float = 0.0
    error_message: str | None = None


class ProxyChecker:
    """Verifies proxy connectivity directly against the SEPREC target endpoint."""

    def __init__(
        self, timeout_seconds: float = DEFAULT_CHECK_TIMEOUT_SECONDS, concurrency: int = DEFAULT_CHECK_CONCURRENCY
    ) -> None:
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self.semaphore = asyncio.Semaphore(concurrency)

    async def verify_single(self, session: aiohttp.ClientSession, proxy_url: str) -> VerifiedProxy:
        """Test a single proxy against the SEPREC catalog endpoint."""
        start_time = time.monotonic()
        async with self.semaphore:
            try:
                async with session.post(
                    SEPREC_CHECK_URL, json={"codigo": ""}, proxy=proxy_url,
                    headers=CHECK_HEADERS, timeout=self.timeout, ssl=False,
                ) as response:
                    latency = round((time.monotonic() - start_time) * 1000, 1)

                    is_ready = False
                    if response.status in (200, 201):
                        payload = await response.json(content_type=None)
                        is_ready = isinstance(payload, dict) and bool(payload.get("finalizado"))

                    return VerifiedProxy(
                        url=proxy_url, is_working=is_ready, latency_ms=latency,
                        error_message=None if is_ready else f"HTTP {response.status}",
                    )

            except Exception as error:
                return VerifiedProxy(url=proxy_url, is_working=False, error_message=type(error).__name__)

    async def verify_many(self, proxy_urls: Sequence[str]) -> list[VerifiedProxy]:
        """Test a batch of proxies concurrently and return all verification results."""
        if not proxy_urls:
            return []

        connector = aiohttp.TCPConnector(ssl=False, limit=0)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [self.verify_single(session, url) for url in proxy_urls]
            return await asyncio.gather(*tasks)

    async def find_working(self, proxy_urls: Sequence[str]) -> list[VerifiedProxy]:
        """Verify proxies and return only the responsive ones sorted by latency."""
        results = await self.verify_many(proxy_urls)
        working = [result for result in results if result.is_working]
        working.sort(key=lambda item: item.latency_ms)
        return working
