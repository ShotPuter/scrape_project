"""Collect free proxy candidates from public sources and local files."""

from __future__ import annotations

import csv
import logging
import re
from pathlib import Path

import aiohttp

from src.proxy.rotator import normalize_proxy_url

logger = logging.getLogger("proxy_collector")

PUBLIC_PROXY_SOURCES = [
    "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
]


class ProxyCollector:
    """Discovers and aggregates free public proxy addresses from various sources."""

    @staticmethod
    def from_file(file_path: Path | str) -> list[str]:
        """Load and normalize proxy addresses from a local text or CSV file."""
        target = Path(file_path)
        if not target.exists():
            return []

        candidates: list[str] = []

        with target.open(encoding="utf-8", errors="ignore") as handle:
            if target.suffix.lower() == ".csv":
                addresses = (
                    row.get("proxy") or f"{row.get('protocol', 'http')}://{row.get('ip', '')}:{row.get('port', '')}"
                    for row in csv.DictReader(handle)
                )
            else:
                addresses = (line.strip() for line in handle)
            for address in addresses:
                normalized = normalize_proxy_url(address)
                if normalized:
                    candidates.append(normalized)

        return list(dict.fromkeys(candidates))

    @staticmethod
    async def from_public_sources(limit_per_source: int = 150) -> list[str]:
        """Fetch free proxy lists from public GitHub repositories."""
        candidates: list[str] = []
        timeout = aiohttp.ClientTimeout(total=10.0)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            for url in PUBLIC_PROXY_SOURCES:
                try:
                    async with session.get(url) as response:
                        if response.status != 200:
                            continue
                        text = await response.text()
                        matches = re.findall(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{2,5}\b", text)
                        for match in matches[:limit_per_source]:
                            normalized = normalize_proxy_url(match)
                            if normalized:
                                candidates.append(normalized)
                except Exception as error:
                    logger.debug("Failed fetching from public proxy source %s: %s", url, error)

        return list(dict.fromkeys(candidates))
