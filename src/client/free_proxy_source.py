"""Load candidate proxies from the optional ``free-proxy`` package.

The package is imported as ``fp`` (PyPI name: ``free-proxy``).  It only
scrapes public proxy lists; every candidate is still verified against SEPREC
by :mod:`src.services.proxy_collector` before it reaches SQLite.
"""

from __future__ import annotations

import asyncio
from urllib.parse import urlsplit

from src.client.proxy_source import SUPPORTED_SCHEMES
from src.utils.logger import setup_logger

logger = setup_logger("free_proxy_source")

INSTALL_HINT = "free-proxy is not installed; run `pip install free-proxy`"
FREE_PROXY_SOURCES = (
    {"repeat": False, "country_id": None},
    {"repeat": True, "country_id": None},
    {"repeat": False, "country_id": ["US"]},
    {"repeat": False, "country_id": ["GB"]},
)


def _import_free_proxy():
    try:
        from fp.errors import FreeProxyException
        from fp.fp import FreeProxy
    except ImportError as exc:
        raise RuntimeError(INSTALL_HINT) from exc
    return FreeProxy, FreeProxyException


def normalise_proxy_address(address: str) -> str | None:
    """Turn ``ip:port`` (or a URL) into a supported proxy URL."""
    value = address.strip()
    if not value:
        return None
    url = value if "://" in value else f"http://{value}"
    try:
        parsed = urlsplit(url)
    except ValueError:
        return None
    if parsed.scheme.lower() not in SUPPORTED_SCHEMES:
        return None
    if not parsed.hostname or not parsed.port:
        return None
    return f"{parsed.scheme.lower()}://{parsed.hostname}:{parsed.port}"


async def load_free_proxy_candidates(*, request_timeout: float = 10) -> list[str]:
    """Return deduplicated proxy URLs from all ``free-proxy`` list sources."""
    FreeProxy, FreeProxyException = _import_free_proxy()

    async def fetch(source: dict) -> list[str]:
        proxy = FreeProxy(country_id=source["country_id"], request_timeout=request_timeout)
        try:
            addresses = await asyncio.to_thread(proxy.get_proxy_list, source["repeat"])
        except FreeProxyException as exc:
            logger.warning("free-proxy source failed: %s", exc)
            return []
        return addresses

    results = await asyncio.gather(*(fetch(source) for source in FREE_PROXY_SOURCES), return_exceptions=True)

    candidates: list[str] = []
    seen: set[str] = set()
    for result in results:
        if isinstance(result, BaseException):
            logger.warning("free-proxy source failed: %s", result)
            continue
        for address in result:
            url = normalise_proxy_address(str(address))
            if url is not None and url not in seen:
                seen.add(url)
                candidates.append(url)
    return candidates
