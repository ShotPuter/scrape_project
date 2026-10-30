"""Authenticated, bounded Webshare inventory fetch; credentials are never cached."""

from __future__ import annotations

import asyncio
from urllib.parse import quote, urlencode

from src.network.aiohttp_session import AiohttpSession
from src.proxy.sources.catalog import ProxyCandidate
from src.utils.proxy_url import normalize_proxy_url

API_URL = "https://proxy.webshare.io/api/v2/proxy/list/"
PAGE_SIZE = 100


class WebshareError(ValueError):
    """Sanitized provider error safe to include in collection reports."""


def _candidate(row: dict, mode: str) -> ProxyCandidate | None:
    if row.get("valid") is False:
        return None
    host = row.get("proxy_address") if mode == "direct" else "p.webshare.io"
    if not isinstance(host, str) or not host:
        return None
    try:
        port = int(row["port"]) if mode == "direct" else 80
    except (KeyError, TypeError, ValueError):
        return None
    username, password = row.get("username"), row.get("password")
    if not isinstance(username, str) or not username or not isinstance(password, str) or not password:
        return None
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    credentials = f"{quote(username, safe='')}:{quote(password, safe='')}"
    url = normalize_proxy_url(f"http://{credentials}@{host}:{port}")
    if url is None:
        return None
    return ProxyCandidate(url, "http", "webshare", country=row.get("country_code"), city=row.get("city_name"))


async def load_webshare_candidates(
    token: str,
    *,
    mode: str = "direct",
    timeout: float = 30,  # noqa: ASYNC109 - bounded entire inventory fetch
    max_bytes: int = 32 * 1024 * 1024,
    max_records: int = 10_000,
) -> list[ProxyCandidate]:
    if not token.strip():
        raise WebshareError("WEBSHARE_API_TOKEN is empty")
    if mode not in {"direct", "backbone"} or min(timeout, max_bytes, max_records) <= 0:
        raise WebshareError("Invalid Webshare inventory settings")
    headers = {"Authorization": f"Token {token.strip()}", "Accept": "application/json"}
    candidates: dict[str, ProxyCandidate] = {}
    consumed = seen = 0
    try:
        async with asyncio.timeout(timeout), AiohttpSession(headers=headers, timeout=timeout, max_clients=1) as session:
            for page in range(1, max_records + 1):
                query = urlencode({"mode": mode, "page": page, "page_size": PAGE_SIZE})
                # Build each URL ourselves; never forward the token to a returned next URL or redirect.
                response = await session.get(
                    f"{API_URL}?{query}", max_response_bytes=max_bytes - consumed, allow_redirects=False
                )
                if response.status_code != 200:
                    raise WebshareError(f"Webshare API returned HTTP {response.status_code}")
                consumed += len(response.body)
                payload = response.json()
                if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
                    raise WebshareError("Webshare API returned an invalid inventory")
                for row in payload["results"]:
                    seen += 1
                    if isinstance(row, dict) and (candidate := _candidate(row, mode)) is not None:
                        candidates[candidate.url] = candidate
                    if seen >= max_records:
                        return list(candidates.values())
                if not payload.get("next"):
                    return list(candidates.values())
                if not payload["results"] or consumed >= max_bytes:
                    raise WebshareError("Webshare inventory exceeded its bounded pagination budget")
    except WebshareError:
        raise
    except Exception as exc:  # noqa: BLE001 - never disclose provider headers or credentials in transport errors
        raise WebshareError(f"Webshare inventory unavailable ({type(exc).__name__})") from None
    return list(candidates.values())
