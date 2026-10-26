"""Refresh the public iplocate proxy list with a local, conditional cache."""

from __future__ import annotations

import csv
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.utils.atomic import atomic_text_writer, atomic_write_json
from src.utils.proxy_url import SUPPORTED_PROXY_SCHEMES, parse_proxy_url

DEFAULT_SOURCE_URL = "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/all-proxies.txt"
SUPPORTED_SCHEMES = SUPPORTED_PROXY_SCHEMES
DEFAULT_REFRESH_MINUTES = 30.0
DEFAULT_TIMEOUT_SECONDS = 30.0
CSV_FIELDNAMES = ("alive", "protocol", "proxy")
USER_AGENT = "gaceta-scraper/1.0"


class ProxySourceError(Exception):
    """The upstream proxy list could not be refreshed."""


def parse_proxies(text: str) -> list[tuple[str, str]]:
    seen: set[tuple[str, str]] = set()
    proxies: list[tuple[str, str]] = []
    for line in text.splitlines():
        parsed = _parse_proxy_line(line)
        if parsed is not None and parsed not in seen:
            seen.add(parsed)
            proxies.append(parsed)
    return proxies


def write_proxy_csv(proxies: list[tuple[str, str]], path: Path) -> None:
    with atomic_text_writer(path, newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(CSV_FIELDNAMES)
        writer.writerows(("true", protocol, address) for protocol, address in proxies)


def sync_proxy_source(
    source_url: str = DEFAULT_SOURCE_URL,
    csv_path: Path | None = None,
    state_path: Path | None = None,
    refresh_minutes: float = DEFAULT_REFRESH_MINUTES,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    force: bool = False,
) -> dict[str, Any]:
    _validate_sync(source_url, csv_path, refresh_minutes, timeout)
    assert csv_path is not None
    state_path = state_path or csv_path.with_suffix(".state.json")
    state = _load_state(state_path)
    checked_at = _utc_now()

    if not force and not _refresh_due(state, checked_at, refresh_minutes):
        return _build_report(False, "refresh_interval", csv_path, state, checked_at)

    downloaded = _download(source_url, _conditional_headers(state), timeout)
    if downloaded is None:
        return _mark_not_modified(state, state_path, csv_path, checked_at)
    return _apply_download(source_url, downloaded, csv_path, state_path, state, checked_at)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _parse_proxy_line(line: str) -> tuple[str, str] | None:
    address = parse_proxy_url(line)
    if address is None:
        return None
    return address.scheme, address.host_port


def _validate_sync(source_url: str, csv_path: Path | None, refresh_minutes: float, timeout: float) -> None:
    if not source_url:
        raise ValueError("Proxy source URL is required")
    if refresh_minutes < 0 or timeout <= 0:
        raise ValueError("Proxy refresh interval and timeout must be positive")
    if csv_path is None:
        raise ValueError("Proxy CSV path is required")


def _mark_not_modified(state: dict[str, Any], state_path: Path, csv_path: Path, now: datetime) -> dict[str, Any]:
    state["last_checked"] = now.isoformat()
    _save_state(state_path, state)
    return _build_report(False, "not_modified", csv_path, state, now)


def _apply_download(
    source_url: str,
    downloaded: tuple[str, str | None, str | None],
    csv_path: Path,
    state_path: Path,
    state: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    body, etag, last_modified = downloaded
    proxies = parse_proxies(body)
    if not proxies:
        raise ProxySourceError("Proxy source returned no usable proxies")

    write_proxy_csv(proxies, csv_path)
    state.update(
        {
            "source_url": source_url,
            "last_checked": now.isoformat(),
            "last_downloaded": now.isoformat(),
            "etag": etag,
            "last_modified": last_modified,
            "count": len(proxies),
        }
    )
    _save_state(state_path, state)
    return _build_report(True, "downloaded", csv_path, state, now)


def _refresh_due(state: dict[str, Any], now: datetime, refresh_minutes: float) -> bool:
    raw = state.get("last_checked")
    if not raw:
        return True
    try:
        last_checked = datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        return True
    if last_checked.tzinfo is None:
        last_checked = last_checked.replace(tzinfo=UTC)
    return now - last_checked >= timedelta(minutes=refresh_minutes)


def _conditional_headers(state: dict[str, Any]) -> dict[str, str]:
    headers = {"User-Agent": USER_AGENT, "Accept": "text/plain"}
    if state.get("etag"):
        headers["If-None-Match"] = state["etag"]
    if state.get("last_modified"):
        headers["If-Modified-Since"] = state["last_modified"]
    return headers


def _download(source_url: str, headers: dict[str, str], timeout: float):
    request = Request(source_url, headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            response_text = response.read().decode("utf-8-sig", errors="replace")
            return response_text, response.headers.get("ETag"), response.headers.get("Last-Modified")
    except HTTPError as exc:
        if exc.code == 304:
            return None
        raise ProxySourceError(f"Proxy source returned HTTP {exc.code}") from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise ProxySourceError(f"Could not download proxy source: {exc}") from exc


def _build_report(updated: bool, reason: str, csv_path: Path, state: dict[str, Any], now: datetime) -> dict[str, Any]:
    last_checked = state.get("last_checked") or now.isoformat()
    return {
        "updated": updated,
        "reason": reason,
        "path": str(csv_path),
        "last_checked": last_checked,
        "count": state.get("count"),
    }


def _load_state(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _save_state(path: Path, state: dict[str, Any]) -> None:
    atomic_write_json(path, state, indent=2)
