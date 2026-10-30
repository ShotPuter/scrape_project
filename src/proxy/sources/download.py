"""Bounded source downloads and validated, conditional per-file caching."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Callable, Mapping
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypeVar

from src.network.aiohttp_session import AIOHTTP_AVAILABLE, AiohttpSession
from src.network.budget import TimeoutPolicy
from src.network.errors import ResponseTooLargeError
from src.network.transport import Response
from src.utils.atomic import atomic_write_bytes, atomic_write_json

logger = logging.getLogger(__name__)
T = TypeVar("T")


async def fetch_bytes(url: str, timeout: float, max_bytes: int, headers: Mapping[str, str]) -> Response:  # noqa: ASYNC109
    policy = TimeoutPolicy(total=timeout, connect=min(10, timeout), sock_connect=min(10, timeout), sock_read=timeout)
    if AIOHTTP_AVAILABLE:
        try:
            async with AiohttpSession(headers=dict(headers), timeout=policy, max_clients=1) as session:
                result = await asyncio.wait_for(session.get(url, max_response_bytes=max_bytes), timeout + 1)
                return Response(result.status_code, {k.lower(): v for k, v in result.headers.items()}, result.body)
        except ResponseTooLargeError:
            raise
        except Exception as exc:  # noqa: BLE001 - preserve the existing alternate transport
            logger.debug("Source aiohttp download failed: %s", type(exc).__name__)

    from curl_cffi.requests import AsyncSession

    chunks: list[bytes] = []
    size = 0
    oversized = False

    def receive(chunk: bytes) -> int:
        nonlocal size, oversized
        size += len(chunk)
        if size > max_bytes:
            oversized = True
            return 0  # curl aborts rather than retaining the rest of the body
        chunks.append(chunk)
        return len(chunk)

    async with AsyncSession(headers=dict(headers), trust_env=False, max_clients=1) as session:
        try:
            result = await asyncio.wait_for(session.get(url, timeout=timeout, content_callback=receive), timeout + 1)
        except Exception:
            if oversized:
                raise ResponseTooLargeError(f"Response exceeds {max_bytes} bytes") from None
            raise
    return Response(result.status_code, {k.lower(): v for k, v in result.headers.items()}, b"".join(chunks))


def _cache_paths(directory: Path, url: str) -> tuple[Path, Path]:
    key = hashlib.sha256(url.encode()).hexdigest()
    return directory / f"{key}.body", directory / f"{key}.json"


def _read_cache(directory: Path | None, url: str, max_bytes: int) -> tuple[bytes, dict] | None:
    if directory is None:
        return None
    body_path, state_path = _cache_paths(directory, url)
    try:
        state = json.loads(state_path.read_text())
        if body_path.stat().st_size > max_bytes or state.get("url") != url:
            return None
        body = body_path.read_bytes()
        if hashlib.sha256(body).hexdigest() != state.get("sha256"):
            return None
        for key in ("downloaded_at", "checked_at"):
            stamp = datetime.fromisoformat(state[key])
            if stamp.tzinfo is None or stamp > datetime.now(UTC):
                return None
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return None
    return body, state


def _save_cache(directory: Path, url: str, body: bytes, state: dict) -> None:
    body_path, state_path = _cache_paths(directory, url)
    atomic_write_bytes(body_path, body)
    atomic_write_json(state_path, {**state, "url": url, "sha256": hashlib.sha256(body).hexdigest()})


async def load_document(
    url: str,
    parser: Callable[[bytes, datetime], list[T]],
    *,
    timeout: float,  # noqa: ASYNC109 - bounded remote request
    max_bytes: int,
    refresh_minutes: float,
    cache_dir: Path | None,
    max_cache_age_hours: float,
    network_slots: asyncio.Semaphore,
) -> tuple[list[T], str | None]:
    cached = await asyncio.to_thread(_read_cache, cache_dir, url, max_bytes)
    now = datetime.now(UTC)
    previous: list[T] | None = None
    state: dict = {}
    if cached:
        body, state = cached
        try:
            previous = await asyncio.to_thread(parser, body, datetime.fromisoformat(state["downloaded_at"]))
            if not previous:
                raise ValueError("Cached source contains no usable proxies")
        except Exception:  # noqa: BLE001 - invalid cache must trigger a remote refresh
            cached = None
        else:
            age = now - datetime.fromisoformat(state["checked_at"])
            if age <= timedelta(minutes=refresh_minutes) and age <= timedelta(hours=max_cache_age_hours):
                return previous, None

    headers = {"User-Agent": "gaceta-scraper/1.0", "Accept": "application/json, text/plain, */*"}
    if cached:
        if state.get("etag"):
            headers["If-None-Match"] = state["etag"]
        if state.get("last_modified"):
            headers["If-Modified-Since"] = state["last_modified"]
    try:
        async with network_slots:
            response = await fetch_bytes(url, timeout, max_bytes, headers)
        if response.status_code == 304:
            if not cached or previous is None:
                raise ValueError("HTTP 304 without a valid cached source")
            body, _ = cached
            state = {**state, "checked_at": now.isoformat()}
            items = previous
        else:
            if response.status_code != 200:
                raise ValueError(f"HTTP {response.status_code}")
            body = response.body
            items = await asyncio.to_thread(parser, body, now)
            if not items:
                raise ValueError("Source returned no usable proxies")
            state = {
                "downloaded_at": now.isoformat(),
                "checked_at": now.isoformat(),
                "etag": response.headers.get("etag"),
                "last_modified": response.headers.get("last-modified"),
            }
    except Exception as exc:  # noqa: BLE001 - keep a bounded last known valid source
        if cached and previous and max_cache_age_hours > 0:
            age = now - datetime.fromisoformat(cached[1]["checked_at"])
            if age <= timedelta(hours=max_cache_age_hours):
                return previous, f"Using cached source after {type(exc).__name__}: {exc}"
        raise
    if cache_dir is not None:
        with suppress(OSError):
            await asyncio.to_thread(_save_cache, cache_dir, url, body, state)
    return items, None
