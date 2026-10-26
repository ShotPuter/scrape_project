"""Transport/session construction and cookie loading."""

from __future__ import annotations

import logging
import math
from http.cookiejar import MozillaCookieJar
from pathlib import Path
from typing import Any, cast

from curl_cffi import CurlInfo, CurlOpt
from curl_cffi.requests import AsyncSession

from src.client.aiohttp_session import AIOHTTP_AVAILABLE, AiohttpSession
from src.client.network import proxy_id

logger = logging.getLogger(__name__)


class SessionFactory:
    def __init__(
        self, *, headers: dict[str, str],
        timeout: float, connection_limit: int, impersonate: str,
        cookies_file: Path | None, transport: str, sock_read_timeout: float = 30,
    ):
        self.sock_read_timeout = sock_read_timeout
        self.headers = headers
        self.timeout = timeout
        self.connection_limit = connection_limit
        self.impersonate = impersonate
        self.cookies_file = cookies_file
        self.transport = transport.lower()
        self._curl_fallback = self.transport == "curl_cffi"

    @property
    def using_curl(self) -> bool:
        return self._curl_fallback

    def create(self, proxy: str | None = None, *, transport: str | None = None, user_agent: str | None = None):
        requested = (transport or self.transport).lower()
        headers = dict(self.headers)
        if user_agent:
            headers["User-Agent"] = user_agent

        if requested == "curl_cffi" or (transport is None and self._curl_fallback):
            return self.make_curl(proxy, headers=headers)
        if requested == "auto" and self._curl_fallback:
            return self.make_curl(proxy, headers=headers)
        if not AIOHTTP_AVAILABLE:
            logger.warning("aiohttp is not installed; falling back to curl_cffi")
            self._curl_fallback = True
            return self.make_curl(proxy, headers=headers)
        try:
            return self.make_aiohttp(proxy, headers=headers)
        except (ImportError, RuntimeError) as exc:
            logger.warning("aiohttp transport unavailable for %s (%s); using curl_cffi", proxy_id(proxy), type(exc).__name__)
            if proxy is None:
                self._curl_fallback = True
            return self.make_curl(proxy, headers=headers)

    def make_curl(self, proxy: str | None = None, *, headers: dict[str, str] | None = None) -> AsyncSession:
        extra: dict[str, Any] = {}
        if proxy:
            extra["proxy"] = proxy
        return AsyncSession(
            headers=headers or self.headers, timeout=self.timeout, max_clients=self.connection_limit,
            impersonate=self.impersonate, cookies=_load_mozilla_cookies(self.cookies_file), trust_env=False,
            curl_options={CurlOpt.LOW_SPEED_LIMIT: 1, CurlOpt.LOW_SPEED_TIME: math.ceil(self.sock_read_timeout)},
            curl_infos=[
                CurlInfo.NAMELOOKUP_TIME,
                CurlInfo.CONNECT_TIME,
                CurlInfo.APPCONNECT_TIME,
                CurlInfo.STARTTRANSFER_TIME,
                CurlInfo.TOTAL_TIME,
            ],
            **extra,
        )

    def make_aiohttp(self, proxy: str | None = None, *, headers: dict[str, str] | None = None) -> AiohttpSession:
        cookie_jar = self._make_aiohttp_cookie_jar() if self.cookies_file else None
        return AiohttpSession(
            headers=headers or self.headers, timeout=self.timeout, max_clients=self.connection_limit,
            cookie_jar=cookie_jar, **({"proxy": proxy} if proxy else {}),
        )

    def switch_to_curl(self) -> None:
        self._curl_fallback = True

    def _make_aiohttp_cookie_jar(self):
        from aiohttp import CookieJar
        from yarl import URL

        jar = CookieJar(unsafe=True)
        for cookie in _load_mozilla_cookies(self.cookies_file) or []:
            domain = (cookie.domain or "localhost").lstrip(".")
            scheme = "https" if cookie.secure else "http"
            path = cookie.path or "/"
            try:
                jar.update_cookies(
                    {cookie.name: cast(str, cookie.value)},
                    response_url=URL(f"{scheme}://{domain}{path}"),
                )
            except (TypeError, ValueError):
                continue
        return jar


def _load_mozilla_cookies(path: Path | None) -> MozillaCookieJar | None:
    if path is None:
        return None
    cookies = MozillaCookieJar(str(path))
    cookies.load(ignore_discard=True, ignore_expires=True)
    for cookie in cookies:
        if cookie.expires == 0:
            cookie.expires = None
            cookie.discard = True
    cookies.clear_expired_cookies()
    return cookies
