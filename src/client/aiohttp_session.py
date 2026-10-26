"""aiohttp adapter with the small synchronous response surface used by the scraper."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self

from src.client.network import TimeoutPolicy, mark, trace_config
from src.utils.proxy_url import is_socks_proxy

try:
    from aiohttp import ClientError, ClientSession, ClientTimeout, TCPConnector

    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover - only minimal installs lack aiohttp
    ClientError = OSError  # type: ignore[misc,assignment]
    ClientSession = None  # type: ignore[assignment,misc]
    ClientTimeout = None  # type: ignore[assignment,misc]
    TCPConnector = None  # type: ignore[assignment,misc]
    AIOHTTP_AVAILABLE = False

try:
    from aiohttp_socks import (
        ProxyConnectionError,
        ProxyConnector,
        ProxyError,
        ProxyTimeoutError,
        SocksConnectionError,
        SocksError,
    )

    AIOHTTP_SOCKS_ERRORS = (ProxyError, ProxyConnectionError, ProxyTimeoutError, SocksError, SocksConnectionError)
except ImportError:  # SOCKS proxies fall back to curl_cffi
    ProxyConnector = None  # type: ignore[assignment,misc]
    AIOHTTP_SOCKS_ERRORS = ()  # type: ignore[assignment]

NETWORK_ERRORS = (ClientError, TimeoutError, asyncio.IncompleteReadError) + AIOHTTP_SOCKS_ERRORS


@dataclass(slots=True)
class AiohttpResponse:
    status_code: int
    headers: Mapping[str, str]
    body: bytes

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return json.loads(self.body)


class AiohttpSession:
    def __init__(
        self,
        headers: dict[str, str] | None = None,
        timeout: float | TimeoutPolicy = 30,
        max_clients: int = 9,
        proxy: str | None = None,
        cookie_jar: Any = None,
    ):
        if not AIOHTTP_AVAILABLE:
            raise ImportError("aiohttp is not installed")

        self.proxy = proxy
        self._uses_socks = _is_socks_proxy(proxy)
        if self._uses_socks and ProxyConnector is None:
            raise ImportError("aiohttp_socks is required for SOCKS proxies")
        connector: Any
        if self._uses_socks:
            assert proxy is not None
            connector = ProxyConnector.from_url(proxy, limit=max_clients)
        else:
            connector = TCPConnector(limit=max_clients)
        self._session = ClientSession(
            headers=headers,
            timeout=(timeout.aiohttp() if isinstance(timeout, TimeoutPolicy) else ClientTimeout(total=timeout)),
            connector=connector,
            cookie_jar=cookie_jar,
            trust_env=False,
            trace_configs=[trace_config()],
        )

    async def request(
        self,
        method: str,
        url: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        timeout: float | TimeoutPolicy | None = None,  # noqa: ASYNC109 - forwarded to aiohttp
        **kwargs: Any,
    ) -> AiohttpResponse:
        options = _request_options(
            json_data=json, params=params, proxy=self.proxy if not self._uses_socks else None, timeout=timeout
        )
        options.update(kwargs)

        started = time.monotonic()
        mark("stage", "connection_setup")
        async with self._session.request(method, url, **options) as response:
            headers_at = time.monotonic()
            mark("response_headers_ms", round((headers_at - started) * 1000, 3))
            mark("http_status", response.status)
            mark("stage", "body_read")
            chunks: list[bytes] = []
            size = 0
            async for chunk in response.content.iter_chunked(65536):
                if not chunks:
                    mark("first_body_chunk_ms", round((time.monotonic() - started) * 1000, 3))
                chunks.append(chunk)
                size += len(chunk)
                mark("response_bytes", size)
            mark("body_read_ms", round((time.monotonic() - headers_at) * 1000, 3))
            return AiohttpResponse(response.status, response.headers, b"".join(chunks))

    async def get(self, url: str, **kwargs: Any) -> AiohttpResponse:
        """GET is used by source downloads and the stage-1 HTTPS probe."""
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> AiohttpResponse:
        """POST is used by the proxy readiness probe across both transports."""
        return await self.request("POST", url, **kwargs)

    async def close(self) -> None:
        await self._session.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        await self.close()


def _is_socks_proxy(proxy: str | None) -> bool:
    return is_socks_proxy(proxy)


def _request_options(
    *,
    json_data: dict[str, Any] | None,
    params: dict[str, Any] | None,
    proxy: str | None,
    timeout: float | TimeoutPolicy | None,
) -> dict[str, Any]:
    options: dict[str, Any] = {}
    if json_data is not None:
        options["json"] = json_data
    if params is not None:
        options["params"] = params
    if proxy:
        options["proxy"] = proxy
    if timeout is not None:
        options["timeout"] = timeout.aiohttp() if isinstance(timeout, TimeoutPolicy) else ClientTimeout(total=timeout)
    return options
