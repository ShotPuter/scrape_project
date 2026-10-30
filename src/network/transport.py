"""Transport abstraction: one response shape and adapters for aiohttp/curl_cffi."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, cast

from curl_cffi import CurlInfo
from curl_cffi.requests import AsyncSession

from src.network.aiohttp_session import AiohttpSession
from src.network.budget import TimeoutPolicy, mark


@dataclass(slots=True)
class Response:
    """Transport-independent response with the synchronous surface callers need."""

    status_code: int
    headers: Mapping[str, str]
    body: bytes = b""
    infos: Mapping[Any, float] = field(default_factory=dict)
    transport: str = ""

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return json.loads(self.body)


class Transport(Protocol):
    """Minimal async transport contract used by ``HttpClient``."""

    name: str
    raw: Any

    async def request(
        self,
        method: str,
        url: str,
        *,
        json_data: dict[str, Any] | None,
        params: dict[str, Any] | None,
        timeout: TimeoutPolicy,  # noqa: ASYNC109 - forwarded to the transport
    ) -> Response: ...

    async def close(self) -> None: ...


class AiohttpTransport:
    name = "aiohttp"

    def __init__(self, session: AiohttpSession) -> None:
        self.raw = session

    async def request(
        self,
        method: str,
        url: str,
        *,
        json_data: dict[str, Any] | None,
        params: dict[str, Any] | None,
        timeout: TimeoutPolicy,  # noqa: ASYNC109 - forwarded to aiohttp
    ) -> Response:
        response = await self.raw.request(method, url, json=json_data, params=params, timeout=timeout)
        return Response(status_code=response.status_code, headers=response.headers, body=response.body, transport=self.name)

    async def close(self) -> None:
        await self.raw.close()


CURL_INFO_KEYS = (
    (CurlInfo.NAMELOOKUP_TIME, "curl_namelookup_ms"),
    (CurlInfo.CONNECT_TIME, "curl_connect_ms"),
    (CurlInfo.APPCONNECT_TIME, "curl_appconnect_ms"),
    (CurlInfo.STARTTRANSFER_TIME, "curl_starttransfer_ms"),
    (CurlInfo.TOTAL_TIME, "curl_total_ms"),
)


class CurlTransport:
    name = "curl_cffi"

    def __init__(self, session: AsyncSession[Any]) -> None:
        self.raw = session

    async def request(
        self,
        method: str,
        url: str,
        *,
        json_data: dict[str, Any] | None,
        params: dict[str, Any] | None,
        timeout: TimeoutPolicy,  # noqa: ASYNC109 - forwarded to curl_cffi
    ) -> Response:
        # curl_cffi sums the pair; keep the resulting total equal to policy.total.
        connect = min(timeout.connect, timeout.sock_connect, timeout.total)
        response = await self.raw.request(
            cast(Any, method), url, json=json_data, params=params, timeout=(connect, max(0, timeout.total - connect))
        )
        infos = getattr(response, "infos", None)
        if isinstance(infos, Mapping):
            for info, key in CURL_INFO_KEYS:
                if info in infos:
                    mark(key, round(infos[info] * 1000, 3))
        content = getattr(response, "content", b"")
        body = bytes(content) if isinstance(content, (bytes, bytearray)) else b""
        if isinstance(content, (bytes, bytearray)):
            mark("response_bytes", len(body))
        return Response(
            status_code=response.status_code,
            headers=response.headers,
            body=body,
            infos=infos if isinstance(infos, Mapping) else {},
            transport=self.name,
        )

    async def close(self) -> None:
        await self.raw.close()


def transport_for(session: Any) -> Transport:
    """Wrap a concrete session in the transport adapter it needs.

    This is the only place that knows about concrete transports; the HTTP client
    itself works exclusively against the ``Transport`` protocol.
    """
    if isinstance(session, AiohttpSession):
        return AiohttpTransport(session)
    return CurlTransport(session)
