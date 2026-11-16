"""Sunny9577 labels the transport in `type`, not `protocol`."""

from collections.abc import Iterator, Mapping
from datetime import datetime
from typing import Any


def adapt(row: Mapping[str, Any], now: datetime) -> Iterator[dict[str, Any]]:
    kind = str(row.get("type", "")).upper().strip()
    protocol = {"HTTP": "http", "HTTPS": "http", "HTTP/HTTPS": "http", "SOCKS4": "socks4", "SOCKS5": "socks5"}.get(kind)
    if protocol:
        yield {**row, "protocol": protocol, "https_supported": "HTTPS" in kind}
