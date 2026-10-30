"""Parsing, validation, and safe display helpers for proxy addresses."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlsplit

SUPPORTED_PROXY_SCHEMES = frozenset({"http", "https", "socks4", "socks4a", "socks5", "socks5h"})
DEFAULT_PROXY_SCHEME = "http"


def redact_proxy_credentials(text: str) -> str:
    """Remove URL userinfo from public reports and diagnostics."""
    return re.sub(r"((?:https?|socks4a?|socks5h?)://)[^\s/@]+@", r"\1***@", text)


@dataclass(frozen=True, slots=True)
class ProxyAddress:
    """A validated proxy URL split into the parts callers need."""

    url: str
    scheme: str
    host: str
    port: int
    username: str | None = None
    password: str | None = None

    @property
    def host_port(self) -> str:
        """Return ``host:port`` with IPv6 hosts wrapped in brackets."""
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"{host}:{self.port}"


def proxy_url_from_row(row: Mapping[str, str]) -> str | None:
    """Normalize a proxy CSV row that uses ``protocol``/``proxy`` or ``ip``/``port``."""
    protocol = row.get("protocol", "http").strip().lower()
    address = row.get("proxy", "").strip() or f"{row.get('ip', '')}:{row.get('port', '')}"
    if not address.strip():
        return None
    url = address if "://" in address else f"{protocol}://{address}"
    return normalize_proxy_url(url)


def parse_proxy_url(raw_value: str, *, default_scheme: str = DEFAULT_PROXY_SCHEME) -> ProxyAddress | None:
    """Validate a proxy line and return its structured representation.

    Accepts ``host:port``, ``scheme://host:port`` and URLs with credentials.
    Comments, missing ports, unsupported schemes and malformed ports are
    rejected rather than propagated to the transport layer.
    """
    if not isinstance(raw_value, str):
        return None

    cleaned = raw_value.strip()
    if not cleaned or cleaned.startswith("#"):
        return None

    candidate = cleaned if "://" in cleaned else f"{default_scheme}://{cleaned}"
    try:
        parsed = urlsplit(candidate)
        port = parsed.port
    except ValueError:
        return None

    scheme = parsed.scheme.lower()
    host = parsed.hostname
    if scheme not in SUPPORTED_PROXY_SCHEMES or not host or not port:
        return None

    return ProxyAddress(
        url=f"{scheme}://{parsed.netloc}",
        scheme=scheme,
        host=host,
        port=port,
        username=parsed.username,
        password=parsed.password,
    )


def normalize_proxy_url(raw_value: str, *, default_scheme: str = DEFAULT_PROXY_SCHEME) -> str | None:
    """Return a canonical proxy URL, or ``None`` when the value is unusable."""
    address = parse_proxy_url(raw_value, default_scheme=default_scheme)
    return address.url if address is not None else None


def proxy_host(raw_value: str) -> str:
    """Return the hostname used for per-IP fairness and request tracking."""
    try:
        return urlsplit(raw_value).hostname or raw_value
    except ValueError:
        return raw_value


def is_socks_proxy(raw_value: str | None) -> bool:
    """Return whether the proxy URL uses a SOCKS transport."""
    if not raw_value:
        return False
    try:
        return urlsplit(raw_value).scheme.lower().startswith("socks")
    except ValueError:
        return False
