"""Compatibility imports; use src.proxy.sources.iplocate for new code."""

from src.proxy.sources.iplocate import (
    CSV_FIELDNAMES,
    DEFAULT_REFRESH_MINUTES,
    DEFAULT_SOURCE_URL,
    DEFAULT_TIMEOUT_SECONDS,
    USER_AGENT,
    ProxySourceError,
    parse_proxies,
    sync_proxy_source,
    write_proxy_csv,
)

__all__ = [
    "CSV_FIELDNAMES",
    "DEFAULT_REFRESH_MINUTES",
    "DEFAULT_SOURCE_URL",
    "DEFAULT_TIMEOUT_SECONDS",
    "ProxySourceError",
    "USER_AGENT",
    "parse_proxies",
    "sync_proxy_source",
    "write_proxy_csv",
]
