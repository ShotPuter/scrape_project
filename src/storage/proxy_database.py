"""Compatibility imports; use src.proxy.repository for new code."""

from pathlib import Path

from config import settings
from src.proxy.repository import (
    CREATE_VERIFIED_PROXIES,
    CREATE_VERIFIED_PROXIES_INDEX,
    EXTRA_COLUMNS,
    SQLITE_BUSY_TIMEOUT_MS,
    UPSERT_VERIFIED_PROXY,
    ProxyDatabase as _ProxyDatabase,
    VerifiedProxy,
)


class ProxyDatabase(_ProxyDatabase):
    def __init__(self, db_path: Path | str | None = None):
        super().__init__(settings.proxy_db_path if db_path is None else db_path)


__all__ = [
    "CREATE_VERIFIED_PROXIES",
    "CREATE_VERIFIED_PROXIES_INDEX",
    "EXTRA_COLUMNS",
    "ProxyDatabase",
    "SQLITE_BUSY_TIMEOUT_MS",
    "UPSERT_VERIFIED_PROXY",
    "VerifiedProxy",
]
