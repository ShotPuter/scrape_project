"""In-memory round-robin proxy rotator with automatic fault detection and cooldown."""

from __future__ import annotations

import csv
import time
from collections.abc import Iterable
from pathlib import Path

from src.utils.proxy_url import SUPPORTED_PROXY_SCHEMES, normalize_proxy_url

DEFAULT_MAX_CONSECUTIVE_FAILURES = 2
DEFAULT_COOLDOWN_SECONDS = 180.0
SUPPORTED_SCHEMES = SUPPORTED_PROXY_SCHEMES


class ProxyRotator:
    """
    Thread-safe and async-friendly round-robin proxy rotator.
    Automatically cools down unreliable or failing proxies.
    """

    def __init__(
        self, proxies: Iterable[str] | None = None, max_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
        cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
    ) -> None:
        self.max_failures = max_failures
        self.cooldown_seconds = cooldown_seconds
        self._pool: list[str] = []
        self._index: int = 0
        self._failures: dict[str, int] = {}
        self._cooldown_until: dict[str, float] = {}

        if proxies:
            self.add_proxies(proxies)

    def add_proxies(self, proxies: Iterable[str]) -> int:
        """Add unique, normalized proxy URLs to the pool."""
        added_count = 0
        existing_set = set(self._pool)

        for candidate in proxies:
            normalized = normalize_proxy_url(candidate)
            if normalized and normalized not in existing_set:
                self._pool.append(normalized)
                existing_set.add(normalized)
                added_count += 1

        return added_count

    def get_proxy(self) -> str | None:
        """
        Return the next available healthy proxy using round-robin rotation.
        Returns None if the pool is empty or all proxies are in cooldown.
        """
        if not self._pool:
            return None

        current_time = time.monotonic()
        total_proxies = len(self._pool)

        for _ in range(total_proxies):
            candidate = self._pool[self._index % total_proxies]
            self._index = (self._index + 1) % total_proxies

            cooldown_expiry = self._cooldown_until.get(candidate, 0.0)
            if current_time >= cooldown_expiry:
                return candidate

        return None

    def mark_failure(self, proxy_url: str) -> None:
        """Record an error for a proxy, placing it into cooldown if threshold is reached."""
        current_failures = self._failures.get(proxy_url, 0) + 1
        self._failures[proxy_url] = current_failures

        if current_failures >= self.max_failures:
            self._cooldown_until[proxy_url] = time.monotonic() + self.cooldown_seconds

    def mark_success(self, proxy_url: str) -> None:
        """Reset consecutive failure counter on successful request."""
        self._failures.pop(proxy_url, None)
        self._cooldown_until.pop(proxy_url, None)

    @property
    def total_count(self) -> int:
        """Total number of registered proxies."""
        return len(self._pool)

    @property
    def healthy_count(self) -> int:
        """Number of currently active proxies not in cooldown."""
        current_time = time.monotonic()
        return sum(1 for p in self._pool if current_time >= self._cooldown_until.get(p, 0.0))

    def load_from_file(self, file_path: Path | str) -> int:
        """
        Load proxy URLs from a plain text or CSV file.
        Plain text: one proxy URL per line.
        CSV: parses 'proxy', 'ip', 'port', 'protocol' columns if available.
        """
        target = Path(file_path)
        if not target.exists():
            return 0

        with target.open(encoding="utf-8", errors="ignore") as handle:
            if target.suffix.lower() == ".csv":
                addresses = (
                    row.get("proxy") or f"{row.get('protocol', 'http')}://{row.get('ip', '')}:{row.get('port', '')}"
                    for row in csv.DictReader(handle)
                )
            else:
                addresses = (line.strip() for line in handle)
            loaded_urls = list(addresses)
        return self.add_proxies(loaded_urls)

    def save_to_file(self, file_path: Path | str) -> int:
        """Export all registered proxy URLs to a clean text file."""
        target = Path(file_path)
        target.parent.mkdir(parents=True, exist_ok=True)

        with target.open("w", encoding="utf-8") as handle:
            for url in self._pool:
                handle.write(f"{url}\n")

        return len(self._pool)
