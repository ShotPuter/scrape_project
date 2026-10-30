"""Bounded User-Agent candidates from fake-useragent's bundled desktop dataset."""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any

logger = logging.getLogger(__name__)
USER_AGENT_RETRY_ERRORS = frozenset({"http_403", "invalid_json", "unexpected_response"})
MAX_USER_AGENT_LIMIT = 10
FALLBACK_USER_AGENTS = {
    "Chrome": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Firefox": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
    "Edge": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
    ),
    "Safari": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/18.3 Safari/605.1.15"
    ),
}


def browser_family(profile: str) -> str:
    lowered = profile.lower()
    return next((name for name in FALLBACK_USER_AGENTS if lowered.startswith(name.lower())), "Chrome")


@lru_cache(maxsize=4)
def _generator(browser: str) -> Any:
    from fake_useragent import UserAgent

    return UserAgent(browsers=[browser], platforms=["desktop"], fallback=FALLBACK_USER_AGENTS[browser])


def generate_user_agents(*, limit: int, preferred: str | None = None, profile: str = "chrome") -> list[str]:
    """Keep an explicit header first; stop after a bounded number of unique draws."""
    if not 1 <= limit <= MAX_USER_AGENT_LIMIT:
        raise ValueError(f"User-Agent limit must be between 1 and {MAX_USER_AGENT_LIMIT}")
    values: list[str] = []
    if preferred:
        if "\r" in preferred or "\n" in preferred:
            raise ValueError("User-Agent must not contain line breaks")
        values.append(preferred)
    if len(values) == limit:
        return values
    browser = browser_family(profile)
    try:
        generator = _generator(browser)
        for _ in range(limit * 20):
            value = generator.random
            if isinstance(value, str) and value.strip() and "\r" not in value and "\n" not in value and value not in values:
                values.append(value)
            if len(values) >= limit:
                break
    except Exception as exc:  # noqa: BLE001 - generation must not take the collector down
        logger.warning("User-Agent generation unavailable (%s); using available headers", type(exc).__name__)
    if not values:
        values.append(FALLBACK_USER_AGENTS[browser])
    return values
