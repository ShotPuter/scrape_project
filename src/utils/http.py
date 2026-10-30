"""Small HTTP header parsing helpers shared by the client and proxy tooling."""

from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any


def retry_after_from_response(response: Any) -> str | None:
    """Use the API's explicit retry_after field, otherwise its HTTP header."""
    header = response.headers.get("Retry-After")
    try:
        payload = response.json()
    except (ValueError, UnicodeError):
        return header
    if not isinstance(payload, dict):
        return header
    value = payload.get("retry_after")
    return str(value) if value is not None else header


def parse_retry_after(raw: str) -> float | None:
    """Parse a numeric or HTTP-date ``Retry-After`` value into seconds."""
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        pass

    try:
        deadline = parsedate_to_datetime(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    if deadline.tzinfo is None:
        deadline = deadline.replace(tzinfo=UTC)
    return max(0.0, (deadline - datetime.now(UTC)).total_seconds())
