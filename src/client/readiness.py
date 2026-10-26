"""Shared validators for SEPREC readiness payloads."""

from __future__ import annotations

from typing import Any


def is_catalog_payload(data: Any) -> bool:
    """Return whether ``data`` is a completed catalog response."""
    return isinstance(data, dict) and data.get("finalizado") is True and isinstance(data.get("datos"), list)


def is_search_payload(data: Any) -> bool:
    """Return whether ``data`` is a completed publication-search response."""
    if not isinstance(data, dict) or data.get("finalizado") is not True:
        return False
    payload = data.get("datos")
    return isinstance(payload, dict) and isinstance(payload.get("filas"), list) and "total" in payload


def is_ready_response(response: Any) -> bool:
    """Validate an HTTP readiness probe response without leaking transport details."""
    status = getattr(response, "status_code", None)
    if not isinstance(status, int) or not 200 <= status < 300:
        return False
    try:
        data = response.json()
    except (ValueError, UnicodeError):
        return False
    return is_catalog_payload(data)
