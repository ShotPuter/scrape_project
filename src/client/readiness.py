"""Shared validators for SEPREC readiness payloads."""

from __future__ import annotations

from collections.abc import Callable
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
    return _is_ready_response(response, is_catalog_payload)


def is_search_ready_response(response: Any) -> bool:
    """Validate the same small publication search used by the collector."""
    return _is_ready_response(response, is_search_payload)


def _is_ready_response(response: Any, validator: Callable[[Any], bool]) -> bool:
    status = getattr(response, "status_code", None)
    if not isinstance(status, int) or not 200 <= status < 300:
        return False
    try:
        data = response.json()
    except (ValueError, UnicodeError):
        return False
    return validator(data)
