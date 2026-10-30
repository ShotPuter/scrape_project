"""Site-independent request specifications for proxy checks."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


def is_success_response(response: Any) -> bool:
    status = getattr(response, "status_code", None)
    return isinstance(status, int) and 200 <= status < 300


@dataclass(frozen=True, slots=True)
class ProbeRequest:
    method: str
    url: str
    validator: Callable[[Any], bool]
    payload: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class TargetProbe:
    requests: tuple[ProbeRequest, ...]

    def __post_init__(self) -> None:
        if not self.requests:
            raise ValueError("A target probe requires at least one request")
