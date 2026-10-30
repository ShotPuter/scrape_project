"""Provider-specific metadata normalization."""

from collections.abc import Iterator, Mapping
from datetime import datetime
from typing import Any


def adapt(row: Mapping[str, Any], now: datetime) -> Iterator[dict[str, Any]]:
    yield {**row, "latency_ms": row.get("response_ms", row.get("latency_ms"))}
