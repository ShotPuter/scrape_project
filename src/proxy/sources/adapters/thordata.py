"""Provider-specific metadata normalization."""

from collections.abc import Iterator, Mapping
from datetime import datetime
from typing import Any


def adapt(row: Mapping[str, Any], now: datetime) -> Iterator[dict[str, Any]]:
    yield {
        **row,
        "protocol": row.get("type", row.get("protocol")),
        "last_checked": row.get("updated_utc", row.get("last_checked")),
    }
