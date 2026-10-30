"""Provider-specific metadata normalization."""

from collections.abc import Iterator, Mapping
from datetime import datetime
from typing import Any


def adapt(row: Mapping[str, Any], now: datetime) -> Iterator[dict[str, Any]]:
    item = dict(row)
    if isinstance(row.get("timeout"), (int, float)):
        item["latency_ms"] = row["timeout"] * 1000
    geo = row.get("geolocation") or {}
    for key in ("country", "city"):
        value = geo.get(key) or {}
        if isinstance(value, Mapping):
            item[key] = (
                (value.get("iso_code") or (value.get("names") or {}).get("en"))
                if key == "country"
                else (value.get("names") or {}).get("en")
            )
    yield item
