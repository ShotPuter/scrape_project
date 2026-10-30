"""Provider-specific metadata normalization."""

from collections.abc import Iterator, Mapping
from datetime import datetime
from typing import Any


def adapt(row: Mapping[str, Any], now: datetime) -> Iterator[dict[str, Any]]:
    if not isinstance(row.get("protocols"), list):
        yield dict(row)
        return
    item = dict(row)
    location = row.get("location") or {}
    item["country"] = location.get("isocode") or location.get("country")
    item["city"] = location.get("city")
    item["last_checked"] = row.get("lastSuccess") or row.get("lastTested")
    for entry in row["protocols"]:
        if not isinstance(entry, Mapping):
            continue
        protocol = str(entry.get("type", "")).lower()
        yield {
            **item,
            "protocol": "http" if protocol == "https" else protocol,
            "port": entry.get("port", row.get("port")),
            "https_supported": True if protocol == "https" else None,
        }
