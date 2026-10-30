"""Provider-specific metadata normalization."""

from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta
from typing import Any


def adapt(row: Mapping[str, Any], now: datetime) -> Iterator[dict[str, Any]]:
    item = dict(row)
    item["latency_ms"] = row.get("delay")
    try:
        up, down = float(row.get("checks_up", 0)), float(row.get("checks_down", 0))
        if up + down > 0:
            item["uptime_percent"] = 100 * up / (up + down)
        if row.get("lastseen") is not None:
            item["last_checked"] = now - timedelta(seconds=max(0, float(row["lastseen"])))
    except (TypeError, ValueError, OverflowError):
        pass
    for protocol in ("http", "socks4", "socks5"):
        if str(row.get(protocol, "0")) == "1":
            yield {**item, "protocol": protocol, "https_supported": str(row.get("ssl", "0")) == "1"}
