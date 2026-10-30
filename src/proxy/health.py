"""Read the scraper's live pool count without exposing proxy credentials."""

import json
from datetime import UTC, datetime
from pathlib import Path


def pool_health_path(database: Path) -> Path:
    return database.with_suffix(".pool-health.json")


def read_webshare_health(database: Path, max_age_seconds: float = 60) -> int | None:
    try:
        payload = json.loads(pool_health_path(database).read_text())
        stamp = datetime.fromisoformat(payload["updated_at"])
        count = payload["webshare_usable"]
        if stamp.tzinfo is None or payload.get("status") != "running":
            return None
        if not 0 <= (datetime.now(UTC) - stamp).total_seconds() <= max_age_seconds:
            return None
        return count if type(count) is int and count >= 0 else None
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return None
