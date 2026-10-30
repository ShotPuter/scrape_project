"""Provider-specific metadata normalization."""

from collections.abc import Iterator, Mapping
from datetime import datetime
from typing import Any


def adapt(row: Mapping[str, Any], now: datetime) -> Iterator[dict[str, Any]]:
    yield dict(row)
