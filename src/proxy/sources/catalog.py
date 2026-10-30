"""Download, normalise, de-duplicate, and prioritise public proxy candidates.

The collector deliberately keeps this layer independent from the target API:
it only turns the upstream JSON/text lists into :class:`ProxyCandidate` objects
and applies the cheap metadata filters.  A real HTTPS request is performed by
the verifier in :mod:`src.proxy.checker`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

from src.proxy.sources.adapters import adapt_rows
from src.proxy.sources.download import load_document
from src.proxy.sources.registry import SOURCE_REGISTRY, SourceDefinition, SourceFile
from src.utils.proxy_url import SUPPORTED_PROXY_SCHEMES, normalize_proxy_url

logger = logging.getLogger(__name__)

PROXYSCRAPE_URL = "https://cdn.jsdelivr.net/gh/ProxyScrape/free-proxy-list@main/proxies/all/data.json"
HPROXY_URL = "https://hproxy.com/api/proxy-list?format=json"
# Proxifly's all-proxies JSON is larger than jsDelivr's 20 MB file limit.
PROXIFLY_URL = "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.json"
SOURCE_URLS = {name: source.files[0].url for name, source in SOURCE_REGISTRY.items()}
PUBLIC_SOURCES = tuple(SOURCE_URLS)
DEAD_STATUSES = {"dead", "down", "offline", "inactive", "removed", "failed", "unreachable"}
SOURCE_RANK = {"proxyscrape": 0, "hproxy": 0, "iplocate": 1, "free-proxy": 1, "proxifly": 2}
USER_AGENT = "gaceta-scraper/1.0"


class ProxyCatalogError(Exception):
    """An upstream proxy source could not be downloaded or parsed."""


@dataclass(frozen=True, slots=True)
class ProxyCandidate:
    """A proxy URL plus all metadata available from the upstream source."""

    url: str
    protocol: str
    source: str
    country: str | None = None
    city: str | None = None
    anonymity: str | None = None
    latency_ms: float | None = None
    uptime_percent: float | None = None
    last_checked: datetime | None = None
    https_supported: bool | None = None
    score: float | None = None

    @property
    def metadata_missing(self) -> bool:
        """True when at least one useful quality signal is absent."""
        return self.uptime_percent is None or self.latency_ms is None or self.last_checked is None

    @property
    def priority(self) -> tuple:
        """Sort key: lower values are checked first."""
        return (
            self.source.split("+")[0] not in {"webshare", "paid"},
            self.metadata_missing,
            SOURCE_RANK.get(self.source.split("+")[0], 3),
            0 if self.https_supported is not False else 1,
            -(self.uptime_percent if self.uptime_percent is not None else 0),
            self.latency_ms if self.latency_ms is not None else float("inf"),
            -(self.score if self.score is not None else 0),
            -(self.last_checked.timestamp() if self.last_checked else 0),
        )


@dataclass(slots=True)
class CatalogLoadResult:
    candidates: list[ProxyCandidate] = field(default_factory=list)
    source_counts: dict[str, int] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class CatalogSelection:
    candidates: list[ProxyCandidate] = field(default_factory=list)
    rejected: Counter[str] = field(default_factory=Counter)
    source_counts: dict[str, int] = field(default_factory=dict)


def _first(mapping: Mapping[str, object], *keys: str) -> object | None:
    for key in keys:
        value = mapping.get(key)
        if value is not None and value != "":
            return value
    return None


def _as_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        text = str(value).strip().replace("%", "")
        number = float(text)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _as_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return None


def _as_datetime(value: object) -> datetime | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, (int, float)):
        stamp = float(value)
        # Some lists use milliseconds; the public lists use seconds.
        if stamp > 10_000_000_000:
            stamp /= 1000
        try:
            return datetime.fromtimestamp(stamp, UTC)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        numeric = float(text)
    except ValueError:
        numeric = None
    if numeric is not None:
        return _as_datetime(numeric)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _normalise_protocol(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    protocol = value.strip().lower()
    if protocol in {"socks5h", "socks5"}:
        return protocol
    if protocol in {"socks4a", "socks4"}:
        return protocol
    if protocol in {"http", "https"}:
        return protocol
    return None


def _choose_protocol(raw: Mapping[str, object]) -> str:
    raw_protocol = raw.get("protocol")
    if isinstance(raw_protocol, str) and raw_protocol.strip() and _normalise_protocol(raw_protocol) is None:
        return raw_protocol.strip().lower()
    explicit = _normalise_protocol(raw_protocol)
    if explicit:
        return explicit

    protocols = raw.get("protocols")
    values: list[str] = []
    if isinstance(protocols, str):
        values = [item.strip().lower() for item in protocols.replace(";", ",").split(",") if item.strip()]
    elif isinstance(protocols, Sequence) and not isinstance(protocols, (str, bytes)):
        values = [str(item).strip().lower() for item in protocols]
    for preferred in ("http", "https", "socks5", "socks4"):
        if preferred in values or f"{preferred}h" in values:
            return preferred

    proxy = _first(raw, "proxy", "url", "proxy_url")
    if isinstance(proxy, str) and "://" in proxy:
        return _normalise_protocol(urlsplit(proxy).scheme) or "http"
    return "http"


def _address(raw: Mapping[str, object]) -> str | None:
    proxy = _first(raw, "proxy", "url", "proxy_url")
    if isinstance(proxy, str) and proxy.strip():
        return proxy.strip()
    ip = _first(raw, "ip", "host", "hostname", "address")
    port = _first(raw, "port")
    if ip is None or port is None:
        return None
    host = str(ip).strip()
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"{host}:{str(port).strip()}"


def _candidate_url(address: str, protocol: str) -> str | None:
    value = address.strip()
    if not value:
        return None
    url = value if "://" in value else f"{protocol}://{value}"
    return normalize_proxy_url(url)


def _https_supported(raw: Mapping[str, object], protocol: str) -> bool | None:
    for key in ("https_supported", "https", "ssl", "tls"):
        if key not in raw:
            continue
        value = _as_bool(raw[key])
        if value is not None:
            return value
    protocols = raw.get("protocols")
    if isinstance(protocols, str) and "https" in protocols.lower():
        return True
    if (
        isinstance(protocols, Sequence)
        and not isinstance(protocols, (str, bytes))
        and any(str(item).lower() == "https" for item in protocols)
    ):
        return True
    if protocol == "https":
        return True
    return None


def _uptime(raw: Mapping[str, object]) -> float | None:
    value = _first(raw, "uptime_percent", "uptime_pct", "uptime_24h", "uptime_7d", "uptime")
    number = _as_float(value)
    if number is None:
        return None
    if not any(key in raw for key in ("uptime_percent", "uptime_pct")) and 0 <= number <= 1:
        number *= 100
    return max(0.0, min(100.0, number))


def _latency(raw: Mapping[str, object]) -> float | None:
    number = _as_float(_first(raw, "latency_ms", "latency", "response_time_ms", "delay_ms"))
    if number is None or number < 0:
        return None
    return number


def _country(raw: Mapping[str, object]) -> str | None:
    geo = raw.get("geolocation")
    geo_map = geo if isinstance(geo, Mapping) else {}
    value = _first(raw, "country_code", "country", "countryCode") or _first(geo_map, "country", "country_code")
    return str(value).strip() if value is not None else None


def _city(raw: Mapping[str, object]) -> str | None:
    geo = raw.get("geolocation")
    geo_map = geo if isinstance(geo, Mapping) else {}
    value = _first(raw, "city") or _first(geo_map, "city")
    return str(value).strip() if value is not None else None


def normalise_candidate(raw: Mapping[str, object], *, source: str) -> ProxyCandidate | None:
    """Convert one upstream row into a candidate, or ``None`` if unusable."""
    if not isinstance(raw, Mapping):
        return None
    if raw.get("alive") is False or raw.get("is_alive") is False:
        return None
    status = str(raw.get("status", "")).strip().lower()
    if status in DEAD_STATUSES:
        return None

    protocol = _choose_protocol(raw)
    address = _address(raw)
    if address is None:
        return None
    url = _candidate_url(address, protocol)
    if url is None:
        return None

    return ProxyCandidate(
        url=url,
        protocol=urlsplit(url).scheme.lower(),
        source=source,
        country=_country(raw),
        city=_city(raw),
        anonymity=str(raw["anonymity"]).strip() if raw.get("anonymity") not in (None, "") else None,
        latency_ms=_latency(raw),
        uptime_percent=_uptime(raw),
        last_checked=_as_datetime(
            _first(raw, "last_checked", "last_verified_at", "last_alive_at", "checked_at", "updated_at")
        ),
        https_supported=_https_supported(raw, protocol),
        score=_as_float(raw.get("score")),
    )


def parse_proxy_text(
    text: str, *, source: str, default_protocol: str = "http", max_candidates: int | None = None
) -> list[ProxyCandidate]:
    """Parse a plain ``ip:port`` / ``scheme://ip:port`` list."""
    candidates: list[ProxyCandidate] = []
    for line in text.splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        protocol = (_normalise_protocol(urlsplit(value).scheme) or default_protocol) if "://" in value else default_protocol
        url = _candidate_url(value, protocol)
        if url is None:
            continue
        candidates.append(ProxyCandidate(url=url, protocol=urlsplit(url).scheme.lower(), source=source))
        if max_candidates is not None and len(candidates) >= max_candidates:
            break
    return candidates


def parse_source_payload(
    payload: object,
    *,
    source: str,
    default_protocol: str = "http",
    now: datetime | None = None,
    max_candidates: int | None = None,
) -> list[ProxyCandidate]:
    """Parse the JSON (or text) payload returned by one source."""
    if isinstance(payload, str):
        return parse_proxy_text(payload, source=source, default_protocol=default_protocol, max_candidates=max_candidates)

    rows: list[object]
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, Mapping):
        list_value = next(
            (payload[key] for key in ("proxies", "data", "results", "items") if isinstance(payload.get(key), list)), None
        )
        rows = list_value if list_value is not None else [payload]
    else:
        raise ProxyCatalogError(f"{source}: unsupported payload type {type(payload).__name__}")

    candidates: list[ProxyCandidate] = []
    now = now or datetime.now(UTC)
    for row in rows:
        if max_candidates is not None and len(candidates) >= max_candidates:
            break
        if isinstance(row, str):
            remaining = None if max_candidates is None else max_candidates - len(candidates)
            candidates.extend(
                parse_proxy_text(row, source=source, default_protocol=default_protocol, max_candidates=remaining)
            )
            continue
        if not isinstance(row, Mapping):
            continue
        try:
            for adapted in adapt_rows(row, source, now):
                candidate = normalise_candidate(adapted, source=source)
                if candidate is not None:
                    candidates.append(candidate)
                if max_candidates is not None and len(candidates) >= max_candidates:
                    break
        except (TypeError, ValueError, AttributeError):
            continue
    return candidates


def merge_candidates(candidates: Iterable[ProxyCandidate]) -> list[ProxyCandidate]:
    """Merge duplicate URLs while retaining the best metadata from each source."""
    merged: dict[str, ProxyCandidate] = {}
    for candidate in candidates:
        current = merged.get(candidate.url)
        if current is None:
            merged[candidate.url] = candidate
            continue

        sources = list(dict.fromkeys([*current.source.split("+"), *candidate.source.split("+")]))
        uptime_values = [value for value in (current.uptime_percent, candidate.uptime_percent) if value is not None]
        latency_values = [value for value in (current.latency_ms, candidate.latency_ms) if value is not None]
        checked_values = [value for value in (current.last_checked, candidate.last_checked) if value is not None]
        https_values = [value for value in (current.https_supported, candidate.https_supported) if value is not None]
        score_values = [value for value in (current.score, candidate.score) if value is not None]
        merged[candidate.url] = replace(
            current,
            source="+".join(sources),
            country=current.country or candidate.country,
            city=current.city or candidate.city,
            anonymity=current.anonymity or candidate.anonymity,
            latency_ms=min(latency_values) if latency_values else None,
            uptime_percent=max(uptime_values) if uptime_values else None,
            last_checked=max(checked_values) if checked_values else None,
            https_supported=any(https_values) if https_values else None,
            score=max(score_values) if score_values else None,
        )
    return list(merged.values())


async def load_public_catalog(
    sources: Sequence[str] = PUBLIC_SOURCES,
    *,
    timeout: float = 30,  # noqa: ASYNC109 - bounded source downloads
    cache_dir: Path | None = None,
    max_cache_age_hours: float = 24,
    max_response_bytes: int = 32 * 1024 * 1024,
    download_concurrency: int = 3,
    max_records: int = 10_000,
    source_overrides: Mapping[str, SourceDefinition] | None = None,
) -> CatalogLoadResult:
    """Load registered sources independently, preserving partial successes."""
    requested = list(dict.fromkeys(source.strip().lower() for source in sources if source.strip()))
    unknown = [source for source in requested if source not in SOURCE_REGISTRY]
    if unknown:
        raise ValueError(f"Unknown proxy source(s): {', '.join(unknown)}")
    if timeout <= 0 or download_concurrency < 1 or max_response_bytes < 1 or max_records < 1:
        raise ValueError("Source timeout, concurrency, byte and record limits must be positive")
    result = CatalogLoadResult()
    definitions = {**SOURCE_REGISTRY, **(source_overrides or {})}
    slots = asyncio.Semaphore(download_concurrency)
    work_slots = asyncio.Semaphore(download_concurrency)

    async def read_file(source: str, file: SourceFile) -> None:
        def parse(body: bytes, fetched_at: datetime) -> list[ProxyCandidate]:
            text = body.decode("utf-8-sig")
            payload = json.loads(text) if file.format == "json" else text
            return parse_source_payload(
                payload, source=source, default_protocol=file.protocol, now=fetched_at, max_candidates=max_records
            )

        try:
            candidates, warning = await load_document(
                file.url,
                parse,
                timeout=timeout,
                max_bytes=max_response_bytes,
                refresh_minutes=definitions[source].refresh_minutes,
                cache_dir=cache_dir,
                max_cache_age_hours=max_cache_age_hours,
                network_slots=slots,
            )
            result.candidates.extend(candidates)
            result.source_counts[source] = result.source_counts.get(source, 0) + len(candidates)
            if warning:
                result.errors[f"{source}:{file.protocol}"] = warning
        except Exception as exc:  # noqa: BLE001 - one file must not disable other sources or shards
            result.errors[f"{source}:{file.protocol}"] = str(exc) or type(exc).__name__
            logger.warning("Proxy source %s (%s) failed: %s", source, file.protocol, type(exc).__name__)

    async def load_file(source: str, file: SourceFile) -> None:
        async with work_slots:
            await read_file(source, file)

    await asyncio.gather(*(load_file(source, file) for source in requested for file in definitions[source].files))
    result.candidates = merge_candidates(result.candidates)
    return result


def filter_and_limit_candidates(
    candidates: Iterable[ProxyCandidate],
    *,
    min_uptime: float | None = None,
    max_latency_ms: float | None = None,
    max_age_minutes: float | None = None,
    preferred_age_minutes: float | None = None,
    require_https: bool | None = None,
    max_per_source: int | None = None,
    max_total: int | None = None,
    now: datetime | None = None,
) -> CatalogSelection:
    """Apply metadata thresholds, then cap each source and the total pool.

    Missing metadata is intentionally kept.  Those candidates simply sort after
    candidates with complete metadata and are verified by the real HTTPS probe.
    """
    min_uptime = 80 if min_uptime is None else min_uptime
    max_latency_ms = 1500 if max_latency_ms is None else max_latency_ms
    max_age_minutes = 1440 if max_age_minutes is None else max_age_minutes
    preferred_age_minutes = 15 if preferred_age_minutes is None else preferred_age_minutes
    require_https = True if require_https is None else require_https
    max_per_source = 1000 if max_per_source is None else max_per_source
    max_total = 2000 if max_total is None else max_total
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)

    def priority(candidate: ProxyCandidate) -> tuple:
        return _candidate_priority(candidate, now, preferred_age_minutes, require_https)

    selection = CatalogSelection()
    candidates_by_source: dict[str, list[ProxyCandidate]] = {}
    for candidate in candidates:
        rejection = _rejection_reason(candidate, now, min_uptime, max_latency_ms, max_age_minutes)
        if rejection:
            selection.rejected[rejection] += 1
            continue
        candidates_by_source.setdefault(candidate.source.split("+")[0], []).append(candidate)

    selection.candidates = _limit_candidates(candidates_by_source, max_per_source, max_total, priority)
    selection.source_counts = dict(Counter(candidate.source.split("+")[0] for candidate in selection.candidates))
    return selection


def _candidate_age(candidate: ProxyCandidate, now: datetime) -> timedelta | None:
    if candidate.last_checked is None:
        return None
    checked_at = candidate.last_checked
    if checked_at.tzinfo is None:
        checked_at = checked_at.replace(tzinfo=UTC)
    return now - checked_at


def _candidate_priority(
    candidate: ProxyCandidate, now: datetime, preferred_age_minutes: float, require_https: bool
) -> tuple:
    age = _candidate_age(candidate, now)
    preferred_penalty = 0
    if preferred_age_minutes > 0 and age is not None and age > timedelta(minutes=preferred_age_minutes):
        preferred_penalty = 1
        # Public https=false metadata does not prove SOCKS cannot reach HTTPS.
    https_penalty = 1 if require_https and candidate.https_supported is False else 0
    return (preferred_penalty, https_penalty, *candidate.priority)


def _rejection_reason(
    candidate: ProxyCandidate, now: datetime, min_uptime: float, max_latency_ms: float, max_age_minutes: float
) -> str | None:
    if candidate.protocol not in SUPPORTED_PROXY_SCHEMES:
        return "unsupported_protocol"
    if candidate.uptime_percent is not None and candidate.uptime_percent < min_uptime:
        return "low_uptime"
    if candidate.latency_ms is not None and candidate.latency_ms > max_latency_ms:
        return "high_latency"
    age = _candidate_age(candidate, now)
    if age is not None and max_age_minutes > 0 and age > timedelta(minutes=max_age_minutes):
        return "stale"
    return None


def _limit_candidates(
    candidates_by_source: dict[str, list[ProxyCandidate]],
    max_per_source: int,
    max_total: int,
    priority: Callable[[ProxyCandidate], tuple],
) -> list[ProxyCandidate]:
    capped_by_source: dict[str, list[ProxyCandidate]] = {}
    for source, source_candidates in candidates_by_source.items():
        source_candidates.sort(key=priority)
        limited_candidates = source_candidates[:max_per_source] if max_per_source > 0 else source_candidates
        capped_by_source[source] = limited_candidates

    if max_total <= 0:
        selected = [candidate for source_candidates in capped_by_source.values() for candidate in source_candidates]
        return sorted(selected, key=priority)

    # Reserve a fair share for each source, then fill by global priority.
    fair_share = max_total // max(1, len(capped_by_source))
    selected = []
    overflow: list[ProxyCandidate] = []
    for source_candidates in capped_by_source.values():
        selected.extend(source_candidates[:fair_share])
        overflow.extend(source_candidates[fair_share:])
    overflow.sort(key=priority)
    selected.extend(overflow[: max(0, max_total - len(selected))])
    selected.sort(key=priority)
    return selected


def candidate_as_dict(candidate: ProxyCandidate) -> dict[str, object]:
    """Serialise a candidate for the JSON report."""
    return {
        "url": candidate.url,
        "protocol": candidate.protocol,
        "source": candidate.source,
        "country": candidate.country,
        "city": candidate.city,
        "anonymity": candidate.anonymity,
        "latency_ms": candidate.latency_ms,
        "uptime_percent": candidate.uptime_percent,
        "last_checked": (candidate.last_checked.isoformat() if candidate.last_checked else None),
        "https_supported": candidate.https_supported,
        "score": candidate.score,
        "metadata_missing": candidate.metadata_missing,
    }
