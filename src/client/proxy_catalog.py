"""Download, normalise, de-duplicate, and prioritise public proxy candidates.

The collector deliberately keeps this layer independent from the target API:
it only turns the upstream JSON/text lists into :class:`ProxyCandidate` objects
and applies the cheap metadata filters.  A real HTTPS request is performed by
the verifier in :mod:`src.services.proxy_verifier`.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

from config import settings
from src.client.aiohttp_session import AIOHTTP_AVAILABLE, AiohttpSession
from src.client.network import TimeoutPolicy
from src.utils.logger import setup_logger

logger = setup_logger("proxy_catalog")

PROXYSCRAPE_URL = "https://cdn.jsdelivr.net/gh/ProxyScrape/free-proxy-list@main/proxies/all/data.json"
HPROXY_URL = "https://hproxy.com/api/proxy-list?format=json"
# Proxifly's all-proxies JSON is larger than jsDelivr's 20 MB file limit.
PROXIFLY_URL = "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.json"
SOURCE_URLS = {"proxyscrape": PROXYSCRAPE_URL, "hproxy": HPROXY_URL, "proxifly": PROXIFLY_URL}
PUBLIC_SOURCES = tuple(SOURCE_URLS)
SUPPORTED_SCHEMES = {"http", "https", "socks4", "socks4a", "socks5", "socks5h"}
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
        return float(text)
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
        return "socks5"
    if protocol in {"socks4a", "socks4"}:
        return "socks4"
    if protocol in {"http", "https"}:
        return protocol
    return None


def _choose_protocol(raw: Mapping[str, object]) -> str:
    explicit = _normalise_protocol(raw.get("protocol"))
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
    try:
        parsed = urlsplit(url)
    except ValueError:
        return None
    scheme = parsed.scheme.lower()
    if scheme not in SUPPORTED_SCHEMES or not parsed.hostname or not parsed.port:
        return None
    netloc = parsed.netloc
    return f"{scheme}://{netloc}"


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
    is_protocol_list = isinstance(protocols, Sequence) and not isinstance(protocols, (str, bytes))
    if is_protocol_list and any(str(item).lower() == "https" for item in protocols):
        return True
    if protocol == "https":
        return True
    return None


def _uptime(raw: Mapping[str, object]) -> float | None:
    value = _first(raw, "uptime_percent", "uptime_pct", "uptime_24h", "uptime_7d", "uptime")
    number = _as_float(value)
    if number is None:
        return None
    if 0 <= number <= 1:
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
        url=url, protocol=urlsplit(url).scheme.lower(), source=source,
        country=_country(raw), city=_city(raw),
        anonymity=str(raw["anonymity"]).strip() if raw.get("anonymity") not in (None, "") else None,
        latency_ms=_latency(raw), uptime_percent=_uptime(raw),
        last_checked=_as_datetime(
            _first(raw, "last_checked", "last_verified_at", "last_alive_at", "checked_at", "updated_at")
        ),
        https_supported=_https_supported(raw, protocol), score=_as_float(raw.get("score")),
    )


def parse_proxy_text(text: str, *, source: str) -> list[ProxyCandidate]:
    """Parse a plain ``ip:port`` / ``scheme://ip:port`` list."""
    candidates: list[ProxyCandidate] = []
    for line in text.splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        if "://" in value:
            protocol = _normalise_protocol(urlsplit(value).scheme) or "http"
        else:
            protocol = "http"
        url = _candidate_url(value, protocol)
        if url is None:
            continue
        candidates.append(ProxyCandidate(url=url, protocol=urlsplit(url).scheme.lower(), source=source))
    return candidates


def parse_source_payload(payload: object, *, source: str) -> list[ProxyCandidate]:
    """Parse the JSON (or text) payload returned by one source."""
    if isinstance(payload, str):
        return parse_proxy_text(payload, source=source)

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
    for row in rows:
        if isinstance(row, str):
            candidates.extend(parse_proxy_text(row, source=source))
            continue
        candidate = normalise_candidate(row, source=source)
        if candidate is not None:
            candidates.append(candidate)
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
            current, source="+".join(sources), country=current.country or candidate.country,
            city=current.city or candidate.city, anonymity=current.anonymity or candidate.anonymity,
            latency_ms=min(latency_values) if latency_values else None,
            uptime_percent=max(uptime_values) if uptime_values else None,
            last_checked=max(checked_values) if checked_values else None,
            https_supported=any(https_values) if https_values else None, score=max(score_values) if score_values else None,
        )
    return list(merged.values())


async def _fetch_source(source: str, timeout: float) -> object:  # noqa: ASYNC109 - network deadline
    url = SOURCE_URLS[source]
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json, text/plain, */*"}
    policy = TimeoutPolicy(total=timeout, connect=min(10.0, timeout), sock_connect=min(10.0, timeout), sock_read=timeout)
    last_error: Exception | None = None
    if AIOHTTP_AVAILABLE:
        session = None
        try:
            session = AiohttpSession(headers=headers, timeout=policy, max_clients=2)
            response = await asyncio.wait_for(session.request("GET", url), timeout + 5)
            return _source_response_payload(response)
        except Exception as exc:  # noqa: BLE001 - curl_cffi is the fallback
            last_error = exc
        finally:
            if session is not None:
                try:
                    await session.close()
                except Exception:  # noqa: BLE001
                    pass

    try:
        from curl_cffi.requests import AsyncSession

        session = AsyncSession(headers=headers, timeout=(min(10.0, timeout), timeout), trust_env=False, max_clients=2)
        try:
            response = await asyncio.wait_for(session.get(url), timeout + 5)
            return _source_response_payload(response)
        finally:
            await session.close()
    except Exception as exc:  # noqa: BLE001
        raise ProxyCatalogError(f"{source}: {exc}") from (last_error or exc)


def _source_response_payload(response) -> object:
    if not 200 <= response.status_code < 300:
        raise ProxyCatalogError(f"HTTP {response.status_code}")
    try:
        return response.json()
    except (ValueError, UnicodeError):
        return response.text


async def load_public_catalog(
    sources: Sequence[str] = PUBLIC_SOURCES, *,
    timeout: float | None = None,  # noqa: ASYNC109 - network deadline
) -> CatalogLoadResult:
    """Download all requested sources concurrently and return raw candidates."""
    requested = list(dict.fromkeys(source.strip().lower() for source in sources if source.strip()))
    unknown = [source for source in requested if source not in SOURCE_URLS]
    if unknown:
        raise ValueError(f"Unknown proxy source(s): {', '.join(unknown)}")

    source_timeout = timeout if timeout is not None else settings.proxy_source_timeout
    result = CatalogLoadResult()
    payloads = await asyncio.gather(*(_fetch_source(source, source_timeout) for source in requested), return_exceptions=True)
    for source, payload in zip(requested, payloads, strict=True):
        if isinstance(payload, BaseException):
            message = str(payload) or type(payload).__name__
            result.errors[source] = message
            logger.warning("Proxy source %s failed: %s", source, message)
            continue
        try:
            candidates = parse_source_payload(payload, source=source)
        except ProxyCatalogError as exc:
            result.errors[source] = str(exc)
            logger.warning("Proxy source %s could not be parsed: %s", source, exc)
            continue
        result.source_counts[source] = len(candidates)
        result.candidates.extend(candidates)
    result.candidates = merge_candidates(result.candidates)
    return result


def filter_and_limit_candidates(
    candidates: Iterable[ProxyCandidate], *, min_uptime: float | None = None,
    max_latency_ms: float | None = None, max_age_minutes: float | None = None, preferred_age_minutes: float | None = None,
    require_https: bool | None = None, max_per_source: int | None = None, max_total: int | None = None,
    now: datetime | None = None,
) -> CatalogSelection:
    """Apply metadata thresholds, then cap each source and the total pool.

    Missing metadata is intentionally kept.  Those candidates simply sort after
    candidates with complete metadata and are verified by the real HTTPS probe.
    """
    min_uptime = settings.proxy_stage1_min_uptime if min_uptime is None else min_uptime
    max_latency_ms = settings.proxy_stage1_max_latency_ms if max_latency_ms is None else max_latency_ms
    max_age_minutes = settings.proxy_stage1_max_age_minutes if max_age_minutes is None else max_age_minutes
    preferred_age_minutes = (
        settings.proxy_stage1_preferred_age_minutes if preferred_age_minutes is None else preferred_age_minutes
    )
    require_https = settings.proxy_stage1_require_https if require_https is None else require_https
    max_per_source = settings.proxy_max_per_source if max_per_source is None else max_per_source
    max_total = settings.proxy_max_candidates if max_total is None else max_total
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
    if candidate.protocol not in SUPPORTED_SCHEMES:
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
    candidates_by_source: dict[str, list[ProxyCandidate]], max_per_source: int, max_total: int,
    priority: Callable[[ProxyCandidate], tuple],
) -> list[ProxyCandidate]:
    capped_by_source: dict[str, list[ProxyCandidate]] = {}
    for source, source_candidates in candidates_by_source.items():
        source_candidates.sort(key=priority)
        if max_per_source > 0:
            source_candidates = source_candidates[:max_per_source]
        capped_by_source[source] = source_candidates

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
