"""Collect, filter, and verify proxies against the real SEPREC API.

The standalone collector has two explicit stages:

1. Download public proxy lists, normalise metadata, de-duplicate, apply cheap
   metadata filters, then make a real HTTPS request through each candidate.
2. Check the proxies that survived stage 1 against the exact catalog and
   publication-search endpoints used by the scraper, with retries and
   per-proxy success statistics.

Only combinations that pass stage 2 are written to SQLite.  The scraper can
then pair each proxy URL with the User-Agent and transport that actually worked.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
import tempfile
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from config import settings
from src.client.free_proxy_source import load_free_proxy_candidates
from src.client.network import NetworkBudget
from src.client.proxy_catalog import (
    PUBLIC_SOURCES,
    CatalogLoadResult,
    ProxyCandidate,
    candidate_as_dict,
    filter_and_limit_candidates,
    load_public_catalog,
    merge_candidates,
)
from src.client.proxy_pool import dead_proxy_path
from src.client.proxy_source import ProxySourceError, sync_proxy_source
from src.services.proxy_verifier import Stage1Result, Stage2Result, compatible_transports, verify_stage1, verify_stage2
from src.storage.proxy_database import ProxyDatabase, VerifiedProxy

PROGRESS_EVERY = 25
SUPPORTED_SCHEMES = {"http", "https", "socks4", "socks4a", "socks5", "socks5h"}
DEFAULT_USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
)
PROVIDER_CHOICES = ("public", "all", "both", "proxyscrape", "hproxy", "proxifly", "iplocate", "free-proxy")


def _write_status(text: str) -> None:
    sys.stdout.write(f"{text}\n")
    sys.stdout.flush()


@dataclass(frozen=True, slots=True)
class Stage1Success:
    candidate: ProxyCandidate
    transport: str
    user_agent: str
    latency_ms: float


@dataclass(frozen=True, slots=True)
class Stage1Failure:
    candidate: ProxyCandidate
    error_type: str
    detail: str


@dataclass(slots=True)
class Stage2Check:
    stage1: Stage1Success
    result: Stage2Result


def proxy_url(row: dict[str, str]) -> str | None:
    if row.get("alive", "true").strip().lower() == "false":
        return None
    protocol = row.get("protocol", "http").strip().lower()
    address = row.get("proxy", "").strip() or f"{row.get('ip', '')}:{row.get('port', '')}"
    url = address if "://" in address else f"{protocol}://{address}"
    try:
        parsed = urlsplit(url)
        if parsed.scheme not in SUPPORTED_SCHEMES or not parsed.hostname or not parsed.port:
            return None
    except ValueError:
        return None
    return url


def read_candidates(path: Path, limit: int | None = None) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            url = proxy_url(row)
            if url is None or url in seen:
                continue
            seen.add(url)
            candidates.append(url)
            if limit is not None and len(candidates) >= limit:
                break
    return candidates


def user_agents() -> list[str]:
    values = [settings.default_headers.get("User-Agent", ""), *DEFAULT_USER_AGENTS]
    return list(dict.fromkeys(value for value in values if value))


def clear_dead_proxies(db_path: Path) -> None:
    """Remove the scraper's dead-proxy sidecar after a successful DB refresh."""
    path = dead_proxy_path(db_path)
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except OSError as exc:
        _write_status(f"Warning: could not remove dead-proxy sidecar {path}: {exc}")
        return
    _write_status(f"Removed stale dead-proxy sidecar {path}")


def _candidate_from_url(url: str, source: str) -> ProxyCandidate | None:
    try:
        parsed = urlsplit(url)
    except ValueError:
        return None
    if parsed.scheme not in SUPPORTED_SCHEMES or not parsed.hostname or not parsed.port:
        return None
    return ProxyCandidate(url=url, protocol=parsed.scheme.lower(), source=source)


def _candidates_from_urls(urls: Iterable[str], source: str) -> list[ProxyCandidate]:
    result: list[ProxyCandidate] = []
    seen: set[str] = set()
    for url in urls:
        candidate = _candidate_from_url(url, source)
        if candidate is not None and candidate.url not in seen:
            seen.add(candidate.url)
            result.append(candidate)
    return result


def resolve_providers(provider: str) -> list[str]:
    """Resolve a CLI provider name into concrete source names."""
    value = provider.strip().lower()
    if value == "public":
        return list(PUBLIC_SOURCES)
    if value == "all":
        return [*PUBLIC_SOURCES, "iplocate", "free-proxy"]
    if value == "both":
        # Backwards-compatible alias for the pre-multi-source default.
        return ["iplocate", "free-proxy"]
    if value in {*PUBLIC_SOURCES, "iplocate", "free-proxy"}:
        return [value]
    raise ValueError(f"provider must be one of: {', '.join(PROVIDER_CHOICES)}")


async def _load_iplocate_candidates(csv_path: Path, source_url: str, *, only_source: bool) -> list[str]:
    if source_url:
        try:
            await asyncio.to_thread(
                sync_proxy_source, source_url=source_url, csv_path=csv_path,
                state_path=settings.proxy_state_path, refresh_minutes=settings.proxy_source_refresh_minutes,
                timeout=settings.proxy_source_timeout,
            )
        except ProxySourceError:
            cached = await asyncio.to_thread(csv_path.exists)
            if not cached and only_source:
                raise
            if not cached:
                _write_status("Proxy CSV source unavailable; continuing with other sources")
    if not await asyncio.to_thread(csv_path.exists):
        return []
    return await asyncio.to_thread(read_candidates, csv_path)


async def _load_candidate_sources(providers: list[str], csv_path: Path, source_url: str) -> CatalogLoadResult:
    catalog = CatalogLoadResult()
    public_sources = [name for name in providers if name in PUBLIC_SOURCES]
    if public_sources:
        public = await load_public_catalog(public_sources, timeout=settings.proxy_source_timeout)
        catalog.candidates.extend(public.candidates)
        catalog.source_counts.update(public.source_counts)
        catalog.errors.update(public.errors)

    if "iplocate" in providers:
        urls = await _load_iplocate_candidates(csv_path, source_url, only_source=providers == ["iplocate"])
        catalog.candidates.extend(_candidates_from_urls(urls, "iplocate"))
        if urls:
            catalog.source_counts["iplocate"] = len(urls)

    if "free-proxy" in providers:
        try:
            urls = await load_free_proxy_candidates(request_timeout=settings.proxy_source_timeout)
        except RuntimeError as exc:
            if providers == ["free-proxy"]:
                raise
            _write_status(f"free-proxy unavailable ({exc}); continuing with other sources")
            urls = []
        catalog.candidates.extend(_candidates_from_urls(urls, "free-proxy"))
        if urls:
            catalog.source_counts["free-proxy"] = len(urls)

    catalog.candidates = merge_candidates(catalog.candidates)
    return catalog


async def collect(
    *, csv_path: Path, db_path: Path,
    source_url: str = "", provider: str = "public", limit: int | None = None,
    concurrency: int = 50,
    timeout: float = 30,  # noqa: ASYNC109 - forwarded to stage 2
    transports: Sequence[str] = ("aiohttp", "curl_cffi"),
    requests_per_second: float | None = None, max_per_source: int | None = None, stage2_concurrency: int | None = None,
    stage1_check_url: str | None = None, stage1_timeout: float | None = None, stage1_connect_timeout: float | None = None,
    min_uptime: float | None = None, max_latency_ms: float | None = None, preferred_age_minutes: float | None = None,
    max_age_minutes: float | None = None, require_https: bool | None = None, stage2_attempts: int | None = None,
    stage2_retry_delay: float | None = None, stage2_max_retry_delay: float | None = None,
    min_success_rate: float | None = None, min_successes: int | None = None, report_path: Path | None = None,
    publish_after: int | None = None,
) -> dict:
    if concurrency < 1 or timeout <= 0:
        raise ValueError("concurrency must be positive and timeout must be > 0")
    if limit is not None and limit < 0:
        raise ValueError("limit must be >= 0 (0 = all)")
    if max_per_source is not None and max_per_source < 0:
        raise ValueError("max_per_source must be >= 0 (0 = all)")
    if requests_per_second is not None and requests_per_second < 0:
        raise ValueError("requests_per_second must be >= 0")
    if publish_after is not None and publish_after < 1:
        raise ValueError("publish_after must be >= 1")

    providers = resolve_providers(provider)
    catalog = await _load_candidate_sources(providers, csv_path, source_url)
    raw_candidates = catalog.candidates
    if not raw_candidates:
        raise ValueError("No proxy candidates found")

    max_total = settings.proxy_max_candidates if limit is None else limit
    max_per = settings.proxy_max_per_source if max_per_source is None else max_per_source
    selection = filter_and_limit_candidates(
        raw_candidates, min_uptime=min_uptime, max_latency_ms=max_latency_ms,
        max_age_minutes=max_age_minutes, preferred_age_minutes=preferred_age_minutes, require_https=require_https,
        max_per_source=max_per, max_total=max_total,
    )
    candidates = selection.candidates
    _write_status(
        f"Catalog: raw={len(raw_candidates)}, selected={len(candidates)}, "
        f"rejected={dict(selection.rejected)}, sources={selection.source_counts}"
    )

    stage1_successes: list[Stage1Success] = []
    stage1_failures: list[Stage1Failure] = []
    if candidates:
        stage1_successes, stage1_failures = await _stage1_scan(
            candidates, concurrency=concurrency, transports=list(transports),
            check_url=stage1_check_url, timeout=stage1_timeout, connect_timeout=stage1_connect_timeout,
        )
    _write_status(f"Stage 1: passed={len(stage1_successes)}, failed={len(stage1_failures)}")

    effective_min_successes = settings.proxy_stage2_min_successes if min_successes is None else min_successes
    effective_min_rate = settings.proxy_stage2_min_success_rate if min_success_rate is None else min_success_rate
    records_by_key: dict[tuple[str, str, str], VerifiedProxy] = {}
    verified_urls: set[str] = set()
    preview_published = False

    async def publish_if_ready(check: Stage2Check) -> None:
        nonlocal preview_published
        record = _record_from_check(check, effective_min_successes, effective_min_rate)
        if record is None:
            return
        records_by_key[(record.url, record.user_agent, record.transport)] = record
        verified_urls.add(record.url)
        if publish_after is None or preview_published or len(verified_urls) < publish_after:
            return
        async with ProxyDatabase(db_path) as database:
            await database.replace_all(list(records_by_key.values()))
        clear_dead_proxies(db_path)
        preview_published = True
        _write_status(f"Published {len(verified_urls)} verified proxies; scraping can start")

    stage2_checks: list[Stage2Check] = []
    if stage1_successes:
        stage2_checks = await _stage2_scan(
            stage1_successes, concurrency=stage2_concurrency or settings.proxy_stage2_concurrency, timeout=timeout,
            requests_per_second=(
                settings.proxy_stage2_requests_per_second if requests_per_second is None else requests_per_second
            ),
            attempts=stage2_attempts, retry_delay=stage2_retry_delay, max_retry_delay=stage2_max_retry_delay,
            on_check=publish_if_ready if publish_after is not None else None,
        )

    for check in stage2_checks:
        record = _record_from_check(check, effective_min_successes, effective_min_rate)
        if record is not None:
            records_by_key[(record.url, record.user_agent, record.transport)] = record
    records = list(records_by_key.values())

    database_replaced = bool(records)
    if records:
        async with ProxyDatabase(db_path) as database:
            await database.replace_all(records)
        clear_dead_proxies(db_path)
        _write_status(f"Replaced {db_path} with {len(records)} fresh verified combinations")
    else:
        print(f"No verified proxies found; keeping existing database at {db_path} unchanged", flush=True)

    report = _build_report(
        provider=provider, providers=providers, raw_candidates=raw_candidates,
        selection=selection, raw_source_counts=catalog.source_counts, catalog_errors=catalog.errors,
        stage1_successes=stage1_successes, stage1_failures=stage1_failures, stage2_checks=stage2_checks,
        records=records, db_path=db_path, database_replaced=database_replaced,
        csv_candidates=catalog.source_counts.get("iplocate", 0),
        free_proxy_candidates=catalog.source_counts.get("free-proxy", 0),
    )
    if report_path is not None:
        report["report_path"] = str(report_path)
        _write_report(report_path, report)
    return report


async def _stage1_scan(
    candidates: list[ProxyCandidate], *, concurrency: int,
    transports: list[str], check_url: str | None,
    timeout: float | None,  # noqa: ASYNC109 - forwarded to stage 1
    connect_timeout: float | None,
) -> tuple[list[Stage1Success], list[Stage1Failure]]:
    queue: asyncio.Queue[ProxyCandidate | None] = asyncio.Queue()
    for candidate in candidates:
        queue.put_nowait(candidate)

    successes: list[Stage1Success] = []
    failures: list[Stage1Failure] = []
    checked = 0
    active = 0
    lock = asyncio.Lock()
    budget = NetworkBudget(concurrency, 0)
    host_locks: dict[str, asyncio.Lock] = {}

    async def check(candidate: ProxyCandidate) -> tuple[list[Stage1Success], Stage1Failure | None]:
        compatible = compatible_transports(candidate.protocol, transports)
        if not compatible:
            return [], Stage1Failure(candidate, "unsupported_protocol", "no compatible HTTP transport")
        host = urlsplit(candidate.url).hostname or candidate.url
        found: list[Stage1Success] = []
        last_failure: Stage1Result | None = None
        async with host_locks.setdefault(host, asyncio.Lock()):
            for transport in compatible:
                for user_agent in user_agents():
                    result = await verify_stage1(
                        candidate.url, transport=transport, user_agent=user_agent,
                        check_url=check_url, timeout=timeout, connect_timeout=connect_timeout,
                        budget=budget,
                    )
                    if result.ok:
                        found.append(Stage1Success(candidate, transport, user_agent, result.latency_ms))
                        break
                    last_failure = result
        if found:
            return found, None
        if last_failure is None:
            return [], Stage1Failure(candidate, "unknown_error", "no stage-1 attempt was made")
        return [], Stage1Failure(candidate, last_failure.error_type, last_failure.detail)

    async def worker() -> None:
        nonlocal checked, active
        while True:
            candidate = await queue.get()
            try:
                if candidate is None:
                    return
                async with lock:
                    active += 1
                try:
                    found, failure = await check(candidate)
                finally:
                    async with lock:
                        active -= 1
                async with lock:
                    successes.extend(found)
                    if failure is not None:
                        failures.append(failure)
                    checked += 1
                    _report_progress(checked, len(candidates), len(successes), active, prefix="Stage 1")
            finally:
                queue.task_done()

    worker_count = min(concurrency, len(candidates))
    print(
        f"Starting stage 1: candidates={len(candidates)}, "
        f"concurrent_checks={worker_count}, "
        f"transports={','.join(transports)}",
        flush=True,
    )
    tasks = [asyncio.create_task(worker()) for _ in range(worker_count)]
    try:
        for _ in tasks:
            queue.put_nowait(None)
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return successes, failures


async def _stage2_scan(
    stage1_successes: list[Stage1Success], *, concurrency: int,
    timeout: float,  # noqa: ASYNC109 - forwarded to stage 2
    requests_per_second: float,
    attempts: int | None, retry_delay: float | None, max_retry_delay: float | None,
    on_check: Callable[[Stage2Check], Awaitable[None]] | None = None,
) -> list[Stage2Check]:
    queue: asyncio.Queue[Stage1Success | None] = asyncio.Queue()
    for success in stage1_successes:
        queue.put_nowait(success)

    checks: list[Stage2Check] = []
    checked = 0
    active = 0
    lock = asyncio.Lock()
    budget = NetworkBudget(concurrency, requests_per_second)
    host_locks: dict[str, asyncio.Lock] = {}

    async def worker() -> None:
        nonlocal checked, active
        while True:
            success = await queue.get()
            try:
                if success is None:
                    return
                async with lock:
                    active += 1
                try:
                    host = urlsplit(success.candidate.url).hostname or success.candidate.url
                    async with host_locks.setdefault(host, asyncio.Lock()):
                        result = await verify_stage2(
                            success.candidate.url, transport=success.transport, user_agent=success.user_agent,
                            attempts=attempts, timeout=timeout,
                            connect_timeout=min(timeout, settings.proxy_stage2_connect_timeout), retry_delay=retry_delay,
                            max_retry_delay=max_retry_delay, budget=budget,
                        )
                finally:
                    async with lock:
                        active -= 1
                check = Stage2Check(success, result)
                async with lock:
                    checks.append(check)
                    if on_check is not None:
                        await on_check(check)
                    checked += 1
                    _report_progress(
                        checked, len(stage1_successes), sum(1 for item in checks if item.result.ok), active, prefix="Stage 2"
                    )
            finally:
                queue.task_done()

    worker_count = min(concurrency, len(stage1_successes))
    print(
        f"Starting stage 2: combinations={len(stage1_successes)}, "
        f"concurrent_checks={worker_count}, "
        f"attempts={attempts or settings.proxy_stage2_attempts}",
        flush=True,
    )
    tasks = [asyncio.create_task(worker()) for _ in range(worker_count)]
    try:
        for _ in tasks:
            queue.put_nowait(None)
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return checks


def _record_from_check(check: Stage2Check, min_successes: int, min_success_rate: float) -> VerifiedProxy | None:
    result = check.result
    stats = result.stats
    if not result.ok or stats.successful_requests < min_successes:
        return None
    if stats.success_rate < min_success_rate:
        return None
    candidate = check.stage1.candidate
    checked_at = datetime.now(UTC).isoformat()
    return VerifiedProxy(
        url=candidate.url, user_agent=check.stage1.user_agent, transport=check.stage1.transport,
        latency_ms=stats.average_latency_ms, checked_at=checked_at, protocol=candidate.protocol,
        source=candidate.source, country=candidate.country, city=candidate.city,
        anonymity=candidate.anonymity, uptime_percent=candidate.uptime_percent, source_latency_ms=candidate.latency_ms,
        last_checked=(candidate.last_checked.isoformat() if candidate.last_checked else None),
        https_supported=candidate.https_supported, successful_requests=stats.successful_requests,
        failed_requests=stats.failed_requests, success_rate=round(stats.success_rate, 4),
        average_latency_ms=stats.average_latency_ms, last_success=stats.last_success, last_failure=stats.last_failure,
        consecutive_failures=stats.consecutive_failures, error_types=dict(stats.error_types),
    )


def _build_report(
    *, provider: str, providers: list[str],
    raw_candidates: list[ProxyCandidate], selection, raw_source_counts: dict[str, int],
    catalog_errors: dict[str, str], stage1_successes: list[Stage1Success], stage1_failures: list[Stage1Failure],
    stage2_checks: list[Stage2Check], records: list[VerifiedProxy], db_path: Path,
    database_replaced: bool, csv_candidates: int, free_proxy_candidates: int,
) -> dict:
    verified_keys = {(record.url, record.user_agent, record.transport) for record in records}
    stage2_error_counts: Counter[str] = Counter()
    for check in stage2_checks:
        stage2_error_counts.update(check.result.stats.error_types)
    stage2_results = [
        {
            "candidate": candidate_as_dict(check.stage1.candidate),
            "transport": check.stage1.transport,
            "user_agent": check.stage1.user_agent,
            "stage1_latency_ms": check.stage1.latency_ms,
            "ok": check.result.ok,
            "verified": (check.stage1.candidate.url, check.stage1.user_agent, check.stage1.transport) in verified_keys,
            "error_type": check.result.error_type,
            "detail": check.result.detail,
            "stats": check.result.stats.as_dict(),
        }
        for check in stage2_checks
    ]
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "provider": provider,
        "providers": providers,
        "catalog": {
            "raw_candidates": len(raw_candidates),
            "selected_candidates": len(selection.candidates),
            "raw_source_counts": raw_source_counts,
            "source_counts": selection.source_counts,
            "rejected": dict(selection.rejected),
            "errors": catalog_errors,
            "csv_candidates": csv_candidates,
            "free_proxy_candidates": free_proxy_candidates,
        },
        "stage1": {
            "passed": len(stage1_successes),
            "failed": len(stage1_failures),
            "error_types": dict(Counter(failure.error_type for failure in stage1_failures)),
            "successes": [
                {
                    "candidate": candidate_as_dict(success.candidate),
                    "transport": success.transport,
                    "user_agent": success.user_agent,
                    "latency_ms": success.latency_ms,
                }
                for success in stage1_successes
            ],
            "failures": [
                {
                    "candidate": candidate_as_dict(failure.candidate),
                    "error_type": failure.error_type,
                    "detail": failure.detail,
                }
                for failure in stage1_failures
            ],
        },
        "stage2": {
            "checked": len(stage2_checks),
            "verified": len(records),
            "error_types": dict(stage2_error_counts),
            "results": stage2_results,
        },
        "candidates": len(selection.candidates),
        "csv_candidates": csv_candidates,
        "free_proxy_candidates": free_proxy_candidates,
        "source_counts": selection.source_counts,
        "stage1_passed": len(stage1_successes),
        "stage1_failed": len(stage1_failures),
        "stage2_checked": len(stage2_checks),
        "verified_combinations": len(records),
        "verified_proxies": len({record.url for record in records}),
        "by_transport": dict(Counter(record.transport for record in records)),
        "database": str(db_path),
        "database_replaced": database_replaced,
    }


def _write_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, ensure_ascii=False, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def _report_progress(checked: int, total: int, verified: int, active: int, *, prefix: str = "Checked") -> None:
    if checked % PROGRESS_EVERY and checked != total:
        return
    print(f"{prefix} {checked}/{total}, verified combinations={verified}, active_checks={active}", flush=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=settings.proxy_list_path)
    parser.add_argument("--db", type=Path, default=settings.proxy_db_path)
    parser.add_argument(
        "--publish-after", type=int, metavar="N", help="Publish the first N distinct verified proxies before the scan ends."
    )
    parser.add_argument(
        "--provider", choices=list(PROVIDER_CHOICES), default="public",
        help=(
            "Candidate source: public (ProxyScrape/HProxy/Proxifly, default), all, "
            "both (iplocate + free-proxy), or one named source."
        ),
    )
    parser.add_argument(
        "--free-proxy", action="store_const", const="free-proxy", dest="provider", help="Shortcut for --provider free-proxy."
    )
    parser.add_argument("--source", default=settings.proxy_source_url)
    parser.add_argument(
        "--limit", type=int, default=None,
        help=("Total candidate cap after filtering; 0 = all (default: PROXY_MAX_CANDIDATES)."),
    )
    parser.add_argument(
        "--max-per-source", type=int, default=None,
        help="Per-source cap after filtering; 0 = all (default: PROXY_MAX_PER_SOURCE).",
    )
    parser.add_argument(
        "--concurrency", type=int, default=settings.proxy_check_concurrency,
        help="Stage-1 concurrent proxy checks (default: %(default)s).",
    )
    parser.add_argument(
        "--stage2-concurrency", type=int, default=settings.proxy_stage2_concurrency,
        help="Stage-2 concurrent target checks (default: %(default)s).",
    )
    parser.add_argument(
        "--timeout", type=float, default=settings.proxy_stage2_timeout,
        help="Stage-2 target request timeout (default: %(default)s).",
    )
    parser.add_argument(
        "--stage1-timeout", type=float, default=settings.proxy_stage1_timeout,
        help="Stage-1 HTTPS check total timeout (default: %(default)s).",
    )
    parser.add_argument(
        "--stage1-connect-timeout", type=float, default=settings.proxy_stage1_connect_timeout,
        help="Stage-1 connect timeout (default: %(default)s).",
    )
    parser.add_argument(
        "--stage1-check-url", default=settings.proxy_stage1_check_url,
        help="HTTPS endpoint used for the stage-1 reachability check.",
    )
    parser.add_argument(
        "--min-uptime", type=float, default=settings.proxy_stage1_min_uptime,
        help=("Reject candidates with measured uptime below this value (default: %(default)s)."),
    )
    parser.add_argument(
        "--max-latency-ms", type=float, default=settings.proxy_stage1_max_latency_ms,
        help=("Reject candidates with measured latency above this value (default: %(default)s)."),
    )
    parser.add_argument(
        "--preferred-age-minutes", type=float, default=settings.proxy_stage1_preferred_age_minutes,
        help=("Prefer candidates checked within this many minutes (default: %(default)s)."),
    )
    parser.add_argument(
        "--max-age-minutes", type=float, default=settings.proxy_stage1_max_age_minutes,
        help=("Hard-reject candidates older than this many minutes (default: %(default)s)."),
    )
    parser.add_argument(
        "--require-https", action=argparse.BooleanOptionalAction, default=settings.proxy_stage1_require_https,
        help="Reject candidates explicitly marked as not supporting HTTPS.",
    )
    parser.add_argument(
        "--requests-per-second", type=float, default=settings.proxy_stage2_requests_per_second,
        help="Stage-2 target request start rate; 0 disables pacing.",
    )
    parser.add_argument(
        "--stage2-attempts", type=int, default=settings.proxy_stage2_attempts,
        help="Stage-2 attempts per proxy combination (default: %(default)s).",
    )
    parser.add_argument(
        "--stage2-retry-delay", type=float, default=settings.proxy_stage2_retry_delay,
        help="Base stage-2 retry delay in seconds (default: %(default)s).",
    )
    parser.add_argument(
        "--stage2-max-retry-delay", type=float, default=settings.proxy_stage2_max_retry_delay,
        help="Maximum stage-2 retry delay in seconds (default: %(default)s).",
    )
    parser.add_argument(
        "--min-success-rate", type=float, default=settings.proxy_stage2_min_success_rate,
        help="Minimum stage-2 success rate to store a proxy (default: %(default)s).",
    )
    parser.add_argument(
        "--min-successes", type=int, default=settings.proxy_stage2_min_successes,
        help=("Minimum successful stage-2 attempts to store a proxy (default: %(default)s)."),
    )
    parser.add_argument(
        "--report", type=Path, default=settings.proxy_report_path, help="JSON report path (default: %(default)s)."
    )
    parser.add_argument(
        "--transport", choices=["both", "aiohttp", "curl_cffi"], default="both",
        help="Verify one transport or both (default: both).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be >= 0 (0 = all)")
    if args.max_per_source is not None and args.max_per_source < 0:
        parser.error("--max-per-source must be >= 0 (0 = all)")
    if args.requests_per_second < 0:
        parser.error("--requests-per-second must be non-negative")
    if args.concurrency < 1 or args.stage2_concurrency < 1:
        parser.error("--concurrency and --stage2-concurrency must be >= 1")
    if args.timeout <= 0 or args.stage1_timeout <= 0 or args.stage1_connect_timeout <= 0:
        parser.error("timeouts must be > 0")
    if args.stage1_connect_timeout > args.stage1_timeout:
        parser.error("--stage1-connect-timeout must not exceed --stage1-timeout")
    if not 0 <= args.min_success_rate <= 1:
        parser.error("--min-success-rate must be between 0 and 1")
    if args.stage2_attempts < 1 or args.min_successes < 0:
        parser.error("--stage2-attempts must be >= 1 and --min-successes must be >= 0")
    if args.publish_after is not None and args.publish_after < 1:
        parser.error("--publish-after must be >= 1")
    transports = ("aiohttp", "curl_cffi") if args.transport == "both" else (args.transport,)
    try:
        report = asyncio.run(
            collect(
                csv_path=args.csv, db_path=args.db, source_url=args.source,
                provider=args.provider, limit=args.limit, concurrency=args.concurrency,
                timeout=args.timeout, transports=transports, requests_per_second=args.requests_per_second,
                max_per_source=args.max_per_source, stage2_concurrency=args.stage2_concurrency,
                stage1_check_url=args.stage1_check_url, stage1_timeout=args.stage1_timeout,
                stage1_connect_timeout=args.stage1_connect_timeout, min_uptime=args.min_uptime,
                max_latency_ms=args.max_latency_ms, preferred_age_minutes=args.preferred_age_minutes,
                max_age_minutes=args.max_age_minutes, require_https=args.require_https, stage2_attempts=args.stage2_attempts,
                stage2_retry_delay=args.stage2_retry_delay, stage2_max_retry_delay=args.stage2_max_retry_delay,
                min_success_rate=args.min_success_rate, min_successes=args.min_successes, report_path=args.report,
                publish_after=args.publish_after,
            )
        )
    except (RuntimeError, ValueError, ProxySourceError) as exc:
        parser.error(str(exc))
    summary_keys = (
        "provider",
        "candidates",
        "stage1_passed",
        "stage1_failed",
        "stage2_checked",
        "verified_combinations",
        "verified_proxies",
        "by_transport",
        "database",
        "database_replaced",
        "report_path",
    )
    summary = {key: report[key] for key in summary_keys if key in report}
    print(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True), flush=True)
    return 0 if report["verified_combinations"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
