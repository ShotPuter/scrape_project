"""Collect, filter, and verify proxies against an injected target profile.

The standalone collector has two explicit stages:

1. Download public proxy lists, normalise metadata, de-duplicate, apply cheap
   metadata filters, then make a real HTTPS request through each candidate.
2. Check the survivors against the caller's request profile, with retries
   and per-proxy success statistics.

Only combinations that pass stage 2 are written to SQLite.  The scraper can
then pair each proxy URL with the User-Agent and transport that actually worked.
"""

from __future__ import annotations

import asyncio
import csv
import sys
from collections import Counter
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

from src.network.budget import NetworkBudget
from src.proxy.checker import Stage1Result, Stage2Result, compatible_transports, verify_stage1, verify_stage2
from src.proxy.config import CollectorConfig
from src.proxy.pool import dead_proxy_path
from src.proxy.probes import TargetProbe
from src.proxy.repository import ProxyDatabase, VerifiedProxy
from src.proxy.sources.catalog import (
    PUBLIC_SOURCES,
    CatalogLoadResult,
    ProxyCandidate,
    candidate_as_dict,
    filter_and_limit_candidates,
    load_public_catalog,
    merge_candidates,
)
from src.proxy.sources.free_proxy import load_free_proxy_candidates
from src.proxy.sources.iplocate import ProxySourceError, sync_proxy_source
from src.proxy.sources.paid import load_paid_candidates
from src.proxy.sources.registry import SOURCE_REGISTRY, SourceDefinition, SourceFile, resolve_sources
from src.proxy.sources.webshare import WebshareError, load_webshare_candidates
from src.proxy.user_agents import MAX_USER_AGENT_LIMIT, USER_AGENT_RETRY_ERRORS, generate_user_agents
from src.utils.atomic import atomic_write_json
from src.utils.proxy_url import parse_proxy_url, proxy_host, proxy_url_from_row, redact_proxy_credentials

PROGRESS_EVERY = 25
PROVIDER_CHOICES = ("public", "all", *SOURCE_REGISTRY, "free-proxy", "paid", "webshare")


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
    transport: str | None = None
    user_agent: str | None = None

    @property
    def verified_transport(self) -> str:
        return self.transport or self.stage1.transport

    @property
    def verified_user_agent(self) -> str:
        return self.user_agent or self.stage1.user_agent


def read_candidates(path: Path, limit: int | None = None) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("alive", "true").strip().lower() == "false":
                continue
            url = proxy_url_from_row(row)
            if url is None or url in seen:
                continue
            seen.add(url)
            candidates.append(url)
            if limit is not None and len(candidates) >= limit:
                break
    return candidates


def user_agents(config: CollectorConfig) -> list[str]:
    return generate_user_agents(
        limit=config.proxy_user_agent_limit,
        preferred=config.default_headers.get("User-Agent"),
        profile=config.browser_profile,
    )


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
    address = parse_proxy_url(url)
    if address is None:
        return None
    return ProxyCandidate(url=address.url, protocol=address.scheme, source=source)


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
    return resolve_sources(provider)


async def _load_iplocate_candidates(
    csv_path: Path, source_url: str, *, only_source: bool, config: CollectorConfig
) -> list[str]:
    if source_url:
        try:
            await asyncio.to_thread(
                sync_proxy_source,
                source_url=source_url,
                csv_path=csv_path,
                state_path=config.proxy_state_path,
                refresh_minutes=config.proxy_source_refresh_minutes,
                timeout=config.proxy_source_timeout,
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


async def _load_candidate_sources(
    providers: list[str], csv_path: Path, source_url: str, config: CollectorConfig
) -> CatalogLoadResult:
    catalog = CatalogLoadResult()
    if "webshare" in providers:
        try:
            private = await load_webshare_candidates(
                config.webshare_api_token,
                mode=config.webshare_proxy_mode,
                timeout=config.proxy_source_timeout,
                max_bytes=config.proxy_source_max_response_bytes,
                max_records=config.proxy_source_max_records,
            )
        except Exception as exc:  # noqa: BLE001 - provider outages must leave public reserves available
            reason = str(exc) if isinstance(exc, WebshareError) else type(exc).__name__
            catalog.errors["webshare"] = f"Webshare inventory unavailable: {reason}"
            _write_status(catalog.errors["webshare"])
        else:
            catalog.candidates.extend(private)
            catalog.source_counts["webshare"] = len(private)
    if "paid" in providers:
        paid = await asyncio.to_thread(load_paid_candidates, config.proxy_paid_file)
        catalog.candidates.extend(paid)
        catalog.source_counts["paid"] = len(paid)
    public_sources = [name for name in providers if name in PUBLIC_SOURCES and (name != "iplocate" or source_url)]
    if public_sources:
        public = await load_public_catalog(
            public_sources,
            timeout=config.proxy_source_timeout,
            cache_dir=config.proxy_catalog_cache_dir,
            max_cache_age_hours=config.proxy_source_cache_max_age_hours,
            max_response_bytes=config.proxy_source_max_response_bytes,
            download_concurrency=config.proxy_source_download_concurrency,
            max_records=config.proxy_source_max_records,
            source_overrides={
                "iplocate": SourceDefinition((SourceFile(source_url, "text"),), config.proxy_source_refresh_minutes)
            }
            if "iplocate" in public_sources
            else None,
        )
        catalog.candidates.extend(public.candidates)
        catalog.source_counts.update(public.source_counts)
        catalog.errors.update(public.errors)

    if "iplocate" in providers and not source_url:
        urls = await _load_iplocate_candidates(csv_path, source_url, only_source=providers == ["iplocate"], config=config)
        catalog.candidates.extend(_candidates_from_urls(urls, "iplocate"))
        if urls:
            catalog.source_counts["iplocate"] = len(urls)

    if "free-proxy" in providers:
        try:
            urls = await load_free_proxy_candidates(request_timeout=config.proxy_source_timeout)
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
    *,
    config: CollectorConfig,
    target: TargetProbe,
    csv_path: Path,
    db_path: Path,
    source_url: str = "",
    provider: str = "public",
    sources: str | None = None,
    limit: int | None = None,
    concurrency: int = 50,
    timeout: float = 30,  # noqa: ASYNC109 - forwarded to stage 2
    transports: Sequence[str] = ("aiohttp", "curl_cffi"),
    requests_per_second: float | None = None,
    max_per_source: int | None = None,
    stage2_concurrency: int | None = None,
    stage1_check_url: str | None = None,
    stage1_timeout: float | None = None,
    stage1_connect_timeout: float | None = None,
    min_uptime: float | None = None,
    max_latency_ms: float | None = None,
    preferred_age_minutes: float | None = None,
    max_age_minutes: float | None = None,
    require_https: bool | None = None,
    stage2_attempts: int | None = None,
    stage2_retry_delay: float | None = None,
    stage2_max_retry_delay: float | None = None,
    min_success_rate: float | None = None,
    min_successes: int | None = None,
    report_path: Path | None = None,
    publish_after: int | None = None,
    user_agent_limit: int | None = None,
) -> dict:
    if user_agent_limit is not None:
        config = replace(config, proxy_user_agent_limit=user_agent_limit)
    if not 1 <= config.proxy_user_agent_limit <= MAX_USER_AGENT_LIMIT:
        raise ValueError(f"User-Agent limit must be between 1 and {MAX_USER_AGENT_LIMIT}")
    publish_after = config.proxy_min_ready if publish_after is None else publish_after
    if publish_after < 1:
        raise ValueError("publish_after must be >= 1")
    db_path = await asyncio.to_thread(lambda: db_path.expanduser().resolve())
    _write_status(
        f"Proxy database: {db_path}; publish batch={publish_after}; "
        f"min_successes={config.proxy_stage2_min_successes if min_successes is None else min_successes}; "
        f"min_success_rate={config.proxy_stage2_min_success_rate if min_success_rate is None else min_success_rate}"
    )
    selection_names = (
        sources if sources is not None else ((config.proxy_sources if provider == "public" else "") or provider)
    )
    providers = resolve_sources(selection_names)
    if config.webshare_api_token.strip() and "webshare" not in providers:
        providers.insert(0, "webshare")
    catalog = await _load_candidate_sources(providers, csv_path, source_url, config)
    raw_candidates = catalog.candidates
    if not raw_candidates:
        raise ValueError("No proxy candidates found")

    max_total = config.proxy_max_candidates if limit is None else limit
    max_per = config.proxy_max_per_source if max_per_source is None else max_per_source
    selection = filter_and_limit_candidates(
        raw_candidates,
        min_uptime=config.proxy_stage1_min_uptime if min_uptime is None else min_uptime,
        max_latency_ms=config.proxy_stage1_max_latency_ms if max_latency_ms is None else max_latency_ms,
        max_age_minutes=config.proxy_stage1_max_age_minutes if max_age_minutes is None else max_age_minutes,
        preferred_age_minutes=(
            config.proxy_stage1_preferred_age_minutes if preferred_age_minutes is None else preferred_age_minutes
        ),
        require_https=config.proxy_stage1_require_https if require_https is None else require_https,
        max_per_source=max_per,
        max_total=max_total,
    )
    candidates = selection.candidates
    _write_status(
        f"Catalog: raw={len(raw_candidates)}, selected={len(candidates)}, "
        f"rejected={dict(selection.rejected)}, sources={selection.source_counts}"
    )

    stage1_successes: list[Stage1Success] = []
    stage1_failures: list[Stage1Failure] = []
    stage2_slots = stage2_concurrency or config.proxy_stage2_concurrency
    queue: asyncio.Queue[Stage1Success | None] = asyncio.Queue(maxsize=stage2_slots * 2)

    async def produce() -> None:
        nonlocal stage1_successes, stage1_failures
        stage1_successes, stage1_failures = await _stage1_scan(
            candidates,
            config=config,
            concurrency=concurrency,
            transports=list(transports),
            check_url=stage1_check_url,
            timeout=stage1_timeout,
            connect_timeout=stage1_connect_timeout,
            on_success=queue.put,
        )
        await queue.put(None)
        _write_status(f"Stage 1: passed={len(stage1_successes)}, failed={len(stage1_failures)}")

    async def survivors() -> AsyncIterator[Stage1Success]:
        while (success := await queue.get()) is not None:
            yield success

    effective_min_successes = config.proxy_stage2_min_successes if min_successes is None else min_successes
    effective_min_rate = config.proxy_stage2_min_success_rate if min_success_rate is None else min_success_rate
    records_by_key: dict[tuple[str, str, str], VerifiedProxy] = {}
    pending: dict[tuple[str, str, str], VerifiedProxy] = {}
    pending_urls: set[str] = set()

    async def publish_if_ready(check: Stage2Check) -> None:
        record = _record_from_check(check, effective_min_successes, effective_min_rate)
        if record is None:
            return
        key = (record.url, record.user_agent, record.transport)
        records_by_key[key] = record
        pending[key] = record
        pending_urls.add(record.url)
        if len(pending_urls) < publish_after and record.source != "webshare":
            return
        async with ProxyDatabase(db_path) as database:
            await database.upsert_many(list(pending.values()))
        clear_dead_proxies(db_path)
        _write_status(
            f"Published {len(pending_urls)} eligible proxies to {db_path}; "
            f"{len(records_by_key)} verified combinations published in this scan"
        )
        pending.clear()
        pending_urls.clear()

    stage2_checks: list[Stage2Check] = []

    async def consume() -> None:
        nonlocal stage2_checks
        stage2_checks = await _stage2_scan(
            survivors(),
            config=config,
            target=target,
            transports=transports,
            concurrency=stage2_slots,
            timeout=timeout,
            requests_per_second=(
                config.proxy_stage2_requests_per_second if requests_per_second is None else requests_per_second
            ),
            attempts=stage2_attempts,
            retry_delay=stage2_retry_delay,
            max_retry_delay=stage2_max_retry_delay,
            on_check=publish_if_ready,
            min_successes=effective_min_successes,
            min_success_rate=effective_min_rate,
        )

    async with asyncio.TaskGroup() as group:
        group.create_task(produce())
        group.create_task(consume())

    for check in stage2_checks:
        key = (check.stage1.candidate.url, check.verified_user_agent, check.verified_transport)
        if key in records_by_key:
            continue  # keep the timestamp of the actual check, not the end of the scan
        record = _record_from_check(check, effective_min_successes, effective_min_rate)
        if record is not None:
            records_by_key[(record.url, record.user_agent, record.transport)] = record
    records = list(records_by_key.values())

    database_replaced = await _persist_verified(records, db_path)

    report = _build_report(
        provider=provider,
        providers=providers,
        raw_candidates=raw_candidates,
        selection=selection,
        raw_source_counts=catalog.source_counts,
        catalog_errors=catalog.errors,
        stage1_successes=stage1_successes,
        stage1_failures=stage1_failures,
        stage2_checks=stage2_checks,
        records=records,
        db_path=db_path,
        database_replaced=database_replaced,
    )
    report = _redact_report(report)
    if report_path is not None:
        report["report_path"] = str(report_path)
        atomic_write_json(report_path, report, indent=2, sort_keys=True)
    return report


def _redact_report(value):
    if isinstance(value, dict):
        return {key: _redact_report(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_report(item) for item in value]
    if isinstance(value, str):
        return redact_proxy_credentials(value)
    return value


async def _stage1_scan(
    candidates: list[ProxyCandidate],
    *,
    config: CollectorConfig,
    concurrency: int,
    transports: list[str],
    check_url: str | None,
    timeout: float | None,  # noqa: ASYNC109 - forwarded to stage 1
    connect_timeout: float | None,
    on_success: Callable[[Stage1Success], Awaitable[None]] | None = None,
) -> tuple[list[Stage1Success], list[Stage1Failure]]:
    successes: list[Stage1Success] = []
    failures: list[Stage1Failure] = []
    checked = active = 0
    lock = asyncio.Lock()
    slots = asyncio.Semaphore(concurrency)
    budget = NetworkBudget(concurrency, 0)
    host_locks: dict[str, asyncio.Lock] = {}
    agents = await asyncio.to_thread(user_agents, config)

    async def check(candidate: ProxyCandidate) -> tuple[list[Stage1Success], Stage1Failure | None]:
        compatible = compatible_transports(candidate.protocol, transports)
        if not compatible:
            return [], Stage1Failure(candidate, "unsupported_protocol", "no compatible HTTP transport")
        last_failure: Stage1Result | None = None
        async with host_locks.setdefault(proxy_host(candidate.url), asyncio.Lock()):
            for transport in compatible:
                for user_agent in agents[:1]:
                    result = await verify_stage1(
                        candidate.url,
                        transport=transport,
                        user_agent=user_agent,
                        check_url=check_url or config.proxy_stage1_check_url,
                        timeout=config.proxy_stage1_timeout if timeout is None else timeout,
                        connect_timeout=config.proxy_stage1_connect_timeout if connect_timeout is None else connect_timeout,
                        budget=budget,
                        headers=config.default_headers,
                        impersonate=config.browser_profile,
                    )
                    if result.ok:
                        return [Stage1Success(candidate, transport, user_agent, result.latency_ms)], None
                    last_failure = result
        if last_failure is None:
            return [], Stage1Failure(candidate, "unknown_error", "no stage-1 attempt was made")
        return [], Stage1Failure(candidate, last_failure.error_type, last_failure.detail)

    async def run(candidate: ProxyCandidate) -> None:
        nonlocal checked, active
        async with slots:
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
            if on_success is not None:
                for success in found:
                    await on_success(success)

    print(
        f"Starting stage 1: candidates={len(candidates)}, ",
        f"concurrent_checks={min(concurrency, len(candidates))}, ",
        f"transports={','.join(transports)}",
        flush=True,
    )
    remaining = iter(candidates)

    async def worker() -> None:
        for candidate in remaining:
            await run(candidate)

    async with asyncio.TaskGroup() as group:
        for _ in range(min(concurrency, len(candidates))):
            group.create_task(worker())
    return successes, failures


async def _stage2_scan(
    stage1_successes: list[Stage1Success] | AsyncIterable[Stage1Success],
    *,
    config: CollectorConfig,
    target: TargetProbe,
    concurrency: int,
    timeout: float,  # noqa: ASYNC109 - forwarded to stage 2
    requests_per_second: float,
    attempts: int | None,
    retry_delay: float | None,
    max_retry_delay: float | None,
    on_check: Callable[[Stage2Check], Awaitable[None]] | None = None,
    transports: Sequence[str] = ("aiohttp", "curl_cffi"),
    min_successes: int | None = None,
    min_success_rate: float | None = None,
) -> list[Stage2Check]:
    checks: list[Stage2Check] = []
    eligible_urls: set[str] = set()
    required_successes = config.proxy_stage2_min_successes if min_successes is None else min_successes
    required_rate = config.proxy_stage2_min_success_rate if min_success_rate is None else min_success_rate
    checked = active = 0
    lock = asyncio.Lock()
    slots = asyncio.Semaphore(concurrency)
    budget = NetworkBudget(concurrency, requests_per_second)
    host_locks: dict[str, asyncio.Lock] = {}
    unique_successes: dict[str, Stage1Success] = {}
    agents = await asyncio.to_thread(user_agents, config)
    streaming = isinstance(stage1_successes, AsyncIterable)
    if not streaming:
        for success in stage1_successes:
            unique_successes.setdefault(success.candidate.url, success)

    async def run(success: Stage1Success) -> None:
        nonlocal checked, active
        async with host_locks.setdefault(proxy_host(success.candidate.url), asyncio.Lock()), slots:
            async with lock:
                active += 1
            try:
                candidates = [success.transport, *compatible_transports(success.candidate.protocol, tuple(transports))]
                for transport in dict.fromkeys(candidates):
                    variants = list(dict.fromkeys([success.user_agent, *agents]))[: config.proxy_user_agent_limit]
                    for user_agent in variants:
                        result = await verify_stage2(
                            success.candidate.url,
                            transport=transport,
                            user_agent=user_agent,
                            target=target,
                            attempts=config.proxy_stage2_attempts if attempts is None else attempts,
                            timeout=timeout,
                            connect_timeout=min(timeout, config.proxy_stage2_connect_timeout),
                            retry_delay=config.proxy_stage2_retry_delay if retry_delay is None else retry_delay,
                            max_retry_delay=config.proxy_stage2_max_retry_delay
                            if max_retry_delay is None
                            else max_retry_delay,
                            budget=budget,
                            headers=config.default_headers,
                            impersonate=config.browser_profile,
                        )
                        check = Stage2Check(success, result, transport, user_agent)
                        async with lock:
                            checks.append(check)
                            if _is_eligible(result, required_successes, required_rate):
                                eligible_urls.add(success.candidate.url)
                            if on_check is not None:
                                await on_check(check)
                        if result.ok or result.error_type not in USER_AGENT_RETRY_ERRORS:
                            break
                    if result.ok:
                        break
            finally:
                async with lock:
                    active -= 1
            async with lock:
                checked += 1
                _report_progress(
                    checked,
                    len(unique_successes),
                    len(eligible_urls),
                    active,
                    prefix="Stage 2",
                    verified_label="eligible proxies",
                )

    print(
        f"Starting stage 2: proxies={'streaming' if streaming else len(unique_successes)}, ",
        f"concurrent_checks={concurrency}, ",
        f"attempts={attempts or config.proxy_stage2_attempts}",
        f"user_agents<={config.proxy_user_agent_limit}",
        flush=True,
    )
    if streaming:
        iterator = aiter(stage1_successes)
        input_lock = asyncio.Lock()

        async def worker() -> None:
            while True:
                async with input_lock:
                    success = await anext(iterator, None)
                    if success is None:
                        return
                    if success.candidate.url in unique_successes:
                        continue
                    unique_successes[success.candidate.url] = success
                await run(success)

        async with asyncio.TaskGroup() as group:
            for _ in range(concurrency):
                group.create_task(worker())
    else:
        await asyncio.gather(*(run(success) for success in unique_successes.values()))
    return checks


def _is_eligible(result: Stage2Result, min_successes: int, min_success_rate: float) -> bool:
    return result.ok and result.stats.successful_requests >= min_successes and result.stats.success_rate >= min_success_rate


def _record_from_check(check: Stage2Check, min_successes: int, min_success_rate: float) -> VerifiedProxy | None:
    result = check.result
    stats = result.stats
    if not _is_eligible(result, min_successes, min_success_rate):
        return None
    candidate = check.stage1.candidate
    checked_at = datetime.now(UTC).isoformat()
    return VerifiedProxy(
        url=candidate.url,
        user_agent=check.verified_user_agent,
        transport=check.verified_transport,
        latency_ms=stats.average_latency_ms,
        checked_at=checked_at,
        protocol=candidate.protocol,
        source=candidate.source,
        country=candidate.country,
        city=candidate.city,
        anonymity=candidate.anonymity,
        uptime_percent=candidate.uptime_percent,
        source_latency_ms=candidate.latency_ms,
        last_checked=(candidate.last_checked.isoformat() if candidate.last_checked else None),
        https_supported=candidate.https_supported,
        successful_requests=stats.successful_requests,
        failed_requests=stats.failed_requests,
        success_rate=round(stats.success_rate, 4),
        average_latency_ms=stats.average_latency_ms,
        last_success=stats.last_success,
        last_failure=stats.last_failure,
        consecutive_failures=stats.consecutive_failures,
        error_types=dict(stats.error_types),
    )


async def _persist_verified(records: list[VerifiedProxy], db_path: Path) -> bool:
    if not records:
        print(f"No verified proxies found; keeping existing database at {db_path} unchanged", flush=True)
        return False
    async with ProxyDatabase(db_path) as database:
        await database.replace_all(records)
    clear_dead_proxies(db_path)
    _write_status(f"Replaced {db_path} with {len(records)} fresh verified combinations")
    return True


def _build_report(
    *,
    provider: str,
    providers: list[str],
    raw_candidates: list[ProxyCandidate],
    selection,
    raw_source_counts: dict[str, int],
    catalog_errors: dict[str, str],
    stage1_successes: list[Stage1Success],
    stage1_failures: list[Stage1Failure],
    stage2_checks: list[Stage2Check],
    records: list[VerifiedProxy],
    db_path: Path,
    database_replaced: bool,
) -> dict:
    csv_candidates = raw_source_counts.get("iplocate", 0)
    free_proxy_candidates = raw_source_counts.get("free-proxy", 0)
    verified_keys = {(record.url, record.user_agent, record.transport) for record in records}
    stage2_error_counts: Counter[str] = Counter()
    for check in stage2_checks:
        stage2_error_counts.update(check.result.stats.error_types)
    stage2_results = [
        {
            "candidate": candidate_as_dict(check.stage1.candidate),
            "transport": check.verified_transport,
            "stage1_transport": check.stage1.transport,
            "user_agent": check.verified_user_agent,
            "stage1_user_agent": check.stage1.user_agent,
            "stage1_latency_ms": check.stage1.latency_ms,
            "ok": check.result.ok,
            "verified": (check.stage1.candidate.url, check.verified_user_agent, check.verified_transport) in verified_keys,
            "error_type": check.result.error_type,
            "detail": check.result.detail,
            "stats": check.result.stats.as_dict(),
        }
        for check in stage2_checks
    ]
    catalog = {
        "raw_candidates": len(raw_candidates),
        "selected_candidates": len(selection.candidates),
        "raw_source_counts": raw_source_counts,
        "source_counts": selection.source_counts,
        "rejected": dict(selection.rejected),
        "errors": catalog_errors,
        "csv_candidates": csv_candidates,
        "free_proxy_candidates": free_proxy_candidates,
    }
    stage1 = {
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
            {"candidate": candidate_as_dict(failure.candidate), "error_type": failure.error_type, "detail": failure.detail}
            for failure in stage1_failures
        ],
    }
    stage2 = {
        "checked": len(stage2_checks),
        "verified": len(records),
        "error_types": dict(stage2_error_counts),
        "results": stage2_results,
    }
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "provider": provider,
        "providers": providers,
        "catalog": catalog,
        "stage1": stage1,
        "stage2": stage2,
        "candidates": catalog["selected_candidates"],
        "csv_candidates": csv_candidates,
        "free_proxy_candidates": free_proxy_candidates,
        "source_counts": selection.source_counts,
        "stage1_passed": stage1["passed"],
        "stage1_failed": stage1["failed"],
        "stage2_checked": stage2["checked"],
        "verified_combinations": len(records),
        "verified_proxies": len({record.url for record in records}),
        "by_transport": dict(Counter(record.transport for record in records)),
        "database": str(db_path),
        "database_replaced": database_replaced,
    }


def _report_progress(
    checked: int,
    total: int,
    verified: int,
    active: int,
    *,
    prefix: str = "Checked",
    verified_label: str = "verified combinations",
) -> None:
    if checked % PROGRESS_EVERY and checked != total:
        return
    print(f"{prefix} {checked}/{total}, {verified_label}={verified}, active_checks={active}", flush=True)
