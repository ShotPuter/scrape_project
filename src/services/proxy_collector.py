"""Application composition for the standalone proxy collector."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections.abc import Sequence
from dataclasses import fields
from pathlib import Path
from typing import Any

from config import settings
from src.proxy.collector import PROVIDER_CHOICES, collect as collect_proxies
from src.proxy.config import CollectorConfig
from src.proxy.sources.iplocate import ProxySourceError
from src.proxy.sources.registry import DEFAULT_SOURCES, SOURCE_REGISTRY, resolve_sources
from src.proxy.user_agents import MAX_USER_AGENT_LIMIT
from src.services.seprec_proxy_probe import seprec_target_probe
from src.utils.logger import setup_logger


def collector_config_from_settings(app_settings: Any = None) -> CollectorConfig:
    source = settings if app_settings is None else app_settings
    values = {item.name: getattr(source, item.name) for item in fields(CollectorConfig)}
    values["default_headers"] = dict(source.default_headers)
    return CollectorConfig(**values)


async def collect(**kwargs: Any) -> dict:
    """Compatibility entry point: construct the SEPREC job at the app boundary."""
    setup_proxy_logging()
    return await collect_proxies(
        config=collector_config_from_settings(), target=seprec_target_probe(settings.api_base_url), **kwargs
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=settings.proxy_list_path)
    parser.add_argument("--db", type=Path, default=settings.proxy_db_path)
    parser.add_argument(
        "--publish-after",
        type=int,
        metavar="N",
        default=settings.proxy_min_ready,
        help="Publish each batch of N eligible proxies (default: PROXY_MIN_READY).",
    )
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument(
        "--provider",
        choices=list(PROVIDER_CHOICES),
        default="public",
        help="Candidate source: public (default subset), all, or one named source.",
    )
    selection.add_argument("--sources", help="Comma-separated source names; overrides PROXY_SOURCES.")
    selection.add_argument(
        "--free-proxy", action="store_const", const="free-proxy", dest="provider", help="Shortcut for --provider free-proxy."
    )
    parser.add_argument("--source", default=settings.proxy_source_url)
    parser.add_argument("--list-sources", action="store_true", help="List source URLs and refresh intervals, then exit.")
    parser.add_argument("--watch", action="store_true", help="Repeat collection until interrupted.")
    parser.add_argument("--interval", type=float, default=300, help="Seconds between completed scans in watch mode.")
    parser.add_argument(
        "--user-agent-limit",
        type=int,
        default=settings.proxy_user_agent_limit,
        help="Maximum User-Agents per proxy/transport after a block (default: %(default)s; 1 disables rotation).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=("Total candidate cap after filtering; 0 = all (default: PROXY_MAX_CANDIDATES)."),
    )
    parser.add_argument(
        "--max-per-source",
        type=int,
        default=None,
        help="Per-source cap after filtering; 0 = all (default: PROXY_MAX_PER_SOURCE).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=settings.proxy_check_concurrency,
        help="Stage-1 concurrent proxy checks (default: %(default)s).",
    )
    parser.add_argument(
        "--stage2-concurrency",
        type=int,
        default=settings.proxy_stage2_concurrency,
        help="Stage-2 concurrent target checks (default: %(default)s).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=settings.proxy_stage2_timeout,
        help="Stage-2 target request timeout (default: %(default)s).",
    )
    parser.add_argument(
        "--stage1-timeout",
        type=float,
        default=settings.proxy_stage1_timeout,
        help="Stage-1 HTTPS check total timeout (default: %(default)s).",
    )
    parser.add_argument(
        "--stage1-connect-timeout",
        type=float,
        default=settings.proxy_stage1_connect_timeout,
        help="Stage-1 connect timeout (default: %(default)s).",
    )
    parser.add_argument(
        "--stage1-check-url",
        default=settings.proxy_stage1_check_url,
        help="HTTPS endpoint used for the stage-1 reachability check.",
    )
    parser.add_argument(
        "--min-uptime",
        type=float,
        default=settings.proxy_stage1_min_uptime,
        help=("Reject candidates with measured uptime below this value (default: %(default)s)."),
    )
    parser.add_argument(
        "--max-latency-ms",
        type=float,
        default=settings.proxy_stage1_max_latency_ms,
        help=("Reject candidates with measured latency above this value (default: %(default)s)."),
    )
    parser.add_argument(
        "--preferred-age-minutes",
        type=float,
        default=settings.proxy_stage1_preferred_age_minutes,
        help=("Prefer candidates checked within this many minutes (default: %(default)s)."),
    )
    parser.add_argument(
        "--max-age-minutes",
        type=float,
        default=settings.proxy_stage1_max_age_minutes,
        help=("Hard-reject candidates older than this many minutes (default: %(default)s)."),
    )
    parser.add_argument(
        "--require-https",
        action=argparse.BooleanOptionalAction,
        default=settings.proxy_stage1_require_https,
        help="Reject candidates explicitly marked as not supporting HTTPS.",
    )
    parser.add_argument(
        "--requests-per-second",
        type=float,
        default=settings.proxy_stage2_requests_per_second,
        help="Stage-2 target request start rate; 0 disables pacing.",
    )
    parser.add_argument(
        "--stage2-attempts",
        type=int,
        default=settings.proxy_stage2_attempts,
        help="Stage-2 attempts per proxy combination (default: %(default)s).",
    )
    parser.add_argument(
        "--stage2-retry-delay",
        type=float,
        default=settings.proxy_stage2_retry_delay,
        help="Base stage-2 retry delay in seconds (default: %(default)s).",
    )
    parser.add_argument(
        "--stage2-max-retry-delay",
        type=float,
        default=settings.proxy_stage2_max_retry_delay,
        help="Maximum stage-2 retry delay in seconds (default: %(default)s).",
    )
    parser.add_argument(
        "--min-success-rate",
        type=float,
        default=settings.proxy_stage2_min_success_rate,
        help="Minimum stage-2 success rate to store a proxy (default: %(default)s).",
    )
    parser.add_argument(
        "--min-successes",
        type=int,
        default=settings.proxy_stage2_min_successes,
        help=("Minimum successful stage-2 attempts to store a proxy (default: %(default)s)."),
    )
    parser.add_argument(
        "--report", type=Path, default=settings.proxy_report_path, help="JSON report path (default: %(default)s)."
    )
    parser.add_argument(
        "--transport",
        choices=["both", "aiohttp", "curl_cffi"],
        default="both",
        help="Verify one transport or both (default: both).",
    )
    return parser


def setup_proxy_logging() -> None:
    for name in (
        "src.proxy.collector",
        "src.proxy.checker",
        "src.proxy.repository",
        "src.proxy.sources.catalog",
        "src.proxy.sources.free_proxy",
        "src.proxy.user_agents",
    ):
        setup_logger(name)


def main(argv: Sequence[str] | None = None) -> int:
    setup_proxy_logging()
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.list_sources:
        print(
            json.dumps(
                {
                    name: {
                        "default": name in DEFAULT_SOURCES,
                        "refresh_minutes": source.refresh_minutes,
                        "urls": [file.url for file in source.files],
                    }
                    for name, source in SOURCE_REGISTRY.items()
                },
                indent=2,
            )
        )
        return 0
    try:
        resolve_sources(
            args.sources
            if args.sources is not None
            else ((settings.proxy_sources if args.provider == "public" else "") or args.provider)
        )
    except ValueError as exc:
        parser.error(str(exc))
    if not args.interval > 0:
        parser.error("--interval must be positive")
    if not 1 <= args.user_agent_limit <= MAX_USER_AGENT_LIMIT:
        parser.error(f"--user-agent-limit must be between 1 and {MAX_USER_AGENT_LIMIT}")
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
            run_collection(
                watch=args.watch,
                interval=args.interval,
                csv_path=args.csv,
                db_path=args.db,
                source_url=args.source,
                provider=args.provider,
                sources=args.sources,
                user_agent_limit=args.user_agent_limit,
                limit=args.limit,
                concurrency=args.concurrency,
                timeout=args.timeout,
                transports=transports,
                requests_per_second=args.requests_per_second,
                max_per_source=args.max_per_source,
                stage2_concurrency=args.stage2_concurrency,
                stage1_check_url=args.stage1_check_url,
                stage1_timeout=args.stage1_timeout,
                stage1_connect_timeout=args.stage1_connect_timeout,
                min_uptime=args.min_uptime,
                max_latency_ms=args.max_latency_ms,
                preferred_age_minutes=args.preferred_age_minutes,
                max_age_minutes=args.max_age_minutes,
                require_https=args.require_https,
                stage2_attempts=args.stage2_attempts,
                stage2_retry_delay=args.stage2_retry_delay,
                stage2_max_retry_delay=args.stage2_max_retry_delay,
                min_success_rate=args.min_success_rate,
                min_successes=args.min_successes,
                report_path=args.report,
                publish_after=args.publish_after,
            )
        )
    except KeyboardInterrupt:
        return 130
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


async def run_collection(*, watch: bool, interval: float, **kwargs: Any) -> dict:
    """Run scans sequentially; recover from a failed scan only in watch mode."""
    while True:
        try:
            report = await collect(**kwargs)
        except Exception:  # noqa: BLE001 - a later scan can recover without clearing the database
            if not watch:
                raise
            logging.getLogger(__name__).exception("Proxy collection failed; retrying after %.0fs", interval)
        else:
            if not watch:
                return report
            print(json.dumps({key: report[key] for key in ("verified_proxies", "database_replaced")}), flush=True)
        await asyncio.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
