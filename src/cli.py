"""
Minimal command-line entry point for the SEPREC scraper.

Examples:
    python main.py                       # incremental run over all categories
    python main.py --year 2025           # only publications from 2025
    python main.py --category 1 --max-items 5
    python main.py --schedule            # repeat every SCHEDULE_INTERVAL_HOURS
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from config import BASE_DIR, settings
from src import __version__
from src.client.proxy_source import ProxySourceError, sync_proxy_source
from src.models.publication import STATUS_FAILED
from src.services.scheduler import AsyncScheduler
from src.services.scraper import SeprecScraper
from src.storage.proxy_database import ProxyDatabase
from src.utils.logger import setup_logger

logger = setup_logger("cli")

ALL_CATEGORY = "all"
CUSTOM_RANGE_FILTER = "5"
MIN_YEAR = 1900
MAX_YEAR = 2100
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_INTERRUPTED = 130
PROXY_READY_POLL_SECONDS = 0.5

CATEGORY_CODES: dict[str, list[str]] = {"all": ["1", "2", "3", "4"], "1": ["1"], "2": ["2"], "3": ["3"], "4": ["4"]}


def _positive_int(raw: str) -> int:
    value = int(raw)
    if value < 1:
        raise argparse.ArgumentTypeError("value must be >= 1")
    return value


def _positive_float(raw: str) -> float:
    value = float(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("value must be > 0")
    return value


def _year(raw: str) -> int:
    value = int(raw)
    if not MIN_YEAR <= value <= MAX_YEAR:
        raise argparse.ArgumentTypeError(f"year must be between {MIN_YEAR} and {MAX_YEAR}")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gaceta-scraper",
        description=(
            "Async scraper for the Bolivian Gaceta Electrónica de Comercio "
            "(SEPREC). Downloads original PDFs, "
            "generates PDFs for text-only publications, and stores metadata in CSV."
        ),
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    _add_scrape_arguments(parser)
    _add_proxy_arguments(parser)
    _add_schedule_arguments(parser)
    return parser


def _add_scrape_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--audit-storage", action="store_true", help="Check CSV records and PDF files offline.")
    parser.add_argument("--audit-output", type=Path, help="Write the offline storage report as JSON.")
    parser.add_argument("--year", type=_year, help="Only collect publications from this calendar year.")
    parser.add_argument(
        "--category", choices=list(CATEGORY_CODES), default=ALL_CATEGORY,
        help=("Main category: all, 1=INSTRUMENTOS, 2=CONVOCATORIAS, 3=MEMORIAS, 4=BALANCES."),
    )
    parser.add_argument(
        "--sub-category", default="", help="Specific parametric code (e.g. 1001). Requires a single --category."
    )
    parser.add_argument("--max-items", type=_positive_int, help="Process at most N publications across all categories.")
    parser.add_argument("--max-pages", type=_positive_int, help="Process at most N API pages per category.")
    parser.add_argument("--concurrency", type=_positive_int, default=settings.max_concurrent_requests)
    parser.add_argument(
        "--transport", choices=["aiohttp", "curl_cffi", "auto"], default=settings.http_transport,
        help=("HTTP transport; aiohttp is used first with curl_cffi fallback (default: aiohttp)."),
    )
    parser.add_argument("--full", action="store_true", help="Re-download and overwrite existing publications.")


def _add_proxy_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--proxy-mode", choices=["direct", "fallback", "always", "pool"], default=settings.proxy_mode,
        help=(
            "Routing: direct = never use proxies; fallback = direct first, "
            "proxies only after 403/429 blocks; pool (default) = proxies first, "
            "direct only if the pool is empty; always = every request through proxies."
        ),
    )
    parser.add_argument(
        "--no-proxy-warmup", action="store_false", dest="proxy_warmup",
        help="Skip proxy readiness checks at startup and use candidates directly.",
    )
    parser.add_argument(
        "--proxy-source", default=settings.proxy_source_url, metavar="URL",
        help="Refresh the proxy CSV from this upstream list in CSV mode.",
    )
    parser.add_argument(
        "--no-proxy-source", action="store_const", const="",
        dest="proxy_source", help="Do not download the upstream proxy list in CSV mode.",
    )
    parser.add_argument(
        "--proxy-file", default=None, help="Use a local proxy CSV or TXT list instead of automatic database refresh."
    )
    parser.add_argument(
        "--proxy-db", type=Path, default=None,
        metavar="PATH", help="Use this verified proxy database (default with refresh: PROXY_DB_PATH).",
    )
    parser.add_argument(
        "--refresh-proxies", action=argparse.BooleanOptionalAction, default=True,
        help=("Refresh the proxy database in a separate process during scraping (default: enabled)."),
    )


def _add_schedule_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--schedule", action="store_true", help="Repeat forever instead of running once.")
    parser.add_argument("--interval-hours", type=_positive_float, default=settings.schedule_interval_hours)


def _validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if args.sub_category and args.category == ALL_CATEGORY:
        parser.error("--sub-category requires a specific --category (1, 2, 3 or 4).")


async def _execute_scrape(scraper: SeprecScraper, args: argparse.Namespace) -> list[dict[str, Any]]:
    start_date, end_date, date_filter = _date_range(args)
    remaining_items = args.max_items
    reports: list[dict[str, Any]] = []

    for category in CATEGORY_CODES[args.category]:
        if remaining_items is not None and remaining_items <= 0:
            break
        report = await scraper.run(
            category=category, sub_category=args.sub_category, date_filter=date_filter,
            start_date=start_date, end_date=end_date, max_items=remaining_items,
            max_pages=getattr(args, "max_pages", None), incremental=not args.full,
        )
        reports.append(report)
        if remaining_items is not None:
            remaining_items -= report["items_scraped"] + report["items_failed"]

    for report in reports:
        _write_stdout(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return reports


def _write_stdout(text: str) -> None:
    sys.stdout.write(f"{text}\n")


def _date_range(args: argparse.Namespace) -> tuple[str, str, str]:
    if not args.year:
        return "", "", ""
    return f"{args.year}-01-01", f"{args.year}-12-31", CUSTOM_RANGE_FILTER


def _prepare_proxy_file(args: argparse.Namespace) -> str | None:
    proxy_path = Path(getattr(args, "proxy_file", None) or settings.proxy_file or settings.proxy_list_path)
    source = getattr(args, "proxy_source", "") or ""
    if not source:
        return str(proxy_path) if proxy_path.exists() else None

    try:
        report = sync_proxy_source(
            source_url=source, csv_path=proxy_path, state_path=settings.proxy_state_path,
            refresh_minutes=settings.proxy_source_refresh_minutes, timeout=settings.proxy_source_timeout,
        )
        if report.get("updated"):
            logger.info("Proxy list refreshed from %s: %d proxies", source, report.get("count", 0))
        else:
            logger.info("Proxy list is up to date (%s)", report.get("reason", "cached"))
    except ProxySourceError as exc:
        logger.warning("Could not refresh proxy list: %s", exc)
    return str(proxy_path) if proxy_path.exists() else None


async def _run_once(args: argparse.Namespace) -> list[dict[str, Any]]:
    settings.ensure_directories()
    proxy_db = getattr(args, "proxy_db", None)
    proxy_mode = getattr(args, "proxy_mode", settings.proxy_mode)
    refresh_proxies = (
        getattr(args, "refresh_proxies", True)
        and proxy_mode != "direct"
        and (proxy_db is not None or not getattr(args, "proxy_file", None))
    )
    if not refresh_proxies:
        return await _run_scraper(args, proxy_db, proxy_mode)

    proxy_db = proxy_db or settings.proxy_db_path
    async with ProxyDatabase(proxy_db) as database:
        ready = len(await database.list_active(limit=settings.proxy_min_ready)) >= settings.proxy_min_ready
    if ready:
        return await _run_scraper(args, proxy_db, proxy_mode, start_collector=True)

    logger.info("Waiting for %d verified proxies before scraping", settings.proxy_min_ready)
    async with _proxy_collector(proxy_db) as process:
        await _wait_for_proxies(proxy_db, process, settings.proxy_min_ready)
        return await _run_scraper(args, proxy_db, proxy_mode, collector=process)


async def _wait_for_proxies(db_path: Path, process: asyncio.subprocess.Process, required: int) -> None:
    async with ProxyDatabase(db_path) as database:
        while True:
            available = len(await database.list_active(limit=required))
            if available >= required:
                logger.info("Verified proxy pool ready: %d available", available)
                return
            if process.returncode is not None:
                raise RuntimeError(f"Proxy collection stopped with {available}/{required} verified proxies")
            await asyncio.sleep(PROXY_READY_POLL_SECONDS)


async def _run_scraper(
    args: argparse.Namespace, proxy_db: Path | None, proxy_mode: str,
    *, collector: asyncio.subprocess.Process | None = None, start_collector: bool = False,
) -> list[dict[str, Any]]:
    proxy_file = None
    if proxy_mode != "direct" and not proxy_db:
        proxy_file = await asyncio.to_thread(_prepare_proxy_file, args)
    async with SeprecScraper(
        max_concurrency=args.concurrency, proxy_warmup=args.proxy_warmup, proxy_file=proxy_file,
        proxy_db_path=proxy_db, transport=getattr(args, "transport", None), proxy_mode=proxy_mode,
    ) as scraper:
        if start_collector:
            async with _proxy_collector(proxy_db) as process:
                return await _scrape_during_collection(scraper, args, process)
        if collector is not None:
            return await _scrape_during_collection(scraper, args, collector)
        return await _execute_scrape(scraper, args)


async def _scrape_during_collection(
    scraper: SeprecScraper, args: argparse.Namespace, process: asyncio.subprocess.Process
) -> list[dict[str, Any]]:
    reports = await _execute_scrape(scraper, args)
    exit_code = await process.wait()
    if exit_code:
        logger.warning("Proxy collection finished with exit code %d; scraping used available proxies", exit_code)
    return reports


@asynccontextmanager
async def _proxy_collector(db_path: Path) -> AsyncIterator[asyncio.subprocess.Process]:
    process = await asyncio.create_subprocess_exec(
        sys.executable, str(BASE_DIR / "collect_proxies.py"), "--db",
        str(db_path), "--publish-after", str(settings.proxy_min_ready),
    )
    try:
        yield process
    finally:
        if process.returncode is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
        await process.wait()


async def _run_schedule(args: argparse.Namespace) -> int:
    async def job() -> None:
        reports = await _run_once(args)
        if reports and all(report["status"] == STATUS_FAILED for report in reports):
            raise RuntimeError("All scheduled category runs failed")

    scheduler = AsyncScheduler(job=job, interval_hours=args.interval_hours)
    scheduler.install_signal_handlers()
    await scheduler.run()
    return EXIT_SUCCESS


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _validate_args(args, parser)

    try:
        if args.audit_storage:
            return asyncio.run(_audit_storage(args))
        if args.schedule:
            return asyncio.run(_run_schedule(args))
        reports = asyncio.run(_run_once(args))
        if not reports:
            logger.warning("Nothing to scrape: the item budget was exhausted.")
            return EXIT_SUCCESS
        return EXIT_FAILURE if any(report["status"] == STATUS_FAILED for report in reports) else EXIT_SUCCESS
    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Shutting down...")
        return EXIT_INTERRUPTED
    except Exception as exc:
        logger.exception("Fatal error: %s", exc)
        return EXIT_FAILURE


async def _audit_storage(args: argparse.Namespace) -> int:
    from src.storage.integrity import inspect_storage

    report = await asyncio.to_thread(inspect_storage, settings.csv_export_path)
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.audit_output:
        await asyncio.to_thread(args.audit_output.write_text, payload + "\n", encoding="utf-8")
    _write_stdout(payload)
    return EXIT_FAILURE if report["invalid"] else EXIT_SUCCESS
