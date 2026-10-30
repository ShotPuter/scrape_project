"""Command-line entry point for the SEPREC scraper.

The CLI intentionally exposes only the workflows this project needs:

    python main.py                      # incremental scrape of every category
    python main.py --year 2025          # only publications from one year
    python main.py --category 1         # only INSTRUMENTOS
    python main.py --collect-proxies    # build/refresh data/proxies.db and exit

Routing, transports, timeouts and other tuning live in environment variables
(see ``.env.example``), not in per-run command-line switches.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from config import settings
from src import __version__
from src.client.seprec_client import SeprecClient
from src.models.publication import STATUS_FAILED
from src.services.proxy_bootstrap import count_fresh_proxies, proxy_collector, wait_for_proxies
from src.services.scraper import SeprecScraper
from src.storage.discard_store import DiscardStore
from src.storage.file_storage import FileStorage
from src.utils.atomic import atomic_write_json
from src.utils.logger import setup_logger

logger = setup_logger("cli")

ALL_CATEGORY = "all"
CUSTOM_RANGE_FILTER = "5"
MIN_YEAR = 1900
MAX_YEAR = 2100
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_INTERRUPTED = 130


CATEGORY_CODES: dict[str, list[str]] = {"all": ["1", "2", "3", "4"], "1": ["1"], "2": ["2"], "3": ["3"], "4": ["4"]}


def _year(raw: str) -> int:
    value = int(raw)
    if not MIN_YEAR <= value <= MAX_YEAR:
        raise argparse.ArgumentTypeError(f"year must be between {MIN_YEAR} and {MAX_YEAR}")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gaceta-scraper",
        description=(
            "Async scraper for the Bolivian Gaceta Electrónica de Comercio (SEPREC). "
            "Downloads PDFs and maintains an incremental CSV index."
        ),
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--year", type=_year, help="Only collect publications from this calendar year.")
    parser.add_argument(
        "--discard-files",
        action="store_true",
        help="Download and validate PDFs without writing PDFs or the publication CSV.",
    )
    parser.add_argument(
        "--stress-minutes", type=float, help="Repeat discard-mode downloads for this many minutes after startup."
    )
    parser.add_argument(
        "--stress-report",
        type=Path,
        default=Path("data/stress-report.json"),
        help="Discard-mode JSON report, updated every metrics interval and on exit.",
    )
    parser.add_argument(
        "--category",
        choices=list(CATEGORY_CODES),
        default=ALL_CATEGORY,
        help="Main category: all, 1=INSTRUMENTOS, 2=CONVOCATORIAS, 3=MEMORIAS, 4=BALANCES.",
    )
    parser.add_argument(
        "--collect-proxies",
        action="store_true",
        help="Collect and verify public proxies into the configured database, then exit.",
    )
    return parser


async def _execute_scrape(scraper: SeprecScraper, args: argparse.Namespace) -> list[dict[str, Any]]:
    start_date = f"{args.year}-01-01" if args.year else ""
    end_date = f"{args.year}-12-31" if args.year else ""
    date_filter = CUSTOM_RANGE_FILTER if args.year else ""
    reports: list[dict[str, Any]] = []
    for category in CATEGORY_CODES[args.category]:
        report = await scraper.run(
            category=category, date_filter=date_filter, start_date=start_date, end_date=end_date, incremental=True
        )
        reports.append(report)
    for report in reports:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str), flush=True)
    return reports


async def _run_once(args: argparse.Namespace) -> list[dict[str, Any]]:
    settings.ensure_directories()
    if settings.proxy_mode == "direct":
        logger.info("PROXY_MODE=direct: scraping without proxies")
        return await _run_scraper(args, None)

    db_path = settings.proxy_db_path
    fresh = await count_fresh_proxies(db_path, max_age_hours=settings.proxy_db_max_age_hours)
    if fresh >= settings.proxy_min_ready:
        logger.info("Using %d fresh verified proxies from %s", fresh, db_path)
        return await _run_scraper(args, db_path)
    if fresh:
        logger.warning(
            "Only %d/%d fresh verified proxies are available; starting with the current pool",
            fresh,
            settings.proxy_min_ready,
        )
        return await _run_scraper(args, db_path)

    if not settings.proxy_autocollect:
        logger.info("Waiting for independently managed proxy collector | DB: %s", db_path.resolve())
        fresh = await wait_for_proxies(
            db_path,
            None,
            required=1 if settings.webshare_api_token.strip() else settings.proxy_min_ready,
            timeout=settings.proxy_startup_timeout_seconds,
            max_age_hours=settings.proxy_db_max_age_hours,
        )
        if not fresh and settings.proxy_mode == "always":
            raise RuntimeError("External proxy collector produced no fresh verified proxies")
        return await _run_scraper(args, db_path)

    logger.info(
        "No fresh verified proxies in %s; collecting before scraping (max %.0fs)",
        db_path,
        settings.proxy_startup_timeout_seconds,
    )
    async with proxy_collector(db_path) as process:
        fresh = await wait_for_proxies(
            db_path,
            process,
            required=1 if settings.webshare_api_token.strip() else settings.proxy_min_ready,
            timeout=settings.proxy_startup_timeout_seconds,
            max_age_hours=settings.proxy_db_max_age_hours,
        )
        if not fresh:
            if settings.proxy_mode == "always":
                raise RuntimeError("Proxy collection produced no fresh verified proxies")
            logger.warning("Proxy collection produced no usable proxies; continuing with direct requests")
            return await _run_scraper(args, db_path)
        logger.info("Starting scrape with %d fresh verified proxies", fresh)
        return await _run_scraper(args, db_path)


async def _run_scraper(args: argparse.Namespace, proxy_db: Path | None) -> list[dict[str, Any]]:
    client_config = SeprecClient.config_from_settings(proxy_db_path=proxy_db)
    if args.discard_files:
        return await _run_discard(args, client_config)
    async with SeprecScraper(client_config=client_config) as scraper:
        return await _execute_scrape(scraper, args)


async def _run_discard(args: argparse.Namespace, client_config: Any) -> list[dict[str, Any]]:
    store = DiscardStore()
    async with SeprecScraper(client_config=client_config, store=store, file_storage=FileStorage(discard=True)) as scraper:
        started = time.monotonic()
        timed = args.stress_minutes is not None

        def snapshot(status: str) -> dict[str, Any]:
            elapsed = time.monotonic() - started
            rss_kib = None
            try:
                for line in Path("/proc/self/status").read_text().splitlines():
                    if line.startswith("VmRSS:"):
                        rss_kib = int(line.split()[1])
                        break
            except (OSError, ValueError):
                pass
            return {
                "status": status,
                "mode": "discard",
                "duration_seconds": round(elapsed, 2),
                "rss_kib": rss_kib,
                "documents_checked": store.completed,
                "original_files": store.original,
                "generated_files": store.generated,
                "items_failed": scraper.failed_documents,
                "pdf_bytes_checked": store.byte_count,
                "files_per_minute": round(store.completed * 60 / max(elapsed, 0.001), 2),
                "http_metrics": scraper.client.metrics.snapshot(),
            }

        async def monitor() -> None:
            while True:
                await asyncio.sleep(settings.metrics_interval_seconds)
                report = snapshot("RUNNING")
                await asyncio.to_thread(atomic_write_json, args.stress_report, report)
                logger.info(
                    "Discard test: checked=%d, failed=%d, speed=%.1f/min, rss_kib=%s",
                    report["documents_checked"],
                    report["items_failed"],
                    report["files_per_minute"],
                    report["rss_kib"],
                )

        async def download() -> None:
            while True:
                reports = await _execute_scrape(scraper, args)
                if any(report["status"] == STATUS_FAILED for report in reports):
                    raise RuntimeError("Stress scan failed; see category report")
                if not timed:
                    return
                store.next_cycle()
                await asyncio.sleep(1)  # avoid spinning when a year/category has no publications

        monitor_task = asyncio.create_task(monitor())
        status = "FAILED"
        deadline = asyncio.timeout(args.stress_minutes * 60 if timed else None)
        try:
            async with deadline:
                await download()
            status = "COMPLETED"
        except TimeoutError:
            if not deadline.expired():
                raise
            status = "DURATION_REACHED"
        except asyncio.CancelledError:
            status = "INTERRUPTED"
            raise
        finally:
            monitor_task.cancel()
            await asyncio.gather(monitor_task, return_exceptions=True)
            report = snapshot(status)
            await asyncio.to_thread(atomic_write_json, args.stress_report, report)
            print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return [report]


async def _collect_proxies() -> int:
    settings.ensure_directories()
    from src.services.proxy_collector import collect

    report = await collect(
        csv_path=settings.proxy_list_path,
        db_path=settings.proxy_db_path,
        source_url=settings.proxy_source_url,
        report_path=settings.proxy_report_path,
    )
    return EXIT_SUCCESS if report["verified_combinations"] else EXIT_FAILURE


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.stress_minutes is not None:
        if not 0 < args.stress_minutes < float("inf"):
            parser.error("--stress-minutes must be finite and positive")
        args.discard_files = True
    if args.collect_proxies and args.discard_files:
        parser.error("--collect-proxies cannot be combined with discard/stress mode")

    try:
        if args.collect_proxies:
            return asyncio.run(_collect_proxies())
        reports = asyncio.run(_run_once(args))
        if not reports:
            logger.warning("Nothing to scrape.")
            return EXIT_SUCCESS
        return EXIT_FAILURE if any(report["status"] == STATUS_FAILED for report in reports) else EXIT_SUCCESS
    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Shutting down...")
        return EXIT_INTERRUPTED
    except Exception as exc:  # noqa: BLE001 - CLI boundary converts failures into an exit code
        logger.exception("Fatal error: %s", exc)
        return EXIT_FAILURE
