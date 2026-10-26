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
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from config import settings
from src import __version__
from src.client.seprec_client import SeprecClient
from src.models.publication import STATUS_FAILED
from src.services.proxy_bootstrap import count_fresh_proxies, proxy_collector, wait_for_proxies
from src.services.scraper import SeprecScraper
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
    start_date, end_date, date_filter = _date_range(args)
    reports: list[dict[str, Any]] = []

    for category in CATEGORY_CODES[args.category]:
        report = await scraper.run(
            category=category, date_filter=date_filter, start_date=start_date, end_date=end_date, incremental=True
        )
        reports.append(report)

    for report in reports:
        _write_stdout(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return reports


def _write_stdout(text: str) -> None:
    sys.stdout.write(f"{text}\n")


def _date_range(args: argparse.Namespace) -> tuple[str, str, str]:
    if not args.year:
        return "", "", ""
    return f"{args.year}-01-01", f"{args.year}-12-31", CUSTOM_RANGE_FILTER


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

    logger.info(
        "No fresh verified proxies in %s; collecting before scraping (max %.0fs)",
        db_path,
        settings.proxy_startup_timeout_seconds,
    )
    async with proxy_collector(db_path) as process:
        fresh = await wait_for_proxies(
            db_path,
            process,
            required=settings.proxy_min_ready,
            timeout=settings.proxy_startup_timeout_seconds,
            max_age_hours=settings.proxy_db_max_age_hours,
        )
        if not fresh:
            if settings.proxy_mode == "always":
                raise RuntimeError("Proxy collection produced no fresh verified proxies")
            logger.warning("Proxy collection produced no usable proxies; continuing with direct requests")
            return await _run_scraper(args, None)
        logger.info("Starting scrape with %d fresh verified proxies", fresh)
        return await _run_scraper(args, db_path)


async def _run_scraper(args: argparse.Namespace, proxy_db: Path | None) -> list[dict[str, Any]]:
    client_config = SeprecClient.config_from_settings(proxy_db_path=proxy_db)
    async with SeprecScraper(client_config=client_config) as scraper:
        return await _execute_scrape(scraper, args)


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
