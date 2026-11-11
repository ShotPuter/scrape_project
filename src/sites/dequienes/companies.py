"""Collect company names from DeQuienes' public Diario Oficial archive."""

from __future__ import annotations

import argparse
import asyncio
import csv
import fcntl
import hashlib
import re
import sqlite3
import unicodedata
import xml.etree.ElementTree as ET
from contextlib import ExitStack, contextmanager
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

from src.utils.atomic import atomic_text_writer, atomic_write_json, atomic_write_text

BASE = "https://dequienes.cl"
INDEX = BASE + "/diario-oficial/sitemap-index.xml"
MONTH_PATH = re.compile(r"/diario-oficial/(\d{4}/\d{2})/sitemap\.xml")
ARTICLE_PATH = re.compile(r"/diario-oficial/(\d{4}/\d{2}/\d{2})/[^/]+")


def source_url(url: str) -> str:
    parts = urlsplit(url)
    if (
        parts.scheme != "https"
        or parts.hostname != "dequienes.cl"
        or parts.port not in (None, 443)
        or parts.username
        or parts.password
        or parts.query
        or parts.fragment
    ):
        raise ValueError("Unexpected archive URL")
    if parts.path != "/diario-oficial/sitemap-index.xml" and not (
        MONTH_PATH.fullmatch(parts.path) or ARTICLE_PATH.fullmatch(parts.path)
    ):
        raise ValueError("Unexpected archive path")
    return url


def month_value(value: str) -> str:
    if value.isascii() and value.isdecimal() and 1 <= int(value) <= 12:
        return str(int(value))
    if not re.fullmatch(r"\d{4}-\d{2}", value):
        raise argparse.ArgumentTypeError("Month must be YYYY-MM")
    try:
        date.fromisoformat(value + "-01")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Invalid month") from exc
    return value


def select_months(available: dict[str, str], args) -> list[str]:
    selected = sorted(available, reverse=True)
    if args.month:
        requested = []
        for raw_month in args.month:
            month = raw_month
            if month.isdecimal():
                if args.year is None:
                    raise ValueError("Numeric --month requires --year")
                month = f"{args.year:04d}-{int(month):02d}"
            if args.year is not None and not month.startswith(f"{args.year:04d}-"):
                raise ValueError("--month and --year refer to different years")
            requested.append(month)
        missing = set(requested) - available.keys()
        if missing:
            raise ValueError(f"Months not present in archive: {sorted(missing)}")
        selected = [month for month in selected if month in requested]
    elif args.year is not None:
        selected = [month for month in selected if month.startswith(f"{args.year:04d}-")]
        if not selected:
            raise ValueError(f"Year {args.year} is not present in the archive")
    elif not args.all_months:
        selected = selected[:1]
    return selected


def parse_sitemap(raw: str, *, index: bool) -> list[str]:
    root = ET.fromstring(raw)
    namespace = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
    if root.tag != namespace + ("sitemapindex" if index else "urlset"):
        raise ValueError("Unexpected sitemap document (challenge or changed format)")
    links, seen = [], set()
    for node in root.findall(f"{namespace}{'sitemap' if index else 'url'}/{namespace}loc"):
        url = source_url((node.text or "").strip())
        pattern = MONTH_PATH if index else ARTICLE_PATH
        if not pattern.fullmatch(urlsplit(url).path):
            raise ValueError("Unexpected sitemap entry")
        if url not in seen:
            seen.add(url)
            links.append(url)
    if not links:
        raise ValueError("Sitemap has no entries")
    return links


def parse_company(html: str, url: str) -> dict:
    source_url(url)
    match = ARTICLE_PATH.fullmatch(urlsplit(url).path)
    if not match:
        raise ValueError("Not a publication URL")
    publication_date = datetime.strptime(match[1], "%Y/%m/%d").date().isoformat()
    soup = BeautifulSoup(html, "html.parser")
    headings = soup.select("h1")
    if len(headings) != 1:
        raise ValueError("Publication heading missing or ambiguous")
    heading = " ".join(headings[0].get_text(" ", strip=True).split())
    dates = [h.get_text(" ", strip=True) for h in soup.select("h2") if "Publicado el" in h.get_text()]
    actual_date = datetime.strptime(dates[0].split("Publicado el", 1)[1].strip(), "%d/%m/%Y").date() if dates else None
    if actual_date is None or actual_date.isoformat() != publication_date:
        raise ValueError("Publication date missing or does not match URL")
    rut_match = re.search(r"\s+(\d{1,2}(?:\.\d{3}){2}-[\dkK])\s*\*?$", heading)
    name = heading[: rut_match.start()].strip() if rut_match else heading
    if not name:
        raise ValueError("Empty company name")
    return {
        "company_name": name,
        "name_key": unicodedata.normalize("NFKC", name).casefold(),
        "rut": rut_match[1] if rut_match else "",
        "publication_title": heading,
        "publication_date": publication_date,
        "source_url": url,
    }


def open_database(path: Path) -> sqlite3.Connection:
    """Legacy storage API; pipeline.QueueStore performs the additive migration."""
    db = sqlite3.connect(path)
    db.executescript("""CREATE TABLE IF NOT EXISTS publications (
        source_url TEXT PRIMARY KEY, month TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
        company_name TEXT, name_key TEXT, rut TEXT, publication_title TEXT, publication_date TEXT, error TEXT);""")
    return db


def export(db: sqlite3.Connection, output: Path) -> None:
    columns = ["company_name", "rut", "publication_date", "source_url", "publication_title"]
    with atomic_text_writer(output / "publications.csv", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(
            db.execute(f"SELECT {','.join(columns)} FROM publications WHERE status='collected' ORDER BY source_url")
        )
    with atomic_text_writer(output / "companies.csv", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["company_name", "publication_count", "first_publication", "last_publication", "example_url"])
        writer.writerows(
            db.execute("""SELECT min(company_name),count(*),min(publication_date),max(publication_date),
            min(source_url) FROM publications WHERE status='collected' GROUP BY name_key ORDER BY name_key""")
        )
    atomic_write_json(
        output / "summary.json",
        {
            "statuses": dict(db.execute("SELECT status,count(*) FROM publications GROUP BY status")),
            "unique_names": db.execute(
                "SELECT count(DISTINCT name_key) FROM publications WHERE status='collected'"
            ).fetchone()[0],
            "deduplication": "Unicode-normalized case-insensitive company name; not a legal-entity identity match",
        },
    )


async def fetch(page, url: str, args, *, sitemap_index: bool | None = None):
    error = None
    attempts = getattr(args, "fetch_attempts", 3)
    for attempt in range(attempts):
        try:
            gate = getattr(args, "request_gate", None)
            if gate:
                await gate()
            response = await page.goto(source_url(url), wait_until="domcontentloaded", timeout=args.timeout * 1000)
            source_url(page.url)
            if not response or response.status != 200:
                raise ValueError(f"HTTP {response.status if response else 'no response'}")
            if sitemap_index is not None:
                return parse_sitemap(await response.text(), index=sitemap_index)
            await page.locator("h1").first.wait_for(timeout=args.timeout * 1000)
            return parse_company(await page.content(), url)
        except Exception as exc:
            error = exc
            if attempt < attempts - 1:
                await asyncio.sleep(max(args.delay, 5))
    key = hashlib.sha256(url.encode()).hexdigest()[:16]
    atomic_write_text(args.output / "errors" / f"{key}.html", await page.content())
    raise ValueError(f"Unable to collect {url}: {error}") from error


async def collect_jobs(context, db: sqlite3.Connection, jobs: list[tuple[str]], args) -> int:
    """Compatibility helper for the former in-process batch API; CLI uses leased queues."""
    pending = iter(jobs)
    stopped = asyncio.Event()
    completed = failures = consecutive_failures = 0

    async def worker():
        nonlocal completed, failures, consecutive_failures
        page = await context.new_page()
        try:
            while not stopped.is_set():
                job = next(pending, None)
                if job is None:
                    return
                (url,) = job
                try:
                    item = await fetch(page, url, args)
                    with db:
                        db.execute(
                            """UPDATE publications SET status='collected',company_name=:company_name,
                            name_key=:name_key,rut=:rut,publication_title=:publication_title,
                            publication_date=:publication_date,error=NULL WHERE source_url=:source_url""",
                            item,
                        )
                    completed += 1
                    consecutive_failures = 0
                except Exception as exc:
                    completed += 1
                    failures += 1
                    consecutive_failures += 1
                    with db:
                        db.execute("UPDATE publications SET status='failed',error=? WHERE source_url=?", (str(exc), url))
                    if consecutive_failures >= 3:
                        stopped.set()
                if completed % 25 == 0:
                    export(db, args.output)
                if not stopped.is_set():
                    await asyncio.sleep(args.delay)
        finally:
            await page.close()

    async with asyncio.TaskGroup() as group:
        for _ in range(min(args.workers, len(jobs))):
            group.create_task(worker())
    return failures


@contextmanager
def exclusive_run(args):
    """Legacy whole-run guard. The pipeline locks profiles, allowing shared queues."""
    with ExitStack() as stack:
        for directory in sorted({args.output.resolve(), args.profile.resolve()}):
            directory.mkdir(parents=True, exist_ok=True)
            handle = stack.enter_context((directory / ".collector.lock").open("a"))
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ValueError(f"Another collector is using {directory}; stop it before restarting") from exc
        yield


@contextmanager
def exclusive_profile(profile: Path):
    profile.mkdir(parents=True, exist_ok=True)
    with (profile / ".collector.lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError(f"Another collector is using profile {profile}") from exc
        yield


async def run(args) -> int:
    from src.sites.dequienes.pipeline import run_public

    return await run_public(args)


def main() -> None:
    from src.sites.dequienes.pipeline import settings_from_args

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, help="Pipeline TOML configuration")
    parser.add_argument("--transport", choices=["hybrid", "browser"], help="Public transport (default: hybrid)")
    parser.add_argument("--session-refreshes", type=int, help="Automatic session refresh limit after bootstrap")
    parser.add_argument("--run-id", help="Join/resume a named run with a shared distinct-job limit")
    parser.add_argument("--retry-errors", action="store_true", help="Explicitly reset failed/blocked public jobs")
    parser.add_argument("--show-config", action="store_true", help="Validate/display settings without browser or database")
    parser.add_argument(
        "--rescan-legacy", action="store_true", help="Explicitly requeue old collected pages for CVE scanning"
    )
    parser.add_argument("--action", choices=["collect", "status", "export"], default="collect")
    parser.add_argument(
        "--import-publication",
        nargs=2,
        action="append",
        metavar=("URL", "HTML"),
        help="Import saved publication(s) without network/browser",
    )
    parser.add_argument("--month", action="append", type=month_value, help="Month 1–12 with --year, or YYYY-MM")
    parser.add_argument("--all-months", action="store_true", default=None)
    parser.add_argument("--year", type=int)
    parser.add_argument("--limit", type=int, help="Maximum distinct public jobs; 0 does no work, omitted is unlimited")
    parser.add_argument("--max-companies", type=int, help="Account-stage run limit, reserved for its worker")
    parser.add_argument("--max-documents", type=int, help="RES-stage run limit, reserved for its worker")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--workers", type=int, help="Concurrent HTTP workers or browser tabs (default: 4)")
    parser.add_argument("--max-in-flight", type=int, help="Public running-job cap shared through the database")
    parser.add_argument("--rate-per-minute", type=float, help="Public navigation rate shared through the database")
    parser.add_argument("--max-attempts", type=int)
    parser.add_argument("--delay", type=float)
    parser.add_argument("--timeout", type=float)
    args = parser.parse_args()
    try:
        settings = settings_from_args(args)
    except (ValueError, TypeError, OSError) as exc:
        parser.error(str(exc))
    if args.show_config:
        import json

        print(json.dumps(settings.display(), ensure_ascii=False, indent=2))
        return
    try:
        raise SystemExit(asyncio.run(run(args)))
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception as exc:
        message = str(exc)
        if "launch_persistent_context" in message:
            message = (
                "Camoufox could not start. Close the previous collector/browser using "
                f"profile {settings.profile}. Queue data are saved; retry after closing it."
            )
        print(f"DeQuienes: {message}", flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
