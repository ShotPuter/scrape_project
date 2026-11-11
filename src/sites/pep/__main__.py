"""Run the PEP snapshot-to-local-documents pipeline."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sqlite3
from pathlib import Path
from urllib.parse import urlsplit

from src.sites.pep.browser_download import BrowserDownloader, validate_cdp_url
from src.sites.pep.discovery import DiscoveryClient, TLSPreflightError
from src.sites.pep.documents import MAX_PDF_BYTES, DocumentDownloader, inspect_saved, save_payload
from src.sites.pep.source import RESULT_PATH, parse_results, safe_url
from src.sites.pep.workbook import LINK_COLUMN, read_rows
from src.utils.atomic import atomic_text_writer, atomic_write_json


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def setup_database(path: Path, workbook: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    db = sqlite3.connect(path)
    os.chmod(path, 0o600)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS rows (number INTEGER PRIMARY KEY, data TEXT NOT NULL, link TEXT);
        CREATE TABLE IF NOT EXISTS jobs (link TEXT PRIMARY KEY, status TEXT NOT NULL, result TEXT NOT NULL);
    """)
    digest = sha256_file(workbook)
    current = db.execute("SELECT value FROM meta WHERE key='workbook_sha256'").fetchone()
    if current and current[0] != digest:
        db.close()
        raise ValueError("Output belongs to a different workbook; use a new output directory")
    if not current:
        with db:
            for number, row in read_rows(workbook):
                link = row.get(LINK_COLUMN)
                if link:
                    link = safe_url(str(link))
                    if urlsplit(link).path != RESULT_PATH:
                        raise ValueError(f"Unexpected consultation path in row {number}")
                db.execute("INSERT INTO rows VALUES (?, ?, ?)", (number, json.dumps(row, ensure_ascii=False), link))
                if link:
                    db.execute("INSERT OR IGNORE INTO jobs VALUES (?, 'pending', '[]')", (link,))
            db.execute("INSERT INTO meta VALUES ('workbook_sha256', ?)", (digest,))
    return db


def export_json(db: sqlite3.Connection, destination: Path) -> None:
    query = """SELECT rows.number, rows.data, jobs.status, jobs.result FROM rows
               LEFT JOIN jobs ON rows.link = jobs.link ORDER BY rows.number"""
    with atomic_text_writer(destination, preserve_mode=False) as out:
        out.write('[\n')
        first = True
        for number, raw, status, result in db.execute(query):
            if not first:
                out.write(',\n')
            first = False
            json.dump({"source_row_number": number, "source_row": json.loads(raw),
                       "processing_status": status or "missing_link",
                       "documents": json.loads(result) if result else []}, out, ensure_ascii=False)
        out.write('\n]\n')


async def collect_person(context, url: str, output: Path, timeout_ms: int, delay: float) -> tuple[str, list[dict]]:
    from playwright.async_api import Error as BrowserError

    page = await context.new_page()
    documents: dict[str, dict] = {}
    visited: set[str] = set()
    try:
        while url:
            if url in visited:
                raise ValueError("Pagination loop")
            visited.add(url)
            response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            if response and response.status != 200:
                return f"discovery_failed_http_{response.status}", list(documents.values())
            if not response:
                raise ValueError("Source page not available")
            safe_url(page.url)
            if urlsplit(page.url).path != RESULT_PATH:
                raise ValueError("Unexpected redirect")
            entries, next_url = parse_results(await page.content(), page.url)
            for doc in entries:
                doc_id = doc["id"]
                if doc_id in documents:
                    continue
                doc.update({"local_path": None, "s3_uri": None, "source_page": url,
                            "status": "failed", "publication_year": doc["publication_date"][:4] or None})
                metadata = await asyncio.to_thread(inspect_saved, output, doc_id)
                if metadata:
                    doc.update(metadata)
                    documents[doc_id] = doc
                    continue
                try:
                    # Only click the site's own form. CAPTCHA is handled by the site,
                    # never bypassed or forged by the scraper.
                    form = page.locator('form[action$="/generarReporteConsultaCiudadana"]').filter(
                        has=page.locator(f'input[name="id"][value="{doc_id}"]')
                    )
                    async with page.expect_download(timeout=timeout_ms) as event:
                        await form.locator('button').click(timeout=timeout_ms)
                    download = await event.value
                    temp = await download.path()
                    if temp is None or (await asyncio.to_thread(Path(temp).stat)).st_size > MAX_PDF_BYTES:
                        raise ValueError("Download unavailable or exceeds size limit")
                    data = await asyncio.to_thread(Path(temp).read_bytes)
                    doc.update(await asyncio.to_thread(save_payload, data, output, doc_id))
                except (BrowserError, ValueError, OSError):
                    doc["status"] = "download_failed"
                documents[doc_id] = doc
                if delay:
                    await asyncio.sleep(delay)
            url = next_url
            if delay and url:
                await asyncio.sleep(delay)
        status = "completed" if all(d["status"] == "downloaded" for d in documents.values()) else "partial"
        return status, list(documents.values())
    except BrowserError as exc:
        if "ERR_CERT_" in str(exc):
            return "discovery_failed_tls", list(documents.values())
        return "discovery_failed", list(documents.values())
    except (ValueError, OSError):
        return "discovery_failed", list(documents.values())
    finally:
        await page.close()


async def run(args: argparse.Namespace) -> int:
    args.output.mkdir(parents=True, exist_ok=True, mode=0o700)
    db = setup_database(args.output / "state.sqlite3", args.workbook)
    try:
        if args.import_only:
            export_json(db, args.output / "records.json")
            return 0
        if not args.download_browser:
            return await discover_jobs(db, args)
        jobs = [row[0] for row in db.execute("SELECT link FROM jobs WHERE status != 'completed' ORDER BY rowid")]
        if args.limit is not None:
            jobs = jobs[:args.limit]
        queue: asyncio.Queue[str] = asyncio.Queue()
        for job in jobs:
            queue.put_nowait(job)
        from playwright.async_api import async_playwright

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=not args.headed, channel=args.browser_channel)
            context = await browser.new_context(accept_downloads=True, locale="es-CO")

            async def worker() -> None:
                while True:
                    try:
                        link = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    try:
                        status, documents = await collect_person(context, link, args.output, args.timeout * 1000, args.delay)
                        with db:
                            db.execute("UPDATE jobs SET status=?, result=? WHERE link=?",
                                       (status, json.dumps(documents, ensure_ascii=False), link))
                    finally:
                        queue.task_done()

            try:
                await asyncio.gather(*(worker() for _ in range(min(args.workers, len(jobs)))))
            finally:
                await context.close()
                await browser.close()
        return int(db.execute("SELECT count(*) FROM jobs WHERE status NOT IN ('pending','completed')").fetchone()[0] > 0)
    finally:
        export_json(db, args.output / "records.json")
        atomic_write_json(args.output / "summary.json", {
            "jobs": dict(db.execute("SELECT status, count(*) FROM jobs GROUP BY status")),
            "source_rows": db.execute("SELECT count(*) FROM rows").fetchone()[0],
        })
        db.close()


async def discover_jobs(db: sqlite3.Connection, args: argparse.Namespace) -> int:
    # Keyset paging keeps memory bounded even for large input snapshots.
    queue: asyncio.Queue[tuple[int, str] | None] = asyncio.Queue(maxsize=args.workers * 2)
    client = DiscoveryClient(workers=args.workers, delay=args.delay, timeout=args.timeout, ca_bundle=args.ca_bundle)
    downloader = DocumentDownloader(client, args.output)
    browser_downloader = None
    if getattr(args, "hybrid", False):
        browser_class = BrowserDownloader
        if getattr(args, "browser_engine", "native") == "nodriver":
            from src.sites.pep.nodriver_download import NodriverDownloader

            browser_class = NodriverDownloader
        browser_downloader = browser_class(
            output=args.output, profile=args.browser_profile or args.output / "browser-profile",
            channel=args.browser_channel or "chrome", headed=args.headed, cdp_url=args.cdp_url,
            timeout_ms=args.timeout * 1000, pace=client.pace,
            native=getattr(args, "native_chrome", False),
            headless=getattr(args, "headless", False),
        )
    failed = False
    stop = asyncio.Event()

    async def produce() -> None:
        last = 0
        remaining = args.limit
        while remaining is None or remaining > 0:
            if stop.is_set():
                break
            status_filter = "AND status NOT IN ('completed','discovered')" if args.discover_only else ""
            batch = db.execute(
                f"SELECT rowid, link FROM jobs WHERE rowid > ? {status_filter} "
                "ORDER BY rowid LIMIT ?", (last, min(100, remaining) if remaining is not None else 100)
            ).fetchall()
            if not batch:
                break
            for number, link in batch:
                if stop.is_set():
                    break
                await queue.put((number, link))
                last = number
                if remaining is not None:
                    remaining -= 1
        for _ in range(args.workers):
            await queue.put(None)

    async def worker() -> None:
        nonlocal failed
        while True:
            item = await queue.get()
            try:
                if item is None:
                    return
                number, link = item
                if stop.is_set():
                    continue
                prior_status, raw = db.execute("SELECT status,result FROM jobs WHERE link=?", (link,)).fetchone()
                if prior_status in {"discovered", "completed", "partial"} and not args.discover_only:
                    status, documents = "discovered", json.loads(raw)
                else:
                    status, documents = await client.discover(link)
                previous = json.loads(db.execute("SELECT result FROM jobs WHERE link=?", (link,)).fetchone()[0])
                merged = {doc["id"]: doc for doc in previous}
                for doc in documents:
                    if merged.get(doc["id"], {}).get("status") != "downloaded":
                        merged[doc["id"]] = doc
                if not args.discover_only:
                    for doc_id, doc in merged.items():
                        merged[doc_id] = await downloader.download(doc)
                        if browser_downloader and merged[doc_id]["status"] == "captcha_required":
                            merged[doc_id] = await browser_downloader.download(merged[doc_id])
                        # Persist each finished document, not just the whole person.
                        with db:
                            db.execute("UPDATE jobs SET result=? WHERE link=?",
                                       (json.dumps(list(merged.values()), ensure_ascii=False), link))
                    if status == "discovered":
                        status = "completed" if all(d["status"] == "downloaded" for d in merged.values()) else "partial"
                with db:
                    db.execute("UPDATE jobs SET status=?,result=? WHERE link=?",
                               (status, json.dumps(list(merged.values()), ensure_ascii=False), link))
                failed |= status not in {"discovered", "completed"}
                if status in {"discovery_failed_tls", "discovery_failed_http_403"} or any(
                    doc["status"] == "browser_session_blocked" for doc in merged.values()
                ):
                    stop.set()
                if status not in {"discovered", "completed"} or number % 100 == 0:
                    print(f"PEP job {number}: {status}; documents={len(merged)}", flush=True)
            finally:
                queue.task_done()

    try:
        # Fail once before jobs are modified, rather than reporting TLS for each row.
        await client.preflight(args.output)
        async with asyncio.TaskGroup() as group:
            group.create_task(produce())
            for _ in range(args.workers):
                group.create_task(worker())
    finally:
        try:
            if browser_downloader:
                await browser_downloader.close()
        finally:
            await client.close()
    return int(failed)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import PEP workbook and download available Ley 2013 PDFs locally")
    parser.add_argument("workbook", type=Path)
    parser.add_argument("--output", type=Path, default=Path("data/pep"))
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--timeout", type=int, default=30, help="Per browser operation timeout in seconds")
    parser.add_argument("--limit", type=int, help="Maximum unique links in this run")
    parser.add_argument("--headed", action="store_true", help="Show browser for normal site interactions")
    parser.add_argument("--headless", action="store_true", help="Native Chrome without a display or visible window")
    parser.add_argument(
        "--hybrid", action="store_true", help="HTTP discovery with native browser fallback for CAPTCHA forms"
    )
    parser.add_argument("--cdp-url", help="Attach to an existing local Chrome debugging endpoint (enables hybrid mode)")
    parser.add_argument(
        "--native-chrome", action="store_true", help="Automatically launch ordinary Chrome for hybrid downloads"
    )
    parser.add_argument(
        "--browser-engine", choices=["native", "nodriver"], default="native",
        help="Browser fallback engine for --hybrid (nodriver requires Xvfb and one worker)",
    )
    parser.add_argument("--browser-profile", type=Path, help="Dedicated persistent Chrome profile for hybrid downloads")
    parser.add_argument(
        "--browser-channel", choices=["chrome", "msedge"], help="Use an installed browser for browser downloads"
    )
    parser.add_argument(
        "--download-browser", action="store_true", help="Opt in to browser PDF downloads instead of HTTP collection"
    )
    parser.add_argument(
        "--ca-bundle", type=Path, help="Trusted PEM CA bundle for HTTP discovery; verification stays enabled"
    )
    parser.add_argument("--import-only", action="store_true", help="Import workbook and write pending JSON without network")
    parser.add_argument("--discover-only", action="store_true", help="Find declarations without attempting local downloads")
    args = parser.parse_args()
    if args.headless and args.headed:
        parser.error("--headless and --headed are mutually exclusive")
    if args.headless and not args.native_chrome:
        parser.error("--headless currently requires --native-chrome")
    if args.native_chrome:
        if args.cdp_url:
            parser.error("--native-chrome cannot be combined with --cdp-url")
        args.hybrid = True
    if args.browser_engine == "nodriver":
        if args.cdp_url or args.headless:
            parser.error("nodriver requires its own headed Chrome under Xvfb; no --cdp-url/--headless")
        if args.workers != 1:
            parser.error("nodriver requires --workers 1")
        args.hybrid = True
    if args.cdp_url:
        try:
            validate_cdp_url(args.cdp_url)
        except ValueError as exc:
            parser.error(str(exc))
        args.hybrid = True
    if args.hybrid and (args.discover_only or args.download_browser):
        parser.error("--hybrid cannot be combined with --discover-only or --download-browser")
    if args.headed and not (args.download_browser or args.hybrid):
        parser.error("--headed requires --download-browser or --hybrid")
    if args.browser_profile and not args.hybrid:
        parser.error("--browser-profile requires --hybrid")
    if args.discover_only and args.download_browser:
        parser.error("--discover-only cannot be combined with --download-browser")
    if args.ca_bundle and not args.ca_bundle.is_file():
        parser.error("--ca-bundle must be an existing PEM file")
    if args.workers < 1 or args.delay < 0 or args.timeout < 1 or (args.limit is not None and args.limit < 1):
        parser.error("workers, timeout and limit must be positive; delay must be non-negative")
    try:
        exit_code = asyncio.run(run(args))
    except TLSPreflightError as exc:
        print(f"PEP startup failed: {exc}", flush=True)
        exit_code = 1
    except KeyboardInterrupt:
        exit_code = 130
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
