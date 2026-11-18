"""Download already-issued RES certificate URLs using a bounded direct HTTP queue."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import sys
import time
from collections import Counter
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx

from src.sites.dequienes.source import res_url, validate_cve
from src.sites.res.document_lock import document_lock
from src.sites.res.http_flow import CODIGO_PATTERN, daily_document_limit
from src.sites.res.pdf import MAX_PDF_BYTES, cached_pdf, persist_pdf
from src.sites.res.tracking import TrackingStore


class DownloadStop(Exception):
    """The service denied access or asked the client to stop."""


class ExpiredLink(Exception):
    """The issued link is no longer usable."""


def certificate_url(cve: str, link: str) -> str:
    url = res_url(link)
    parts = urlsplit(url)
    query = parse_qs(parts.query)
    codes = query.get("codigo", [])
    if (
        parts.path.casefold() != "/certificados/escritura.aspx"
        or query.get("cve") != [validate_cve(cve)]
        or len(codes) != 1
        or not CODIGO_PATTERN.fullmatch(codes[0])
    ):
        raise ValueError("Unexpected certificate link")
    return url


async def fetch_pdf(client: httpx.AsyncClient, url: str) -> bytes:
    async with asyncio.timeout(45), client.stream("GET", url, follow_redirects=False) as response:
        code = response.status_code
        if code in {401, 403, 429}:
            raise DownloadStop(f"HTTP {code} from RES")
        if code in {302, 404, 410}:
            raise ExpiredLink(f"HTTP {code} from RES")
        response.raise_for_status()
        if code != 200:
            raise ValueError(f"Unexpected certificate HTTP status {code}")
        content_type = response.headers.get("content-type", "").split(";")[0].strip().lower()
        if content_type != "application/pdf":
            sample = bytearray()
            async for chunk in response.aiter_bytes(chunk_size=4096):
                sample.extend(chunk)
                if len(sample) >= 65536:
                    break
            if daily_document_limit(sample.decode("utf-8", errors="replace")):
                raise DownloadStop("RES daily document limit reached")
            raise ValueError("RES did not return a PDF")
        body = bytearray()
        async for chunk in response.aiter_bytes(chunk_size=65536):
            if len(body) + len(chunk) > MAX_PDF_BYTES:
                raise ValueError("Certificate exceeds the PDF size limit")
            body.extend(chunk)
    return bytes(body)


def candidate_page(store, after):
    return store.db.execute(
        """SELECT d.cve FROM documents d
           WHERE d.cve>? AND d.download_url IS NOT NULL AND d.status NOT IN ('expired_saved_link','expired_codigo')
             AND EXISTS (
                 SELECT 1 FROM company_documents cd
                 JOIN run_companies rc ON rc.company_id=cd.company_id AND rc.run_id=?
                 WHERE cd.cve=d.cve
             )
           ORDER BY d.cve LIMIT 256""",
        (after, store.run_id),
    ).fetchall()


def mark_unusable_link(store, cve, link):
    with store.db:
        store.db.execute(
            "UPDATE documents SET status='expired_saved_link', updated_at=? WHERE cve=? AND download_url=?",
            (time.time(), cve, link),
        )


async def download_ready(store, client, *, limit=100, workers=2, interval=1.0, progress=None, progress_interval=30):
    """Drain a bounded slice of the persistent link queue. Never verify or generate a certificate."""
    if not store.db.execute("SELECT 1 FROM runs WHERE run_id=?", (store.run_id,)).fetchone():
        raise ValueError("Unknown RES run")
    started = time.monotonic()
    counts: Counter = Counter()
    stop_reason = None
    destination = store.base / "ready-downloads"
    rate_lock = asyncio.Lock()
    next_request = 0.0
    inspected = 0

    def can_request():
        return stop_reason is None and (limit is None or counts["requested"] < limit)

    async def process(cve):
        nonlocal stop_reason, next_request, inspected
        async with document_lock(store.path, cve):
            if not can_request():
                return
            inspected += 1
            if store.existing_file(cve):
                counts["reused"] += 1
                return
            recovered = cached_pdf(destination, cve)
            if recovered:
                store.file_result(recovered, destination)
                counts["reused"] += 1
                return
            row = store.db.execute("SELECT download_url,status FROM documents WHERE cve=?", (cve,)).fetchone()
            if not row or not row["download_url"] or row["status"] in {"expired_saved_link", "expired_codigo"}:
                return
            link = row["download_url"]
            try:
                url = certificate_url(cve, link)
            except ValueError:
                mark_unusable_link(store, cve, link)
                counts["invalid_link"] += 1
                return
            try:
                async with rate_lock:
                    await asyncio.sleep(max(0.0, next_request - time.monotonic()))
                    if not can_request():
                        return
                    counts["requested"] += 1
                    next_request = time.monotonic() + interval
                data = await fetch_pdf(client, url)
            except DownloadStop as error:
                stop_reason = str(error)
            except ExpiredLink:
                mark_unusable_link(store, cve, link)
                counts["expired"] += 1
            except (httpx.HTTPError, TimeoutError, ValueError):
                counts["failed"] += 1
            else:
                try:
                    saved = persist_pdf(destination, cve, data, url)
                except ValueError:
                    counts["failed"] += 1
                    return
                store.file_result(saved, destination)
                counts["downloaded"] += 1
                counts["bytes"] += saved["size_bytes"]

    def snapshot():
        elapsed = time.monotonic() - started
        return {
            "run_id": store.run_id,
            "inspected": inspected,
            **{
                name: counts[name]
                for name in ("requested", "downloaded", "reused", "expired", "invalid_link", "failed", "bytes")
            },
            "elapsed_seconds": round(elapsed, 3),
            "pdfs_per_minute": round(counts["downloaded"] * 60 / elapsed, 2) if elapsed else 0,
            "stop_reason": stop_reason,
        }

    async def report_progress():
        while True:
            await asyncio.sleep(progress_interval)
            progress(snapshot())

    reporter = asyncio.create_task(report_progress()) if progress is not None else None
    try:
        after = ""
        while can_request():
            rows = candidate_page(store, after)
            if not rows:
                break
            for offset in range(0, len(rows), workers):
                if not can_request():
                    break
                current = rows[offset : offset + workers]
                async with asyncio.TaskGroup() as batch:
                    for row in current:
                        batch.create_task(process(row["cve"]))
            after = rows[-1]["cve"]
        return snapshot()
    finally:
        if reporter is not None:
            reporter.cancel()
            await asyncio.gather(reporter, return_exceptions=True)


def log_progress(stats):
    print(
        f"RES download-ready: прошло {stats['elapsed_seconds']:.0f} с, "
        f"проверено ссылок {stats['inspected']}, HTTP-запросов {stats['requested']}, "
        f"новых PDF {stats['downloaded']}, из кэша {stats['reused']}, "
        f"просрочено {stats['expired']}, неверных ссылок {stats['invalid_link']}, "
        f"ошибок {stats['failed']}, скорость {stats['pdfs_per_minute']:.2f} PDF/мин",
        file=sys.stderr,
        flush=True,
    )


async def run(args):
    limit = args.limit if args.limit is not None else "all"
    print(f"RES download-ready: запуск {args.run_id}, workers={args.workers}, limit={limit}", file=sys.stderr, flush=True)
    store = TrackingStore(args.db, args.run_id)
    try:
        async with httpx.AsyncClient(timeout=20, trust_env=False) as client:
            result = await download_ready(store, client, limit=args.limit, workers=args.workers, progress=log_progress)
    finally:
        store.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 2 if result["stop_reason"] or result["failed"] else 0


def request_limit(value):
    if value == "all":
        return None
    try:
        limit = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("Use a positive request limit or 'all'") from error
    if limit < 1:
        raise argparse.ArgumentTypeError("Use a positive request limit or 'all'")
    return limit


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("data/res-benchmark/tracking.sqlite3"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--limit", type=request_limit, default=100, help="Max HTTP requests, or all ready links")
    parser.add_argument("--workers", type=int, choices=(1, 2), default=2)
    args = parser.parse_args(argv)
    if not args.db.is_file():
        parser.error("An existing tracking database is required")
    database = args.db.resolve()
    with database.with_name(database.name + ".ready-download.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("A saved-link downloader already uses this tracking database")
        return asyncio.run(run(args))
