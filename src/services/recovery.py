"""Audit unique publication IDs and recover documents or explicitly labelled previews."""

from __future__ import annotations

import argparse
import asyncio
import csv
import sqlite3
import tempfile
from collections import Counter
from contextlib import asynccontextmanager, closing
from datetime import UTC, datetime
from pathlib import Path

from config import settings
from src.client.seprec_client import SeprecClient
from src.models.publication import PublicationSearchFilter, PublicationSummary
from src.services.category_resolver import MAIN_CATEGORY_NAMES
from src.services.pdf_generator import clean_html_text
from src.services.scraper import build_record
from src.storage.file_storage import FileStorage
from src.storage.integrity import SAVED_STATUSES
from src.storage.publication_csv import PublicationCsv
from src.utils.atomic import atomic_write_json, atomic_write_text
from src.utils.helpers import SeprecAPIError, StorageError, decode_base64_pdf


class AuditStore:
    """Durable page checkpoints, unique source IDs, and recovery outcomes."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS pages (
                category TEXT PRIMARY KEY, next_page INTEGER, total INTEGER,
                rows_seen INTEGER, complete INTEGER);
            CREATE TABLE IF NOT EXISTS publications (
                category TEXT, id TEXT, payload TEXT, PRIMARY KEY(category,id));
            CREATE TABLE IF NOT EXISTS outcomes (
                id TEXT PRIMARY KEY, status TEXT, error TEXT, updated_at TEXT);
        """)

    def close(self):
        self.db.close()

    def state(self, category):
        return self.db.execute(
            "SELECT next_page,total,rows_seen,complete FROM pages WHERE category=?", (category,)
        ).fetchone() or (1, 0, 0, 0)

    def save_page(self, category, page, total, rows):
        before = self.state(category)
        with self.db:
            self.db.executemany(
                "INSERT OR REPLACE INTO publications VALUES (?,?,?)",
                [(category, row.id, row.model_dump_json(by_alias=True)) for row in rows],
            )
            self.db.execute(
                "INSERT OR REPLACE INTO pages VALUES (?,?,?,?,0)", (category, page + 1, total, before[2] + len(rows))
            )

    def finish(self, category, total):
        state = self.state(category)
        if state[2] < total:
            raise RuntimeError(f"Category {category}: API returned an empty page before its advertised total")
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO pages VALUES (?,?,?,?,1)", (category, state[0], total, state[2]))

    def entries(self):
        return list(self.db.execute("SELECT category,id,payload FROM publications ORDER BY category,id"))

    def outcome(self, ident, status, error=None):
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO outcomes VALUES (?,?,?,?)", (ident, status, error, datetime.now(UTC).isoformat())
            )


def saved_ids(path):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return {row["id"] for row in csv.DictReader(stream) if row["status"] in SAVED_STATUSES}


def write_report(audit, directory, index):
    saved = saved_ids(index)
    found = set()
    categories = {}
    for category in ("1", "2", "3", "4"):
        ids = {row[0] for row in audit.db.execute("SELECT id FROM publications WHERE category=?", (category,))}
        found.update(ids)
        state = audit.state(category)
        categories[category] = {
            "api_total": state[1],
            "unique_ids_seen": len(ids),
            "rows_seen": state[2],
            "duplicate_rows": state[2] - len(ids),
            "scan_complete": bool(state[3]),
            "source_count_gap": state[1] - len(ids),
            "missing_in_index": len(ids - saved),
        }
    report = {
        "updated_at": datetime.now(UTC).isoformat(),
        "categories": categories,
        "scan_complete": all(value["scan_complete"] for value in categories.values()),
        "unique_source_ids_seen": len(found),
        "saved_unique_ids": len(saved),
        "missing_ids": sorted(found - saved),
        "saved_ids_not_seen": sorted(saved - found),
        "recovery_outcomes": dict(audit.db.execute("SELECT status,COUNT(*) FROM outcomes GROUP BY status")),
        "scope": (
            "IDs observed in API search; previews are excluded from the publication CSV; counts are not a frozen snapshot"
        ),
    }
    atomic_write_json(directory / "report.json", report)
    return report


def export_preview(directory, category, summary, status_code):
    """Preserve source metadata and resumen without presenting it as a full original."""
    if not summary.id.isdecimal():
        raise ValueError("Non-numeric publication ID")
    destination = directory / "text-previews" / summary.id
    payload = {
        "id": summary.id,
        "category": category,
        "http_status": status_code,
        "content_kind": "search_summary_only",
        "original_recovered": False,
        "full_text_verified": False,
        "captured_at": datetime.now(UTC).isoformat(),
        "source": f"{settings.api_base_url}/publicaciones",
        "search_result": summary.model_dump(by_alias=True),
    }
    atomic_write_json(destination.with_suffix(".json"), payload)
    atomic_write_text(
        destination.with_suffix(".txt"),
        "SEARCH PREVIEW ONLY / SOLO RESUMEN. Original file unavailable; full text NOT verified.\n\n"
        f"ID: {summary.id}\nCode: {summary.publication_code or ''}\n"
        f"Title: {summary.title or ''}\nDate: {summary.publication_date or ''}\n\n{summary.summary or ''}\n",
    )


def copy_proxy_database(destination):
    uri = settings.proxy_db_path.resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as source, closing(sqlite3.connect(destination)) as target:
        source.backup(target)


def recoverable_detail(detail):
    if clean_html_text(detail.full_text or "").strip():
        return True
    if detail.document_base64:
        try:
            decode_base64_pdf(detail.document_base64)
        except StorageError:
            return False
        return True
    return False


@asynccontextmanager
async def audit_client():
    # Pool health/dead-proxy files stay in a temporary directory, away from the live scraper.
    with tempfile.TemporaryDirectory(prefix="seprec-recovery-") as directory:
        proxy_db = Path(directory) / "proxies.db"
        await asyncio.to_thread(copy_proxy_database, proxy_db)
        config = SeprecClient.config_from_settings(settings, proxy_db_path=proxy_db)

        async def refresh():
            while True:
                await asyncio.sleep(60)
                try:
                    await asyncio.to_thread(copy_proxy_database, proxy_db)
                except (OSError, sqlite3.Error):
                    print("Proxy snapshot refresh failed; retaining last snapshot", flush=True)

        task = asyncio.create_task(refresh())
        try:
            async with SeprecClient(config=config) as client:
                yield client
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def scan(client, audit, directory):
    for category in ("1", "2", "3", "4"):
        page, total, _, complete = audit.state(category)
        if complete:
            continue
        while True:
            total, rows = await client.search_publications(
                PublicationSearchFilter(category=category, page=page, page_size=100)
            )
            if not rows:
                audit.finish(category, total)
                break
            audit.save_page(category, page, total, rows)
            if page % 50 == 0:
                print(f"Category {category}: page {page}; API total {total}", flush=True)
            page += 1
        report = write_report(audit, directory, settings.csv_export_path)
        print(f"Category {category}: {report['categories'][category]}", flush=True)


async def recover(client, audit, directory):
    if not all(audit.state(category)[3] for category in ("1", "2", "3", "4")):
        raise RuntimeError("Complete the audit before recovery")
    store = PublicationCsv()
    files = FileStorage()
    counts = Counter()
    missing = saved_ids(settings.csv_export_path)
    for category, ident, payload in audit.entries():
        if ident in missing:
            continue
        summary = PublicationSummary.model_validate_json(payload)
        try:
            detail = await client.get_publication_detail(ident)
            if not recoverable_detail(detail):
                export_preview(directory, category, summary, 200)
                audit.outcome(ident, "preview_only", "detail_has_no_valid_original_or_full_text")
                counts["preview_only"] += 1
                missing.add(ident)
                continue
            document = await asyncio.to_thread(files.save_publication_document, detail)
            record = build_record(detail, summary, category, MAIN_CATEGORY_NAMES[category], document)
            await store.save(record)
        except SeprecAPIError as error:
            code = error.status_code
            if code == 412:
                export_preview(directory, category, summary, code)
                audit.outcome(ident, "preview_only", "HTTP_412")
                counts["preview_only"] += 1
            else:
                audit.outcome(ident, "failed", f"HTTP_{code}")
                counts["failed"] += 1
        except Exception as error:  # noqa: BLE001 - keep a durable outcome and continue with other IDs
            audit.outcome(ident, "failed", type(error).__name__)
            counts["failed"] += 1
        else:
            audit.outcome(ident, "original_saved" if document.is_original else "generated_from_detail")
            counts["original_saved" if document.is_original else "generated_from_detail"] += 1
        missing.add(ident)
        if sum(counts.values()) % 10 == 0:
            print(f"Recovery: {dict(counts)}", flush=True)
            write_report(audit, directory, settings.csv_export_path)
    write_report(audit, directory, settings.csv_export_path)
    print(f"Recovery finished: {dict(counts)}", flush=True)


async def run(args):
    directory = settings.storage_base_dir / "recovery"
    directory.mkdir(parents=True, exist_ok=True)
    audit = AuditStore(directory / "audit.db")
    try:
        if args.restart_audit:
            with audit.db:
                audit.db.execute("DELETE FROM pages")
                audit.db.execute("DELETE FROM publications")
        async with audit_client() as client:
            if args.action == "audit":
                await scan(client, audit, directory)
            else:
                await recover(client, audit, directory)
        report = write_report(audit, directory, settings.csv_export_path)
        print(f"DONE. Missing observed IDs: {len(report['missing_ids'])}; report: {directory / 'report.json'}", flush=True)
    finally:
        audit.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("audit", "recover"))
    parser.add_argument(
        "--restart-audit", action="store_true", help="Start a new source scan; retain recovery outcomes/files"
    )
    args = parser.parse_args()
    if args.restart_audit and args.action != "audit":
        parser.error("--restart-audit is only valid with audit")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception as error:  # noqa: BLE001 - never print token-bearing URLs from transport errors
        print(f"Recovery stopped: {type(error).__name__}. Re-run to resume; inspect data/recovery/report.json.", flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
