"""Non-destructive import of legacy bulk checkpoints and validated certificate files."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path

from src.sites.dequienes.source import res_url, validate_cve
from src.sites.res.pdf import certificate_directory, pdf_metadata, read_pdf
from src.sites.res.tracking import TrackingStore
from src.utils.atomic import atomic_write_bytes, atomic_write_json


def import_bulk(source: Path, target: Path, run_id: str) -> dict[str, int]:
    counts = {"files": 0, "reused": 0, "invalid": 0, "days": 0}
    if source.resolve() == target.resolve() or not source.is_file():
        return counts
    store = TrackingStore(target, run_id)
    try:
        with closing(sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)) as old:
            old.row_factory = sqlite3.Row
            tables = {r[0] for r in old.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not {"documents", "document_files"} <= tables:
                return counts
            with store.db:
                for row in old.execute("SELECT * FROM documents"):
                    store.db.execute(
                        "INSERT INTO documents(cve,status,codigo,download_url,verified_at,error,updated_at) "
                        "VALUES(?,?,?,?,?,?,?) ON CONFLICT(cve) DO UPDATE SET "
                        "codigo=COALESCE(documents.codigo,excluded.codigo),"
                        "download_url=COALESCE(documents.download_url,excluded.download_url),"
                        "verified_at=COALESCE(documents.verified_at,excluded.verified_at)",
                        (
                            row["cve"],
                            "verified" if row["download_url"] else "pending",
                            row["codigo"],
                            row["download_url"],
                            row["verified_at"],
                            row["error"],
                            row["updated_at"],
                        ),
                    )
            if "date_searches" in tables:
                for row in old.execute("SELECT day,kind,actions_json,complete FROM date_searches WHERE complete=1"):
                    store.day_result(row["day"], row["kind"], json.loads(row["actions_json"]), complete=True)
                    counts["days"] += 1
            destination = store.base / "imported" / hashlib.sha256(str(source.resolve()).encode()).hexdigest()[:12]
            for row in old.execute("SELECT f.*,d.download_url FROM document_files f JOIN documents d USING(cve)"):
                if store.existing_file(row["cve"]):
                    counts["reused"] += 1
                    continue
                try:
                    cve = validate_cve(row["cve"])
                    original = (source.parent / row["local_path"]).resolve()
                    if not original.is_relative_to(source.parent.resolve()):
                        raise ValueError("Legacy PDF escapes its data directory")
                    data = read_pdf(original)
                    metadata = pdf_metadata(data)
                    if metadata["sha256"] != row["sha256"] or metadata["size_bytes"] != row["size_bytes"]:
                        raise ValueError("Legacy PDF checksum mismatch")
                    relative = certificate_directory(cve) / f"{metadata['sha256']}.pdf"
                    file = destination / relative
                    file.parent.mkdir(parents=True, exist_ok=True)
                    saved = {
                        "cve": cve,
                        "status": "downloaded",
                        **metadata,
                        "local_path": relative.as_posix(),
                        "download_url": res_url(row["download_url"]),
                        "downloaded_at": row["downloaded_at"],
                    }
                    if file.exists():
                        if pdf_metadata(read_pdf(file)) != metadata:
                            raise ValueError("Imported PDF checksum mismatch")
                    else:
                        try:
                            os.link(original, file)
                        except OSError:
                            atomic_write_bytes(file, data)
                    atomic_write_json(destination / certificate_directory(cve) / "manifest.json", saved)
                    store.file_result(saved, destination)
                except (OSError, ValueError, TypeError):
                    counts["invalid"] += 1
                    continue
                counts["files"] += 1
                if counts["files"] % 100 == 0:
                    print(f"RES daily import: {counts['files']} PDFs preserved from {source}", flush=True)
    finally:
        store.close()
    return counts


def recover_worker_files(target: Path, run_id: str) -> int:
    """Recover a PDF written before a crash between manifest and ledger commit."""
    from src.sites.res.pdf import cached_pdf

    store = TrackingStore(target, run_id)
    recovered = 0
    try:
        for manifest in target.parent.glob("*/workers/w*/pdf/*/manifest.json"):
            try:
                row = json.loads(manifest.read_text())
                cve = validate_cve(row["cve"])
                if store.existing_file(cve):
                    continue
                directory = manifest.parents[2]
                saved = cached_pdf(directory, cve)
                if saved:
                    store.file_result(saved, directory)
                    recovered += 1
            except (OSError, ValueError, KeyError, TypeError):
                continue
    finally:
        store.close()
    return recovered


def legacy_sources(args) -> list[Path]:
    base = args.output.parent / "res-bulk"
    candidates = [
        *args.import_db,
        base / os.getenv("RES_CONTINUOUS_RUN_ID", "res-continuous") / "tracking.sqlite3",
        base / "continuous" / "tracking.sqlite3",
        base / "tracking.sqlite3",
    ]
    return list(dict.fromkeys(p.resolve() for p in candidates if p.is_file()))
