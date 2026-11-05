"""Build a self-contained run from verified documents in earlier runs."""

from __future__ import annotations

import copy
import json
import os
import shutil
from pathlib import Path
from urllib.parse import urlsplit

from scripts.pep_camoufox_db import read_report
from src.sites.pep.documents import inspect_saved
from src.sites.pep.source import LEGACY_PATH, safe_url
from src.utils.atomic import atomic_write_json


def resume_history(root: Path, sample: list[dict]) -> dict:
    selected = {row["url"] for row in sample}
    history = {}
    # Root reports already incorporate their retry passes. Do not scan worker reports.
    folders = sorted(
        {path.parent for pattern in ("run-*/report.json", "run-*/state.sqlite3") for path in root.glob(pattern)}
    )
    for folder in folders:
        path = folder / "report.json"
        database = folder / "state.sqlite3"
        report = read_report(database) if database.exists() else json.loads(path.read_text())
        for job in report["jobs"]:
            if job["url"] not in selected:
                continue
            entry = history.setdefault(job["url"], {"documents": {}, "complete": False})
            if job["status"] in {"completed", "completed_with_unavailable"}:
                entry["complete"] = True
            for doc in job.get("documents", []):
                source = None
                if doc["status"] == "downloaded" and doc.get("local_path"):
                    pdf = (path.parent / doc["local_path"]).resolve()
                    if not pdf.is_relative_to(path.parent.resolve()):
                        raise ValueError(f"Saved PDF escapes run folder: {path}")
                    candidate = pdf.parent.parent
                    if inspect_saved(candidate, str(doc["id"])):
                        source = candidate
                unavailable = doc["status"] in {"http_404", "unavailable_http_404"} and bool(
                    doc.get("download_url") and urlsplit(safe_url(doc["download_url"])).path == LEGACY_PATH
                )
                key = str(doc["id"])
                if source or unavailable:
                    saved = copy.deepcopy(doc)
                    if unavailable:
                        saved["status"] = "unavailable_http_404"
                    entry["documents"][key] = (saved, source)
                elif key not in entry["documents"]:
                    entry["documents"][key] = (None, None)
    return history


def materialize_history(history: dict, sample: list[dict], output: Path):
    completed, reusable, unavailable = [], {}, set()
    for row in sample:
        entry = history.get(row["url"])
        if not entry:
            continue
        documents = []
        for doc_id, (saved_doc, source) in entry["documents"].items():
            if saved_doc is None:
                continue
            doc = copy.deepcopy(saved_doc)
            if source:
                metadata = inspect_saved(source, doc_id)
                if not metadata:
                    continue
                destination = output / "reused" / str(row["n"]) / doc_id
                for relative in {item["local_path"] for item in metadata.get("files", [metadata])} | {
                    metadata["local_path"],
                    metadata.get("archive_path"),
                } - {None}:
                    origin = (source / relative).resolve()
                    if not origin.is_relative_to(source.resolve()):
                        raise ValueError("Saved document escapes folder")
                    target = destination / relative
                    target.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        os.link(origin, target)
                    except OSError:
                        shutil.copyfile(origin, target)
                atomic_write_json(destination / "manifests" / f"{doc_id}.json", metadata)
                reusable[(row["url"], doc_id)] = destination
                doc.update(metadata, reused=True)
                for item in [doc, *doc.get("files", [])]:
                    for key in ("local_path", "archive_path"):
                        if item.get(key):
                            item[key] = str((destination / item[key]).relative_to(output))
            else:
                unavailable.add((row["url"], doc_id))
            documents.append(doc)
        if entry["complete"] and len(documents) == len(entry["documents"]):
            status = (
                "completed_with_unavailable"
                if any(doc["status"] == "unavailable_http_404" for doc in documents)
                else "completed"
            )
            completed.append({**row, "documents": documents, "status": status, "error": None})
    return completed, reusable, unavailable
