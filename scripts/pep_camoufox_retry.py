"""Explicit retry passes that retain validated files and previous source records."""

from __future__ import annotations

import copy
import hashlib
import json
import shutil
from pathlib import Path

from src.sites.pep.documents import inspect_saved, save_payload, valid_pdf
from src.sites.pep.source import safe_url
from src.utils.atomic import atomic_write_json


def load_retry(path: Path):
    previous = json.loads(path.read_text())
    selected, reusable = [], {}
    root = path.parent.resolve()
    for job in previous["jobs"]:
        url = safe_url(job["url"])
        for doc in job["documents"]:
            if doc["status"] == "downloaded" and doc.get("local_path"):
                pdf = (root / doc["local_path"]).resolve()
                if not pdf.is_relative_to(root):
                    raise ValueError("Previous PDF path escapes run folder")
                if inspect_saved(pdf.parent.parent, str(doc["id"])):
                    reusable[(url, str(doc["id"]))] = pdf.parent.parent
                else:
                    doc["status"] = "invalid_local_file"
                    job["status"] = "partial"
        if job["status"] != "completed":
            selected.append(
                {key: copy.deepcopy(job[key]) for key in ("n", "source_row", "source_records", "url") if key in job}
            )
    return previous, selected, reusable


def reuse_document(source: Path, destination: Path, doc_id: str):
    metadata = inspect_saved(source, doc_id)
    if not metadata:
        return None
    relative = metadata.get("archive_path") or metadata["local_path"]
    path = (source / relative).resolve()
    if not path.is_relative_to(source.resolve()):
        raise ValueError("Saved document path escapes folder")
    try:
        return save_payload(path.read_bytes(), destination, doc_id)
    except (OSError, ValueError):
        return None


def combined_report(previous: dict, current: dict, old_root: Path, output: Path):
    combined = copy.deepcopy(previous)
    updates = {job["url"]: job for job in current["jobs"]}
    old_root = old_root.resolve()
    for job in combined["jobs"]:
        for doc in job["documents"]:
            if doc["status"] != "downloaded":
                continue
            files = doc.get("files") or [doc]
            for item in files:
                source = (old_root / item["local_path"]).resolve()
                if not source.is_relative_to(old_root):
                    raise ValueError("Previous file escapes run folder")
                raw = source.read_bytes()
                if not valid_pdf(raw) or (item.get("sha256") and hashlib.sha256(raw).hexdigest() != item["sha256"]):
                    raise ValueError("Previous PDF failed integrity validation")
                relative = "previous/" + item["local_path"]
                target = output / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
                item["local_path"] = relative
            doc["local_path"] = files[0]["local_path"]
            if doc.get("archive_path"):
                source = (old_root / doc["archive_path"]).resolve()
                if not source.is_relative_to(old_root):
                    raise ValueError("Previous archive escapes run folder")
                target = output / "previous" / doc["archive_path"]
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
                doc["archive_path"] = "previous/" + doc["archive_path"]
        update = updates.get(job["url"])
        if update is None:
            continue
        documents = {str(doc["id"]): doc for doc in job["documents"]}
        for doc in update["documents"]:
            old = documents.get(str(doc["id"]))
            if old is None or old["status"] != "downloaded" or doc["status"] == "downloaded":
                documents[str(doc["id"])] = copy.deepcopy(doc)
        job["documents"] = list(documents.values())
        job["status"] = (
            "completed"
            if update["status"] == "completed" and all(doc["status"] == "downloaded" for doc in job["documents"])
            else "partial"
            if job["documents"]
            else update["status"]
        )
        job["error"] = update.get("error")
    combined["status"] = current["status"]
    combined["retry_of"] = str(old_root)
    combined["jobs"] = sorted(combined["jobs"], key=lambda job: job["n"])
    # Technical speed/worker counters belong to the retry pass, not the combined dataset.
    combined = {key: combined[key] for key in ("status", "retry_of", "jobs")}
    atomic_write_json(output / "combined-report.json", combined)
    return combined
