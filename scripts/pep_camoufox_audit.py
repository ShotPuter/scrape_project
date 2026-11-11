"""Audit a completed run against Excel and validate every referenced PDF without changing state."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path

from scripts.pep_camoufox_db import read_report
from scripts.pep_camoufox_export import build_export
from src.sites.pep.documents import MAX_PDF_BYTES, valid_pdf
from src.sites.pep.source import safe_url
from src.sites.pep.workbook import LINK_COLUMN, read_rows
from src.utils.atomic import atomic_write_json


def audit(report: dict, root: Path, workbook: Path | None = None) -> dict:
    root = root.resolve()
    records = build_export(report)
    issues, unresolved = [], []
    checked, references = {}, 0
    urls = [record["consultation_url"] for record in records]
    if len(urls) != len(set(urls)):
        issues.append({"reason": "duplicate_consultation_urls"})
    for record in records:
        url = record["consultation_url"]
        if not record["excel_records"]:
            issues.append({"url": url, "reason": "missing_excel_records"})
        if record["status"] not in {"completed", "completed_with_unavailable"}:
            unresolved.append({"url": url, "status": record["status"]})
        for doc in record["declarations"]:
            if doc["download_status"] != "downloaded":
                continue
            if not doc["pdfs"]:
                issues.append({"url": url, "id": doc["id"], "reason": "downloaded_without_pdf"})
            for item in doc["pdfs"]:
                references += 1
                path = (root / item["path"]).resolve()
                errors = []
                if not path.is_relative_to(root):
                    errors.append("path_outside_run")
                else:
                    if path not in checked:
                        try:
                            size = path.stat().st_size
                            raw = path.read_bytes() if 0 < size <= MAX_PDF_BYTES else b""
                            checked[path] = (size, hashlib.sha256(raw).hexdigest(), valid_pdf(raw))
                        except OSError as exc:
                            checked[path] = str(exc)
                    actual = checked[path]
                    if isinstance(actual, str):
                        errors.append("unreadable_file: " + actual)
                    else:
                        size, digest, valid = actual
                        if not valid:
                            errors.append("invalid_pdf_signature_or_eof")
                        if not item.get("sha256"):
                            errors.append("missing_expected_sha256")
                        elif digest != item["sha256"]:
                            errors.append("sha256_mismatch")
                        if item.get("size_bytes") is None:
                            errors.append("missing_expected_size")
                        elif size != item["size_bytes"]:
                            errors.append("size_mismatch")
                if errors:
                    issues.append({"url": url, "id": doc["id"], "path": item["path"], "reasons": errors})
    coverage = None
    if workbook:
        expected = {}
        for number, row in read_rows(workbook):
            if row.get(LINK_COLUMN):
                url = safe_url(str(row[LINK_COLUMN]))
                expected.setdefault(url, []).append({"excel_row": number, "data": row})
        missing, extra = sorted(set(expected) - set(urls)), sorted(set(urls) - set(expected))
        mismatched = [
            record["consultation_url"]
            for record in records
            if record["consultation_url"] in expected and record["excel_records"] != expected[record["consultation_url"]]
        ]
        coverage = {
            "expected_links": len(expected),
            "missing_links": missing,
            "extra_links": extra,
            "mismatched_source_records": mismatched,
        }
        if missing or extra or mismatched:
            issues.append({"reason": "workbook_coverage_mismatch", **coverage})
    return {
        "integrity_ok": not issues,
        "links": len(records),
        "link_statuses": dict(Counter(record["status"] for record in records)),
        "pdf_references": references,
        "unique_pdf_paths_checked": len(checked),
        "coverage": coverage,
        "issues": issues,
        "unresolved_links": unresolved,
        "note": "Signature, EOF, size and SHA256 checks do not verify PDF rendering, content or all publication years.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("--workbook", type=Path)
    args = parser.parse_args()
    path = args.database
    if path.is_dir():
        candidates = list(path.glob("run-*/state.sqlite3"))
        if not candidates:
            parser.error("No run databases found")
        path = max(candidates, key=lambda candidate: candidate.parent.name)
    result = audit(read_report(path), path.parent, args.workbook)
    output = path.parent / "audit.json"
    atomic_write_json(output, result)
    print(
        json.dumps(
            {key: value for key, value in result.items() if key not in {"issues", "unresolved_links", "coverage"}},
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"Issues: {len(result['issues'])}; unresolved links: {len(result['unresolved_links'])}; report: {output}")
    return 0 if result["integrity_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
