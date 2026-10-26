"""Validate publication CSV index integrity and verify PDF file contents."""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.models.publication import STATUS_DOWNLOADED, STATUS_GENERATED_PDF, PublicationRecord

CSV_FIELDS = tuple(PublicationRecord.model_fields)
SAVED_STATUSES = frozenset({STATUS_DOWNLOADED, STATUS_GENERATED_PDF})
PDF_HEADER_MAGIC = b"%PDF"
PDF_TRAILER_MAGIC = b"%%EOF"
TRAILER_SEARCH_WINDOW_BYTES = 2048


def file_problem(path: str | None, expected_size: int | None) -> str | None:
    """
    Examine on-disk document integrity: existence, size match, header, and EOF marker.

    Returns:
        Error reason code as string if flawed, otherwise None.
    """
    if not path:
        return "missing_path"

    try:
        candidate_file = Path(path)
        actual_size = candidate_file.stat().st_size
        if expected_size is None or actual_size != expected_size:
            return "size_mismatch"

        with candidate_file.open("rb") as file_handle:
            if file_handle.read(4) != PDF_HEADER_MAGIC:
                return "invalid_header"

            file_handle.seek(max(0, actual_size - TRAILER_SEARCH_WINDOW_BYTES))
            if PDF_TRAILER_MAGIC not in file_handle.read():
                return "missing_eof"

    except FileNotFoundError:
        return "missing_file"
    except OSError:
        return "unreadable"

    return None


def inspect_storage(csv_path: Path) -> dict[str, Any]:
    """Audit all saved records in CSV index against actual files on disk."""
    records = read_records(csv_path)
    problems = []

    for record in records.values():
        issue_reason = file_problem(record.file_path, record.file_size_bytes)
        if issue_reason is not None:
            problems.append({"id": record.id, "file_path": record.file_path, "reason": issue_reason})

    return {"checked": len(records), "invalid": len(problems), "problems": problems}


def read_records(path: Path) -> dict[str, PublicationRecord]:
    """Read CSV storage index into in-memory dictionary of verified PublicationRecords."""
    if not path.exists():
        return {}

    with path.open(newline="", encoding="utf-8-sig") as file_handle:
        csv_reader = csv.DictReader(file_handle)
        if csv_reader.fieldnames is None:
            return {}

        if tuple(csv_reader.fieldnames) != CSV_FIELDS:
            raise ValueError(f"Unexpected publication CSV columns in {path}")

        records: dict[str, PublicationRecord] = {}
        for row in csv_reader:
            record = _record_from_row(row)
            if record.status in SAVED_STATUSES:
                records[record.id] = record

        return records


def _record_from_row(row: dict[str, str | None]) -> PublicationRecord:
    """Deserialize CSV row dictionary into strongly-typed PublicationRecord."""
    now_timestamp = datetime.now(UTC).isoformat()
    field_values: dict[str, Any] = {field_name: row[field_name] or None for field_name in CSV_FIELDS}
    field_values["has_original_file"] = row["has_original_file"] == "True"
    field_values["file_size_bytes"] = int(row["file_size_bytes"]) if row["file_size_bytes"] else None
    field_values["created_at"] = row["created_at"] or now_timestamp
    field_values["updated_at"] = row["updated_at"] or now_timestamp

    return PublicationRecord.model_validate(field_values)
