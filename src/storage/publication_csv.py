"""Thread-safe and durable CSV index of downloaded publications."""

from __future__ import annotations

import asyncio
import csv
import os
from collections.abc import Iterable, Sequence
from pathlib import Path

from config import settings
from src.models.publication import STATUS_DOWNLOADED, STATUS_GENERATED_PDF, PublicationRecord
from src.storage.integrity import CSV_FIELDS, SAVED_STATUSES, file_problem, read_records
from src.utils.atomic import atomic_text_writer

BYTES_PER_MEGABYTE = 1024 * 1024


class PublicationCsv:
    """Async-safe storage repository managing the persistent CSV publication index."""

    def __init__(self, path: Path | str | None = None, *, verify_files: bool = True) -> None:
        self.path = Path(path or settings.csv_export_path)
        self.verify_files = verify_files
        self._records: dict[str, PublicationRecord] = {}
        self._loaded: bool = False
        self._lock = asyncio.Lock()

    async def load(self) -> None:
        """Load records from disk into memory if not already cached."""
        async with self._lock:
            if self._loaded:
                return
            self._records = await asyncio.to_thread(read_records, self.path)
            self._loaded = True

    async def save(self, record: PublicationRecord) -> None:
        """
        Persist a single record to the index.
        Updates existing entries via full atomic snapshot; appends new entries to end of file.
        """
        if record.status not in SAVED_STATUSES:
            return

        await self.load()
        async with self._lock:
            if record.id in self._records:
                updated_mapping = {**self._records, record.id: record}
                await asyncio.to_thread(_write_snapshot, self.path, updated_mapping.values())
            else:
                await asyncio.to_thread(_append_or_rewrite, self.path, record, list(self._records.values()))

            # Only mutate the in-memory index after the durable write succeeded.
            self._records[record.id] = record

    async def filter_unprocessed_ids(self, publication_ids: Sequence[str]) -> list[str]:
        """
        Identify publication IDs that have not yet been successfully saved.
        Optionally validates that on-disk PDF files still exist and match expected size.
        """
        await self.load()
        existing_records = [
            self._records[publication_id] for publication_id in publication_ids if publication_id in self._records
        ]
        valid_identifiers = await asyncio.to_thread(_extract_valid_identifiers, existing_records, self.verify_files)
        return [publication_id for publication_id in publication_ids if publication_id not in valid_identifiers]

    async def get_all_records(self) -> list[PublicationRecord]:
        """Return all indexed records sorted by publication date descending."""
        await self.load()
        records_by_id = sorted(self._records.values(), key=lambda record: record.id)
        return sorted(records_by_id, key=lambda record: record.publication_date or "", reverse=True)

    async def get_stats(self) -> dict[str, int | float]:
        """Compute aggregated volume and storage statistics across index."""
        await self.load()
        records = list(self._records.values())
        total_bytes = sum(record.file_size_bytes or 0 for record in records)

        return {
            "total_items": len(records),
            "downloaded_items": sum(record.status == STATUS_DOWNLOADED for record in records),
            "generated_items": sum(record.status == STATUS_GENERATED_PDF for record in records),
            "total_mb": round(total_bytes / BYTES_PER_MEGABYTE, 2),
        }


def _extract_valid_identifiers(records: list[PublicationRecord], verify_files: bool) -> set[str]:
    """Filter records whose stored documents pass on-disk verification."""
    if not verify_files:
        return {record.id for record in records}

    return {record.id for record in records if file_problem(record.file_path, record.file_size_bytes) is None}


def _append_or_rewrite(path: Path, record: PublicationRecord, existing: list[PublicationRecord]) -> None:
    """Append when the file is healthy, otherwise repair it with a full snapshot."""
    if _can_append_safely(path):
        try:
            _append_record(path, record)
            return
        except OSError:
            # A partially written append is repaired below instead of being
            # left in place as a malformed trailing row.
            pass
    _write_snapshot(path, [*existing, record])


def _can_append_safely(path: Path) -> bool:
    """Check the CSV header and trailing newline before appending in place."""
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return True
    except OSError:
        return False
    if size == 0:
        return True

    try:
        with path.open("rb") as file_handle:
            header_line = file_handle.readline()
            file_handle.seek(-1, os.SEEK_END)
            last_byte = file_handle.read(1)
    except OSError:
        return False

    try:
        header = header_line.decode("utf-8-sig").rstrip("\r\n")
        fields = tuple(next(csv.reader([header])))
    except (UnicodeDecodeError, csv.Error, StopIteration):
        return False
    return fields == CSV_FIELDS and last_byte in {b"\n", b"\r"}


def _append_record(path: Path, record: PublicationRecord) -> None:
    """Append a single record row to the CSV file, adding headers if newly created."""
    path.parent.mkdir(parents=True, exist_ok=True)
    is_new_file = not path.exists() or path.stat().st_size == 0
    file_mode = "w" if is_new_file else "a"
    file_encoding = "utf-8-sig" if is_new_file else "utf-8"

    with path.open(file_mode, newline="", encoding=file_encoding) as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=CSV_FIELDS)
        if is_new_file:
            writer.writeheader()
        writer.writerow(record.model_dump(mode="json"))
        file_handle.flush()
        os.fsync(file_handle.fileno())


def _write_snapshot(path: Path, records: Iterable[PublicationRecord]) -> None:
    """Atomically rewrite a complete CSV snapshot via a sibling temporary file."""
    with atomic_text_writer(path, encoding="utf-8-sig", newline="") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(record.model_dump(mode="json") for record in records)
