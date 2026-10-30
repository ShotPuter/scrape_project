"""Bounded-content accounting for download tests; no publication CSV or PDF paths persisted."""

from collections.abc import Sequence
from pathlib import Path

from src.models.publication import PublicationRecord
from src.storage.publication_csv import PublicationCsv


class DiscardStore(PublicationCsv):
    """Keep identifiers and aggregate counters, never document text or base64 payloads."""

    persistent = False

    def __init__(self) -> None:
        super().__init__(path=Path("discarded"), verify_files=False)
        self.seen: set[str] = set()
        self.completed = self.original = self.generated = self.byte_count = 0

    async def load(self) -> None:
        pass

    async def save(self, record: PublicationRecord) -> None:
        self.seen.add(record.id)
        self.completed += 1
        self.original += int(record.has_original_file)
        self.generated += int(not record.has_original_file)
        self.byte_count += record.file_size_bytes or 0

    async def filter_unprocessed_ids(self, publication_ids: Sequence[str]) -> list[str]:
        return [identifier for identifier in publication_ids if identifier not in self.seen]

    async def get_stats(self) -> dict[str, int | float]:
        return {
            "total_items": self.completed,
            "downloaded_items": self.original,
            "generated_items": self.generated,
            "total_mb": round(self.byte_count / 1048576, 2),
        }

    def next_cycle(self) -> None:
        self.seen.clear()
