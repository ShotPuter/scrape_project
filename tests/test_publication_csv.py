"""CSV persistence and PDF integrity checks."""

import csv

import pytest

from src.models.publication import STATUS_FAILED, PublicationRecord
from src.storage import publication_csv
from src.storage.integrity import CSV_FIELDS, inspect_storage, read_records
from src.storage.publication_csv import PublicationCsv


@pytest.mark.asyncio
async def test_saved_publication_survives_restart(tmp_path):
    document = tmp_path / "document.pdf"
    document.write_bytes(b"%PDF-1.4\n%%EOF\n")
    csv_path = tmp_path / "publications.csv"
    record = PublicationRecord(id="1", file_path=str(document), file_size_bytes=document.stat().st_size)

    await PublicationCsv(csv_path).save(record)
    reopened = PublicationCsv(csv_path)

    assert await reopened.get_all_records() == [record]
    assert await reopened.filter_unprocessed_ids(["1", "2"]) == ["2"]
    assert inspect_storage(csv_path) == {"checked": 1, "invalid": 0, "problems": []}


@pytest.mark.asyncio
async def test_missing_pdf_is_scheduled_again(tmp_path):
    document = tmp_path / "document.pdf"
    document.write_bytes(b"%PDF-1.4\n%%EOF\n")
    store = PublicationCsv(tmp_path / "publications.csv")
    await store.save(PublicationRecord(id="1", file_path=str(document), file_size_bytes=document.stat().st_size))

    document.unlink()

    assert await store.filter_unprocessed_ids(["1"]) == ["1"]
    assert inspect_storage(store.path)["problems"][0]["reason"] == "missing_file"


@pytest.mark.asyncio
async def test_updates_replace_existing_row_and_failures_are_not_stored(tmp_path):
    store = PublicationCsv(tmp_path / "publications.csv", verify_files=False)
    await store.save(PublicationRecord(id="1", title="first"))
    await store.save(PublicationRecord(id="1", title="updated"))
    await store.save(PublicationRecord(id="2", status=STATUS_FAILED))

    with store.path.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["title"] == "updated"
    assert await store.filter_unprocessed_ids(["2"]) == ["2"]


@pytest.mark.asyncio
async def test_corrupted_header_is_repaired_with_a_snapshot(tmp_path):
    store = PublicationCsv(tmp_path / "publications.csv", verify_files=False)
    await store.save(PublicationRecord(id="1", title="first"))
    assert store.path.exists()

    # Simulate an external truncation/corruption after the index was loaded.
    store.path.write_text("broken\n", encoding="utf-8")

    await store.save(PublicationRecord(id="2", title="second"))

    records = read_records(store.path)
    assert set(records) == {"1", "2"}
    with store.path.open(newline="", encoding="utf-8-sig") as handle:
        assert tuple(csv.DictReader(handle).fieldnames or ()) == CSV_FIELDS


@pytest.mark.asyncio
async def test_failed_append_does_not_mutate_the_in_memory_index(tmp_path, monkeypatch):
    store = PublicationCsv(tmp_path / "publications.csv", verify_files=False)
    record = PublicationRecord(id="1", title="first")

    def fail(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(publication_csv, "_append_or_rewrite", fail)

    with pytest.raises(OSError, match="disk full"):
        await store.save(record)

    assert store._records == {}
    assert not store.path.exists()


@pytest.mark.asyncio
async def test_failed_update_keeps_the_previous_record_in_memory(tmp_path, monkeypatch):
    store = PublicationCsv(tmp_path / "publications.csv", verify_files=False)
    original = PublicationRecord(id="1", title="first")
    await store.save(original)

    def fail(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(publication_csv, "_write_snapshot", fail)

    with pytest.raises(OSError, match="disk full"):
        await store.save(PublicationRecord(id="1", title="updated"))

    assert store._records["1"] == original
    assert read_records(store.path)["1"] == original
