"""CSV persistence and PDF integrity checks."""

import csv

import pytest

from src.models.publication import STATUS_FAILED, PublicationRecord
from src.storage.integrity import inspect_storage
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
