import asyncio
import csv
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from src.models.publication import PublicationDetail, PublicationSummary
from src.services import recovery
from src.utils.helpers import SeprecAPIError


def make_index(path, rows=()):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["id", "status"])
        writer.writeheader()
        writer.writerows(rows)


def completed_audit(tmp_path, summaries):
    audit = recovery.AuditStore(tmp_path / "audit.db")
    for cat in ("1", "2", "3", "4"):
        rows = summaries if cat == "1" else []
        audit.save_page(cat, 1, len(rows), rows)
        audit.finish(cat, len(rows))
    return audit


def test_durable_checkpoints_unique_ids_and_count_gaps(tmp_path):
    index = tmp_path / "publications.csv"
    make_index(index, [{"id": "1", "status": "DOWNLOADED"}])
    audit = recovery.AuditStore(tmp_path / "audit.db")
    try:
        rows = [PublicationSummary(id="1"), PublicationSummary(id="2"), PublicationSummary(id="2")]
        audit.save_page("1", 1, 3, rows)
        audit.finish("1", 3)
        report = recovery.write_report(audit, tmp_path, index)
        assert report["missing_ids"] == ["2"]
        assert report["categories"]["1"]["duplicate_rows"] == 1
        assert report["categories"]["1"]["source_count_gap"] == 1
        assert not report["scan_complete"]
    finally:
        audit.close()
    reopened = recovery.AuditStore(tmp_path / "audit.db")
    try:
        assert reopened.state("1") == (2, 3, 3, 1)
    finally:
        reopened.close()


def test_empty_page_before_total_does_not_mark_complete(tmp_path):
    audit = recovery.AuditStore(tmp_path / "audit.db")
    try:
        audit.save_page("1", 1, 5, [PublicationSummary(id="1")])
        with pytest.raises(RuntimeError, match="empty page"):
            audit.finish("1", 5)
        assert audit.state("1")[3] == 0
    finally:
        audit.close()


def test_preview_is_explicitly_partial_and_not_a_pdf(tmp_path):
    summary = PublicationSummary(id="100874", resumen="ESCRITURA PUBLICA", codigo="16471/2022")
    recovery.export_preview(tmp_path, "1", summary, 412)
    data = json.loads((tmp_path / "text-previews/100874.json").read_text())
    assert data["content_kind"] == "search_summary_only"
    assert data["original_recovered"] is False
    assert data["full_text_verified"] is False
    assert data["search_result"]["resumen"] == "ESCRITURA PUBLICA"
    assert "full text NOT verified" in (tmp_path / "text-previews/100874.txt").read_text()
    assert not list(tmp_path.rglob("*.pdf"))


async def test_scan_resumes_without_refetching_completed_categories(tmp_path, monkeypatch):
    index = tmp_path / "publications.csv"
    make_index(index)
    monkeypatch.setattr(recovery.settings, "csv_export_path", index)
    audit = completed_audit(tmp_path, [PublicationSummary(id="1")])
    client = AsyncMock()
    try:
        await recovery.scan(client, audit, tmp_path)
        client.search_publications.assert_not_awaited()
    finally:
        audit.close()


async def test_recovery_412_exports_preview_but_never_updates_index(tmp_path, monkeypatch):
    index = tmp_path / "publications.csv"
    make_index(index)
    monkeypatch.setattr(recovery.settings, "csv_export_path", index)
    store = AsyncMock()
    monkeypatch.setattr(recovery, "PublicationCsv", lambda: store)
    files = AsyncMock()
    monkeypatch.setattr(recovery, "FileStorage", lambda: files)
    client = AsyncMock()
    client.get_publication_detail.side_effect = SeprecAPIError("HTTP 412 from API", status_code=412)
    audit = completed_audit(tmp_path, [PublicationSummary(id="100874", resumen="Preview only")])
    try:
        await recovery.recover(client, audit, tmp_path)
        store.save.assert_not_awaited()
        files.save_publication_document.assert_not_called()
        assert (tmp_path / "text-previews/100874.txt").exists()
        assert recovery.saved_ids(index) == set()
        assert dict(audit.db.execute("SELECT status,COUNT(*) FROM outcomes GROUP BY status")) == {"preview_only": 1}
    finally:
        audit.close()


async def test_recovery_does_not_export_transient_errors_as_source_previews(tmp_path, monkeypatch):
    index = tmp_path / "publications.csv"
    make_index(index)
    monkeypatch.setattr(recovery.settings, "csv_export_path", index)
    monkeypatch.setattr(recovery, "PublicationCsv", AsyncMock)
    monkeypatch.setattr(recovery, "FileStorage", AsyncMock)
    client = AsyncMock()
    client.get_publication_detail.side_effect = SeprecAPIError("HTTP 503", status_code=503)
    audit = completed_audit(tmp_path, [PublicationSummary(id="1", resumen="Preview")])
    try:
        await recovery.recover(client, audit, tmp_path)
        assert not list(tmp_path.rglob("*.txt"))
        assert dict(audit.db.execute("SELECT status,COUNT(*) FROM outcomes GROUP BY status")) == {"failed": 1}
    finally:
        audit.close()


async def test_recovery_rejects_detail_without_original_or_full_text(tmp_path, monkeypatch):
    index = tmp_path / "publications.csv"
    make_index(index)
    monkeypatch.setattr(recovery.settings, "csv_export_path", index)
    store = AsyncMock()
    monkeypatch.setattr(recovery, "PublicationCsv", lambda: store)
    monkeypatch.setattr(recovery, "FileStorage", AsyncMock)
    client = AsyncMock()
    client.get_publication_detail.return_value = PublicationDetail(id="1", resumen="Just a summary")
    audit = completed_audit(tmp_path, [PublicationSummary(id="1", resumen="Just a summary")])
    try:
        await recovery.recover(client, audit, tmp_path)
        store.save.assert_not_awaited()
        assert (tmp_path / "text-previews/1.json").exists()
    finally:
        audit.close()


def test_invalid_original_or_empty_html_cannot_be_recorded_as_a_recovered_pdf():
    assert not recovery.recoverable_detail(PublicationDetail(id="1", documento="invalid base64", resumen="Preview"))
    assert not recovery.recoverable_detail(PublicationDetail(id="1", textoCompleto="<p> </p>", resumen="Preview"))
    assert recovery.recoverable_detail(PublicationDetail(id="1", textoCompleto="<p>Full source text</p>"))


async def test_full_detail_text_is_saved_and_indexed_with_category(tmp_path, monkeypatch):
    from src.storage.file_storage import FileStorage

    index = tmp_path / "publications.csv"
    make_index(index)
    monkeypatch.setattr(recovery.settings, "csv_export_path", index)
    store = AsyncMock()
    monkeypatch.setattr(recovery, "PublicationCsv", lambda: store)
    files = FileStorage(pdf_dir=tmp_path / "pdf")
    monkeypatch.setattr(recovery, "FileStorage", lambda: files)
    client = AsyncMock()
    client.get_publication_detail.return_value = PublicationDetail(
        id="1", titulo="Source title", textoCompleto="<p>Complete source body.</p>", fechaPublicacion="2024-01-01"
    )
    audit = completed_audit(tmp_path, [PublicationSummary(id="1", resumen="Short preview")])
    try:
        await recovery.recover(client, audit, tmp_path)
        store.save.assert_awaited_once()
        record = store.save.call_args.args[0]
        assert record.category_code == "1"
        assert record.status == "GENERATED_PDF"
        assert (await asyncio.to_thread(Path(record.file_path).read_bytes)).startswith(b"%PDF")
        assert not (tmp_path / "text-previews").exists()
    finally:
        audit.close()
