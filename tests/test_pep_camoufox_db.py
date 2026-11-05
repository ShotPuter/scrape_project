import json
import sqlite3

import pytest

from scripts.pep_camoufox_db import StateDB, read_report
from scripts.pep_camoufox_export import build_export
from scripts.pep_camoufox_resume import resume_history
from src.sites.pep.documents import save_payload

BASE = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?n="


def job(number):
    return {
        "n": number,
        "url": BASE + str(number),
        "status": "completed",
        "documents": [],
        "source_records": [{"excel_row": number + 1, "data": {"NAME": "Политик", "DATE": "2026-11-05", "OTHER": None}}],
    }


def test_batches_500_and_final_remainder_preserve_all_fields(tmp_path):
    store = StateDB(tmp_path / "state.sqlite3")
    original = [job(i) for i in range(1, 501)]
    store.stage(original[:499])
    assert store.flush() == 0
    assert read_report(store.path)["jobs"] == []
    store.stage(original[499:])
    assert store.flush() == 500
    last = job(501)
    store.stage([last])
    assert store.flush(force=True, metadata={"status": "finished", "jobs": []}) == 1
    report = read_report(store.path)
    assert report["jobs"] == [*original, last]
    assert report["status"] == "finished"
    assert build_export(report)[0]["excel_records"] == original[0]["source_records"]


def test_document_status_paths_and_resume_without_json(tmp_path):
    root = tmp_path / "run-1"
    metadata = save_payload(b"%PDF-1.4\ntest\n%%EOF", root / "worker-1", "1")
    record = job(1)
    doc = {"id": "1", **metadata}
    for item in [doc, *doc["files"]]:
        item["local_path"] = "worker-1/" + item["local_path"]
    record["documents"] = [doc]
    store = StateDB(root / "state.sqlite3")
    store.stage([record])
    store.flush(force=True)
    history = resume_history(tmp_path, [record])
    assert history[record["url"]]["complete"]
    assert history[record["url"]]["documents"]["1"][1] == root / "worker-1"
    with store.connect() as db:
        assert db.execute("SELECT status,local_path FROM documents").fetchone() == ("downloaded", "worker-1/pdf/1.pdf")
    record["documents"][0].update(status="timeout", error={"stage": "saving_file"})
    record["status"] = "partial"
    store.stage([record])
    store.flush(force=True)
    with store.connect() as db:
        assert db.execute("SELECT status FROM links").fetchone()[0] == "partial"
        assert json.loads(db.execute("SELECT payload FROM documents").fetchone()[0])["error"]["stage"] == "saving_file"


def test_transaction_rolls_back_and_keeps_batch_on_failure(tmp_path):
    store = StateDB(tmp_path / "state.sqlite3")
    record = job(1)
    record["documents"] = [{"id": "1", "status": "timeout"}] * 2
    store.stage([record])
    with pytest.raises(sqlite3.IntegrityError):
        store.flush(force=True)
    assert read_report(store.path)["jobs"] == []
    assert len(store.pending) == 1
