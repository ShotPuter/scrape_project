import json

import pytest

from scripts.pep_camoufox_export import build_export
from scripts.pep_camoufox_logging import report_totals
from scripts.pep_camoufox_parallel import run_parallel
from scripts.pep_camoufox_resume import materialize_history, resume_history
from src.sites.pep.documents import inspect_saved, save_payload

BASE = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?n="
PDF = b"%PDF-1.4\ntest\n%%EOF"


def prior(root, name, jobs):
    folder = root / name
    folder.mkdir(exist_ok=True)
    records = []
    for number, status, documents in jobs:
        docs = []
        for doc_id, downloaded in documents:
            doc = {"id": doc_id, "status": "timeout"}
            if downloaded:
                doc.update(save_payload(PDF, folder / str(number), doc_id))
                for item in [doc, *doc.get("files", [])]:
                    item["local_path"] = str(number) + "/" + item["local_path"]
            docs.append(doc)
        records.append({"n": number, "url": BASE + str(number), "status": status, "documents": docs})
    (folder / "report.json").write_text(json.dumps({"jobs": records}))
    return folder


def test_union_verifies_files_and_preserves_partial_downloads(tmp_path):
    first = prior(tmp_path, "run-1", [(1, "completed", [("1", True)]), (2, "partial", [("2", True), ("3", False)])])
    prior(tmp_path, "run-2", [(2, "completed", [("3", True)]), (3, "completed", [("4", True)])])
    (tmp_path / "run-2/3/pdf/4.pdf").write_bytes(b"corrupt")
    sample = [{"n": i, "url": BASE + str(i), "source_records": [{"data": {"NAME": str(i)}}]} for i in range(1, 5)]
    history = resume_history(tmp_path, sample)
    output = tmp_path / "run-3"
    completed, reusable, _ = materialize_history(history, sample, output)
    assert [job["n"] for job in completed] == [1, 2]
    assert len(reusable) == 3
    assert inspect_saved(reusable[(BASE + "2", "2")], "2")
    assert (first / "1/pdf/1.pdf").stat().st_ino == (output / completed[0]["documents"][0]["local_path"]).stat().st_ino
    exported = build_export({"jobs": completed})
    assert exported[0]["excel_records"][0]["data"]["NAME"] == "1"
    assert all(
        (output / pdf["path"]).exists() for person in exported for doc in person["declarations"] for pdf in doc["pdfs"]
    )


@pytest.mark.asyncio
async def test_all_cached_finishes_without_opening_browsers(tmp_path):
    prior(tmp_path, "run-1", [(1, "completed", [("1", True)])])
    sample = [{"n": 1, "url": BASE + "1"}]
    notices = []

    async def notify(report, *, final):
        notices.append(final)

    output = tmp_path / "run-2"
    report = await run_parallel(
        sample,
        output,
        workers=25,
        request_seconds=60,
        ca_bundle=tmp_path / "absent.pem",
        proxy_ca=None,
        max_seconds=60,
        proxy=None,
        notify=notify,
        resume_history=resume_history(tmp_path, sample),
    )
    assert report["status"] == "finished"
    assert report["workers"] == []
    assert report_totals(report)["reused"] == 1
    assert report_totals(report)["downloaded_new"] == 0
    assert (output / "politicians.json").exists()
    assert notices == [True]
