import json

from scripts.pep_camoufox_retry import combined_report, load_retry, reuse_document
from src.sites.pep.documents import save_payload

BASE = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?numeroDocumento="
PDF = b"%PDF-1.4\ntest\n%%EOF"


def test_retry_selects_incomplete_reuses_verified_files_and_merges_all_politicians(tmp_path):
    old, new = tmp_path / "old", tmp_path / "new"
    metadata = save_payload(PDF, old, "1")
    previous = {
        "jobs": [
            {
                "n": 1,
                "url": BASE + "1",
                "source_records": [{"data": {"NAME": "A"}}],
                "status": "partial",
                "documents": [{"id": "1", **metadata}, {"id": "2", "status": "timeout"}],
            },
            {"n": 2, "url": BASE + "2", "status": "completed", "documents": []},
        ]
    }
    path = old / "report.json"
    path.write_text(json.dumps(previous))
    previous, selected, reusable = load_retry(path)
    assert [row["n"] for row in selected] == [1]
    assert selected[0]["source_records"] == previous["jobs"][0]["source_records"]
    copied = reuse_document(reusable[(BASE + "1", "1")], new, "1")
    second = save_payload(PDF, new, "2")
    current = {
        "status": "finished",
        "jobs": [
            {"n": 1, "url": BASE + "1", "status": "completed", "documents": [{"id": "1", **copied}, {"id": "2", **second}]}
        ],
    }
    merged = combined_report(previous, current, old, new)
    assert len(merged["jobs"]) == 2
    assert all(job["status"] == "completed" for job in merged["jobs"])
    assert len(merged["jobs"][0]["documents"]) == 2
    assert (new / "pdf/1.pdf").read_bytes() == PDF
    assert (old / "pdf/1.pdf").read_bytes() == PDF
    assert (new / "combined-report.json").is_file()


def test_retry_includes_corrupt_completed_files(tmp_path):
    metadata = save_payload(PDF, tmp_path, "1")
    (tmp_path / "pdf/1.pdf").write_bytes(b"bad")
    path = tmp_path / "report.json"
    path.write_text(
        json.dumps({"jobs": [{"n": 1, "url": BASE + "1", "status": "completed", "documents": [{"id": "1", **metadata}]}]})
    )
    previous, selected, reusable = load_retry(path)
    assert len(selected) == 1
    assert not reusable
    assert previous["jobs"][0]["documents"][0]["status"] == "invalid_local_file"


def test_inplace_retry_keeps_old_paths_and_updates_same_root(tmp_path):
    root = tmp_path / "original"
    first = save_payload(PDF, root, "1")
    previous = {
        "jobs": [
            {
                "n": 1,
                "url": BASE + "1",
                "status": "partial",
                "documents": [{"id": "1", **first}, {"id": "2", "status": "timeout"}],
            }
        ]
    }
    attempt = root / "retries" / "pass1"
    references = reuse_document(root, attempt, "1", True)
    assert not (attempt / "pdf/1.pdf").exists()
    assert (attempt / references["local_path"]).resolve() == (root / "pdf/1.pdf").resolve()
    new_pdf = save_payload(PDF, attempt, "2")
    merged = combined_report(
        previous,
        {
            "status": "finished",
            "attempt_folder": str(attempt),
            "jobs": [
                {
                    "n": 1,
                    "url": BASE + "1",
                    "status": "completed",
                    "documents": [{"id": "1", **references}, {"id": "2", **new_pdf}],
                }
            ],
        },
        root,
        root,
    )
    documents = merged["jobs"][0]["documents"]
    assert documents[0]["local_path"] == "pdf/1.pdf"
    assert documents[1]["local_path"] == "retries/pass1/pdf/2.pdf"
    assert not (root / "previous").exists()
    assert (root / "report.json").is_file()
    assert merged["pdf_count"] == 2
    assert all((root / doc["local_path"]).is_file() for doc in documents)


def test_same_folder_retries_are_exclusive(tmp_path):
    import pytest

    from scripts.pep_camoufox_retry import retry_lock

    with retry_lock(tmp_path), pytest.raises(ValueError, match="already running"), retry_lock(tmp_path):
        pass
    with retry_lock(tmp_path):
        pass
