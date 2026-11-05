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
