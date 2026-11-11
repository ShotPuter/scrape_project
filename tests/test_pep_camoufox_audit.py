import hashlib

from scripts.pep_camoufox_audit import audit


def report_for(raw):
    return {
        "jobs": [
            {
                "url": "https://example.com",
                "status": "completed",
                "source_records": [{"excel_row": 2}],
                "documents": [
                    {
                        "id": "1",
                        "status": "downloaded",
                        "files": [
                            {"local_path": "pdf/1.pdf", "size_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
                        ],
                    }
                ],
            }
        ]
    }


def test_audit_detects_corrupted_missing_and_escaped_pdf(tmp_path):
    raw = b"%PDF-1.4\nexample\n%%EOF\n"
    path = tmp_path / "pdf/1.pdf"
    path.parent.mkdir()
    path.write_bytes(raw)
    report = report_for(raw)
    assert audit(report, tmp_path)["integrity_ok"]
    path.write_bytes(raw.replace(b"example", b"changed"))
    assert "sha256_mismatch" in audit(report, tmp_path)["issues"][0]["reasons"]
    path.unlink()
    assert not audit(report, tmp_path)["integrity_ok"]
    report["jobs"][0]["documents"][0]["files"][0]["local_path"] = "../escape.pdf"
    assert audit(report, tmp_path)["issues"][0]["reasons"] == ["path_outside_run"]


def test_audit_keeps_unavailable_and_unresolved_separate(tmp_path):
    report = {
        "jobs": [
            {
                "url": "https://example.com",
                "status": "completed_with_unavailable",
                "source_records": [{"excel_row": 2}],
                "documents": [{"id": "legacy-1", "status": "unavailable_http_404"}],
            }
        ]
    }
    result = audit(report, tmp_path)
    assert result["integrity_ok"] and not result["unresolved_links"]
    report["jobs"][0]["status"] = "partial"
    assert len(audit(report, tmp_path)["unresolved_links"]) == 1


def test_audit_compares_all_workbook_fields(tmp_path, monkeypatch):
    report = {
        "jobs": [
            {
                "url": "https://example.com",
                "status": "completed",
                "source_records": [{"excel_row": 2, "data": {"NAME": "wrong"}}],
                "documents": [],
            }
        ]
    }
    from scripts import pep_camoufox_audit as module

    monkeypatch.setattr(module, "safe_url", lambda url: url)
    monkeypatch.setattr(module, "read_rows", lambda path: [(2, {module.LINK_COLUMN: "https://example.com", "NAME": "José"})])
    result = audit(report, tmp_path, tmp_path / "input.xlsx")
    assert not result["integrity_ok"]
    assert result["coverage"]["mismatched_source_records"] == ["https://example.com"]
