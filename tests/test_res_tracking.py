import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from scripts import res_free_pilot as pilot
from src.sites.res.tracking import TrackingStore, import_run, main
from src.utils.atomic import atomic_write_json

CVE = "ACuKDHhqsvAd"
CVE2 = "ACfJoaiSFQ8v"
PDF = b"%PDF-1.4\nfixture\n%%EOF"
COMPANY = {"company_id": "rut:76615169-8", "company_name": "VERTIKAL GROUP SpA", "rut": "76615169-8"}
LINK = f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={CVE}&codigo=valid"


def fixture_run(tmp_path, *, partial=False):
    root = tmp_path / "sample"
    worker = root / "workers/w00"
    worker.mkdir(parents=True)
    atomic_write_json(root / "selection.json", {"companies": [COMPANY], "config": {"limit": 1, "browsers": 1}})
    cves = [CVE, CVE2] if partial else [CVE]
    state = {
        "companies": {COMPANY["company_id"]: {"status": "completed", "cves": cves, "searched_at": 123}},
        "cves": {CVE: {"codigo": "valid", "link": LINK, "verified_at": 124, "company_id": COMPANY["company_id"]}},
    }
    atomic_write_json(worker / "state.json", state)
    saved = pilot.persist_pdf(worker, CVE, PDF, LINK)
    result = {**COMPANY, "status": "downloaded", "documents": [saved]}
    atomic_write_json(
        worker / "report-1.json",
        {
            "run_id": "old",
            "companies": [result],
            "attempts": [{"company_id": COMPANY["company_id"], "attempt": 1, "seconds": 10, "result": result}],
        },
    )
    return root, worker, saved


def test_offline_import_is_idempotent_and_preserves_pdf_bytes_and_mtime(tmp_path):
    root, worker, saved = fixture_run(tmp_path)
    path = worker / saved["local_path"]
    before = (path.read_bytes(), path.stat().st_mtime_ns)
    store = TrackingStore(tmp_path / "tracking.sqlite3", "sample")
    try:
        import_run(store, root)
        import_run(store, root)
        assert store.db.execute("SELECT count(*) FROM attempts").fetchone()[0] == 1
        row = store.rows()[0]
        assert row["status"] == "downloaded" and row["pdfs_saved"] == 1
        tracked = store.existing_file(CVE)
        assert tracked["local_path"] == str(Path("sample/workers/w00") / saved["local_path"])
        assert tracked["sha256"] == saved["sha256"]
        assert (path.read_bytes(), path.stat().st_mtime_ns) == before
    finally:
        store.close()


def test_import_detects_partial_success_even_when_legacy_report_says_downloaded(tmp_path):
    root, _, _ = fixture_run(tmp_path, partial=True)
    store = TrackingStore(tmp_path / "tracking.sqlite3", "sample")
    try:
        import_run(store, root)
        row = store.rows()[0]
        assert (row["status"], row["cves_found"], row["pdfs_saved"], row["missing_cves"]) == ("partial", 2, 1, CVE2)
    finally:
        store.close()


def test_missing_file_is_not_counted_as_downloaded(tmp_path):
    root, worker, saved = fixture_run(tmp_path)
    store = TrackingStore(tmp_path / "tracking.sqlite3", "sample")
    try:
        import_run(store, root)
        (worker / saved["local_path"]).unlink()
        import_run(store, root)
        row = store.rows()[0]
        assert row["status"] == "missing_pdf" and row["pdfs_saved"] == 0
        assert row["missing_cves"] == CVE
    finally:
        store.close()


def test_sqlite_recovers_missing_checkpoints_and_reuses_files_in_another_batch(tmp_path):
    root, worker, saved = fixture_run(tmp_path)
    original = worker / saved["local_path"]
    before = original.stat().st_mtime_ns
    store = TrackingStore(tmp_path / "tracking.sqlite3", "sample")
    try:
        import_run(store, root)
        (worker / "state.json").unlink()
        import_run(store, root)
        assert store.rows()[0]["status"] == "downloaded"
        store.restore_worker(worker, [COMPANY])
        assert json.loads((worker / "state.json").read_text())["companies"][COMPANY["company_id"]]["cves"] == [CVE]
        new_worker = tmp_path / "another-run/workers/w03"
        new_worker.mkdir(parents=True)
        store.restore_worker(new_worker, [COMPANY])
        restored = pilot.cached_pdf(new_worker, CVE)
        assert restored and (new_worker / restored["local_path"]).stat().st_ino == original.stat().st_ino
        assert original.stat().st_mtime_ns == before
    finally:
        store.close()


def test_reimport_does_not_replace_a_new_failure_with_an_old_report(tmp_path):
    root, worker, _ = fixture_run(tmp_path)
    # Leave a discovered CVE but no downloaded document.
    for p in worker.glob("pdf/**/*.pdf"):
        p.unlink()
    store = TrackingStore(tmp_path / "tracking.sqlite3", "sample")
    try:
        import_run(store, root)
        attempt = store.begin_attempt(COMPANY["company_id"], 1)
        store.finish_attempt(attempt, {**COMPANY, "status": "proxy_failed", "error": "new connection failure"}, 2)
        import_run(store, root)
        row = store.rows()[0]
        assert row["status"] == "proxy_failed" and row["error"] == "new connection failure"
    finally:
        store.close()


def test_five_connections_write_attempts_without_lost_updates(tmp_path):
    db = tmp_path / "tracking.sqlite3"
    companies = [{"company_id": f"company-{i}", "company_name": f"Company {i}", "rut": None} for i in range(5)]
    store = TrackingStore(db, "parallel")
    store.register_run({}, companies)
    store.close()

    def work(company):
        writer = TrackingStore(db, "parallel", worker=company["company_id"])
        try:
            for number in range(10):
                attempt = writer.begin_attempt(company["company_id"], number)
                writer.finish_attempt(attempt, {**company, "status": "captcha_required", "error": "fixture"}, 1)
        finally:
            writer.close()

    with ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(work, companies))
    store = TrackingStore(db, "parallel")
    try:
        assert store.db.execute("SELECT count(*) FROM attempts WHERE status='captcha_required'").fetchone()[0] == 50
        assert store.db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        store.close()


def test_cli_sync_and_inspection_never_need_a_browser(tmp_path, capsys):
    fixture_run(tmp_path, partial=True)
    assert main(["--output", str(tmp_path), "--run-id", "sample", "--action", "sync"]) == 0
    assert json.loads(capsys.readouterr().out)["pdfs"] == 1
    assert main(["--output", str(tmp_path), "--run-id", "sample", "--action", "errors"]) == 0
    assert json.loads(capsys.readouterr().out)[0]["status"] == "partial"
    assert main(["--output", str(tmp_path), "--run-id", "sample", "--action", "documents", "--company", "76615169-8"]) == 0
    docs = json.loads(capsys.readouterr().out)
    assert len(docs) == 2 and any(d["local_path"] for d in docs)


@pytest.mark.asyncio
async def test_live_document_flow_records_verified_link_pdf_and_failed_cve(tmp_path, monkeypatch):
    from collections import Counter

    from src.sites.res.source import SearchIssue

    root, worker, saved = fixture_run(tmp_path, partial=True)
    (worker / saved["local_path"]).unlink()
    store = TrackingStore(tmp_path / "tracking.sqlite3", "sample", worker="w00")
    import_run(store, root)
    args = pilot.parse_args(["--output", str(worker), "--max-documents", "0", "--audio-model", ""])
    args.state_path, args.tracking_store = worker / "state.json", store
    state = pilot.load_state(args.state_path)
    state["cves"] = {}

    class Cascade:
        async def mint(self, *args, **kwargs):
            return "fixture-token"

    def verify(http, cve, token):
        if cve == CVE2:
            raise SearchIssue("captcha_required", "Audio unavailable")
        return cve, "valid", LINK

    monkeypatch.setattr(pilot.http_flow, "verify", verify)
    monkeypatch.setattr(pilot.http_flow, "download", lambda *args: PDF)
    try:
        attempt = store.begin_attempt(COMPANY["company_id"], 1)
        result = await pilot.process_company(Cascade(), None, COMPANY, state, args, worker, Counter())
        store.finish_attempt(attempt, result, 1)
        assert result["status"] == "partial"
        assert store.rows()[0]["pdfs_saved"] == 1
        assert store.db.execute("SELECT status FROM documents WHERE cve=?", (CVE2,)).fetchone()[0] == "captcha_required"
        assert store.db.execute("SELECT codigo FROM documents WHERE cve=?", (CVE,)).fetchone()[0] == "valid"
        assert store.existing_file(CVE)
    finally:
        store.close()


def test_relative_benchmark_path_can_be_imported(tmp_path, monkeypatch):
    fixture_run(tmp_path)
    store = TrackingStore(tmp_path / "tracking.sqlite3", "sample")
    monkeypatch.chdir(tmp_path)
    try:
        import_run(store, Path("sample"))
        assert store.rows()[0]["status"] == "downloaded"
        assert store.db.execute("SELECT count(*) FROM attempts").fetchone()[0] == 1
    finally:
        store.close()
