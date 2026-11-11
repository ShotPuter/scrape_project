import multiprocessing
import sqlite3
from pathlib import Path

import pytest

from src.sites.dequienes.companies import parse_company
from src.sites.dequienes.extraction import extract_candidates
from src.sites.dequienes.queue import LostLease, QueueStore

URL = "https://dequienes.cl/diario-oficial/2025/01/02/company-spa-1234567"
HTML = "<h1>Company SpA</h1><h2>Publicado el 02/01/2025</h2>"


def seed(store, count=1):
    urls = [URL + str(i) for i in range(count)]
    store.add_publications("2025-01", urls)
    return urls


def test_migration_preserves_legacy_rows_and_is_idempotent(tmp_path):
    path = tmp_path / "companies.sqlite3"
    db = sqlite3.connect(path)
    db.execute("""CREATE TABLE publications(source_url TEXT PRIMARY KEY,month TEXT,status TEXT,company_name TEXT,
        name_key TEXT,rut TEXT,publication_title TEXT,publication_date TEXT,error TEXT)""")
    db.execute(
        "INSERT INTO publications VALUES (?,?,?,?,?,?,?,?,?)",
        (URL, "2025-01", "collected", "Company SpA", "company spa", "", "Company SpA", "2025-01-02", None),
    )
    db.commit()
    db.close()
    for _ in range(2):
        store = QueueStore(path)
        try:
            assert store.status()["companies"] == 1
            assert store.status()["publication_scans"] == {"not_scanned": 1}
            run = store.start_run("public_discovery")
            assert store.claim(run, "worker") is None
            assert store.status()["queues"]["company_lookup"] == {"pending": 1}
            assert store.db.execute("SELECT company_name FROM publications").fetchone()[0] == "Company SpA"
        finally:
            store.close()


def test_limit_counts_running_and_rejected_duplicates(tmp_path):
    store = QueueStore(tmp_path / "q.db")
    try:
        seed(store, 5)
        run = store.start_run("public_discovery", 2)
        first = store.claim(run, "one", now=100)
        second = store.claim(run, "two", now=100)
        assert first["key"] != second["key"]
        assert store.claim(run, "three", now=100) is None
        assert store.db.execute("SELECT claimed FROM runs WHERE run_id=?", (run,)).fetchone()[0] == 2
        zero = store.start_run("public_discovery", 0)
        assert store.claim(zero, "none", now=100) is None
    finally:
        store.close()


def test_expired_lease_is_recovered_and_old_writer_is_fenced(tmp_path):
    store = QueueStore(tmp_path / "q.db")
    try:
        seed(store)
        first = store.claim(store.start_run("public_discovery"), "one", now=100, lease_seconds=10)
        assert store.claim(store.start_run("public_discovery"), "two", now=105) is None
        second = store.claim(store.start_run("public_discovery"), "two", now=111)
        assert second["id"] == first["id"] and second["token"] != first["token"]
        with pytest.raises(LostLease):
            store.finish_publication(first, parse_company(HTML, first["key"]), [], now=112)
        assert store.status()["companies"] == 0
        store.finish_publication(second, parse_company(HTML, second["key"]), [], now=112)
        assert store.status()["companies"] == 1
        assert dict(store.db.execute("SELECT status,count(*) FROM job_attempts GROUP BY status")) == {
            "expired": 1,
            "completed": 1,
        }
    finally:
        store.close()


def test_heartbeat_and_retry_exhaustion(tmp_path):
    store = QueueStore(tmp_path / "q.db")
    try:
        seed(store)
        first = store.claim(store.start_run("public_discovery"), "one", now=100, lease_seconds=10)
        store.heartbeat(first, lease_seconds=10, now=105)
        assert store.claim(store.start_run("public_discovery"), "two", now=112) is None
        store.finish(first, "retry_wait", now=113, retry_seconds=5)
        assert store.claim(store.start_run("public_discovery"), "two", now=117) is None
        for index in range(2):
            job = store.claim(store.start_run("public_discovery"), "two", now=120 + index)
            store.finish(job, "retry_wait", now=120 + index)
        assert store.status()["queues"]["public_discovery"] == {"failed": 1}
    finally:
        store.close()


def test_public_completion_is_atomic_and_res_codes_are_case_sensitive(tmp_path):
    store = QueueStore(tmp_path / "q.db")
    try:
        urls = seed(store, 2)
        for url in urls:
            job = store.claim(store.start_run("public_discovery"), "worker")
            item = parse_company(HTML, job["key"])
            candidates = [
                {"code": code, "kind": "res_reference", "evidence": "RES link"} for code in ["ACiWqbFTpsmy", "ACI WqbFTpsmy"]
            ]
            with pytest.raises(ValueError):
                store.finish_publication(job, item, candidates)
            assert store.status()["companies"] == (0 if url == urls[0] else 1)
            assert store.status()["documents"] == (0 if url == urls[0] else 2)
            candidates[1]["code"] = "ACIWqbFTpsmy"
            store.finish_publication(job, item, candidates)
        assert store.status()["documents"] == 2
        assert store.status()["companies"] == 1
        assert store.db.execute("SELECT count(*) FROM document_sources").fetchone()[0] == 4
        assert store.status()["queues"]["certificate_download"] == {"pending": 2}
        assert store.status()["queues"]["company_lookup"] == {}
    finally:
        store.close()


def test_global_concurrency_and_rate_budget_across_connections(tmp_path):
    first, second = QueueStore(tmp_path / "q.db"), QueueStore(tmp_path / "q.db")
    try:
        seed(first, 2)
        job = first.claim(first.start_run("public_discovery"), "one", now=100, max_in_flight=1)
        assert second.claim(second.start_run("public_discovery"), "two", now=100, max_in_flight=1) is None
        assert first.request_delay("public", 60, now=100) == 0
        assert second.request_delay("public", 60, now=100) == 1
        first.finish(job, now=101)
        assert second.claim(second.start_run("public_discovery"), "two", now=102, max_in_flight=1)
    finally:
        first.close()
        second.close()


def _claim_in_process(path, run_id, event, results):
    store = QueueStore(Path(path))
    try:
        event.wait(10)
        keys = []
        while job := store.claim(run_id, str(multiprocessing.current_process().pid), max_in_flight=20):
            keys.append(job["key"])
            store.finish(job)
        results.put(keys)
    finally:
        store.close()


def test_two_processes_share_limit_and_never_duplicate(tmp_path):
    path = tmp_path / "q.db"
    store = QueueStore(path)
    seed(store, 20)
    run = store.start_run("public_discovery", 7)
    ctx = multiprocessing.get_context("spawn")
    event, results = ctx.Event(), ctx.Queue()
    processes = [ctx.Process(target=_claim_in_process, args=(str(path), run, event, results)) for _ in range(2)]
    try:
        for process in processes:
            process.start()
        event.set()
        keys = results.get(timeout=20) + results.get(timeout=20)
        for process in processes:
            process.join(10)
            assert process.exitcode == 0
        assert len(keys) == len(set(keys)) == 7
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(5)
        results.close()
        store.close()


def test_code_classification_and_script_exclusion():
    html = (
        HTML
        + """<a href="https://www.registrodeempresasysociedades.cl/VerificarCertificados.aspx?cve=ACiWqbFTpsmy">RES</a>
        <p>CVE: 123456789</p><p>CVE RES: AbC123xyz</p>
        <script>var text="CVE RES: Hidden123"</script>"""
    )
    candidates = extract_candidates(html, URL)
    res = {c["code"] for c in candidates if c["kind"] == "res_reference"}
    assert res == {"ACiWqbFTpsmy", "AbC123xyz"}
    assert any(c["kind"] == "diario_oficial" and c["code"] == "1234567" for c in candidates)
    assert any(c["kind"] == "ambiguous" and c["code"] == "123456789" for c in candidates)
    assert not any(c["code"] == "Hidden123" for c in candidates)


def test_named_run_reuses_reservation_after_expiration(tmp_path):
    store = QueueStore(tmp_path / "q.db")
    try:
        seed(store, 3)
        run = store.start_run("public_discovery", 1, run_id="pilot")
        first = store.claim(run, "one", now=100, lease_seconds=1)
        assert store.start_run("public_discovery", 1, run_id="pilot") == run
        second = store.claim(run, "two", now=102)
        assert second["id"] == first["id"]
        assert store.db.execute("SELECT claimed FROM runs WHERE run_id=?", (run,)).fetchone()[0] == 1
        store.finish(second, now=103)
        assert store.claim(run, "three", now=104) is None
        with pytest.raises(ValueError, match="different queue or limit"):
            store.start_run("public_discovery", 2, run_id="pilot")
    finally:
        store.close()


def test_operator_retry_does_not_touch_running_jobs(tmp_path):
    store = QueueStore(tmp_path / "q.db")
    try:
        seed(store, 2)
        blocked = store.claim(store.start_run("public_discovery"), "one")
        store.finish(blocked, "blocked")
        active = store.claim(store.start_run("public_discovery"), "two")
        assert store.retry_errors() == 1
        assert store.status()["queues"]["public_discovery"] == {"pending": 1, "running": 1}
        store.finish(active)
    finally:
        store.close()


def test_rut_identity_is_validated():
    from src.sites.dequienes.queue import normalized_rut

    assert normalized_rut("77.492.450-7") == "77492450-7"
    assert normalized_rut("77.492.450-0") is None


def test_explicit_rescan_preserves_metadata(tmp_path):
    store = QueueStore(tmp_path / "q.db")
    try:
        urls = seed(store)
        job = store.claim(store.start_run("public_discovery"), "one")
        store.finish_publication(job, parse_company(HTML, urls[0]), [])
        with store.transaction():
            store.db.execute("UPDATE publication_scans SET status='not_scanned'")
        assert store.rescan_legacy() == 1
        assert store.rescan_legacy() == 0
        assert store.db.execute("SELECT company_name FROM publications").fetchone()[0] == "Company SpA"
        assert store.claim(store.start_run("public_discovery"), "two")["key"] == urls[0]
    finally:
        store.close()
