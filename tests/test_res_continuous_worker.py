"""Dynamic consumption, bounded retries and durable acknowledgement without live sites."""

from collections import Counter
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripts import res_free_pilot as pilot
from src.sites.res import continuous_worker as worker
from src.sites.res.free_queue import FreeQueue
from src.sites.res.tracking import TrackingStore


def setup_session(tmp_path, number):
    companies = [
        {"company_id": f"id-{index:03d}", "company_name": f"Company {index}", "rut": str(index)} for index in range(number)
    ]
    queue = FreeQueue(tmp_path / "runtime.sqlite3", "test")
    queue.add_jobs(companies)
    queue.add_candidates([("http://11.2.3.4:80", "fixture")], capacity=0)
    queue.finish_probe("http://11.2.3.4:80", exit_ip="11.2.3.4")
    _, initial = queue.claim_batch("owner", 1)
    store = TrackingStore(tmp_path / "tracking.sqlite3", "test", worker="w00")
    store.register_run({}, companies)
    args = pilot.parse_args(
        [
            "--output",
            str(tmp_path),
            "--company-attempts",
            "1",
            "--max-failures",
            "1",
            "--stream-owner",
            "owner",
            "--stream-retry-delay",
            "0",
        ]
    )
    args.state_path = tmp_path / "state.json"
    args.tracking_store = store
    return queue, store, args, initial[0]


async def run_session(queue, args, initial, monkeypatch, process):
    monkeypatch.setattr(worker, "check_route", AsyncMock())
    monkeypatch.setattr(pilot, "process_company", process)
    report = {"run_id": "unit", "attempts": [], "companies": []}
    state = {"companies": {}, "cves": {}}
    stats = {"index": 0, "http_ip": "11.2.3.4", "companies": 0, "challenges": 0}
    cascade = SimpleNamespace(minted=0, events=[])
    await worker.consume_session(cascade, object(), initial, queue, args, state, Counter(), report, stats)
    return report, stats


async def test_more_than_fifty_companies_use_same_browser_and_only_one_job_is_leased(tmp_path, monkeypatch):
    queue, store, args, initial = setup_session(tmp_path, 53)
    seen = []

    async def process(cascade, http, company, state, args, output, counters):
        assert queue.counts()["jobs"]["leased"] == 1
        seen.append((company["company_id"], id(cascade), id(http)))
        return {**company, "status": "not_found", "documents": []}

    try:
        report, _ = await run_session(queue, args, initial, monkeypatch, process)
        assert len(seen) == 53 and len({row[1:] for row in seen}) == 1
        assert queue.counts()["jobs"] == {"done": 53}
        assert report["stop_reason"]["code"] == "queue_drained"
    finally:
        queue.close()
        store.close()


async def test_network_retry_keeps_browser_then_continues_next_company(tmp_path, monkeypatch):
    queue, store, args, initial = setup_session(tmp_path, 2)
    calls = []

    async def process(cascade, http, company, state, args, output, counters):
        calls.append((company["company_id"], id(cascade)))
        if len(calls) <= 2:
            raise pilot.SearchIssue("proxy_failed", "RES POST connection failed (ReadTimeout); token not replayed")
        return {**company, "status": "not_found", "documents": []}

    try:
        report, _ = await run_session(queue, args, initial, monkeypatch, process)
        assert len(calls) == 4 and len({row[1] for row in calls}) == 1
        assert report["same_session_network_retries"] == 2
        assert queue.counts()["jobs"] == {"done": 2}
        assert queue.database.execute("SELECT MAX(attempts) FROM free_jobs").fetchone()[0] == 1
    finally:
        queue.close()
        store.close()


@pytest.mark.parametrize(
    "status,error,retries",
    [
        ("proxy_failed", "HTTP GET connection failed (ConnectTimeout)", 2),
        ("captcha_required", "Google declined audio challenge", 0),
        ("daily_limit", "No es posible generar más de 100 documentos diarios", 0),
    ],
)
async def test_final_failure_reports_actual_cause_and_releases_unfinished_company(
    tmp_path, monkeypatch, status, error, retries
):
    queue, store, args, initial = setup_session(tmp_path, 2)
    calls = []

    async def process(cascade, http, company, state, args, output, counters):
        calls.append(company["company_id"])
        raise pilot.SearchIssue(status, error)

    try:
        report, stats = await run_session(queue, args, initial, monkeypatch, process)
        assert len(calls) == retries + 1
        assert report["stop_reason"]["error"] == error and stats["error"] == error
        assert "Queue paused" not in stats["error"]
        assert queue.counts()["jobs"] == {"pending": 2}
        assert not queue.owned_companies("owner")
    finally:
        queue.close()
        store.close()


async def test_saved_pdf_is_reused_without_minting_or_downloading(tmp_path, monkeypatch):
    queue, store, args, initial = setup_session(tmp_path, 1)
    cve = "ACuKDHhqsvAd"
    store.search_result(initial["company_id"], {"status": "completed", "cves": [cve]})
    saved = pilot.persist_pdf(
        tmp_path,
        cve,
        b"%PDF-1.4\nfixture\n%%EOF",
        f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={cve}&codigo=valid",
    )
    store.file_result(saved, tmp_path, initial["company_id"])
    pdf = tmp_path / saved["local_path"]
    before = pdf.read_bytes(), pdf.stat().st_mtime_ns
    monkeypatch.setattr(pilot.http_flow, "download", lambda *args: pytest.fail("No PDF download expected"))
    try:
        report, _ = await run_session(queue, args, initial, monkeypatch, pilot.process_company)
        assert queue.counts()["jobs"] == {"done": 1}
        assert report["progress"]["reused"] == 1
        assert (pdf.read_bytes(), pdf.stat().st_mtime_ns) == before
    finally:
        queue.close()
        store.close()


async def test_route_reputation_is_checked_before_any_network_request(monkeypatch):
    from unittest.mock import Mock

    from src.proxy.reputation import ReputationDenied

    monkeypatch.setattr(worker, "check_proxy", Mock(side_effect=ReputationDenied("listed")))
    get_ip = Mock(side_effect=AssertionError("No connection to a listed proxy"))
    monkeypatch.setattr(pilot, "http_exit_ip", get_ip)
    with pytest.raises(ReputationDenied):
        await worker.check_route(object(), "11.2.3.4", "http://11.2.3.4:80")
    get_ip.assert_not_called()


def test_repeated_claim_cannot_reserve_two_companies_for_one_browser(tmp_path):
    queue, store, args, initial = setup_session(tmp_path, 2)
    try:
        assert queue.claim_company("owner")["company_id"] == initial["company_id"]
        assert queue.counts()["jobs"] == {"leased": 1, "pending": 1}
    finally:
        queue.close()
        store.close()
