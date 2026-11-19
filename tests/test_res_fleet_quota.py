"""Real document flow and separate SQLite connections share one hard quota."""

import sqlite3
from collections import Counter
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripts import res_free_pilot as pilot
from src.sites.res import continuous_worker, fleet, fleet_quota
from src.sites.res.fleet_quota import FleetQuota, quota_day
from src.sites.res.free_queue import FreeQueue
from src.sites.res.source import SearchIssue
from src.sites.res.tracking import TrackingStore


@pytest.fixture
def queue(tmp_path):
    q = fleet.FleetQueue(tmp_path / "fleet.sqlite3", "test")
    yield q
    q.close()


def lease(queue, owner="owner", endpoint="9.9.9.9:80"):
    queue.add_route("paid", endpoint, state="ready")
    return queue.claim_route(owner, ["paid"], {}, quota_day(), 95)


def test_live_meter_is_not_counted_twice_when_worker_finishes(queue):
    route = lease(queue)
    meter = FleetQuota(queue.database, "owner", 2, 0, 0)
    for _ in range(2):
        with meter.document():
            pass
    with pytest.raises(SearchIssue, match="document cap"), meter.document():
        pytest.fail("Must not send another document request")
    assert queue.finish_route(route, documents=2, route_docs=2, reset_at=1) == "parked"
    assert queue.provider_spent("paid", quota_day()) == 2


def test_overlapping_workers_cannot_spend_the_same_remaining_budget(queue, tmp_path):
    lease(queue)
    lease(queue, "other", "8.8.8.8:80")
    second = sqlite3.connect(tmp_path / "fleet.sqlite3")
    first_meter = FleetQuota(queue.database, "owner", 95, 1, 0)
    second_meter = FleetQuota(second, "other", 95, 1, 0)
    try:
        with first_meter.document(), pytest.raises(SearchIssue, match="provider daily budget"), second_meter.document():
            pytest.fail("Budget already reserved by another process")
        assert queue.provider_spent("paid", quota_day()) == 1
    finally:
        second.close()


def test_failed_document_releases_reservation_and_database_lock(queue, tmp_path):
    lease(queue)
    meter = FleetQuota(queue.database, "owner", 1, 1, 0)
    with pytest.raises(RuntimeError, match="failed"), meter.document():
        other = sqlite3.connect(tmp_path / "fleet.sqlite3", timeout=0.1)
        try:
            with other:
                other.execute("UPDATE fleet_routes SET tokens=tokens+1")
        finally:
            other.close()
        raise RuntimeError("failed")
    assert queue.provider_spent("paid", quota_day()) == 0
    with meter.document():
        pass


def test_crash_reservations_survive_recovery_without_exceeding_cap(queue):
    lease(queue)
    # Simulate a worker killed after admission, before recording the response.
    guard = FleetQuota(queue.database, "owner", 1, 1, 0).document()
    guard.__enter__()
    queue.recover()
    route = queue.claim_route("next", ["paid"], {"paid": 1}, quota_day(), 1)
    assert route is None
    assert queue.provider_spent("paid", quota_day()) == 1
    # A normal exit does not refund a document that might have reached RES.
    guard.__exit__(None, None, None)


def test_reset_hour_applies_to_budgets_and_live_route_counters(queue, monkeypatch):
    lease(queue)
    meter = FleetQuota(queue.database, "owner", 1, 1, 6)
    now = datetime(2026, 11, 19, 5, 59, tzinfo=UTC)
    monkeypatch.setattr(fleet_quota, "quota_day", lambda hour: quota_day(hour, now))
    with meter.document():
        pass
    assert queue.provider_spent("paid", "2026-11-18") == 1
    now = datetime(2026, 11, 19, 6, 0, tzinfo=UTC)
    with meter.document():
        pass
    assert queue.provider_spent("paid", "2026-11-19") == 1
    assert queue.database.execute("SELECT documents FROM fleet_routes").fetchone()[0] == 1


async def test_continuous_worker_stops_mid_company_and_next_route_resumes(tmp_path, queue, monkeypatch):
    companies = [{"company_id": "c1", "company_name": "Test", "rut": "1"}]
    cves = ["ACuKDHhqsvAd", "BCuKDHhqsvAd", "CCuKDHhqsvAd"]
    route = lease(queue)
    queue.add_jobs(companies)
    initial = queue.claim_first_job("owner")
    child = FreeQueue(tmp_path / "fleet.sqlite3", "test", initialize=False)
    store = TrackingStore(tmp_path / "tracking.sqlite3", "test", worker="w00")
    store.register_run({}, companies)
    store.search_result("c1", {"status": "completed", "cves": cves})
    args = pilot.parse_args(["--output", str(tmp_path / "worker"), "--max-documents", "0", "--delay", "0"])
    args.output.mkdir()
    args.state_path = args.output / "state.json"
    args.stream_owner = "owner"
    args.tracking_store = store
    args.fleet_quota = FleetQuota(child.database, "owner", 2, 0, 0)
    download = AsyncMock()
    monkeypatch.setattr(continuous_worker, "check_route", AsyncMock())
    monkeypatch.setattr(
        pilot.http_flow,
        "verify",
        lambda http, cve, token: (
            cve,
            "code",
            f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={cve}&codigo=valid",
        ),
    )
    calls = []

    def pdf(http, link):
        calls.append(link)
        return b"%PDF-1.4\nfixture\n%%EOF"

    monkeypatch.setattr(pilot.http_flow, "download", pdf)
    cascade = SimpleNamespace(minted=0, events=[], mint=download)
    counters = Counter()
    report = {"run_id": "audit", "attempts": [], "companies": []}
    stats = {"index": 0, "http_ip": "9.9.9.9", "companies": 0, "challenges": 0}
    try:
        await continuous_worker.consume_session(cascade, object(), initial, child, args, {}, counters, report, stats)
        assert len(calls) == counters["downloaded"] == 2
        assert report["stop_reason"]["code"] == "fleet_route_limit"
        assert queue.counts()["jobs"] == {"pending": 1}
        assert queue.database.execute("SELECT attempts FROM free_jobs").fetchone()[0] == 0
        queue.finish_route(route, documents=2, outcome="budget", route_docs=2, reset_at=1)
        route = lease(queue, "next", "8.8.8.8:80")
        args.stream_owner = "next"
        args.fleet_quota = FleetQuota(child.database, "next", 2, 0, 0)
        initial = queue.claim_first_job("next")
        await continuous_worker.consume_session(cascade, object(), initial, child, args, {}, counters, report, stats)
        assert len(calls) == 3
        assert counters["reused"] == 2
        assert queue.counts()["jobs"] == {"done": 1}
        queue.finish_route(route, documents=1, route_docs=2)
        assert queue.provider_spent("paid", quota_day()) == 3
    finally:
        child.close()
        store.close()
