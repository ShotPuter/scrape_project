"""Offline regressions against the claimed RES fleet behavior; no external I/O."""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripts import res_free_pilot as pilot
from src.sites.res import fleet
from src.sites.res.tracking import TrackingStore


@pytest.fixture
def queue(tmp_path):
    q = fleet.FleetQueue(tmp_path / "fleet.sqlite3", "audit")
    yield q
    q.close()


async def test_free_probe_success_becomes_ready(queue, tmp_path, monkeypatch):
    args = fleet.parse_args(["--providers", "free"])
    stop = asyncio.Event()
    p = fleet.FleetProducer(args, tmp_path, queue, asyncio.Event(), stop)
    queue.add_route("free", "http://9.9.9.9:80")

    async def verify(endpoint):
        stop.set()
        return "9.9.9.9"

    monkeypatch.setattr(p, "verify", verify)
    await asyncio.wait_for(p.scan(), 2)
    assert queue.counts()["routes"] == {"ready": 1}


async def test_brightdata_seeds_five_claimable_sessions(queue, monkeypatch):
    monkeypatch.setenv("BRIGHTDATA_PROXY_USERNAME", "audit-user")
    monkeypatch.setenv("BRIGHTDATA_PROXY_PASSWORD", "audit-password")
    args = fleet.parse_args(["--providers", "brightdata", "--browsers", "5"])
    seeded = await fleet.seed_routes(args, queue)
    assert seeded == {"brightdata": 5}
    assert queue.counts()["routes"] == {"ready": 5}
    owners = [f"owner-{index}" for index in range(5)]
    routes = [queue.claim_route(owner, ["brightdata"], {}, fleet.current_day(), 95) for owner in owners]
    assert all(routes)
    assert len({route["route_id"] for route in routes}) == 5
    assert queue.counts()["proxies"] == {"leased": 5}
    for route in routes:
        queue.finish_route(route)
    assert queue.counts()["proxies"] == {}


def test_expired_free_route_cannot_be_leased_before_maintenance(queue):
    queue.add_route("free", "http://9.9.9.9:80", state="ready")
    with queue.database:
        queue.database.execute("UPDATE fleet_routes SET expires_at=?", (time.time() - 1,))
    assert queue.claim_route("owner", ["free"], {}, fleet.current_day(), 95) is None


async def test_failed_and_duplicate_free_probes_do_not_crash_scanners(queue, tmp_path, monkeypatch):
    args = fleet.parse_args(["--providers", "free", "--proxy-rate-per-minute", "60000"])
    stop = asyncio.Event()
    producer = fleet.FleetProducer(args, tmp_path, queue, asyncio.Event(), stop)
    for index in range(3):
        queue.add_route("free", f"http://9.9.9.{index + 1}:80")
    calls = 0

    async def verify(endpoint):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("offline timeout")
        if calls == 3:
            stop.set()
        return "8.8.8.8"

    monkeypatch.setattr(producer, "verify", verify)
    await asyncio.wait_for(producer.scan(), 2)
    assert queue.counts()["routes"] == {"ready": 1, "cooldown": 2}
    assert producer.stats["connection_failed"] == 1


def test_finish_route_removes_live_worker_lease(queue):
    queue.add_route("paid", "9.9.9.9:80", state="ready")
    route = queue.claim_route("owner", ["paid"], {}, fleet.current_day(), 95)
    queue.finish_route(route)
    assert queue.counts()["proxies"] == {}
    queue.add_jobs([{"company_id": "c1"}])
    assert queue.claim_first_job("owner") is None


def test_restart_preserves_daily_limit_park(queue):
    queue.add_route("paid", "9.9.9.9:80", state="ready")
    route = queue.claim_route("owner", ["paid"], {}, fleet.current_day(), 95)
    queue.finish_route(route, documents=1, outcome="daily_limit", reset_at=time.time() + 3600)
    queue.recover()
    # This is what seed_routes does at startup for existing paid routes.
    queue.add_route("paid", "9.9.9.9:80", state="ready")
    assert queue.counts()["routes"] == {"parked": 1}


def test_restart_recovers_interrupted_free_probe(queue):
    queue.add_route("free", "http://9.9.9.9:80")
    with queue.database:
        queue.database.execute("UPDATE fleet_routes SET state='probing'")
    queue.recover()
    assert queue.counts()["routes"] == {"pending": 1}


def test_paid_wake_waits_for_duplicate_exit_without_crashing_other_routes(queue):
    queue.add_route("paid", "9.9.9.9:80", state="parked")
    queue.add_route("paid", "8.8.8.8:80", state="parked")
    queue.add_route("free", "http://7.7.7.7:80", state="ready")
    with queue.database:
        queue.database.execute("UPDATE fleet_routes SET parked_until=1 WHERE provider='paid'")
        queue.database.execute(
            "UPDATE fleet_routes SET exit_ip='1.1.1.1' WHERE endpoint IN ('9.9.9.9:80','http://7.7.7.7:80')"
        )
    assert queue.wake_parked(time.time()) == 1
    assert queue.counts()["routes"] == {"parked": 1, "ready": 2}


@pytest.mark.parametrize("keep,expected", [(False, "retired"), (True, "parked")])
def test_dead_free_route_is_retired_or_parked(queue, keep, expected):
    args = fleet.parse_args(["--providers", "free"])
    dispatcher = fleet.FleetDispatcher(args, queue, None, None, asyncio.Event(), asyncio.Event())
    queue.add_route("free", "http://9.9.9.9:80", state="ready")
    route = queue.claim_route("owner", ["free"], {}, fleet.current_day(), 95)
    outcome = dispatcher.route_outcome(fleet.BatchResult(error="HTTP GET connection failed (ReadTimeout)"), "free")
    assert outcome == "dead"
    assert queue.finish_route(route, outcome=outcome, keep_free=keep, reset_at=time.time() + 3600) == expected


def test_restart_after_reset_can_reuse_exhausted_paid_route(queue):
    queue.add_route("paid", "9.9.9.9:80", state="ready")
    route = queue.claim_route("owner", ["paid"], {}, fleet.current_day(), 95)
    queue.finish_route(route, documents=95, reset_at=time.time() - 3600)
    queue.recover()
    queue.add_route("paid", "9.9.9.9:80", state="ready")
    queue.wake_parked(time.time())
    assert queue.claim_route("new-owner", ["paid"], {}, fleet.current_day(), 95) is not None


async def test_idle_slots_do_not_remain_active(queue, tmp_path):
    queue.add_route("paid", "9.9.9.9:80", state="ready")
    args = fleet.parse_args(["--providers", "paid", "--browsers", "1"])
    stop = asyncio.Event()
    store = TrackingStore(tmp_path / "tracking.sqlite3", "audit")
    d = fleet.FleetDispatcher(args, queue, SimpleNamespace(notifications=None), store, asyncio.Event(), stop)
    task = asyncio.create_task(d.consume(0))
    try:
        await asyncio.sleep(0.05)
        assert d.active == {}
    finally:
        stop.set()
        await task
        store.close()


async def test_cooldown_does_not_start_another_browser(queue, tmp_path):
    queue.add_route("paid", "9.9.9.9:80", state="ready")
    queue.add_jobs([{"company_id": "c1"}])
    args = fleet.parse_args(["--providers", "paid", "--browsers", "1"])
    stop = asyncio.Event()
    store = TrackingStore(tmp_path / "tracking.sqlite3", "audit")
    store.register_run({}, [{"company_id": "c1", "company_name": "Audit company"}])
    store.request_cooldown(3600, None)
    worker = AsyncMock()
    worker.notifications = None

    async def call(*args):
        stop.set()
        return fleet.BatchResult()

    worker.side_effect = call
    d = fleet.FleetDispatcher(args, queue, worker, store, asyncio.Event(), stop)
    task = asyncio.create_task(d.consume(0))
    try:
        await asyncio.sleep(0.05)
        worker.assert_not_called()
    finally:
        stop.set()
        await task
        store.close()


async def test_residential_file_preserves_explicit_ca_and_expected_ip(tmp_path):
    proxy_file = tmp_path / "proxy.txt"
    proxy_file.write_text("http://test-user:test-password@brd.superproxy.io:44445\n")
    ca_file = tmp_path / "ca.crt"
    args = pilot.parse_args(
        [
            "--proxy-provider",
            "residential",
            "--proxy-file",
            str(proxy_file),
            "--proxy-ca-file",
            str(ca_file),
            "--expected-proxy-ip",
            "9.9.9.9",
        ]
    )
    routes = await pilot.load_proxy_endpoints(args)
    assert len(routes) == 1
    assert routes[0].ca_file == ca_file
    assert routes[0].expected_ip == "9.9.9.9"


async def test_execute_resumes_completed_run_without_network_or_credentials(tmp_path, monkeypatch):
    from src.utils.atomic import atomic_write_json

    args = fleet.parse_args(["--output", str(tmp_path), "--limit", "1", "--no-telegram"])
    root = tmp_path / args.run_id
    root.mkdir()
    company = {"company_id": "c1", "company_name": "Company", "rut": "1"}
    atomic_write_json(
        root / "selection.json", {"config": {"year": args.year, "month": args.month, "limit": 1}, "companies": [company]}
    )
    store = TrackingStore(tmp_path / "tracking.sqlite3", args.run_id)
    store.register_run({}, [company])
    store.search_result("c1", {"status": "not_found", "cves": []})
    store.close()
    monkeypatch.setattr(fleet, "seed_routes", AsyncMock(side_effect=AssertionError("No provider I/O")))
    monkeypatch.setattr(fleet.FleetProducer, "run", AsyncMock(side_effect=AssertionError("No discovery")))
    monkeypatch.setattr(fleet.FleetBrowserWorker, "__call__", AsyncMock(side_effect=AssertionError("No browser")))
    assert await asyncio.wait_for(fleet.execute(args, root), 5) == 0


async def test_browser_command_passes_shared_document_limits(queue, tmp_path, monkeypatch):
    args = fleet.parse_args(
        ["--providers", "paid", "--output", str(tmp_path), "--route-docs", "2", "--budget-paid", "3", "--reset-hour", "6"]
    )
    root = tmp_path / args.run_id
    root.mkdir()
    store = TrackingStore(tmp_path / "tracking.sqlite3", args.run_id)
    company = {"company_id": "c1", "company_name": "Company", "rut": "1"}
    store.register_run({}, [company])
    queue.add_route("paid", "9.9.9.9:80", state="ready")
    route = queue.claim_route("owner", ["paid"], {}, fleet.current_day(), 95)
    worker = fleet.FleetBrowserWorker(args, root, store, queue)

    async def run_child(slot, route, companies, owner, directory, prior, command):
        parsed = pilot.parse_args(command[command.index("--db") :])
        assert (parsed.fleet_route_docs, parsed.fleet_provider_budget, parsed.fleet_reset_hour) == (2, 3, 6)
        assert parsed.stream_db == tmp_path / "fleet-runtime.sqlite3"
        assert parsed.stream_owner == "owner"
        return fleet.BatchResult()

    monkeypatch.setattr(worker, "run_child", run_child)
    try:
        await worker(0, route, [company], "owner")
    finally:
        store.close()


@pytest.mark.parametrize("company_count", [1, 7])
async def test_execute_finishes_with_spare_slots_and_bounds_concurrency(tmp_path, monkeypatch, company_count):
    from src.utils.atomic import atomic_write_json

    args = fleet.parse_args(
        ["--providers", "paid", "--output", str(tmp_path), "--limit", str(company_count), "--no-telegram", "--browsers", "5"]
    )
    root = tmp_path / args.run_id
    root.mkdir()
    companies = [{"company_id": f"c{i}", "company_name": f"Company {i}", "rut": str(i)} for i in range(company_count)]
    atomic_write_json(
        root / "selection.json",
        {"config": {"year": args.year, "month": args.month, "limit": company_count}, "companies": companies},
    )
    active, peak = 0, 0

    async def seed(args, queue):
        for index in range(5):
            queue.add_route("paid", f"9.9.9.{index + 1}:80", state="ready")
        return {"paid": 5}

    async def worker(self, slot, route, companies, owner):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.05)
            identifier = companies[0]["company_id"]
            self.store.search_result(identifier, {"status": "not_found", "cves": []})
            return fleet.BatchResult(completed={identifier}, attempted={identifier})
        finally:
            active -= 1

    monkeypatch.setattr(fleet, "seed_routes", seed)
    monkeypatch.setattr(fleet.FleetBrowserWorker, "__call__", worker)
    assert await asyncio.wait_for(fleet.execute(args, root), 5) == 0
    assert active == 0
    assert peak == min(5, company_count)
