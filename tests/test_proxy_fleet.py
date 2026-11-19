"""Proxy fleet queue, budget and lifecycle tests."""

import asyncio
import time

import pytest

from src.proxy.fleet import (
    FleetArgs,
    FleetDispatcher,
    FleetQueue,
    LeaseResult,
    build_fleet_args,
    build_parser,
    current_day,
    next_reset,
    route_proxy_options,
    run_fleet,
)


@pytest.fixture
def queue(tmp_path):
    fleet_queue = FleetQueue(tmp_path / "fleet.sqlite3", "test")
    yield fleet_queue
    fleet_queue.close()


def finish(queue: FleetQueue, route: dict, *, documents: int = 1, outcome: str = "ok", **kwargs) -> str:
    options = {"documents": documents, "outcome": outcome, "reset_at": time.time() + 3600, "route_docs": 95}
    options.update(kwargs)
    return queue.finish_route(route, **options)


def test_route_quota_parks_until_reset_then_wakes(queue):
    queue.add_route("paid", "1.2.3.4:8080", proxy_url="http://u:p@1.2.3.4:8080", state="ready")
    budgets = {"paid": 100_000}
    day = current_day()

    route = queue.claim_route("owner-1", ["paid"], budgets, day, route_docs=2)
    assert route and route["state"] == "leased"
    assert finish(queue, route, route_docs=2) == "ready"

    route = queue.claim_route("owner-2", ["paid"], budgets, day, route_docs=2)
    assert route is not None
    assert finish(queue, route, route_docs=2) == "parked"
    assert queue.claim_route("owner-3", ["paid"], budgets, day, route_docs=2) is None
    assert queue.counts()["routes"] == {"parked": 1}

    assert queue.wake_parked(time.time() + 7200) == 1
    route = queue.claim_route("owner-4", ["paid"], budgets, day, route_docs=2)
    assert route is not None
    assert route["documents"] == 0


def test_provider_budget_blocks_claims_and_parks_remaining_routes(queue):
    queue.add_route("paid", "1.2.3.4:8080", state="ready")
    queue.add_route("paid", "5.6.7.8:8080", state="ready")
    day = current_day()
    budgets = {"paid": 1}

    route = queue.claim_route("owner", ["paid"], budgets, day, route_docs=95)
    assert route is not None
    assert finish(queue, route, documents=1) == "ready"
    assert queue.claim_route("owner", ["paid"], budgets, day, route_docs=95) is None
    assert queue.park_provider("paid", time.time() + 3600) == 2
    assert queue.counts()["routes"] == {"parked": 2}


def test_free_route_parks_when_keep_enabled(queue):
    queue.add_route("free", "9.9.9.9:3128", state="ready")
    route = queue.claim_route("owner", ["free"], {"free": 100}, current_day(), route_docs=1)
    assert route is not None

    assert finish(queue, route, route_docs=1, keep_free=True) == "parked"
    assert queue.wake_parked(time.time() + 7200) == 1
    # Free routes re-probe instead of trusting the old verification.
    assert queue.counts()["routes"] == {"pending": 1}


def test_free_route_is_retired_when_keep_disabled(queue):
    queue.add_route("free", "9.9.9.9:3128", state="ready")
    route = queue.claim_route("owner", ["free"], {"free": 100}, current_day(), route_docs=1)
    assert route is not None

    assert finish(queue, route, route_docs=1, keep_free=False) == "retired"
    assert queue.counts()["routes"] == {}


def test_repeated_failures_park_a_route(queue):
    queue.add_route("paid", "1.2.3.4:8080", state="ready")
    day = current_day()
    route = queue.claim_route("owner", ["paid"], {"paid": 100}, day, route_docs=95)
    assert route is not None

    assert finish(queue, route, outcome="dead", max_failures=2, retry_seconds=0) == "cooldown"
    # Cooldown expiry promotes paid routes straight back to ready.
    assert queue.wake_cooldown(time.time() + 1) == 1
    route = queue.claim_route("owner", ["paid"], {"paid": 100}, day, route_docs=95)
    assert route is not None

    assert finish(queue, route, outcome="dead", max_failures=2) == "parked"


def test_expired_free_routes_are_reprobed(queue):
    queue.add_route("free", "9.9.9.9:3128", state="ready")
    with queue.write():
        queue.database.execute("UPDATE fleet_routes SET expires_at=?", (time.time() - 1,))

    assert queue.expire_free(time.time()) == 1
    assert queue.counts()["routes"] == {"pending": 1}
    # Paid routes never expire from verification TTL.
    queue.add_route("paid", "1.2.3.4:8080", state="ready")
    with queue.write():
        queue.database.execute("UPDATE fleet_routes SET expires_at=? WHERE provider='paid'", (time.time() - 1,))
    assert queue.expire_free(time.time()) == 0


def test_recover_returns_leases_to_pending(queue):
    queue.add_route("paid", "1.2.3.4:8080", state="ready")
    assert queue.claim_route("owner", ["paid"], {"paid": 100}, current_day(), 95) is not None

    queue.recover()
    assert queue.counts()["routes"] == {"pending": 1}


def test_route_proxy_options_carry_brightdata_ca_bundle():
    options = route_proxy_options(
        {
            "endpoint": "brd.superproxy.io:44445",
            "proxy_url": "http://user:pass@brd.superproxy.io:44445",
            "expected_ip": "203.0.113.7",
            "ca_file": "docker/brightdata-proxy-ca.crt",
        }
    )
    assert options == {
        "proxy_url": "http://user:pass@brd.superproxy.io:44445",
        "ca_file": "docker/brightdata-proxy-ca.crt",
        "expected_ip": "203.0.113.7",
    }


def test_next_reset_is_in_the_future():
    assert next_reset(0) > time.time()


def test_dispatcher_day_rollover_resets_route_counters(tmp_path):
    args = build_fleet_args(
        build_parser().parse_args(["--output", str(tmp_path), "--providers", "paid", "--browsers", "1"]), tmp_path
    )
    fleet_queue = FleetQueue(args.database, args.run_id)
    try:
        fleet_queue.add_route("paid", "1.2.3.4:8080", state="ready")
        dispatcher = FleetDispatcher(args, fleet_queue, _noop_worker, asyncio.Event(), asyncio.Event())
        with fleet_queue.write():
            fleet_queue.database.execute("UPDATE fleet_routes SET documents=40,failures=2")
        dispatcher.day = "2000-01-01"
        dispatcher.rollover_day()
        row = fleet_queue.database.execute("SELECT documents,failures FROM fleet_routes").fetchone()
        assert (row["documents"], row["failures"]) == (0, 0)
        assert dispatcher.day == current_day()
    finally:
        fleet_queue.close()


async def _noop_worker(slot: int, route: dict) -> LeaseResult:
    return LeaseResult(route=route)


def test_dispatcher_reports_exhausted_budgets(tmp_path):
    args = build_fleet_args(
        build_parser().parse_args(
            [
                "--output",
                str(tmp_path),
                "--providers",
                "paid,free",
                "--browsers",
                "1",
                "--budget-paid",
                "2",
                "--budget-free",
                "3",
            ]
        ),
        tmp_path,
    )
    fleet_queue = FleetQueue(args.database, args.run_id)
    try:
        dispatcher = FleetDispatcher(args, fleet_queue, _noop_worker, asyncio.Event(), asyncio.Event())
        assert not dispatcher.budgets_exhausted()
        day = current_day()
        with fleet_queue.write():
            fleet_queue.database.execute("INSERT INTO fleet_budgets(provider,day,documents) VALUES('paid',?,2)", (day,))
        assert not dispatcher.budgets_exhausted()
        with fleet_queue.write():
            fleet_queue.database.execute("INSERT INTO fleet_budgets(provider,day,documents) VALUES('free',?,3)", (day,))
        assert dispatcher.budgets_exhausted()
    finally:
        fleet_queue.close()


def test_dispatcher_rejects_more_than_ten_browsers(tmp_path):
    args = build_fleet_args(
        build_parser().parse_args(["--output", str(tmp_path), "--providers", "paid", "--browsers", "11"]), tmp_path
    )
    fleet_queue = FleetQueue(args.database, args.run_id)
    try:
        with pytest.raises(ValueError):
            FleetDispatcher(args, fleet_queue, _noop_worker, asyncio.Event(), asyncio.Event())
    finally:
        fleet_queue.close()


async def test_run_fleet_run_once_exits_without_network(tmp_path, monkeypatch):
    monkeypatch.delenv("ALLOW_PUBLIC_PROXY_COLLECTION", raising=False)
    args = build_parser().parse_args(["--output", str(tmp_path), "--providers", "free", "--browsers", "2", "--run-once"])
    await asyncio.wait_for(run_fleet(args, _noop_worker), timeout=10)

    status = build_parser().parse_args(["--output", str(tmp_path), "--providers", "free", "--status"])
    await asyncio.wait_for(run_fleet(status, _noop_worker), timeout=10)


def test_fleet_args_budget_defaults(tmp_path):
    args: FleetArgs = build_fleet_args(
        build_parser().parse_args(["--output", str(tmp_path), "--providers", "paid,webshare"]), tmp_path
    )
    assert args.browsers == 5
    assert args.provider_set == frozenset({"paid", "webshare"})
    assert args.free_keep is True
    assert args.database == tmp_path / "fleet.sqlite3"


async def test_producer_queues_only_firewall_eligible_free_candidates(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from src.proxy.fleet import FleetProducer
    from src.proxy.sources.catalog import ProxyCandidate

    args = build_fleet_args(
        build_parser().parse_args(
            ["--output", str(tmp_path), "--providers", "free", "--proxy-ports", "3128", "--browsers", "1"]
        ),
        tmp_path,
    )
    fleet_queue = FleetQueue(args.database, args.run_id)
    catalog = SimpleNamespace(
        candidates=[
            ProxyCandidate("http://9.9.9.9:3128", "http", "test"),
            ProxyCandidate("http://8.8.8.8:9999", "http", "test"),
            ProxyCandidate("http://10.0.0.1:3128", "http", "test"),
            ProxyCandidate("http://user:pass@7.7.7.7:3128", "http", "test"),
        ],
        errors={},
    )

    async def fake_catalog(*args, **kwargs):
        return catalog

    monkeypatch.setattr("src.proxy.fleet.public_collection_allowed", lambda: True)
    monkeypatch.setattr("src.proxy.fleet.load_public_catalog", fake_catalog)
    producer = FleetProducer(args, tmp_path, fleet_queue, asyncio.Event(), asyncio.Event())
    try:
        await producer.refresh()
        assert fleet_queue.counts()["routes"] == {"pending": 1}
        assert producer.stats["queue_inserted"] == 1
    finally:
        fleet_queue.close()
