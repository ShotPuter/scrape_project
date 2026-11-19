"""RES mixed-provider fleet: quotas, parking, retirement and wake-up rules."""

import asyncio
import time
from types import SimpleNamespace

import pytest

from src.sites.res import fleet
from src.sites.res.free_stream import BatchResult
from src.sites.res.tracking import TrackingStore


@pytest.fixture
def queue(tmp_path):
    database = fleet.FleetQueue(tmp_path / "fleet-runtime.sqlite3", "test-run")
    yield database
    database.close()


def test_fresh_database_schema_and_daily_budget(queue):
    queue.add_route("webshare", "1.2.3.4:8080", state="ready")
    route = queue.claim_route("owner", ["webshare"], {"webshare": 100}, fleet.current_day(), 95)
    assert route is not None

    queue.finish_route(route, documents=3, route_docs=95)
    assert queue.provider_spent("webshare", fleet.current_day()) == 3

    route = queue.claim_route("owner", ["webshare"], {"webshare": 100}, fleet.current_day(), 95)
    assert route is not None
    queue.finish_route(route, documents=2, route_docs=95)
    assert queue.provider_spent("webshare", fleet.current_day()) == 5
    assert queue.provider_spent("webshare", "2020-01-01") == 0


def test_paid_route_drains_to_cap_and_is_parked_until_reset(queue):
    queue.add_route("paid", "1.2.3.4:8080", state="ready")
    day = fleet.current_day()
    reset_at = time.time() + 3600

    route = queue.claim_route("owner", ["paid"], {"paid": 0}, day, route_docs=2)
    assert route is not None
    assert queue.finish_route(route, documents=2, outcome="ok", reset_at=reset_at, route_docs=2) == "parked"
    assert queue.claim_route("owner", ["paid"], {"paid": 0}, day, route_docs=2) is None

    assert queue.wake_parked(reset_at + 1) == 1
    route = queue.claim_route("owner", ["paid"], {"paid": 0}, day, route_docs=2)
    assert route is not None
    assert route["documents"] == 0


def test_free_route_retired_or_kept_after_quota(queue):
    queue.add_route("free", "9.9.9.9:3128", state="ready")
    route = queue.claim_route("owner", ["free"], {"free": 0}, fleet.current_day(), route_docs=1)
    assert route is not None
    assert queue.finish_route(route, documents=1, reset_at=time.time() + 3600, route_docs=1, keep_free=False) == "retired"
    assert queue.counts()["routes"] == {}

    queue.add_route("free", "8.8.8.8:3128", state="ready")
    route = queue.claim_route("owner", ["free"], {"free": 0}, fleet.current_day(), route_docs=1)
    assert route is not None
    assert queue.finish_route(route, documents=1, reset_at=time.time() + 3600, route_docs=1, keep_free=True) == "parked"
    # Free routes must be re-probed after the next quota, unlike paid ones.
    assert queue.wake_parked(time.time() + 7200) == 1
    assert queue.counts()["routes"] == {"pending": 1}


def test_counts_are_compatible_with_shared_stream_summary(queue):
    """The shared StreamNotifications formatter reads counts['proxies']."""
    queue.add_route("paid", "1.2.3.4:8080", state="ready")
    counts = queue.counts()
    assert set(counts) == {"jobs", "routes", "proxies", "budgets"}
    assert counts["proxies"] == {}  # leased routes are mirrored only while owned


def test_expired_free_route_is_reprobed(queue):
    queue.add_route("free", "9.9.9.9:3128", state="ready")
    with queue.database:
        queue.database.execute("UPDATE fleet_routes SET expires_at=?", (time.time() - 1,))
    assert queue.expire_free(time.time()) == 1
    assert queue.counts()["routes"] == {"pending": 1}


def test_provider_budget_blocks_claims_and_parks_paid_routes(queue):
    queue.add_route("webshare", "1.2.3.4:8080", state="ready")
    queue.add_route("webshare", "5.6.7.8:8080", state="ready")
    day = fleet.current_day()

    route = queue.claim_route("owner", ["webshare"], {"webshare": 1}, day, 95)
    assert route is not None
    queue.finish_route(route, documents=1, route_docs=95)
    assert queue.claim_route("owner", ["webshare"], {"webshare": 1}, day, 95) is None
    assert queue.park_provider("webshare", time.time() + 3600) == 2
    assert queue.counts()["routes"] == {"parked": 2}


def test_job_requires_live_route_lease_and_is_released_with_it(queue):
    queue.add_jobs([{"company_id": "company-1"}])
    assert queue.claim_first_job("owner") is None

    queue.add_route("brightdata", "brd.superproxy.io:44445", state="ready")
    route = queue.claim_route("owner", ["brightdata"], {"brightdata": 0}, fleet.current_day(), 95)
    assert route is not None
    job = queue.claim_first_job("owner")
    assert job and job["company_id"] == "company-1"

    queue.finish_route(route, route_docs=95)
    queue.finish_job("company-1", "owner", complete=True, attempted=True, limit=3, retry=0)
    assert queue.claim_first_job("owner") is None
    assert queue.owned_companies("owner") == []
    assert queue.completed_company_ids() == {"company-1"}


def test_route_outcome_classifies_daily_limit_and_dead_routes(tmp_path):
    args = fleet.parse_args(["--providers", "paid", "--browsers", "1"])
    dispatcher = fleet.FleetDispatcher(args, None, None, None, None, None)
    assert dispatcher.route_outcome(SimpleNamespace(quota_limited=True, retry_connection=False, error=None)) == "daily_limit"
    assert dispatcher.route_outcome(SimpleNamespace(quota_limited=False, retry_connection=True, error=None)) == "retry"
    result = SimpleNamespace(quota_limited=False, retry_connection=False, error="proxy auth failed")
    assert dispatcher.route_outcome(result) == "dead"
    assert dispatcher.route_outcome(SimpleNamespace(quota_limited=False, retry_connection=False, error=None)) == "ok"


def test_parse_args_defaults_to_five_browsers_and_all_paid_providers(monkeypatch):
    for name in ("RES_BROWSERS", "RES_FLEET_PROVIDERS", "RES_FLEET_BUDGET_FREE"):
        monkeypatch.delenv(name, raising=False)
    args = fleet.parse_args([])
    assert args.browsers == 5
    assert args.provider_set == {"webshare", "brightdata", "free"}
    assert args.route_docs == 95
    assert args.reset_hour == 0
    assert args.free_keep is False


class FakeWorker:
    """Substitutes the browser worker while exercising the real dispatcher loop."""

    def __init__(self, result: BatchResult, stop: asyncio.Event, *, calls_to_stop: int = 1):
        self.result = result
        self.notifications = None
        self.stop = stop
        self.calls_to_stop = calls_to_stop
        self.calls = 0

    async def __call__(self, slot, route, companies, owner):
        self.calls += 1
        if self.calls >= self.calls_to_stop:
            self.stop.set()
        return self.result


async def test_dispatcher_drains_paid_route_parks_and_wakes_it(tmp_path):
    """Full loop: lease → drain to cap → park → reset → re-lease with zeroed counters."""
    queue = fleet.FleetQueue(tmp_path / "fleet.sqlite3", "run")
    store = TrackingStore(tmp_path / "tracking.sqlite3", "run")
    queue.add_route("webshare", "1.2.3.4:8080", state="ready")
    queue.add_jobs([{"company_id": "c1"}])

    stop = asyncio.Event()
    result = BatchResult(completed={"c1"}, attempted={"c1"}, new_pdfs=2)
    worker = FakeWorker(result, stop)
    args = fleet.parse_args([
        "--providers", "webshare", "--browsers", "1", "--route-docs", "2", "--budget-webshare", "0"
    ])
    dispatcher = fleet.FleetDispatcher(args, queue, worker, store, asyncio.Event(), stop)

    try:
        await asyncio.wait_for(dispatcher.consume(0), 5)
        assert worker.calls == 1
        assert queue.provider_spent("webshare", fleet.current_day()) == 2
        assert queue.counts()["routes"] == {"parked": 1}
        assert queue.counts()["jobs"] == {"done": 1}
        assert dispatcher.total_new_pdfs == 2
        assert dispatcher.site_limits[0]["state"] == "parked"

        # Simulate the quota reset arriving.
        with queue.database:
            queue.database.execute("UPDATE fleet_routes SET parked_until=?", (time.time() - 1,))
        assert queue.wake_parked(time.time()) == 1
        reclaimed = queue.claim_route(
            "next-owner", ["webshare"], {"webshare": 0}, fleet.current_day(), args.route_docs
        )
        assert reclaimed is not None
        assert reclaimed["documents"] == 0
    finally:
        queue.close()
        store.db.close()


async def test_dispatcher_retires_free_route_when_drained(tmp_path):
    """A drained free route is deleted (default), not parked."""
    queue = fleet.FleetQueue(tmp_path / "fleet.sqlite3", "run")
    store = TrackingStore(tmp_path / "tracking.sqlite3", "run")
    queue.add_route("free", "9.9.9.9:8080", state="ready")
    queue.add_jobs([{"company_id": "c1"}])

    stop = asyncio.Event()
    result = BatchResult(completed={"c1"}, attempted={"c1"}, new_pdfs=1)
    worker = FakeWorker(result, stop)
    args = fleet.parse_args(["--providers", "free", "--browsers", "1", "--route-docs", "1"])
    dispatcher = fleet.FleetDispatcher(args, queue, worker, store, asyncio.Event(), stop)

    try:
        await asyncio.wait_for(dispatcher.consume(0), 5)
        assert worker.calls == 1
        assert queue.counts()["routes"] == {}
        assert dispatcher.site_limits[0]["retired"] is True
        assert dispatcher.total_new_pdfs == 1
    finally:
        queue.close()
        store.db.close()
