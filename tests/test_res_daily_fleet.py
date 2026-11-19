"""Day barriers, ten browser slots, legacy PDF preservation and crash recovery."""

import asyncio
import json
from collections import Counter
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripts import res_free_pilot as pilot
from src.sites.res import daily_fleet, daily_queue, daily_worker, fleet
from src.sites.res.daily_queue import DailyQueue
from src.sites.res.fleet_import import import_bulk
from src.sites.res.fleet_quota import FleetQuota
from src.sites.res.pdf import persist_pdf
from src.sites.res.tracking import TrackingStore

DAY = date(2025, 7, 29)
PDF = b"%PDF-1.4\nfixture\n%%EOF"


def action(index=0):
    return {"cve": f"ACuKDHhq{index:04d}", "company_name": f"Company {index}", "rut": f"1234567{index}-0"}


def saved(store, folder, row):
    result = persist_pdf(
        folder,
        row["cve"],
        PDF,
        f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={row['cve']}&codigo=valid",
    )
    store.file_result(result, folder)
    return folder / result["local_path"]


def route(queue, owner="owner", index=0):
    queue.add_route("webshare", f"9.9.9.{index + 1}:80", state="ready")
    return queue.claim_route(owner, ["webshare"], {}, fleet.current_day(), 95)


def finish(queue, job, owner="owner", complete=True, attempted=True):
    queue.finish_job(job["company_id"], owner, complete=complete, attempted=attempted, limit=1, retry=0)


@pytest.fixture
def state(tmp_path):
    queue = DailyQueue(tmp_path / "fleet.sqlite3", "test")
    store = TrackingStore(tmp_path / "tracking.sqlite3", "test")
    store.register_run({}, [])
    yield queue, store
    queue.close()
    store.close()


def test_day_barrier_waits_for_failed_and_leased_documents(state):
    queue, store = state
    older = date(2025, 7, 28)
    assert queue.schedule_day(older, DAY, "action") == DAY.isoformat()
    lease = route(queue)
    job = queue.claim_first_job("owner")
    from src.sites.res.benchmark import write_database

    # The real subprocess adapter requires a non-NULL RUT column even for a date job.
    write_database(queue.path.parent / "worker-input.sqlite3", [job])
    actions = [action(0), action(1)]
    store.day_result(job["day"], "action", actions, complete=True)
    queue.expand_day(job["day"], "action", actions, store)
    finish(queue, job)
    first = queue.claim_first_job("owner")
    finish(queue, first, complete=False)
    assert queue.schedule_day(older, DAY, "action") == DAY.isoformat()
    retried = queue.claim_first_job("owner")
    assert retried["company_id"] != first["company_id"]
    finish(queue, retried)
    last = queue.claim_first_job("owner")
    assert last["company_id"] == first["company_id"]
    assert queue.schedule_day(older, DAY, "action") == DAY.isoformat()
    finish(queue, last)
    assert queue.schedule_day(older, DAY, "action") == older.isoformat()
    assert queue.days_done() == 1
    queue.finish_route(lease)


async def test_restart_after_discovery_commit_rebuilds_missing_cve_jobs(state):
    queue, store = state
    queue.schedule_day(DAY, DAY, "action")
    route(queue)
    job = queue.claim_first_job("owner")
    store.day_result(job["day"], "action", [action()], complete=True)
    # Crash before inserting CVE jobs and acknowledging discovery.
    queue.recover()
    args = SimpleNamespace(start=DAY, end=DAY, kind="action", run_id="test")
    assert await daily_fleet.advance(queue, store, args) == DAY.isoformat()
    assert queue.counts()["jobs"] == {"done": 1, "pending": 1}
    assert queue.schedule_day(DAY, DAY, "action") == DAY.isoformat()


def test_completed_archive_picks_up_new_completed_days(state, monkeypatch):
    queue, store = state
    monkeypatch.setattr(daily_queue, "santiago_today", lambda: date(2025, 7, 30))
    queue.schedule_day(DAY, None, "action")
    lease = route(queue)
    job = queue.claim_first_job("owner")
    store.day_result(job["day"], "action", [], complete=True)
    queue.expand_day(job["day"], "action", [], store)
    finish(queue, job)
    assert queue.schedule_day(DAY, None, "action") is None
    assert queue.archive_current()
    monkeypatch.setattr(daily_queue, "santiago_today", lambda: date(2025, 7, 31))
    assert queue.schedule_day(DAY, None, "action") == "2025-07-30"
    assert not queue.archive_current()
    queue.finish_route(lease)


def test_import_preserves_source_and_skips_corrupt_pdf(tmp_path):
    source = tmp_path / "old/tracking.sqlite3"
    target = tmp_path / "current/tracking.sqlite3"
    old = TrackingStore(source, "old")
    old.register_run({}, [])
    old.day_result(DAY.isoformat(), "action", [action(0), action(1)], complete=True)
    good = saved(old, source.parent / "ready-downloads", action(0))
    bad = saved(old, source.parent / "ready-downloads", action(1))
    bad.write_bytes(b"not-a-pdf")
    before = good.read_bytes(), good.stat().st_mtime_ns
    old.close()
    original_db = source.read_bytes()
    current = TrackingStore(target, "new")
    current.register_run({}, [])
    current.close()
    result = import_bulk(source, target, "new")
    assert result == {"files": 1, "reused": 0, "invalid": 1, "days": 1}
    assert (good.read_bytes(), good.stat().st_mtime_ns) == before
    assert source.read_bytes() == original_db
    current = TrackingStore(target, "new")
    try:
        assert current.existing_file(action(0)["cve"])
        assert not current.existing_file(action(1)["cve"])
        assert current.day_done(DAY.isoformat(), "action")
        assert import_bulk(source, target, "new")["reused"] == 1
    finally:
        current.close()


async def test_daily_mode_preserves_company_files_and_bulk_day_without_network(tmp_path, monkeypatch):
    args = fleet.parse_args(
        [
            "--output",
            str(tmp_path / "current"),
            "--run-id",
            "same-run",
            "--providers",
            "webshare,free",
            "--browsers",
            "10",
            "--start",
            str(DAY),
            "--end",
            str(DAY),
            "--no-telegram",
        ]
    )
    args.output.mkdir()
    root = args.output / args.run_id
    root.mkdir()
    selection = root / "selection.json"
    selection.write_text('{"old_company_selection": true}')
    store = TrackingStore(args.output / "tracking.sqlite3", args.run_id)
    store.register_run({}, [])
    store.day_result(str(DAY), "action", [action()], complete=True)
    original = saved(store, root / "workers/w00", action())
    fingerprint = original.read_bytes(), original.stat().st_mtime_ns
    store.close()
    company_queue = fleet.FleetQueue(args.output / "fleet-runtime.sqlite3", args.run_id)
    company_queue.add_jobs([{"company_id": "legacy"}])
    company_queue.close()
    monkeypatch.setattr(fleet, "seed_routes", AsyncMock(side_effect=AssertionError("No proxy I/O needed")))
    monkeypatch.setattr(fleet.FleetBrowserWorker, "__call__", AsyncMock(side_effect=AssertionError("No browser needed")))
    assert await asyncio.wait_for(daily_fleet.execute_daily(args, root), 5) == 0
    assert original.read_bytes() == fingerprint[0] and original.stat().st_mtime_ns == fingerprint[1]
    assert selection.read_text() == '{"old_company_selection": true}'
    queue = DailyQueue(args.output / "fleet-runtime.sqlite3", args.run_id)
    try:
        assert queue.database.execute("SELECT COUNT(*) FROM free_jobs").fetchone()[0] == 1
        assert queue.days_done() == 1
    finally:
        queue.close()
    summary = json.loads((root / "summary.json").read_text())
    assert summary["total_pdfs"] == 1 and summary["new_pdfs"] == 0


async def test_ten_browsers_drain_one_day_before_previous_and_reuse_tokens(tmp_path, monkeypatch):
    args = fleet.parse_args(
        [
            "--output",
            str(tmp_path),
            "--run-id",
            "daily",
            "--providers",
            "webshare,free",
            "--browsers",
            "10",
            "--start",
            "2025-07-28",
            "--end",
            str(DAY),
            "--retry-seconds",
            "0",
            "--no-telegram",
        ]
    )
    root = tmp_path / args.run_id
    root.mkdir()
    discovery, verified, tokens, peak = [], [], [], [0, 0]

    async def seed(args, queue):
        for i in range(5):
            queue.add_route("webshare", f"9.9.9.{i + 1}:80", state="ready")
            queue.add_route("free", f"http://8.8.8.{i + 1}:80", state="ready")
        return {"webshare": 5, "free": 5}

    async def discover(store, http, minter, date_args):
        day = str(date_args.start)
        if day == "2025-07-28":
            assert len(verified) == 20
        discovery.append(day)
        await minter(pilot.FORM_URL)
        start = 0 if day == str(DAY) else 20
        store.day_result(day, "action", [action(i) for i in range(start, start + 20)], complete=True)

    def verify(http, cve, token):
        verified.append(cve)
        tokens.append(token)
        return (
            cve,
            "code",
            f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={cve}&codigo=valid",
        )

    async def worker(self, slot, leased_route, jobs, owner):
        peak[0] += 1
        peak[1] = max(peak)
        directory = root / "workers" / f"w{slot:02d}"
        directory.mkdir(parents=True, exist_ok=True)
        child_queue = DailyQueue(args.output / "fleet-runtime.sqlite3", args.run_id, initialize=False)
        child_store = TrackingStore(args.output / "tracking.sqlite3", args.run_id, worker=f"w{slot:02d}")
        child_args = pilot.parse_args(["--output", str(directory), "--delay", "0"])
        child_args.tracking_store = child_store
        child_args.state_path = directory / "state.json"
        child_args.stream_owner = owner
        child_args.stream_retry_delay = 0
        child_args.fleet_quota = FleetQuota(child_queue.database, owner, 95, 0, 0)
        cascade = SimpleNamespace(minted=0, events=[])

        async def mint(url):
            await asyncio.sleep(0.01)
            cascade.minted += 1
            return f"token-{owner}-{cascade.minted}"

        cascade.mint = mint
        report = {"run_id": owner, "attempts": [], "companies": []}
        counters = Counter()
        try:
            await daily_worker.consume_days(
                cascade,
                object(),
                child_queue,
                child_args,
                {},
                counters,
                report,
                {"http_ip": leased_route["endpoint"].split(":")[0], "companies": 0},
            )
            ids = {r["company_id"] for r in report["companies"]}
            return fleet.BatchResult(completed=ids, attempted=ids, new_pdfs=counters["downloaded"])
        finally:
            peak[0] -= 1
            child_queue.close()
            child_store.close()

    monkeypatch.setattr(fleet, "seed_routes", seed)
    monkeypatch.setattr(fleet.FleetProducer, "run", AsyncMock())
    monkeypatch.setattr(fleet.FleetBrowserWorker, "__call__", worker)
    monkeypatch.setattr(daily_worker, "discover_dates", discover)

    async def check_route(*args):
        await asyncio.sleep(0.06)  # Emulate a real asynchronous network operation.

    monkeypatch.setattr(daily_worker, "check_route", check_route)
    monkeypatch.setattr(pilot.http_flow, "verify", verify)
    monkeypatch.setattr(pilot.http_flow, "download", lambda *args: PDF)
    monkeypatch.setattr(daily_fleet, "public_collection_allowed", lambda: True)
    assert await asyncio.wait_for(daily_fleet.execute_daily(args, root), 10) == 0
    assert discovery == ["2025-07-29", "2025-07-28"]
    assert len(verified) == len(set(verified)) == 40
    assert peak == [0, 10]
    assert len(set(tokens)) <= 10
    summary = json.loads((root / "summary.json").read_text())
    assert summary["days_completed"] == 2
    assert summary["new_pdfs"] == summary["total_pdfs"] == 40
    assert summary["counts"]["proxies"] == {}
    assert summary["counts"]["budgets"]["webshare"] > 0
    assert summary["counts"]["budgets"]["free"] > 0


async def test_real_subprocess_adapter_accepts_a_date_job(state, tmp_path, monkeypatch):
    queue, store = state
    args = fleet.parse_args(["--mode", "days", "--output", str(tmp_path), "--run-id", "test", "--providers", "webshare"])
    args.runtime_lock_fd = 99
    root = tmp_path / "test"
    root.mkdir()
    queue.schedule_day(DAY, DAY, "action")
    leased_route = route(queue)
    job = queue.claim_first_job("owner")
    worker = fleet.FleetBrowserWorker(args, root, store, queue)
    events = []
    worker.notifications = SimpleNamespace(activity=lambda *args: events.append(args))

    class Process:
        pid = 123456
        returncode = 0

        async def wait(self):
            return 0

    async def spawn(*command, **options):
        assert command[command.index("--stream-task-mode") + 1] == "days"
        directory = root / "workers/w00"
        report = {
            "companies": [{**job, "status": "not_found"}],
            "attempts": [],
            "totals": {"downloaded": 0},
            "stop_reason": {"code": "queue_drained"},
        }
        (directory / "report-unit.json").write_text(json.dumps(report))
        return Process()

    monkeypatch.setattr(fleet.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(fleet, "stop_child", AsyncMock())
    result = await worker(0, leased_route, [job], "owner")
    assert not result.error
    assert job["company_id"] in result.completed
    assert events[0][0] == "browser_started"
    assert "Дневная очередь" in events[0][1]


async def test_daily_document_uses_issued_link_without_search_or_new_token(state, tmp_path, monkeypatch):
    queue, store = state
    row = action()
    store.day_result(str(DAY), "action", [row], complete=True)
    link = f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={row['cve']}&codigo=valid"
    store.document_status(row["cve"], "verified", record={"codigo": "valid", "link": link})
    args = pilot.parse_args(["--output", str(tmp_path / "worker")])
    args.output.mkdir()
    args.state_path = args.output / "state.json"
    args.tracking_store = store
    cascade = daily_worker.ReusingCascade(SimpleNamespace(events=[], mint=AsyncMock(side_effect=AssertionError("No token"))))
    monkeypatch.setattr(pilot.http_flow, "verify", lambda *args: pytest.fail("No re-verification expected"))
    download = []
    monkeypatch.setattr(pilot.http_flow, "download", lambda http, url: (download.append(url), PDF)[1])
    job = {**row, "subject_company_id": "rut:" + row["rut"]}
    counters = Counter()
    await daily_worker.document_job(job, store, object(), cascade, {}, args, counters)
    assert download == [link]
    assert counters["downloaded"] == 1
    assert store.existing_file(row["cve"])


def test_missing_completed_pdf_reopens_day_and_does_not_mix_dates(state, tmp_path):
    queue, store = state
    row = action()
    older = date(2025, 7, 28)
    queue.schedule_day(older, DAY, "action")
    route(queue)
    job = queue.claim_first_job("owner")
    store.day_result(str(DAY), "action", [row], complete=True)
    path = saved(store, tmp_path / "worker", row)
    queue.expand_day(str(DAY), "action", [row], store)
    finish(queue, job)
    assert queue.schedule_day(older, DAY, "action") == str(older)
    path.unlink()
    assert daily_fleet.audit_daily_files(queue.path, store.path, "test") == 1
    assert queue.schedule_day(older, DAY, "action") == str(DAY)
    reopened = queue.claim_first_job("owner")
    assert reopened["task"] == "document" and reopened["day"] == str(DAY)
    assert store.day_done(str(DAY), "action")


async def test_rejected_reused_token_is_replaced_once(state, tmp_path, monkeypatch):
    from src.sites.res.source import SearchIssue

    queue, store = state
    row = action()
    store.day_result(str(DAY), "action", [row], complete=True)
    args = pilot.parse_args(["--output", str(tmp_path / "worker")])
    args.output.mkdir()
    args.state_path = args.output / "state.json"
    args.tracking_store = store
    fresh = AsyncMock(return_value="fresh")
    cascade = daily_worker.ReusingCascade(SimpleNamespace(events=[], mint=fresh))
    cascade.token = "expired"
    seen = []

    def verify(http, cve, token):
        seen.append(token)
        if token == "expired":
            raise SearchIssue("captcha_required", "Rejected reused token")
        return (
            cve,
            "code",
            f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={cve}&codigo=valid",
        )

    monkeypatch.setattr(pilot.http_flow, "verify", verify)
    monkeypatch.setattr(pilot.http_flow, "download", lambda *args: PDF)
    await daily_worker.document_job(
        {**row, "subject_company_id": "rut:" + row["rut"]}, store, object(), cascade, {}, args, Counter()
    )
    assert seen == ["expired", "fresh"]
    fresh.assert_awaited_once()
    assert store.existing_file(row["cve"])
