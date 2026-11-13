import asyncio
from collections import Counter
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from scripts import res_free_pilot as pilot
from src.sites.res import cooldown
from src.sites.res.source import SearchIssue
from src.sites.res.tracking import TrackingStore


def store_for(tmp_path, worker="w00"):
    store = TrackingStore(tmp_path / "tracking.sqlite3", "sample", worker=worker)
    store.register_run({}, [{"company_id": key, "company_name": key, "rut": None} for key in ("A", "B")])
    return store


def test_workers_join_one_deadline_and_pause_budget_is_finite(tmp_path, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(cooldown.time, "time", lambda: now[0])
    first, second = store_for(tmp_path), store_for(tmp_path, "w01")
    try:
        first.prepare_cooldown()
        started = first.request_cooldown(600, 2)
        now[0] += 90
        joined = second.request_cooldown(600, 2)
        assert started["cooldown_until"] == joined["cooldown_until"] == 1600
        assert joined["cycles"] == 1
        first.prepare_cooldown()  # restart during a pause retains its deadline
        assert first.cooldown_state()["cooldown_until"] == 1600
        now[0] = 1601
        first.finish_cooldown()
        second.finish_cooldown()
        assert first.db.execute("SELECT count(*) FROM events WHERE status='resumed'").fetchone()[0] == 1
        second.request_cooldown(600, 2)
        now[0] += 601
        second.finish_cooldown()
        halted = first.request_cooldown(600, 2)
        assert halted["halted"] == 1 and halted["cycles"] == 2 and not halted["active"]
        assert second.cooldown_state()["halted"] == 1
        first.prepare_cooldown()  # an explicit new idle launch gets a new budget
        assert first.cooldown_state()["cycles"] == 0 and first.cooldown_state()["halted"] == 0
    finally:
        first.close()
        second.close()


async def test_pause_resumes_the_same_queue_without_losing_a_deferred_company(tmp_path, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(cooldown.time, "time", lambda: now[0])
    args = pilot.parse_args(["--output", str(tmp_path), "--audio-model", ""])
    args.state_path = tmp_path / "state.json"
    args.error_cooldown_minutes, args.max_cooldowns = 1 / 6, 2
    args.max_failures, args.company_attempts, args.challenges_per_proxy = 1, 2, 1
    args.tracking_store = store_for(tmp_path)
    args.tracking_store.prepare_cooldown()
    companies = [{"company_id": key, "company_name": key, "rut": None} for key in ("A", "B")]
    seen, started_sessions = [], []

    @asynccontextmanager
    async def session(*values):
        started_sessions.append(now[0])
        yield SimpleNamespace(minted=0, events=[], audio=None), None

    async def process(cascade, http, company, *values):
        seen.append(company["company_id"])
        if len(seen) == 1:
            raise SearchIssue("captcha_required", "Google declined audio")
        return {**company, "status": "not_found", "documents": []}

    async def fast_sleep(seconds):
        now[0] += seconds

    monkeypatch.setattr(pilot, "load_companies", lambda *args: companies)
    monkeypatch.setattr(pilot, "endpoint_session", session)
    monkeypatch.setattr(pilot, "process_company", process)
    monkeypatch.setattr(cooldown.asyncio, "sleep", fast_sleep)
    report = {"run_id": "pause", "proxies": []}
    try:
        await pilot.scan([pilot.DIRECT] * 3, args, {"companies": {}, "cves": {}}, Counter(), report, None, "", [])
        assert seen == ["A", "B", "A"]
        assert started_sessions == [1000, 1010]
        assert all(row["status"] == "not_found" for row in report["companies"])
        assert "stop_reason" not in report
        assert args.tracking_store.cooldown_state()["cycles"] == 1
        assert report["cooldown"]["active"] is False
    finally:
        args.tracking_store.close()


async def test_cancelled_pause_is_durable_and_can_be_resumed_after_restart(tmp_path, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(cooldown.time, "time", lambda: now[0])
    store = store_for(tmp_path)
    args = SimpleNamespace(output=tmp_path, tracking_store=store, error_cooldown_minutes=10, max_cooldowns=3)
    runtime = {"stop": False, "failures": 20, "pause_requested": True}
    report = {"run_id": "cancel"}

    async def cancel(seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(cooldown.asyncio, "sleep", cancel)
    try:
        with pytest.raises(asyncio.CancelledError):
            await cooldown.wait(args, runtime, report)
        assert store.cooldown_state()["cooldown_until"] == 1600
        store.prepare_cooldown()
        assert store.cooldown_state()["cooldown_until"] == 1600
        assert runtime["failures"] == 20
    finally:
        store.close()


async def test_shared_pause_does_not_consume_a_proxy_slot_before_resuming(tmp_path, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(cooldown.time, "time", lambda: now[0])
    store = store_for(tmp_path)
    store.prepare_cooldown()
    store.request_cooldown(10, 3)
    args = pilot.parse_args(["--output", str(tmp_path), "--audio-model", ""])
    args.tracking_store, args.state_path = store, tmp_path / "state.json"
    resolved = []

    async def resolve(slot, args):
        resolved.append(now[0])
        return slot

    @asynccontextmanager
    async def session(*args):
        yield SimpleNamespace(minted=0, events=[], audio=None), None

    async def process(cascade, http, company, *args):
        return {**company, "status": "not_found", "documents": []}

    async def sleep(seconds):
        now[0] += seconds

    monkeypatch.setattr(cooldown.asyncio, "sleep", sleep)
    monkeypatch.setattr(pilot, "resolve_proxy", resolve)
    monkeypatch.setattr(pilot, "endpoint_session", session)
    monkeypatch.setattr(pilot, "process_company", process)
    monkeypatch.setattr(pilot, "load_companies", lambda *a: [{"company_id": "A", "company_name": "A", "rut": None}])
    try:
        await pilot.scan(
            [pilot.DIRECT], args, {"companies": {}, "cves": {}}, Counter(), {"run_id": "join", "proxies": []}, None, "", []
        )
        assert resolved == [1010]
    finally:
        store.close()


async def test_pause_limit_stops_every_observer_without_sleep(tmp_path, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(cooldown.time, "time", lambda: now[0])
    store = store_for(tmp_path)
    store.prepare_cooldown()
    store.request_cooldown(10, 1)
    now[0] += 11
    store.finish_cooldown()
    store.request_cooldown(10, 1)
    args = SimpleNamespace(output=tmp_path, tracking_store=store, error_cooldown_minutes=10, max_cooldowns=1)
    runtime = {"stop": False, "failures": 0}

    async def forbidden(seconds):
        pytest.fail("Exhausted cooldowns should stop, not sleep")

    monkeypatch.setattr(cooldown.asyncio, "sleep", forbidden)
    report = {"run_id": "limit"}
    try:
        await cooldown.wait(args, runtime, report)
        assert runtime["stop"] is True
        assert report["stop_reason"]["code"] == "cooldown_limit_reached"
    finally:
        store.close()


def test_simultaneous_pause_requests_create_one_shared_cycle(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    store = store_for(tmp_path)
    store.prepare_cooldown()
    store.close()

    def request(index):
        writer = TrackingStore(tmp_path / "tracking.sqlite3", "sample", worker=f"w{index}")
        try:
            return writer.request_cooldown(600, 3)
        finally:
            writer.close()

    with ThreadPoolExecutor(max_workers=5) as workers:
        states = list(workers.map(request, range(5)))
    assert {s["cycles"] for s in states} == {1}
    assert len({s["cooldown_until"] for s in states}) == 1
    store = store_for(tmp_path)
    try:
        assert len(store.cooldown_events()) == 1
    finally:
        store.close()


async def test_parent_reads_pause_and_resume_events_without_duplicates(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock

    from src.sites.res import benchmark

    now = [1000.0]
    monkeypatch.setattr(cooldown.time, "time", lambda: now[0])
    store = store_for(tmp_path)
    store.prepare_cooldown()
    store.request_cooldown(600, 3)
    now[0] += 601
    store.finish_cooldown()
    args = SimpleNamespace(tracking_store=store, run_id="sample", max_cooldowns=3)
    sender = AsyncMock()
    monkeypatch.setattr(benchmark, "notify", sender)
    try:
        cursor = await benchmark.notify_cooldown_events(args, 0)
        assert sender.await_count == 2
        assert sender.call_args_list[0].args[1].startswith("⏸")
        assert sender.call_args_list[1].args[1].startswith("▶️")
        await benchmark.notify_cooldown_events(args, cursor)
        assert sender.await_count == 2
    finally:
        store.close()


async def test_active_pause_yields_before_starting_the_next_document(tmp_path, monkeypatch):
    first, second = "ACuKDHhqsvAd", "ACfJoaiSFQ8v"
    store = store_for(tmp_path)
    store.prepare_cooldown()
    args = pilot.parse_args(["--output", str(tmp_path), "--audio-model", ""])
    args.tracking_store = store
    state = {"companies": {"A": {"status": "completed", "cves": [first, second]}}, "cves": {}}
    called = []

    async def document(cascade, http, cve, company_id, state, args, output, counters):
        called.append(cve)
        saved = pilot.persist_pdf(
            output,
            cve,
            b"%PDF-1.4\nfixture\n%%EOF",
            f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={cve}&codigo=fixture",
        )
        store.file_result(saved, output, company_id)
        store.request_cooldown(600, 3)
        return pilot.document_summary(saved)

    monkeypatch.setattr(pilot, "ensure_document", document)
    try:
        with pytest.raises(SearchIssue, match="Batch cooldown") as paused:
            await pilot.process_company(
                None, None, {"company_id": "A", "company_name": "A"}, state, args, tmp_path, Counter()
            )
        assert paused.value.status == "cooldown" and called == [first]
        assert store.existing_file(first) and state["companies"]["A"]["cves"] == [first, second]
    finally:
        store.close()
