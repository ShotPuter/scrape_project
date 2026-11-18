"""Continuous queue: concurrency, recoverable quota waits and network-only failover."""

import asyncio
import json
import time
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from scripts import res_free_pilot as pilot
from src.sites.res import bulk_continuous as continuous
from src.sites.res.source import SearchIssue
from src.sites.res.tracking import TrackingStore


@pytest.fixture
def worker(tmp_path):
    store = TrackingStore(tmp_path / "tracking.sqlite3", "test")
    store.register_run({}, [{"company_id": "company", "company_name": "Example"}])
    args = SimpleNamespace(
        run_id="test",
        start=date(2013, 1, 1),
        end=None,
        kind="action",
        quota_hours=24,
        retry_seconds=300,
        network_retry_seconds=10,
        telegram_minutes=5,
        poll_seconds=600,
        workers=4,
        interval=0,
        max_cycles=1,
    )
    notifications = SimpleNamespace(
        state={}, publish=lambda *args: None, record=lambda *args: None, start=AsyncMock(), finish=AsyncMock()
    )
    runner = continuous.ContinuousBulk(args, store, notifications)
    yield runner
    store.close()


@pytest.mark.parametrize("status", ["daily_limit", "captcha_required", "blocked", "failed"])
async def test_restrictions_never_switch_route(worker, monkeypatch, status):
    endpoints = [pilot.DIRECT, SimpleNamespace(label="other")]
    worker.endpoint, worker.endpoints = endpoints[0], endpoints
    monkeypatch.setattr(worker, "cycle", AsyncMock(return_value=status))
    await worker.run()
    assert worker.endpoint is endpoints[0]
    assert worker.state["network_failovers"] == 0
    assert worker.state["resume_at"] > time.time()
    if status == "daily_limit":
        assert worker.state["resume_at"] > time.time() + 86300


async def test_network_failure_changes_route_and_preserves_pending_work(worker, monkeypatch):
    other = SimpleNamespace(label="other")
    worker.endpoint, worker.endpoints = pilot.DIRECT, [pilot.DIRECT, other]
    worker.store.search_result("company", {"cves": ["ACtest0001"]})
    monkeypatch.setattr(worker, "cycle", AsyncMock(side_effect=SearchIssue("proxy_failed", "Connection failed")))
    await worker.run()
    assert worker.endpoint is other
    assert worker.state["network_failovers"] == 1
    assert worker.store.db.execute("SELECT status FROM documents").fetchone()[0] == "pending"


async def test_restart_honors_saved_quota_before_resolving_proxy(worker, monkeypatch):
    worker.pause("daily_limit")
    restored = continuous.ContinuousBulk(worker.args, worker.store, worker.notifications)
    load = AsyncMock(side_effect=AssertionError("Must not resolve another proxy during quota wait"))
    monkeypatch.setattr(restored, "load_endpoints", load)
    task = asyncio.create_task(restored.run())
    await asyncio.sleep(0.02)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    load.assert_not_awaited()
    assert json.loads(worker.path.read_text())["pause_reason"] == "daily_limit"


async def test_expired_quota_resumes_same_route(worker, monkeypatch):
    worker.state.update(resume_at=time.time() - 1, pause_reason="daily_limit")
    worker.endpoint, worker.endpoints = pilot.DIRECT, [pilot.DIRECT]
    cycle = AsyncMock(return_value="ok")
    monkeypatch.setattr(worker, "cycle", cycle)
    await worker.run()
    cycle.assert_awaited_once()
    assert worker.state["resume_at"] == 0 and worker.state["network_failovers"] == 0


def test_newest_completed_day_is_discovered_first_and_resume_skips_it(worker, monkeypatch):
    monkeypatch.setattr(continuous, "santiago_today", lambda: date(2026, 11, 18))
    assert continuous.next_day(worker.store, date(2013, 1, 1), None, "action") == date(2026, 11, 17)
    worker.store.day_result("2026-11-17", "action", [], complete=True)
    assert continuous.next_day(worker.store, date(2013, 1, 1), None, "action") == date(2026, 11, 16)


async def test_saved_pdf_downloads_run_before_verification_finishes(worker):
    cve = "ACtest0001"
    worker.store.search_result("company", {"cves": [cve]})
    downloaded = asyncio.Event()
    finished = asyncio.Event()
    code = "-".join(["AB"] * 16)
    link = f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={cve}&codigo={code}"

    def respond(request):
        assert not finished.is_set()
        downloaded.set()
        return httpx.Response(200, headers={"content-type": "application/pdf"}, content=b"%PDF-1.4\nfixture\n%%EOF")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        task = asyncio.create_task(worker.downloads(client, finished))
        worker.store.document_status(cve, "verified", record={"link": link})
        await asyncio.wait_for(downloaded.wait(), 2)
        finished.set()
        await task
    assert worker.store.db.execute("SELECT count(*) FROM document_files").fetchone()[0] == 1


def test_summary_contains_real_counts_and_incremental_progress(worker):
    worker.state["tokens"] = 2
    message = worker.message("Статистика")
    assert "PDF всего: 0" in message and "+0 за период" in message and "токенов: 2" in message


async def test_http_download_connection_failure_triggers_failover(worker):
    cve = "ACtest0001"
    worker.store.search_result("company", {"cves": [cve]})
    code = "-".join(["AB"] * 16)
    worker.store.document_status(
        cve,
        "verified",
        record={"link": f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={cve}&codigo={code}"},
    )

    def broken(request):
        raise httpx.ConnectError("Connection failed", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(broken)) as client:
        with pytest.raises(SearchIssue, match="transport failed"):
            await worker.downloads(client, asyncio.Event())
    assert worker.store.db.execute("SELECT status FROM documents").fetchone()[0] == "verified"


def test_concurrent_quota_response_prevents_network_failover():
    error = ExceptionGroup("concurrent tasks", [httpx.ConnectError("network"), SearchIssue("daily_limit", "Quota")])
    assert continuous.issue_status(error) == "daily_limit"


async def test_downloads_flush_again_when_verifier_finishes_during_a_page(worker, monkeypatch):
    finished = asyncio.Event()
    calls = []

    async def download(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            # The final links were committed while an earlier page was downloading.
            finished.set()
        return {"downloaded": 1, "failed": 0, "stop_reason": None}

    monkeypatch.setattr(continuous, "download_ready", download)
    await worker.downloads(None, finished)
    assert len(calls) == 2


async def test_proxy_must_pass_screening_before_any_download_transport(worker, monkeypatch):
    from unittest.mock import Mock

    from src.proxy.reputation import ReputationDenied

    worker.endpoint = SimpleNamespace(url="http://127.0.0.1:8080")
    monkeypatch.setattr(continuous, "screen_proxy", AsyncMock(side_effect=ReputationDenied("denied")))
    transport = Mock(side_effect=AssertionError("Rejected proxy must never connect"))
    monkeypatch.setattr(continuous.httpx, "AsyncHTTPTransport", transport)
    with pytest.raises(ReputationDenied):
        await worker.cycle()
    transport.assert_not_called()
