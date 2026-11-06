"""Heartbeat, speed counters, log rotation and credential redaction."""

import asyncio
import json

import pytest

from scripts import pep_camoufox_parallel as parallel
from scripts.pep_camoufox_logging import EventLog


def state(processed, seconds):
    return {
        "status": "running",
        "seconds": seconds,
        "pdf_count": processed,
        "jobs": [
            {
                "status": "completed" if i < processed else "pending",
                "documents": [{"status": "downloaded"}] if i < processed else [],
            }
            for i in range(5)
        ],
    }


def test_average_and_window_speed(tmp_path):
    log = EventLog(tmp_path)
    log.summary("heartbeat", state(2, 60), interval=True)
    log.summary("heartbeat", state(3, 90), interval=True)
    log.close()
    first, second = [json.loads(line) for line in (tmp_path / "run.log").read_text().splitlines()]
    assert first["elapsed_seconds"] == 60
    assert first["processed"] == 2
    assert first["declarations_per_minute"] == first["links_per_minute"] == 2
    assert second["interval_seconds"] == 30
    assert second["interval_processed"] == second["interval_downloaded"] == 1
    assert second["interval_declarations_per_minute"] == 2
    assert second["pending"] == 2


def test_rotated_logs_and_stdout_hide_secrets(tmp_path, capsys):
    log = EventLog(tmp_path, secrets=("private-login", "private-password"), max_bytes=400, backups=2)
    for index in range(20):
        log.emit(
            "test",
            index=index,
            message="private-login/private-password https://user:pass@example.com/path",
            headers={"Authorization": "Bearer hidden-value", "Cookie": "session-value"},
            captchatoken="captcha-value",
        )
    log.close()
    files = list(tmp_path.glob("run.log*"))
    assert len(files) == 3
    text = capsys.readouterr().out + "".join(path.read_text() for path in files)
    for secret in ("private-login", "private-password", "user:pass", "hidden-value", "session-value", "captcha-value"):
        assert secret not in text
    assert "[redacted]" in text
    for path in files:
        for line in path.read_text().splitlines():
            assert json.loads(line)["event"] == "test"


@pytest.mark.asyncio
async def test_heartbeat_records_active_download_without_completed_links(monkeypatch, tmp_path):
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def browser(*args):
        yield object()

    monkeypatch.setattr(parallel, "browser_context", browser)
    monkeypatch.setattr(parallel, "install_intermediate", lambda *args: None)

    async def fake_run(group, output, request_seconds, *, on_state, on_progress, next_job, **kwargs):
        report = {
            "status": "running",
            "seconds": 0,
            "activity": {"stage": "document_download", "job": 1, "document": "123", "started_seconds": 0},
            "jobs": [{**next_job(), "status": "running", "documents": [{"status": "running"}]}],
        }
        on_state(report)
        await asyncio.sleep(0.07)
        report.update(status="finished", seconds=0.07, pdf_count=1)
        report["jobs"][0].update(status="completed", documents=[{"status": "downloaded"}])
        on_state(report)
        await on_progress(report, final=True)

    async def notify(report, *, final):
        return None

    monkeypatch.setattr(parallel, "run", fake_run)
    before = set(asyncio.all_tasks())
    output = tmp_path / "run"
    await parallel.run_parallel(
        [{"n": 1, "url": "https://example.com"}],
        output,
        workers=1,
        request_seconds=10,
        ca_bundle=tmp_path / "ca",
        proxy_ca=tmp_path / "proxy",
        max_seconds=10,
        proxy=None,
        notify=notify,
        log_interval=0.01,
    )
    events = [json.loads(line) for line in (output / "run.log").read_text().splitlines()]
    heartbeats = [row for row in events if row["event"] == "heartbeat"]
    assert heartbeats
    assert heartbeats[0]["processed"] == 0
    assert heartbeats[0]["running"] == 1
    assert heartbeats[0]["workers"][0]["tabs"][0]["activity"]["stage"] == "document_download"
    assert heartbeats[0]["workers"][0]["tabs"][0]["activity_seconds"] > 0
    assert events[-1]["event"] == "run_finished"
    assert events[-1]["processed"] == events[-1]["downloaded"] == 1
    assert set(asyncio.all_tasks()) <= before


def test_resource_snapshot_reads_container_memory_and_oom_counters(tmp_path):
    from scripts.pep_camoufox_logging import resource_snapshot

    (tmp_path / "memory.current").write_text("123456\n")
    (tmp_path / "memory.max").write_text("max\n")
    (tmp_path / "memory.events").write_text("oom 3\noom_kill 2\n")
    resources = resource_snapshot(tmp_path, tmp_path)
    assert resources["memory.current"] == 123456
    assert resources["memory.max"] == "max"
    assert resources["memory.events"] == {"oom": 3, "oom_kill": 2}
    assert resources["shm_available_bytes"] > 0


def test_reused_documents_do_not_inflate_download_speed():
    from scripts.pep_camoufox_logging import report_totals

    stats = report_totals(
        {
            "jobs": [
                {
                    "status": "partial",
                    "documents": [{"status": "downloaded", "reused": True}, {"status": "downloaded"}, {"status": "timeout"}],
                }
            ],
            "pdf_count": 2,
        }
    )
    assert stats["downloaded"] == 2
    assert stats["reused"] == stats["downloaded_new"] == 1
    assert stats["not_downloaded"] == 1


@pytest.mark.asyncio
async def test_global_export_is_throttled_but_final_report_is_complete(tmp_path):
    async def notify(report, *, final):
        pass

    notify.interval_seconds = 1800
    rows = [{"n": i, "url": f"https://example.com/{i}"} for i in range(1, 101)]
    aggregate = parallel.AggregateReport(rows, [rows], tmp_path, notify, dynamic=True)
    calls = []
    persist = aggregate.persist

    def tracked(report):
        calls.append(len([job for job in report["jobs"] if job["status"] == "completed"]))
        persist(report)

    aggregate.persist = tracked
    state = {"status": "running", "jobs": [], "pdf_count": 0}
    for row in rows:
        state["jobs"].append({**row, "status": "completed", "documents": []})
        await aggregate.update(1, state)
    assert calls == [1]
    state["status"] = "finished"
    aggregate.observe(1, state)
    await aggregate.finish()
    assert calls == [1, 100]
    saved = json.loads((tmp_path / "report.json").read_text())
    assert len(saved["jobs"]) == 100 and saved["job_counts"] == {"completed": 100}


@pytest.mark.asyncio
async def test_failed_final_state_write_still_sends_failure_notification(tmp_path):
    notifications = []

    async def notify(report, *, final):
        notifications.append((report, final))

    aggregate = parallel.AggregateReport([], [], tmp_path, notify)

    def fail(report):
        raise RuntimeError("State write failed")

    aggregate.persist = fail
    with pytest.raises(RuntimeError, match="State write failed"):
        await aggregate.finish("error")
    assert len(notifications) == 1
    report, final = notifications[0]
    assert final and report["status"] == "error"
    assert "state_write_failed" in report["stop_reason"]
