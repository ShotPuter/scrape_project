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
