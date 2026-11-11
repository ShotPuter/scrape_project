import argparse
import asyncio
import json
from types import SimpleNamespace

import pytest

from src.sites.res import __main__ as search, download, telegram
from src.sites.res.runtime import validate_runtime


@pytest.fixture
def transport(monkeypatch):
    sent, sessions, events = [], [], asyncio.Queue()

    class Session:
        closed = False

        def __init__(self, **kwargs):
            sessions.append(self)

        async def close(self):
            self.closed = True

    async def send(session, token, chat, text):
        assert token == "private-token"
        assert chat == "private-chat"
        sent.append(text)
        events.put_nowait(text)

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "private-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "private-chat")
    monkeypatch.setenv("RES_TELEGRAM_INTERVAL_MINUTES", "5")
    monkeypatch.setattr("dotenv.load_dotenv", lambda: None)
    monkeypatch.setattr(telegram.aiohttp, "ClientSession", Session)
    monkeypatch.setattr(telegram, "send_telegram", send)
    return SimpleNamespace(sent=sent, sessions=sessions, events=events)


def report():
    return {
        "run_id": "telegram-test",
        "target": 100,
        "browsers": 5,
        "tabs_per_browser": 2,
        "proxy_provider": "webshare",
        "proxy_routes": [{"verified": True} for _ in range(5)],
        "state": "running",
        "progress": {"claimed": 13, "statuses": {"not_found": 3, "running": 10}},
        "cves": ["ACuKDHhqsvAd"],
    }


def options(tmp_path, **overrides):
    return argparse.Namespace(**({"output": tmp_path, "telegram_interval_minutes": None, "no_telegram": False} | overrides))


async def test_start_ready_periodic_and_final_notifications_are_independent_of_results(tmp_path, transport):
    snapshot = report()
    ticks, delays = asyncio.Queue(), []

    async def sleep(seconds):
        delays.append(seconds)
        await ticks.get()

    notifier = telegram.RunNotifications(options(tmp_path), snapshot, stage="search", refresh=lambda: None, sleep=sleep)
    await notifier.start()
    await notifier.queue.join()
    for i in range(4):
        notifier.browser_ready(i)
    assert len(transport.sent) == 1  # No premature "all browsers ready".
    notifier.browser_ready(4)
    notifier.browser_ready(4)
    await notifier.queue.join()
    snapshot["progress"]["statuses"] = {"not_found": 3, "captcha_required": 2, "running": 8}
    ticks.put_nowait(None)
    for _ in range(3):
        await asyncio.wait_for(transport.events.get(), 1)
    assert delays == [300, 300]
    snapshot["state"] = "stopped"
    snapshot["stop_reason"] = "CAPTCHA requires operator interaction"
    await notifier.finish()
    assert [
        "запуск" in transport.sent[0],
        "браузеры подготовлены" in transport.sent[1],
        "статус сбора" in transport.sent[2],
        "завершение" in transport.sent[3],
    ] == [True] * 4
    assert "CAPTCHA: 2" in transport.sent[2]
    assert "CAPTCHA requires operator interaction" in transport.sent[3]
    assert transport.sessions[0].closed
    assert snapshot["telegram"]["sent"] == 4
    records = read_events(notifier.path)
    assert [r["event"] for r in records] == ["started", "browsers_ready", "progress", "finished"]
    assert records[0]["statuses"] == {"not_found": 3, "running": 10}
    assert all(r["delivered"] for r in records)


def read_events(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


async def test_delivery_failure_and_credentials_never_escape_into_history_or_collection(
    tmp_path, transport, monkeypatch, capsys
):
    async def fail(*args):
        raise RuntimeError("https://api.telegram.org/botprivate-token/sendMessage private-chat")

    monkeypatch.setattr(telegram, "send_telegram", fail)
    snapshot = report()
    snapshot["stop_reason"] = "private-token private-chat http://user:password@proxy.example:8080"
    notifier = telegram.RunNotifications(options(tmp_path), snapshot, stage="download", refresh=lambda: None)
    await notifier.start()
    snapshot["state"] = "interrupted"
    await notifier.finish()
    assert snapshot["telegram"]["failed"] == 2
    persisted = notifier.path.read_text() + capsys.readouterr().out + json.dumps(snapshot["telegram"])
    for value in ["private-token", "private-chat", "user:password"]:
        assert value not in persisted
    assert transport.sessions[0].closed


async def test_slow_telegram_does_not_block_browser_start_or_job_work(tmp_path, transport, monkeypatch):
    sending, release = asyncio.Event(), asyncio.Event()

    async def slow(*args):
        sending.set()
        await release.wait()

    monkeypatch.setattr(telegram, "send_telegram", slow)
    notifier = telegram.RunNotifications(options(tmp_path), report(), stage="search", refresh=lambda: None)
    await asyncio.wait_for(notifier.start(), 0.2)
    await sending.wait()
    notifier.browser_ready(0)  # Producer remains non-blocking during delivery.
    release.set()
    await asyncio.wait_for(notifier.finish(), 0.2)
    assert notifier.state["sent"] == 2


async def test_disabled_notifications_create_no_sessions_or_history(tmp_path, transport):
    notifier = telegram.RunNotifications(options(tmp_path, no_telegram=True), report(), stage="search", refresh=lambda: None)
    await notifier.start()
    notifier.browser_ready(0)
    await notifier.finish()
    assert transport.sessions == []
    assert not notifier.path.exists()


def test_interval_environment_cli_and_invalid_values(tmp_path, transport, monkeypatch):
    assert telegram.notification_settings(options(tmp_path))[2] == 300
    monkeypatch.setenv("RES_TELEGRAM_INTERVAL_MINUTES", "10")
    assert telegram.notification_settings(options(tmp_path))[2] == 600
    assert telegram.notification_settings(options(tmp_path, telegram_interval_minutes=7))[2] == 420
    for value in [0, float("nan"), float("inf"), 0.1]:
        args = options(tmp_path, telegram_interval_minutes=value)
        with pytest.raises(ValueError):
            telegram.notification_settings(args)
        with pytest.raises(ValueError):
            validate_runtime(args)
    monkeypatch.setenv("RES_TELEGRAM_INTERVAL_MINUTES", "bad")
    with pytest.raises(ValueError):
        telegram.notification_settings(options(tmp_path))


@pytest.mark.parametrize("stage", [search, download])
async def test_runner_sends_start_and_interrupted_final_after_cancellation(tmp_path, transport, monkeypatch, stage):
    from src.sites.dequienes.queue import QueueStore

    store = QueueStore(tmp_path / "companies.sqlite3")
    with store.transaction():
        store.db.execute("INSERT INTO companies VALUES ('company','TEST SpA','test spa',NULL,'name_candidate')")
        store.db.execute("INSERT INTO documents(cve) VALUES ('CODE000001')")
    store.close()
    started = asyncio.Event()

    async def page(self, *args, **kwargs):
        self.on_ready(0)
        return object()

    async def work(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr("src.sites.res.runtime.BrowserPool.page", page)
    monkeypatch.setattr(stage, "lookup" if stage is search else "download_certificate", work)
    monkeypatch.setattr(QueueStore, "request_delay", lambda *args: 0)
    args = options(
        tmp_path,
        profile=tmp_path / "profile",
        action="collect" if stage is search else "download",
        limit=1,
        run_id="telegram-cancel",
        retry_errors=False,
        max_attempts=3,
        workers=1,
        company_id=None,
        cve=None,
        rate_per_minute=6,
        timeout=60,
        captcha_wait=30,
        headed=False,
    )
    task = asyncio.create_task(stage.run(args))
    await asyncio.wait_for(started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(transport.sent) == 3
    assert "браузеры подготовлены" in transport.sent[1]
    assert "interrupted" in transport.sent[-1]
    store = QueueStore(tmp_path / "companies.sqlite3")
    try:
        queue = "res_company_lookup" if stage is search else "certificate_download"
        assert store.status()["queues"][queue] == {"retry_wait": 1}
    finally:
        store.close()
