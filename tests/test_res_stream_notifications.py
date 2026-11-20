"""Notification timing, delivery isolation and operator-facing counts; no external calls."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

from src.sites.res.stream_notifications import StreamNotifications


def settings(**overrides):
    return SimpleNamespace(**dict({"run_id": "test", "no_telegram": True, "telegram_minutes": 30}, **overrides))


def snapshot(**overrides):
    return dict(
        {
            "counts": {"jobs": {"done": 3, "leased": 2, "pending": 5}, "proxies": {"ready": 4, "probing": 2}},
            "elapsed_seconds": 1800,
            "new_pdfs": 6,
            "total_pdfs": 9,
            "companies_with_pdf": 5,
            "active_browsers": 2,
            "ready_browsers": 1,
            "browser_limit": 5,
            "state": "running",
            "producer": {"checks": 100, "ready": 10, "reputation_denied": 20, "connection_failed": 70},
        },
        **overrides,
    )


def records(notifications):
    if not notifications.path.exists():
        return []
    return [json.loads(line) for line in notifications.path.read_text().splitlines()]


def test_summary_has_counts_speed_and_incremental_period(tmp_path):
    notifications = StreamNotifications(settings(), tmp_path)
    text = notifications.format_summary(snapshot())
    assert "3/10" in text and "6 (+6 за период)" in text
    assert "всего PDF: 9" in text and "PDF у компаний: 5" in text
    assert "0.20 PDF/мин" in text and "Проверок прокси: 100" in text
    assert "отклонено репутацией: 20" in text and "ошибок связи/проверки: 70" in text
    notifications.previous.update(new_pdfs=4, elapsed_seconds=1200)
    assert "6 (+2 за период)" in notifications.format_summary(snapshot())
    assert "за период: 0.20 PDF/мин" in notifications.format_summary(snapshot())


def test_full_summary_every_thirty_minutes_without_duplicate_ticks(tmp_path):
    now = [0]
    notifications = StreamNotifications(settings(), tmp_path, clock=lambda: now[0])
    notifications.update(snapshot(new_pdfs=0))
    now[0] = 1799
    notifications.update(snapshot(new_pdfs=0))
    assert not any(row["event"] == "progress" for row in records(notifications))
    now[0] = 1800
    notifications.update(snapshot())
    notifications.update(snapshot())
    assert sum(row["event"] == "progress" for row in records(notifications)) == 1
    now[0] = 3600
    notifications.update(snapshot(new_pdfs=8, elapsed_seconds=3600))
    progress = [row for row in records(notifications) if row["event"] == "progress"]
    assert len(progress) == 2 and "(+2 за период)" in progress[-1]["text"]


def test_first_event_immediate_then_rotations_batched_every_five_minutes(tmp_path):
    now = [0]
    notifications = StreamNotifications(settings(), tmp_path, clock=lambda: now[0])
    for index in range(20):
        notifications.activity("browser_started", f"slot {index % 5}")
    rows = records(notifications)
    assert sum(row["event"] == "first_browser_started" for row in rows) == 1
    assert len(rows) == 20  # Every rotation remains in the local journal.
    now[0] = 299
    notifications.update(snapshot(new_pdfs=0))
    assert not any(row["event"] == "activity" for row in records(notifications))
    now[0] = 300
    notifications.update(snapshot(new_pdfs=0))
    activity = [row for row in records(notifications) if row["event"] == "activity"]
    assert len(activity) == 1 and "запусков браузеров за 5 мин 20" in activity[0]["text"]
    assert not notifications.pending


def test_health_and_cooldown_transitions_are_deduplicated(tmp_path):
    notifications = StreamNotifications(settings(), tmp_path)
    paused = snapshot(security_paused=True, cooldown={"active": True})
    notifications.update(paused)
    notifications.update(paused)
    notifications.update(snapshot(cooldown={"active": False}))
    events = [row["event"] for row in records(notifications)]
    assert events.count("cooldown") == 1 and events.count("resumed") == 1
    assert events.count("health") == 2
    assert "Непроверенные IP не используются" in next(
        row["text"] for row in records(notifications) if row["event"] == "health"
    )


def test_journal_redacts_credentials_and_rotates_bounded_history(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "secret-telegram-token")
    notifications = StreamNotifications(settings(), tmp_path)
    notifications.path.write_bytes(b" " * (5 * 1024 * 1024))
    notifications.publish("test", "secret-telegram-token http://proxyuser:proxypassword@1.1.1.1:80")
    text = notifications.path.read_text()
    assert "secret-telegram-token" not in text and "proxypassword" not in text
    assert notifications.path.with_suffix(".previous.jsonl").exists()
    assert notifications.path.stat().st_size < 1000


async def test_telegram_disabled_keeps_journal_without_any_delivery(tmp_path):
    deliver = AsyncMock()
    notifications = StreamNotifications(settings(), tmp_path, deliver=deliver)
    await notifications.start()
    notifications.publish("started", "test")
    await notifications.finish("done")
    deliver.assert_not_awaited()
    assert len(records(notifications)) == 2 and notifications.state["enabled"] is False


async def test_delivery_failure_does_not_stop_subsequent_notifications(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "test-chat")
    deliver = AsyncMock(side_effect=[RuntimeError("never log secret remote payload"), None])
    notifications = StreamNotifications(settings(no_telegram=False), tmp_path, deliver=deliver)
    await notifications.start()
    notifications.publish("started", "hello")
    await notifications.finish("done")
    assert notifications.state["failed"] == 1 and notifications.state["sent"] == 1
    assert "remote payload" not in notifications.path.read_text()


async def test_slow_telegram_never_blocks_producer_and_queue_is_bounded(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "test-chat")
    gate = asyncio.Event()

    async def deliver(text):
        await gate.wait()

    notifications = StreamNotifications(settings(no_telegram=False), tmp_path, deliver=deliver)
    await notifications.start()
    for index in range(100):
        notifications.publish("rotation", str(index))
    assert notifications.queue.qsize() == 32 and notifications.state["dropped"] == 68
    gate.set()
    await asyncio.wait_for(notifications.queue.join(), 1)
    await notifications.finish("done")
    assert notifications.state["sent"] == 33


async def test_final_report_takes_priority_over_stale_progress(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "test-chat")
    deliver = AsyncMock()
    notifications = StreamNotifications(settings(no_telegram=False), tmp_path, deliver=deliver)
    await notifications.start()
    for index in range(32):
        notifications.publish("progress", str(index))
    await notifications.finish("final report")
    deliver.assert_awaited_once_with("final report")
    assert notifications.state["dropped"] == 32


def test_first_pdf_and_stall_recovery_messages_are_not_repeated(tmp_path):
    now = [0]
    notifications = StreamNotifications(settings(), tmp_path, clock=lambda: now[0])
    now[0] = 600
    notifications.update(snapshot(new_pdfs=0))
    notifications.update(snapshot(new_pdfs=0))
    now[0] = 610
    notifications.update(snapshot(new_pdfs=1))
    notifications.update(snapshot(new_pdfs=2))
    rows = records(notifications)
    assert sum(row["event"] == "health" for row in rows) == 2
    assert sum(row["event"] == "first_first_pdf" for row in rows) == 1


def test_summary_mode_suppresses_proxy_and_browser_delivery(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "test-chat")
    notifications = StreamNotifications(settings(no_telegram=False, telegram_events="summary"), tmp_path)
    notifications.activity("browser_started", "browser")
    notifications.activity("proxy_ready", "proxy")
    notifications.publish("activity", "five-minute noise")
    notifications.publish("progress", "thirty-minute summary")
    assert notifications.queue.qsize() == 1
    assert notifications.queue.get_nowait() == ("progress", "thirty-minute summary")
    assert len(records(notifications)) == 4


async def test_delivery_log_includes_safe_http_failure_reason(tmp_path, monkeypatch):
    from src.services.telegram import TelegramDeliveryError

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "test-chat")
    deliver = AsyncMock(side_effect=[TelegramDeliveryError("sendMessage: HTTP 403"), None])
    notifications = StreamNotifications(settings(no_telegram=False), tmp_path, deliver=deliver)
    await notifications.start()
    notifications.publish("started", "hello")
    await notifications.finish("done")

    failures = [row["text"] for row in records(notifications) if row["event"] == "telegram_failed"]
    assert len(failures) == 1 and "sendMessage: HTTP 403" in failures[0]
