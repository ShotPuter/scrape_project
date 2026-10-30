import json

from src.services.run_statistics import statistics


def test_errors_include_status_and_redacted_proxy(tmp_path):
    event = {"http_status": 412, "proxy_address": "http://***@1.2.3.4:80"}
    (tmp_path / "scraper.log").write_text('prefix http_error ' + json.dumps(event) + '\n')
    (tmp_path / "scraper.log.1").write_text('prefix http_error ' + json.dumps(event) + '\ninvalid\n')
    text = statistics(tmp_path, errors=True)
    assert "2 попыток" in text
    assert "412" in text
    assert "1.2.3.4:80" in text


def test_missing_and_running_snapshot(tmp_path):
    assert "отсутствует" in statistics(tmp_path)
    (tmp_path / "scraper-status.json").write_text(json.dumps({
        "status": "RUNNING", "items_scraped": 12, "remaining_discovered": 7,
        "http_metrics": {"http_412": 3}, "updated_at": "2026-10-30",
    }))
    text = statistics(tmp_path)
    assert "RUNNING" in text and "412: 3" in text
    assert "очереди: 7" in text



def test_notification_schedule_and_restart_deduplication():
    from src.services.notifications import should_notify

    report = {'run_id': 'a', 'status': 'RUNNING'}
    state = {'run': 'a', 'status': 'RUNNING', 'sent_at': 100}
    assert not should_notify({}, {}, 100, 1800)
    assert should_notify(report, {}, 100, 1800)
    assert not should_notify(report, state, 101, 1800)
    assert should_notify(report, state, 1900, 1800)
    report['status'] = 'COMPLETED'
    assert should_notify(report, state, 101, 1800)
    state['status'] = 'COMPLETED'
    assert not should_notify(report, state, 10000, 1800)
    report['run_id'] = 'b'
    assert should_notify(report, state, 101, 1800)


async def test_telegram_delivery_and_secret_redaction():
    from unittest.mock import AsyncMock, MagicMock

    import aiohttp
    import pytest

    from src.services.notifications import send_telegram

    session = MagicMock()
    response = AsyncMock()
    response.status = 200
    response.json.return_value = {'ok': True}
    session.post.return_value.__aenter__.return_value = response
    await send_telegram(session, 'secret-token', '123', 'statistics')
    assert session.post.call_args.kwargs['json'] == {'chat_id': '123', 'text': 'statistics'}
    session.post.side_effect = aiohttp.ClientError('https://api.telegram.org/botsecret-token/sendMessage')
    with pytest.raises(RuntimeError) as caught:
        await send_telegram(session, 'secret-token', '123', 'statistics')
    assert 'secret-token' not in str(caught.value)
    assert caught.value.__suppress_context__


async def test_startup_message_without_scraper_report(tmp_path, monkeypatch):
    import asyncio
    from unittest.mock import AsyncMock

    import pytest

    from src.services import notifications

    monkeypatch.setenv('TELEGRAM_BOT_TOKEN', 'test-token')
    monkeypatch.setenv('TELEGRAM_CHAT_ID', '123')
    monkeypatch.setattr(notifications, 'read_json', lambda path: {})

    async def immediate(function, *args, **kwargs):
        return function(*args, **kwargs)

    sender = AsyncMock()
    monkeypatch.setattr(notifications, 'send_telegram', sender)
    monkeypatch.setattr(notifications.asyncio, 'to_thread', immediate)
    monkeypatch.setattr(notifications.asyncio, 'sleep', AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await notifications.watch(tmp_path, 1800)
    sender.assert_awaited_once()
    text = sender.call_args.args[3]
    assert 'Сервис уведомлений запущен' in text
    assert '30 мин.' in text
