import asyncio

import pytest

from scripts.pep_camoufox_pause import SitePause


def failed(number):
    return {
        "n": number,
        "url": f"https://example.com/?n={number}",
        "status": "failed",
        "navigation_stage": "waiting_result_html",
        "error": {"type": "TimeoutError"},
    }


@pytest.mark.asyncio
async def test_only_distinct_recent_workers_trigger_pause():
    events = []

    async def notify(event):
        events.append(event)

    now = [0]
    pause = SitePause(6, notify, clock=lambda: now[0])
    await pause.record(1, failed(1))
    await pause.record(1, failed(2))
    await pause.record(1, failed(2))
    await pause.record(2, {**failed(3), "error": {"type": "ValueError"}})
    assert not pause.blocked
    now[0] = 181
    await pause.record(2, failed(4))
    await pause.record(3, failed(5))
    assert not pause.blocked
    await pause.record(4, failed(6))
    assert pause.blocked
    assert pause.snapshot()["remaining_seconds"] == 1200
    assert events == ["site_paused"]


@pytest.mark.asyncio
async def test_one_probe_failed_probe_extends_pause_success_releases_all():
    events = []

    async def notify(event):
        events.append(event)

    now = [0]
    pause = SitePause(4, notify, clock=lambda: now[0])
    await pause.record(1, failed(1))
    await pause.record(2, failed(2))
    tasks = [asyncio.create_task(pause.permission(i)) for i in range(1, 5)]
    try:
        await asyncio.sleep(0)
        assert not any(task.done() for task in tasks)
        now[0] = 1200
        pause.signal()
        await asyncio.sleep(0.01)
        assert sum(task.done() for task in tasks) == 1
        owner = pause.owner
        await pause.probe_result(owner, False)
        assert pause.snapshot()["remaining_seconds"] == 1200
        assert pause.state == "paused"
        now[0] = 2400
        pause.signal()
        await asyncio.sleep(0.01)
        assert sum(task.done() for task in tasks) == 2
        await pause.probe_result(pause.owner, True)
        await asyncio.gather(*tasks)
        assert not pause.blocked
        assert events == ["site_paused", "site_probe_failed", "site_resumed"]
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
