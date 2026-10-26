"""Bounded proxy bootstrap and collector lifecycle tests."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

import pytest

from src.services import proxy_bootstrap
from src.storage.proxy_database import ProxyDatabase, VerifiedProxy


class FakeProcess:
    pid = 1234

    def __init__(self, returncode=None):
        self.returncode = returncode
        self.terminate = Mock()
        self.kill = Mock()

    async def wait(self):
        if self.returncode is None:
            self.returncode = -15
        return self.returncode


@pytest.mark.asyncio
async def test_wait_for_proxies_returns_when_required_fresh_records_exist(tmp_path):
    db_path = tmp_path / "proxies.db"
    async with ProxyDatabase(db_path) as database:
        await database.replace_all([VerifiedProxy("http://fresh.example:80", "UA", "aiohttp")])

    available = await proxy_bootstrap.wait_for_proxies(
        db_path,
        FakeProcess(returncode=None),
        required=1,
        timeout=1,
        max_age_hours=24,
        poll_interval=0.01,
    )

    assert available == 1


@pytest.mark.asyncio
async def test_wait_for_proxies_ignores_stale_records_and_is_bounded(tmp_path):
    db_path = tmp_path / "proxies.db"
    stale = (datetime.now(UTC) - timedelta(days=3)).isoformat()
    async with ProxyDatabase(db_path) as database:
        await database.replace_all([VerifiedProxy("http://stale.example:80", "UA", "aiohttp", checked_at=stale)])

    available = await proxy_bootstrap.wait_for_proxies(
        db_path,
        FakeProcess(returncode=None),
        required=1,
        timeout=0.05,
        max_age_hours=24,
        poll_interval=0.01,
    )

    assert available == 0


@pytest.mark.asyncio
async def test_proxy_collector_starts_and_stops_the_child(monkeypatch, tmp_path):
    db_path = tmp_path / "proxies.db"
    process = FakeProcess(returncode=None)
    commands = []

    async def fake_exec(*command):
        commands.append(command)
        return process

    monkeypatch.setattr(proxy_bootstrap.asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(proxy_bootstrap.settings, "proxy_min_ready", 7)
    monkeypatch.setattr(proxy_bootstrap.settings, "proxy_collector_stop_timeout_seconds", 0.05)

    async with proxy_bootstrap.proxy_collector(db_path) as child:
        assert child is process
        assert Path(commands[0][1]).name == "collect_proxies.py"
        assert commands[0][2:] == ("--db", str(db_path), "--publish-after", "7")

    process.terminate.assert_called_once()
    assert process.returncode == -15


@pytest.mark.asyncio
async def test_stop_process_terminates_and_reaps(monkeypatch):
    process = FakeProcess(returncode=None)
    monkeypatch.setattr(proxy_bootstrap.settings, "proxy_collector_stop_timeout_seconds", 0.05)

    await proxy_bootstrap.stop_process(process, 0.05)

    process.terminate.assert_called_once()
    assert process.returncode == -15


@pytest.mark.asyncio
async def test_stop_process_kills_when_terminate_does_not_finish(monkeypatch):
    class StubbornProcess(FakeProcess):
        async def wait(self):
            if self.returncode is None:
                raise TimeoutError
            return self.returncode

    process = StubbornProcess(returncode=None)

    def kill():
        process.returncode = -9

    process.kill = Mock(side_effect=kill)

    await proxy_bootstrap.stop_process(process, 0.01)

    process.terminate.assert_called_once()
    process.kill.assert_called_once()
    assert process.returncode == -9
