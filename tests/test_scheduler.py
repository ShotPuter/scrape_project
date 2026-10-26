"""Scheduler lifecycle tests."""

import asyncio

import pytest

from src.services.scheduler import AsyncScheduler


@pytest.mark.asyncio
async def test_runs_immediately_and_stops_gracefully():
    runs = {"count": 0}
    scheduler: AsyncScheduler

    async def job():
        runs["count"] += 1
        scheduler.request_stop()

    scheduler = AsyncScheduler(job=job, interval_hours=24, run_immediately=True)
    await asyncio.wait_for(scheduler.run(), timeout=2)

    assert runs["count"] == 1


@pytest.mark.asyncio
async def test_job_exception_does_not_kill_scheduler():
    runs = {"count": 0}
    scheduler: AsyncScheduler

    async def job():
        runs["count"] += 1
        if runs["count"] == 1:
            raise RuntimeError("transient failure")
        scheduler.request_stop()

    scheduler = AsyncScheduler(job=job, interval_hours=0.0001, run_immediately=True)
    await asyncio.wait_for(scheduler.run(), timeout=2)

    assert runs["count"] == 2


@pytest.mark.asyncio
async def test_no_immediate_run_can_be_stopped_while_waiting():
    runs = {"count": 0}
    scheduler: AsyncScheduler

    async def job():
        runs["count"] += 1

    scheduler = AsyncScheduler(job=job, interval_hours=24, run_immediately=False)
    asyncio.get_running_loop().call_later(0.05, scheduler.request_stop)
    await asyncio.wait_for(scheduler.run(), timeout=2)

    assert runs["count"] == 0


def test_rejects_non_positive_interval():
    with pytest.raises(ValueError):
        AsyncScheduler(job=lambda: None, interval_hours=0)
