"""Recurring asyncio scheduler with graceful shutdown on OS termination signals."""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta

from src.utils.logger import setup_logger

logger = setup_logger("scheduler")

Job = Callable[[], Awaitable[None]]


class AsyncScheduler:
    """Manages scheduled periodic task execution with non-blocking sleep and signal handling."""

    def __init__(
        self, job: Job, interval_hours: float, run_immediately: bool = True, logger_instance: logging.Logger | None = None
    ) -> None:
        if interval_hours <= 0:
            raise ValueError("interval_hours must be strictly positive")

        self.job = job
        self.interval = timedelta(hours=interval_hours)
        self.run_immediately = run_immediately
        self.logger = logger_instance or logger
        self._stop_event = asyncio.Event()

    def request_stop(self) -> None:
        """Signal the scheduler to cease recurring iterations gracefully."""
        self._stop_event.set()

    def install_signal_handlers(self) -> None:
        """Attach SIGINT and SIGTERM listeners to the active event loop."""
        current_loop = asyncio.get_running_loop()
        for termination_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                current_loop.add_signal_handler(termination_signal, self.request_stop)
            except NotImplementedError:
                # Platforms without signal handler support in event loops (e.g. Windows)
                pass

    async def run(self) -> None:
        """
        Execute the recurring scheduling loop until stopped via signal or request_stop.
        """
        if not self.run_immediately and await self._wait_before_first_run():
            self.logger.info("Scheduler execution terminated prior to initial task run")
            return

        while not self._stop_event.is_set():
            await self._run_job()

            next_run_time = datetime.now() + self.interval
            self.logger.info(
                "Next recurring scrape iteration scheduled", extra={"scheduled_time": next_run_time.isoformat(" ")}
            )

            is_stopped = await self._sleep(self.interval.total_seconds())
            if is_stopped:
                break

        self.logger.info("Scheduler execution loop stopped cleanly")

    async def _run_job(self) -> None:
        """Execute single job cycle with cancellation preservation and error logging."""
        self.logger.info("Scheduler triggered job execution", extra={"triggered_at": datetime.now().isoformat(" ")})
        try:
            await self.job()
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - a scheduler cycle must survive job failures
            self.logger.exception("Scheduled job run failed; continuing scheduler cycle", extra={"error": str(error)})

    async def _wait_before_first_run(self) -> bool:
        """Delay initial job execution when run_immediately is False."""
        first_run_time = datetime.now() + self.interval
        self.logger.info(
            "Scheduler waiting before first job execution", extra={"first_run_at": first_run_time.isoformat(" ")}
        )
        return await self._sleep(self.interval.total_seconds())

    async def _sleep(self, seconds: float) -> bool:
        """Interruptible sleep returning True if stop_event was triggered."""
        if seconds <= 0:
            return self._stop_event.is_set()
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return False
