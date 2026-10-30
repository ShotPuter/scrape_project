"""Bounded proxy-database startup and collector-process lifecycle.

The CLI uses this module to answer two questions before a scrape:

* does the verified-proxy database contain enough *fresh* records?
* if not, how long may the collector run before the scrape continues?

Every wait is bounded and every spawned collector is stopped deterministically.
"""

from __future__ import annotations

import asyncio
import sys
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path

from config import BASE_DIR, settings
from src.proxy.repository import ProxyDatabase
from src.utils.logger import setup_logger

logger = setup_logger("proxy_bootstrap")

DEFAULT_POLL_INTERVAL_SECONDS = 0.5
WAIT_REPORT_INTERVAL_SECONDS = 15.0


async def count_fresh_proxies(db_path: Path, *, max_age_hours: float, limit: int | None = None) -> int:
    """Return how many usable records in ``db_path`` are recent enough."""
    max_age = max_age_hours if max_age_hours > 0 else None
    async with ProxyDatabase(db_path) as database:
        return len(await database.list_active(limit=limit, max_age_hours=max_age))


async def wait_for_proxies(  # noqa: ASYNC109 - explicit bounded wait
    db_path: Path,
    process: asyncio.subprocess.Process | None,
    *,
    required: int,
    timeout: float,  # noqa: ASYNC109 - bounded wait is the whole point of this helper
    max_age_hours: float,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
) -> int:
    """Wait until ``required`` fresh proxies exist, the collector exits, or time runs out.

    Returns the number of fresh proxies available at the end.  The caller decides
    whether that is enough to scrape or whether a direct fallback is preferable.
    """
    db_path = await asyncio.to_thread(db_path.resolve)
    deadline = time.monotonic() + timeout
    started = time.monotonic()
    last_report = started
    while True:
        available = await count_fresh_proxies(db_path, max_age_hours=max_age_hours, limit=required)
        if available >= required:
            logger.info("Verified proxy pool ready: %d fresh proxies", available)
            return available
        if process is not None and process.returncode is not None:
            logger.info(
                "Proxy collector exited with code %d after publishing %d/%d fresh proxies",
                process.returncode,
                available,
                required,
            )
            return available
        now = time.monotonic()
        remaining = deadline - now
        if remaining <= 0:
            logger.warning(
                "Proxy collection did not reach %d fresh proxies within %.0fs (%d available); continuing without waiting",
                required,
                timeout,
                available,
            )
            return available
        if now - last_report >= WAIT_REPORT_INTERVAL_SECONDS:
            last_report = now
            logger.info(
                "Waiting for proxies: %d/%d fresh, %.0fs elapsed, %.0fs left | DB: %s",
                available,
                required,
                now - started,
                remaining,
                db_path,
            )
        await asyncio.sleep(min(poll_interval, remaining))


@asynccontextmanager
async def proxy_collector(db_path: Path) -> AsyncIterator[asyncio.subprocess.Process]:
    """Start the standalone collector and guarantee a bounded shutdown."""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(BASE_DIR / "collect_proxies.py"),
        "--db",
        str(db_path),
        "--publish-after",
        str(settings.proxy_min_ready),
    )
    logger.info("Started proxy collector (pid %s)", getattr(process, "pid", "unknown"))
    try:
        yield process
    finally:
        await stop_process(process, settings.proxy_collector_stop_timeout_seconds)


async def stop_process(process: asyncio.subprocess.Process, timeout: float) -> None:  # noqa: ASYNC109
    """Terminate a collector, escalating to kill if it does not exit in time."""
    if process.returncode is not None:
        return

    try:
        process.terminate()
    except ProcessLookupError:
        await _reap(process)
        return

    try:
        await asyncio.wait_for(process.wait(), timeout)
    except TimeoutError:
        logger.warning("Proxy collector ignored terminate for %.0fs; killing it", timeout)
        with suppress(ProcessLookupError):
            process.kill()
        await _reap(process)


async def _reap(process: asyncio.subprocess.Process) -> None:
    with suppress(ProcessLookupError):
        await process.wait()
