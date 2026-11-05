"""Fleet-wide cooldown for correlated navigation timeouts."""

from __future__ import annotations

import asyncio
import math
import time


class SitePause:
    def __init__(self, workers, notify, *, seconds=1200, window=180, clock=time.monotonic):
        self.threshold = min(workers, max(2, math.ceil(workers / 2)))
        self.notify, self.seconds, self.window, self.clock = notify, seconds, window, clock
        self.failures, self.seen = {}, set()
        self.state, self.deadline, self.owner = "running", 0, None
        self.probe_url = None
        self.changed = asyncio.Event()
        self.count = 0

    @property
    def blocked(self):
        return self.state != "running"

    def snapshot(self):
        return {
            "state": self.state,
            "remaining_seconds": max(0, round(self.deadline - self.clock())),
            "affected_workers": len(self.failures),
            "threshold": self.threshold,
            "pause_count": self.count,
            "probe_worker": self.owner,
        }

    def signal(self):
        previous = self.changed
        self.changed = asyncio.Event()
        previous.set()

    async def record(self, worker, job):
        if job["status"] not in {"failed", "completed", "completed_with_unavailable", "partial"}:
            return
        key = (worker, job["url"], job.get("started_at", job.get("n")))
        if key in self.seen:
            return
        self.seen.add(key)
        if self.blocked:
            return
        error = job.get("error") or {}
        timeout = error.get("type") == "TimeoutError" and (
            job.get("navigation_stage") == "waiting_result_html" or "Page.goto" in error.get("message", "")
        )
        if not timeout:
            if job["status"] in {"completed", "completed_with_unavailable"}:
                self.failures.pop(worker, None)
            return
        now = self.clock()
        self.failures = {index: at for index, at in self.failures.items() if now - at <= self.window}
        self.failures[worker] = now
        self.probe_url = job["url"]
        if len(self.failures) >= self.threshold:
            self.state, self.deadline = "paused", now + self.seconds
            self.count += 1
            self.signal()
            await self.notify("site_paused")

    async def permission(self, worker):
        """Return True only to the one browser allowed to probe after cooldown."""
        while self.blocked:
            changed = self.changed
            remaining = self.deadline - self.clock()
            if self.state == "paused" and remaining <= 0:
                self.state, self.owner = "probing", worker
                self.signal()
                return True
            try:
                if self.state == "paused":
                    await asyncio.wait_for(changed.wait(), timeout=max(remaining, 0.001))
                else:
                    await changed.wait()
            except TimeoutError:
                continue
        return False

    async def probe_result(self, worker, success):
        if self.state != "probing" or self.owner != worker:
            return
        self.owner = None
        if success:
            self.state = "running"
            self.failures.clear()
            event = "site_resumed"
        else:
            self.state, self.deadline = "paused", self.clock() + self.seconds
            self.count += 1
            event = "site_probe_failed"
        self.signal()
        await self.notify(event)
