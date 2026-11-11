"""Global request pacing without creating an HTTP session."""

from __future__ import annotations

import asyncio


class RequestPacer:
    def __init__(self, delay: float):
        self.delay = delay
        self.lock = asyncio.Lock()
        self.next_request = 0.0

    async def pace(self) -> None:
        async with self.lock:
            loop = asyncio.get_running_loop()
            await asyncio.sleep(max(0, self.next_request - loop.time()))
            self.next_request = loop.time() + self.delay
