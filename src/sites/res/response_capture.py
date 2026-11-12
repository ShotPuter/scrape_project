"""Read genuine RES JSON before a synchronous AJAX redirect discards its body."""

from __future__ import annotations

import asyncio
import base64
import json
from contextlib import suppress
from urllib.parse import urlsplit

from src.sites.res.source import DETAILS_PATH, HOST, SEARCH_PATH, SearchIssue


class ResponseCapture:
    def __init__(self, context, page):
        self.context, self.page = context, page
        self.session = None
        self.tasks = set()
        self.payloads = {path: asyncio.get_running_loop().create_future() for path in (SEARCH_PATH, DETAILS_PATH)}

    async def __aenter__(self):
        self.session = await self.context.new_cdp_session(self.page)
        self.session.on("Fetch.requestPaused", self.paused)
        await self.session.send(
            "Fetch.enable",
            {"patterns": [{"urlPattern": f"https://{HOST}{path}*", "requestStage": "Response"} for path in self.payloads]},
        )
        return self

    def paused(self, event):
        task = asyncio.create_task(self.capture(event))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def capture(self, event):
        request_id = event["requestId"]
        parts = urlsplit(event["request"]["url"])
        future = self.payloads.get(parts.path) if parts.scheme == "https" and parts.hostname == HOST else None
        try:
            if future is not None and not future.done():
                reply = await self.session.send("Fetch.getResponseBody", {"requestId": request_id})
                body = (
                    base64.b64decode(reply["body"], validate=True) if reply.get("base64Encoded") else reply["body"].encode()
                )
                if len(body) > 2 * 1024 * 1024:
                    raise ValueError("RES JSON exceeded size limit")
                future.set_result(json.loads(body))
        except Exception:
            if future is not None and not future.done():
                future.set_exception(SearchIssue("retry_wait", "Could not capture RES JSON before redirect"))
        finally:
            with suppress(Exception):
                await self.session.send("Fetch.continueResponse", {"requestId": request_id})

    async def read(self, response):
        parts = urlsplit(response.url)
        if parts.hostname != HOST or parts.scheme != "https" or parts.path not in self.payloads:
            return await response.json()
        return await asyncio.wait_for(asyncio.shield(self.payloads[parts.path]), 15)

    async def __aexit__(self, *exc):
        with suppress(Exception):
            await self.session.send("Fetch.disable")
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        for future in self.payloads.values():
            if future.done() and not future.cancelled():
                future.exception()
        with suppress(Exception):
            await self.session.detach()
