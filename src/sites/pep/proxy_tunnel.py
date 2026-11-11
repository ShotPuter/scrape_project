"""Owned loopback CONNECT bridge; upstream credentials never reach Chrome/logs."""

from __future__ import annotations

import asyncio
import base64
from contextlib import suppress
from urllib.parse import urlsplit


class ProxyTunnel:
    def __init__(self, settings: dict, *, timeout: float = 30) -> None:
        self.settings = settings
        self.timeout = timeout
        self.server = None
        self.tasks: set[asyncio.Task] = set()
        self.url = None

    async def start(self) -> str:
        if self.server is None:
            self.server = await asyncio.start_server(self.accept, "127.0.0.1", 0, limit=16384)
            port = self.server.sockets[0].getsockname()[1]
            self.url = f"http://127.0.0.1:{port}"
        return self.url

    async def accept(self, reader, writer) -> None:
        task = asyncio.current_task()
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        upstream = None
        try:
            async with asyncio.timeout(self.timeout):
                header = await reader.readuntil(b"\r\n\r\n")
                method, authority, version = header.split(b"\r\n", 1)[0].decode("ascii").split()
                target = urlsplit("//" + authority)
                if (method != "CONNECT" or version not in {"HTTP/1.0", "HTTP/1.1"} or target.port != 443
                        or not target.hostname or target.username or target.password
                        or target.path or target.query or target.fragment):
                    raise ValueError("CONNECT to HTTPS only")
                endpoint = urlsplit(self.settings["server"])
                remote, upstream = await asyncio.open_connection(endpoint.hostname, endpoint.port)
                auth = base64.b64encode(
                    f"{self.settings['username']}:{self.settings['password']}".encode()
                ).decode("ascii")
                upstream.write((f"CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n"
                                f"Proxy-Authorization: Basic {auth}\r\n\r\n").encode("ascii"))
                await upstream.drain()
                response = await remote.readuntil(b"\r\n\r\n")
                if response.split(b"\r\n", 1)[0].split()[1] != b"200":
                    raise ValueError("Upstream CONNECT refused")
                writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await writer.drain()

            async def relay(source, destination):
                while True:
                    async with asyncio.timeout(self.timeout):
                        chunk = await source.read(65536)
                        if not chunk:
                            return
                        destination.write(chunk)
                        await destination.drain()

            directions = [asyncio.create_task(relay(reader, upstream)), asyncio.create_task(relay(remote, writer))]
            try:
                await asyncio.wait(directions, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for direction in directions:
                    direction.cancel()
                await asyncio.gather(*directions, return_exceptions=True)
        except (OSError, ValueError, TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            # Never relay provider bodies/errors (they can echo credentials).
            with suppress(OSError):
                writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                await writer.drain()
        finally:
            for stream in (upstream, writer):
                if stream:
                    stream.close()
            for stream in (upstream, writer):
                if stream:
                    with suppress(OSError):
                        await stream.wait_closed()

    async def close(self) -> None:
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.url = None
