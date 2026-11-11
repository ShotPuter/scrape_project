import asyncio
import base64
from urllib.parse import urlsplit

import pytest

from src.sites.pep.proxy_tunnel import ProxyTunnel


@pytest.mark.asyncio
@pytest.mark.parametrize("refuse", [False, True])
@pytest.mark.parametrize("version", ["HTTP/1.0", "HTTP/1.1"])
async def test_authenticated_connect_is_ready_sanitized_and_fail_closed(refuse, version):
    headers = []
    finished = asyncio.Event()

    async def provider(reader, writer):
        try:
            headers.append(await reader.readuntil(b"\r\n\r\n"))
            if refuse:
                writer.write(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\nsecret-provider-body")
            else:
                writer.write(b"HTTP/1.1 200 OK\r\n\r\n")
                await writer.drain()
                writer.write(await reader.readexactly(4))
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            finished.set()

    server = await asyncio.start_server(provider, "127.0.0.1", 0)
    tunnel = ProxyTunnel({"server": f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}",
                          "username": "user", "password": "p@ss"}, timeout=2)
    try:
        endpoint = urlsplit(await tunnel.start())
        reader, writer = await asyncio.open_connection(endpoint.hostname, endpoint.port)
        writer.write(f"CONNECT example.com:443 {version}\r\nProxy-Authorization: Basic untrusted\r\n\r\n".encode())
        await writer.drain()
        response = await reader.readuntil(b"\r\n\r\n")
        assert b"502" in response if refuse else b"200" in response
        if refuse:
            assert await reader.read() == b""
        else:
            writer.write(b"ping")
            await writer.drain()
            assert await reader.readexactly(4) == b"ping"
        writer.close()
        await writer.wait_closed()
        await asyncio.wait_for(finished.wait(), 2)
        assert base64.b64encode(b"user:p@ss") in headers[0]
        assert b"untrusted" not in headers[0]
        assert b"secret" not in response
    finally:
        await tunnel.close()
        server.close()
        await server.wait_closed()
    assert not tunnel.tasks


@pytest.mark.asyncio
async def test_close_cancels_stalled_connections_and_rejects_http():
    tunnel = ProxyTunnel({"server": "http://127.0.0.1:1", "username": "", "password": ""})
    endpoint = urlsplit(await tunnel.start())
    reader, writer = await asyncio.open_connection(endpoint.hostname, endpoint.port)
    writer.write(b"GET http://example.com HTTP/1.1\r\n\r\n")
    await writer.drain()
    assert b"502" in await reader.read()
    writer.close()
    await writer.wait_closed()
    reader, writer = await asyncio.open_connection(endpoint.hostname, endpoint.port)
    await asyncio.wait_for(tunnel.close(), 2)
    assert await reader.read() == b""
    writer.close()
    await writer.wait_closed()
    assert not tunnel.tasks
