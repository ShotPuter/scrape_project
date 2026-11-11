from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from curl_cffi.requests.exceptions import CertificateVerifyError

from src.sites.pep.discovery import DiscoveryClient

URL = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?find=test"
EMPTY = '''<table class="table"><thead><tr><th>Declarante</th><th>Descargar</th>
<th>Fecha de publicación</th></tr></thead><tbody><tr><td>
No se encuentran declaraciones</td></tr></tbody></table>'''


@pytest.mark.asyncio
async def test_http_discovery_empty_and_tls() -> None:
    client = DiscoveryClient(workers=1, delay=0, timeout=5)
    session = client.session
    try:
        client.session = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(
            status_code=200, url=URL, text=EMPTY, headers={})))
        assert await client.discover(URL) == ("discovered", [])
        client.session.get.side_effect = CertificateVerifyError("certificate failed")
        assert await client.discover(URL) == ("discovery_failed_tls", [])
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_http_discovery_does_not_retry_forbidden() -> None:
    client = DiscoveryClient(workers=1, delay=0, timeout=5)
    session = client.session
    try:
        client.session = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(status_code=403)))
        assert await client.discover(URL) == ("discovery_failed_http_403", [])
        assert client.session.get.await_count == 1
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_pagination_deduplication_and_partial_results() -> None:
    html = '''<table class="table"><thead><tr><th>Declarant</th><th>Download</th>
    <th>Publication date</th></tr></thead><tbody><tr><td>person</td><td>
    <form action="/fdci/consultaCiudadana/generarReporteConsultaCiudadana">
    <input name="id" value="123"></form></td><td>2024-03-01</td></tr></tbody></table>
    <div class="pagination"><a class="nextLink" href="?offset=10">next</a></div>'''
    client = DiscoveryClient(workers=1, delay=0, timeout=5)
    session = client.session
    try:
        client.session = SimpleNamespace(get=AsyncMock(side_effect=[
            SimpleNamespace(status_code=200, url=URL, text=html, headers={}),
            SimpleNamespace(status_code=403),
        ]))
        status, docs = await client.discover(URL)
        assert status == "discovery_failed_http_403"
        assert len(docs) == 1
        assert docs[0]["id"] == "123"
        assert docs[0]["status"] == "discovered"
        assert docs[0]["local_path"] is None
        assert client.session.get.await_count == 2
    finally:
        await session.close()
