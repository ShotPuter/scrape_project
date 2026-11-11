from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.sites.pep.documents import DocumentDownloader
from src.sites.pep.source import parse_results


def test_legacy_asset_link_is_preserved_and_tax_is_not_downloaded() -> None:
    html = '''<table class="table"><thead><tr><th>Declarante</th><th>Descargar</th>
    <th>Fecha de publicación</th></tr></thead><tbody><tr><td>example</td><td>
    <a href="/ley-transparencia-web/declaracionServlet?action=download&amp;file=/ABC_example.pdf">
    Declaración Bienes y rentas y conflictos de interes</a>
    <a href="/ley-transparencia-web/declaracionServlet?action=download&amp;file=/ARD_example.pdf">
    Declaración de Renta</a></td><td>2020-01-01</td></tr></tbody></table>'''
    docs, _ = parse_results(html, "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index")
    assert len(docs) == 1
    assert docs[0]["id"].startswith("legacy-")
    assert not docs[0]["requires_captcha"]
    assert "ABC_example" in docs[0]["download_url"]


@pytest.mark.asyncio
async def test_legacy_download_uses_get(tmp_path) -> None:
    calls = []

    class Response:
        status_code = 200

        async def aiter_content(self):
            yield b"%PDF-1.4\nexample\n%%EOF"

    @asynccontextmanager
    async def stream(method, url, **kwargs):
        calls.append((method, kwargs.get("data")))
        yield Response()

    client = SimpleNamespace(pace=AsyncMock(), session=SimpleNamespace(stream=stream))
    downloader = DocumentDownloader(client, tmp_path)
    result = await downloader.download({
        "id": "legacy-" + "a" * 24, "requires_captcha": False,
        "source_page": "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index",
        "download_url": "https://www.funcionpublica.gov.co/ley-transparencia-web/declaracionServlet?action=download",
    })
    assert result["status"] == "downloaded"
    assert calls == [("GET", None)]
