from contextlib import asynccontextmanager
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zipfile import ZipFile

import pytest

from src.sites.pep.documents import DocumentDownloader, save_payload

PDF = b"%PDF-1.4\nexample\n%%EOF"


def test_zip_preserves_archive_and_extracts_all_pdfs_safely(tmp_path) -> None:
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr("../../escape.pdf", PDF)
        archive.writestr("second.pdf", PDF)
        archive.writestr("FORMULARIO.xlsx", b"example")
    result = save_payload(buffer.getvalue(), tmp_path, "123")
    assert result["status"] == "downloaded"
    assert len(result["files"]) == 2
    assert [f["local_path"] for f in result["files"]] == ["pdf/123.pdf", "pdf/123-2.pdf"]
    assert (tmp_path / result["archive_path"]).read_bytes() == buffer.getvalue()
    assert not (tmp_path.parent / "escape.pdf").exists()


def test_reject_zip_without_pdf(tmp_path) -> None:
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr("FORMULARIO.xlsx", b"example")
    with pytest.raises(ValueError, match="no PDFs"):
        save_payload(buffer.getvalue(), tmp_path, "123")


@pytest.mark.asyncio
async def test_download_save_and_resume(tmp_path) -> None:
    class Response:
        status_code = 200

        async def aiter_content(self):
            yield PDF[:10]
            yield PDF[10:]

    @asynccontextmanager
    async def stream(*args, **kwargs):
        yield Response()

    client = SimpleNamespace(pace=AsyncMock(), session=SimpleNamespace(stream=stream))
    downloader = DocumentDownloader(client, tmp_path)
    doc = {"id": "123", "requires_captcha": False,
           "source_page": "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index"}
    result = await downloader.download(doc)
    assert result["status"] == "downloaded"
    assert (tmp_path / result["local_path"]).read_bytes() == PDF
    assert result["s3_uri"] is None
    assert result["size_bytes"] == len(PDF)
    assert (await downloader.download(doc))["sha256"] == result["sha256"]
    assert client.pace.await_count == 1
    (tmp_path / result["local_path"]).write_bytes(b"broken")
    assert (await downloader.download(doc))["status"] == "downloaded"
    assert client.pace.await_count == 2


@pytest.mark.asyncio
async def test_captcha_not_submitted(tmp_path) -> None:
    client = SimpleNamespace(pace=AsyncMock())
    downloader = DocumentDownloader(client, tmp_path)
    result = await downloader.download({"id": "123", "requires_captcha": True})
    assert result["status"] == "captcha_required"
    assert result["local_path"] is None
    client.pace.assert_not_awaited()


@pytest.mark.asyncio
async def test_full_pipeline_from_workbook_to_json(tmp_path, monkeypatch) -> None:
    import json

    from openpyxl import Workbook

    from src.sites.pep import __main__ as pipeline

    path = tmp_path / "input.xlsx"
    workbook = Workbook()
    sheet = workbook.create_sheet("data")
    sheet.append(["NAME", "ENLACE_CONSULTA_LEY_2013"])
    url = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?find=test"
    sheet.append(["first", url])
    sheet.append(["second", url])
    workbook.save(path)
    output = tmp_path / "out"
    (output / "pdf").mkdir(parents=True)
    (output / "pdf" / "123.pdf").write_bytes(PDF)
    discover = AsyncMock(return_value=("discovered", [{"id": "123", "requires_captcha": True}]))
    monkeypatch.setattr(pipeline.DiscoveryClient, "discover", discover)
    monkeypatch.setattr(pipeline.DiscoveryClient, "preflight", AsyncMock())
    args = SimpleNamespace(output=output, workbook=path, import_only=False, download_browser=False,
                           workers=2, delay=0, timeout=5, ca_bundle=None, limit=None, discover_only=False)
    assert await pipeline.run(args) == 0
    records = json.loads((output / "records.json").read_text())
    assert len(records) == 2
    assert all(row["processing_status"] == "completed" for row in records)
    assert all(row["documents"][0]["status"] == "downloaded" for row in records)
    assert await pipeline.run(args) == 0
    assert discover.await_count == 1
    assert json.loads((output / "summary.json").read_text())["jobs"] == {"completed": 1}
