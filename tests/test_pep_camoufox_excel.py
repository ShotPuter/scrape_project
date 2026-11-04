"""Offline checks for Excel selection and progress delivery; no live messages."""

import json

import pytest
from openpyxl import Workbook

from scripts import pep_camoufox_excel as server

BASE = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?numeroDocumento="


def test_excel_unique_selection_and_offset(tmp_path):
    workbook = Workbook()
    sheet = workbook.create_sheet()
    sheet.append([server.LINK_COLUMN])
    sheet.append([BASE.replace("https:", "http:") + "1"])
    sheet.append([BASE + "1"])
    sheet.append([BASE + "2"])
    sheet.append([None])
    sheet.append([BASE + "3"])
    path = tmp_path / "input.xlsx"
    workbook.save(path)
    workbook.close()
    sample = server.collect_sample(path, limit=1, offset=1)
    assert sample == [{"n": 1, "source_row": 4, "url": BASE + "2"}]
    assert len(server.collect_sample(path, limit=0, offset=0)) == 3


def report(processed):
    return {
        "status": "running",
        "pdf_count": processed,
        "jobs": [
            {
                "status": "completed" if i < processed else "pending",
                "documents": [{"status": "downloaded"}] if i < processed else [],
            }
            for i in range(21)
        ],
    }


@pytest.mark.asyncio
async def test_telegram_every_ten_and_final(monkeypatch, tmp_path):
    messages = []

    async def fake_send(session, token, chat_id, text):
        messages.append(text)

    monkeypatch.setattr(server, "send_telegram", fake_send)
    notify = server.ProgressReporter(None, tmp_path, "test-token", "test-chat")
    for count in range(1, 22):
        await notify(report(count), final=False)
        await notify(report(count), final=False)
    await notify({**report(21), "status": "finished"}, final=True)
    assert len(messages) == 3
    assert "10/21" in messages[0]
    assert "20/21" in messages[1]
    assert "21/21" in messages[2]
    assert "итог" in messages[2]
    saved = [json.loads(line) for line in (tmp_path / "progress.jsonl").read_text().splitlines()]
    assert [item["processed"] for item in saved] == [10, 20, 21]


@pytest.mark.asyncio
async def test_notification_failure_preserves_results(monkeypatch, tmp_path):
    async def unavailable(*args):
        raise RuntimeError("unavailable")

    monkeypatch.setattr(server, "send_telegram", unavailable)
    await server.ProgressReporter(None, tmp_path, "test-token", "test-chat")(report(10), final=False)
    assert json.loads((tmp_path / "progress.jsonl").read_text())["processed"] == 10


def test_partial_links_and_unattempted_documents_are_not_successes():
    sample = {
        "pdf_count": 1,
        "jobs": [
            {"status": "partial", "documents": [{"status": "downloaded"}, {"status": "pending"}]},
            {"status": "failed", "documents": []},
            {"status": "pending", "documents": []},
        ],
    }
    stats = server.totals(sample)
    assert stats["processed"] == 2
    assert stats["completed"] == 0
    assert stats["downloaded"] == stats["not_downloaded"] == 1


def test_empty_results_do_not_count_as_downloads():
    stats = server.totals({"jobs": [{"status": "completed", "documents": []}]})
    assert stats["processed"] == 1
    assert stats["completed"] == stats["downloaded"] == 0
    assert stats["empty"] == 1


@pytest.mark.asyncio
async def test_webshare_configuration_never_imports_brightdata_ca(monkeypatch, tmp_path):
    from types import SimpleNamespace

    assigned = object()
    monkeypatch.setenv("PEP_PROXY_PROVIDER", "webshare")
    monkeypatch.setenv("WEBSHARE_API_TOKEN", "test-api-key")
    monkeypatch.setenv("BRIGHTDATA_PROXY_USERNAME", "unused-zone")
    monkeypatch.setenv("BRIGHTDATA_PROXY_PASSWORD", "unused-password")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "")

    async def pool(token, workers):
        assert token == "test-api-key" and workers == 20
        return assigned

    async def run(sample, output, **kwargs):
        assert kwargs["proxy"] is assigned
        assert kwargs["proxy_ca"] is None
        assert kwargs["workers"] == 20 and kwargs["tabs"] == 3
        return {"status": "finished"}

    monkeypatch.setattr(server, "webshare_pool", pool)
    monkeypatch.setattr(server, "run_parallel", run)
    args = SimpleNamespace(
        output=tmp_path,
        workers=20,
        tabs=3,
        timeout=60,
        max_minutes=120,
        ca_bundle=tmp_path / "ca",
        proxy_ca=tmp_path / "brd",
        log_interval=30,
    )
    assert await server.execute(args, [{}] * 100) == {"status": "finished"}
