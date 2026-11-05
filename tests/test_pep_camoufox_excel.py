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
    assert sample == [
        {
            "n": 1,
            "source_row": 4,
            "url": BASE + "2",
            "source_records": [{"excel_row": 4, "data": {server.LINK_COLUMN: BASE + "2"}}],
        }
    ]
    full = server.collect_sample(path, limit=1, offset=0)
    assert [record["excel_row"] for record in full[0]["source_records"]] == [2, 3]
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
async def test_pause_and_resume_notifications_bypass_thirty_minute_interval(monkeypatch, tmp_path):
    messages = []

    async def send(session, token, chat_id, text):
        messages.append(text)

    monkeypatch.setattr(server, "send_telegram", send)
    reporter = server.ProgressReporter(None, tmp_path, "test-token", "test-chat", interval_seconds=1800)
    paused = {
        **report(0),
        "seconds": 10,
        "event": "site_paused",
        "site_pause": {"state": "paused", "remaining_seconds": 1200, "affected_workers": 13},
    }
    await reporter(paused, final=False)
    await reporter({**paused, "event": "site_resumed", "site_pause": {"state": "running"}}, final=False)
    assert len(messages) == 2
    assert "пауза сайта" in messages[0] and "20.0 мин" in messages[0]
    assert "продолжение" in messages[1]


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
        tab_restarts=3,
        timeout=60,
        max_minutes=120,
        ca_bundle=tmp_path / "ca",
        proxy_ca=tmp_path / "brd",
        log_interval=30,
    )
    assert await server.execute(args, [{"url": BASE + str(i)} for i in range(100)]) == {"status": "finished"}


@pytest.mark.asyncio
async def test_time_notifications_ignore_link_thresholds_and_include_idle_and_final(monkeypatch, tmp_path):
    messages = []

    async def send(*args):
        messages.append(args[-1])

    monkeypatch.setattr(server, "send_telegram", send)
    notify = server.ProgressReporter(None, tmp_path, "token", "chat", interval_seconds=1800)
    for seconds, processed in [(10, 10), (100, 20), (1799, 20), (1800, 20), (1810, 20), (3600, 20)]:
        await notify({**report(processed), "seconds": seconds}, final=False)
    await notify({**report(21), "seconds": 3610, "status": "finished"}, final=True)
    assert len(messages) == 3
    rows = [json.loads(line) for line in (tmp_path / "progress.jsonl").read_text().splitlines()]
    assert [row["elapsed_seconds"] for row in rows] == [1800, 3600, 3610]
    assert [row["final"] for row in rows] == [False, False, True]


@pytest.mark.asyncio
async def test_start_notice_is_immediate_and_reports_unavailable_separately(monkeypatch, tmp_path):
    messages = []

    async def send(*args):
        messages.append(args[-1])

    monkeypatch.setattr(server, "send_telegram", send)
    notify = server.ProgressReporter(None, tmp_path, "token", "chat", interval_seconds=1800)
    state = {
        **report(0),
        "event": "download_started",
        "seconds": 150,
        "ready_browsers": 25,
        "worker_count": 25,
        "tab_count": 75,
    }
    await notify(state, final=False)
    assert len(messages) == 1 and "старт" in messages[0] and "25 / 25" in messages[0]
    await notify({**report(10), "seconds": 160}, final=False)
    assert len(messages) == 1
    stats = server.totals(
        {
            "jobs": [
                {
                    "status": "completed_with_unavailable",
                    "documents": [{"status": "downloaded"}, {"status": "unavailable_http_404"}],
                }
            ]
        }
    )
    assert stats["processed"] == 1 and stats["completed"] == 0
    assert stats["unavailable"] == 1 and stats["not_downloaded"] == 0


def test_existing_legacy_404_report_is_saved_without_starting_browsers(monkeypatch, tmp_path):
    path = tmp_path / "report.json"
    path.write_text(
        json.dumps(
            {
                "status": "finished",
                "jobs": [
                    {
                        "n": 1,
                        "url": BASE + "1",
                        "status": "partial",
                        "documents": [
                            {
                                "id": "legacy-test",
                                "status": "http_404",
                                "publication_type": "OTRO",
                                "download_url": "https://www.funcionpublica.gov.co/ley-transparencia-web/declaracionServlet?action=download",
                            }
                        ],
                    }
                ],
            }
        )
    )
    monkeypatch.setattr("sys.argv", ["pep", "unused.xlsx", "--retry-report", str(path)])
    assert server.main() == 0
    saved = json.loads(path.read_text())
    assert saved["jobs"][0]["status"] == "completed_with_unavailable"
    unavailable = json.loads((tmp_path / "unavailable-documents.json").read_text())
    assert len(unavailable) == 1 and unavailable[0]["publication_type"] == "OTRO"
    assert list((tmp_path / "retries").glob("before-classify-*.json"))
