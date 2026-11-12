import json
from pathlib import Path

import pytest

from scripts import res_free_pilot as pilot
from src.sites.res import benchmark as bench
from src.utils.atomic import atomic_write_json

CVE = "ACuKDHhqsvAd"
CVE2 = "ACfJoaiSFQ8v"
PDF = b"%PDF-1.4\nfixture\n%%EOF"
COMPANY = {"company_id": "rut:76615169-8", "company_name": "VERTIKAL GROUP SpA", "rut": "76615169-8"}
LINK = f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={CVE}&codigo=valid"


def args_for(tmp_path, *extra):
    return bench.parse_args(["--output", str(tmp_path), "--browsers", "1", "--limit", "1", "--no-telegram", *extra])


def seed_state(root, cves):
    directory = root / "workers/w00"
    directory.mkdir(parents=True, exist_ok=True)
    state = {"cves": {}, "companies": {COMPANY["company_id"]: {"status": "completed", "cves": cves}}}
    pilot.save_state(directory / "state.json", state)
    return directory


def test_resume_requires_every_selected_pdf_and_respects_document_limit(tmp_path):
    args = args_for(tmp_path)
    directory = seed_state(tmp_path, [CVE, CVE2])
    saved = pilot.persist_pdf(directory, CVE, PDF, LINK)
    path = directory / saved["local_path"]
    before = path.stat().st_mtime_ns
    queues, baseline = bench.prepare_queues(args, tmp_path, [COMPANY])
    assert len(queues) == 1 and baseline == []  # one of two documents still missing
    args.max_documents = 1
    queues, baseline = bench.prepare_queues(args, tmp_path, [COMPANY])
    assert queues == [] and baseline[0]["status"] == "downloaded"
    assert path.stat().st_mtime_ns == before


def test_saved_captcha_error_is_retried_only_when_requested(tmp_path):
    directory = tmp_path / "workers/w00"
    directory.mkdir(parents=True)
    atomic_write_json(
        directory / "report-1.json", {"companies": [{**COMPANY, "status": "captcha_required", "documents": []}]}
    )
    args = args_for(tmp_path)
    queues, baseline = bench.prepare_queues(args, tmp_path, [COMPANY])
    assert not queues and baseline[0]["status"] == "captcha_required"
    args.retry_errors = True
    queues, baseline = bench.prepare_queues(args, tmp_path, [COMPANY])
    assert len(queues) == 1 and baseline == []


def test_metrics_distinguish_new_files_from_cached_files_and_old_attempts(tmp_path):
    args = args_for(tmp_path)
    directory = seed_state(tmp_path, [CVE, CVE2])
    old = pilot.persist_pdf(directory, CVE, PDF, LINK)
    fresh = pilot.persist_pdf(directory, CVE2, PDF, LINK.replace(CVE, CVE2))
    atomic_write_json(
        directory / "report-old.json", {"attempts": [{"company_id": COMPANY["company_id"], "result": {"status": "failed"}}]}
    )
    atomic_write_json(
        directory / "report-new.json",
        {
            "attempts": [{"company_id": COMPANY["company_id"], "seconds": 30, "result": {"status": "downloaded"}}],
            "http_metrics": {"http_download_seconds": 4},
            "totals": {"downloaded": 2},
            "tokens": [{"stage": "audio"}],
        },
    )
    invocation = {
        "elapsed_seconds": 120,
        "prior_reports": [str(directory / "report-old.json")],
        "prior_pdfs": [str(Path("workers/w00") / old["local_path"])],
    }
    summary = bench.statistics_report(args, tmp_path, [COMPANY], invocation)
    assert summary["new_pdfs"] == 1
    assert summary["pdf_count"] == 2
    assert summary["new_pdf_bytes"] == fresh["size_bytes"]
    assert summary["new_pdfs_per_minute"] == 0.5
    assert summary["company_attempts"] == 1
    assert summary["company_attempt_seconds_p95"] == 30
    assert summary["http_metrics"] == {"http_download_seconds": 4}
    assert [d["reused"] for d in summary["rows"][0]["documents"]] == [True, False]


async def test_completed_restart_opens_no_browser_or_source_connection(tmp_path, monkeypatch):
    args = args_for(tmp_path)
    root = tmp_path / args.run_id
    directory = seed_state(root, [CVE])
    pilot.persist_pdf(directory, CVE, PDF, LINK)
    config = {key: getattr(args, key) for key in ("year", "month", "limit", "browsers")}
    atomic_write_json(root / "selection.json", {"config": config, "companies": [COMPANY]})

    def forbidden(*a, **kw):
        pytest.fail("A completed restart must not access the source or start subprocesses")

    monkeypatch.setattr(bench, "select_companies", forbidden)
    monkeypatch.setattr(bench.asyncio, "create_subprocess_exec", forbidden)
    assert await bench.execute(args, root) == 0
    summary = json.loads((root / "summary.json").read_text())
    assert summary["scheduled_companies"] == 0
    assert summary["skipped_companies"] == 1
    assert summary["new_pdfs"] == 0 and summary["pdf_count"] == 1


async def test_changed_selection_requires_new_run_id(tmp_path):
    args = args_for(tmp_path)
    root = tmp_path / args.run_id
    root.mkdir()
    atomic_write_json(root / "selection.json", {"config": {"limit": 150}, "companies": []})
    with pytest.raises(ValueError, match="new --run-id"):
        await bench.execute(args, root)


@pytest.mark.parametrize("argv", [["--run-id", "../escape"], ["--browsers", "0"], ["--timeout", "nan"]])
def test_invalid_configuration_is_rejected(argv):
    with pytest.raises(SystemExit):
        bench.parse_args(argv)


async def test_isolated_workers_combine_outputs_and_restart_without_new_work(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    args = args_for(tmp_path, "--browsers", "2", "--limit", "2")
    root = tmp_path / args.run_id
    root.mkdir()
    # These two IDs occupy separate stable worker slots.
    first = {**COMPANY, "company_id": "slot-0"}
    second_id = next(
        f"slot-{i}" for i in range(1, 30) if bench.worker_index(f"slot-{i}", 2) != bench.worker_index(first["company_id"], 2)
    )
    companies = [first, {**COMPANY, "company_id": second_id, "company_name": "Company B"}]
    config = {key: getattr(args, key) for key in ("year", "month", "limit", "browsers")}
    atomic_write_json(root / "selection.json", {"config": config, "companies": companies})
    monkeypatch.setenv("BRIGHTDATA_PROXY_USERNAME", "fake")
    monkeypatch.setenv("BRIGHTDATA_PROXY_PASSWORD", "fake")
    outputs = []

    async def spawn(*command, **kwargs):
        directory = Path(command[command.index("--output") + 1])
        outputs.append(directory)
        company = pilot.load_companies(directory / "queue.sqlite3", 0)[0]
        state = {"companies": {company["company_id"]: {"status": "completed", "cves": [CVE]}}, "cves": {}}
        pilot.save_state(directory / "state.json", state)
        doc = pilot.persist_pdf(directory, CVE, PDF, LINK)
        result = {**company, "status": "downloaded", "documents": [doc]}
        atomic_write_json(
            directory / "report-current.json",
            {
                "companies": [result],
                "attempts": [{"company_id": company["company_id"], "seconds": 5, "result": result}],
                "tokens": [{"stage": "native"}],
                "totals": {"downloaded": 1},
            },
        )
        return SimpleNamespace(pid=1000 + len(outputs), returncode=0, wait=AsyncMock(return_value=0))

    monkeypatch.setattr(bench.asyncio, "create_subprocess_exec", spawn)
    assert await bench.execute(args, root) == 0
    assert len(set(outputs)) == 2
    report = json.loads((root / "summary.json").read_text())
    assert report["new_pdfs"] == 2 and report["companies_with_pdf"] == 2
    snapshots = {p: p.stat().st_mtime_ns for p in root.glob("workers/w*/pdf/**/*.pdf")}
    assert await bench.execute(args, root) == 0
    assert len(outputs) == 2  # no third/fourth process on resume
    resumed = json.loads((root / "summary.json").read_text())
    assert resumed["new_pdfs"] == 0 and resumed["skipped_companies"] == 2
    assert {p: p.stat().st_mtime_ns for p in snapshots} == snapshots


def test_second_controller_does_not_overwrite_active_progress(tmp_path, monkeypatch):
    import fcntl

    import dotenv

    monkeypatch.setattr(dotenv, "load_dotenv", lambda *args: False)
    root = tmp_path / "locked-run"
    root.mkdir()
    progress = root / "progress.json"
    progress.write_text('{"state":"running","new_pdfs":42}')
    before = progress.read_bytes()
    with (root / ".lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert bench.main(["--output", str(tmp_path), "--run-id", "locked-run"]) == 1
    assert progress.read_bytes() == before


def test_telegram_progress_includes_real_speed_and_errors(tmp_path):
    args = args_for(tmp_path)
    text = bench.progress_message(
        args,
        {
            "elapsed_seconds": 120,
            "statuses": {"downloaded": 3, "captcha_required": 1},
            "new_pdfs": 6,
            "attempts": 5,
            "error_attempts": 2,
            "captcha_attempts": 1,
            "ready_browsers": 2,
        },
        150,
        2,
    )
    assert "4/150" in text and "3.00 PDF/мин" in text
    assert "ошибочных: 2" in text and "CAPTCHA: 1" in text


async def test_telegram_delivery_uses_configured_credentials_and_survives_failure(tmp_path, monkeypatch, capsys):
    from unittest.mock import AsyncMock

    args = args_for(tmp_path)
    args.no_telegram = False
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "fake-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "fake-chat")
    sender = AsyncMock(side_effect=RuntimeError("Provider unavailable"))
    monkeypatch.setattr(bench, "send_telegram", sender)
    await bench.notify(args, "Progress fixture")
    assert sender.await_count == 1
    assert sender.call_args.args[1:] == ("fake-token", "fake-chat", "Progress fixture")
    output = capsys.readouterr().out
    assert "collection continues" in output and "fake-token" not in output


async def test_telegram_disabled_without_both_credentials(tmp_path, monkeypatch):
    args = args_for(tmp_path)
    args.no_telegram = False
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "fake-token")
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    monkeypatch.setattr(bench.aiohttp, "ClientSession", lambda **kwargs: pytest.fail("No Telegram request expected"))
    await bench.notify(args, "Fixture")


def test_setup_failure_sends_sanitized_telegram_notification(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock

    import dotenv

    monkeypatch.setattr(dotenv, "load_dotenv", lambda *args: False)
    monkeypatch.setattr(
        bench, "execute", AsyncMock(side_effect=ValueError("failed http://user:private-password@proxy.invalid/"))
    )
    sender = AsyncMock()
    monkeypatch.setattr(bench, "notify", sender)
    assert bench.main(["--output", str(tmp_path), "--run-id", "failure"]) == 1
    text = sender.call_args.args[1]
    assert "⛔" in text and "private-password" not in text
    assert json.loads((tmp_path / "failure/progress.json").read_text())["state"] == "failed"
