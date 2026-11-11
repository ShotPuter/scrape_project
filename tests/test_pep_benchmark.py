import asyncio
import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from openpyxl import Workbook

from src.sites.pep import benchmark
from src.sites.pep.browser_download import proxy_settings
from src.sites.pep.documents import save_payload


def make_args(tmp_path):
    path = tmp_path / "input.xlsx"
    workbook = Workbook()
    sheet = workbook.create_sheet("data")
    sheet.append(["ENLACE_CONSULTA_LEY_2013"])
    for number in (1, 2, 2):
        sheet.append([f"https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?row={number}"])
    workbook.save(path)
    return SimpleNamespace(workbook=path, output=tmp_path / "output", count=2, workers=2,
                           delay=0, timeout=5, notify_interval=30, max_minutes=1)


@pytest.mark.asyncio
async def test_benchmark_download_resume_and_nonfatal_telegram(tmp_path, monkeypatch) -> None:
    args = make_args(tmp_path)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "secret-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "test-chat")
    monkeypatch.setattr(benchmark, "send_telegram", AsyncMock(side_effect=RuntimeError("unavailable")))
    monkeypatch.setattr(benchmark, "load_webshare_candidates", AsyncMock(return_value=[
        SimpleNamespace(url="http://user:secret@127.0.0.1:8000"),
        SimpleNamespace(url="http://user:secret@127.0.0.2:8000"),
    ]))
    monkeypatch.setattr(benchmark.DiscoveryClient, "preflight", AsyncMock())

    async def discover(self, url):
        return "discovered", [{"id": url[-1], "source_page": url, "status": "discovered"}]

    async def download(self, doc):
        return {**doc, **save_payload(b"%PDF-1.4\nexample\n%%EOF", args.output, doc["id"])}

    monkeypatch.setattr(benchmark.DiscoveryClient, "discover", discover)
    monkeypatch.setattr(benchmark.DocumentDownloader, "download", download)
    assert await benchmark.run(args) == 0
    report = json.loads((args.output / "benchmark.json").read_text())
    assert report["completed_links"] == 2
    assert report["new_downloads_this_run"] == 2
    assert report["unique_documents_downloaded"] == 2
    assert report["unresolved_documents"] == 0
    assert "secret" not in (args.output / "benchmark.json").read_text()
    assert len(json.loads((args.output / "records.json").read_text())) == 3
    events = [json.loads(line) for line in (args.output / "events.jsonl").read_text().splitlines()]
    finished = [event for event in events if event["event"] == "document_finished"]
    assert len(finished) == 2
    assert all(event["status"] == "downloaded" and event["seconds"] >= 0 for event in finished)
    assert all("link_ref" in event and "document_ref" in event for event in finished)
    assert "secret" not in (args.output / "events.jsonl").read_text()
    assert "?row=" not in (args.output / "events.jsonl").read_text()
    assert report["completed_links_percent"] == 100
    assert await benchmark.run(args) == 0
    report = json.loads((args.output / "benchmark.json").read_text())
    assert report["new_downloads_this_run"] == 0
    assert report["reused_documents_this_run"] == 2
    events = [json.loads(line) for line in (args.output / "events.jsonl").read_text().splitlines()]
    assert len({event["run_id"] for event in events}) == 2


@pytest.mark.asyncio
async def test_provider_failure_leaves_sample_pending_and_reports_failure(tmp_path, monkeypatch) -> None:
    args = make_args(tmp_path)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setenv("WEBSHARE_API_TOKEN", "")
    monkeypatch.setattr(benchmark, "load_webshare_candidates", AsyncMock(side_effect=ValueError("provider failed")))
    assert await benchmark.run(args) == 1
    report = json.loads((args.output / "benchmark.json").read_text())
    assert report["status"] == "FAILED"
    assert report["jobs"] == {"pending": 2}
    assert report["unique_documents_downloaded"] == 0


@pytest.mark.asyncio
async def test_benchmark_loads_dotenv_before_reading_webshare_token(tmp_path, monkeypatch) -> None:
    args = make_args(tmp_path)
    args.network = "webshare"
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.delenv("WEBSHARE_API_TOKEN", raising=False)

    def load_env():
        monkeypatch.setenv("WEBSHARE_API_TOKEN", "loaded-from-dotenv")

    monkeypatch.setattr(benchmark, "load_dotenv", load_env)
    inventory = AsyncMock(return_value=[
        SimpleNamespace(url="http://user:secret@192.0.2.1:8000"),
        SimpleNamespace(url="http://user:secret@192.0.2.2:8000"),
    ])
    monkeypatch.setattr(benchmark, "load_webshare_candidates", inventory)
    monkeypatch.setattr(benchmark.DiscoveryClient, "preflight", AsyncMock())
    monkeypatch.setattr(benchmark.DiscoveryClient, "discover", AsyncMock(return_value=("discovered", [])))

    assert await benchmark.run(args) == 0
    inventory.assert_awaited_once_with("loaded-from-dotenv", mode="direct", max_records=100)


@pytest.mark.asyncio
async def test_direct_mode_does_not_call_webshare(tmp_path, monkeypatch) -> None:
    args = make_args(tmp_path)
    args.network = "direct"
    args.headed = True
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    inventory = AsyncMock(side_effect=AssertionError("Direct mode must not call Webshare"))
    monkeypatch.setattr(benchmark, "load_webshare_candidates", inventory)
    monkeypatch.setattr(benchmark.DiscoveryClient, "preflight", AsyncMock())
    monkeypatch.setattr(benchmark.DiscoveryClient, "discover", AsyncMock(return_value=("discovered", [])))
    assert await benchmark.run(args) == 0
    inventory.assert_not_awaited()
    report = json.loads((args.output / "benchmark.json").read_text())
    assert report["network"] == "direct"
    assert report["workers_available"] == 1
    assert report["completed_links"] == 2


def test_proxy_credentials_are_not_part_of_browser_server_argument() -> None:
    result = proxy_settings("http://user:p%40ss@127.0.0.1:8000")
    assert result == {"server": "http://127.0.0.1:8000", "username": "user", "password": "p@ss"}


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_route", [False, True])
@pytest.mark.parametrize("refusal", [False, True])
async def test_nodriver_rotation_is_sequential_bounded_and_verified(tmp_path, monkeypatch, bad_route, refusal):
    from src.sites.pep.nodriver_download import NodriverDownloader

    args = make_args(tmp_path)
    args.network, args.browser_engine, args.headed = "webshare", "nodriver", True
    args.workers, args.proxy_sessions, args.session_links = 1, 2, 1
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setenv("WEBSHARE_API_TOKEN", "")
    inventory = AsyncMock(return_value=[SimpleNamespace(url=f"http://user:secret@127.0.0.{i}:8000") for i in (1, 2, 3)])
    monkeypatch.setattr(benchmark, "load_webshare_candidates", inventory)
    order = []

    async def verify(browser, address):
        order.append("verify")
        assert address == "192.0.2.1"
        if bad_route:
            raise ValueError("secret upstream credentials")

    async def close(browser):
        order.append("close")

    original_init = benchmark.DiscoveryClient.__init__

    def initialize(client, **kwargs):
        original_init(client, **kwargs)
        # The global pacing-only client does not make requests.
        client.session.get = AsyncMock(return_value=SimpleNamespace(
            status_code=200, json=lambda: {"ip": "192.0.2.1"}))

    async def preflight(client, output):
        order.append("source")

    monkeypatch.setattr(benchmark.DiscoveryClient, "__init__", initialize)
    monkeypatch.setattr(benchmark.DiscoveryClient, "preflight", preflight)
    async def discover_page(url):
        docs = [{"id": url[-1], "source_page": url, "status": "discovered"}] if refusal else []
        return "discovered", docs

    discover = AsyncMock(side_effect=discover_page)
    monkeypatch.setattr(benchmark.DiscoveryClient, "discover", discover)
    monkeypatch.setattr(NodriverDownloader, "verify_egress", verify)
    monkeypatch.setattr(NodriverDownloader, "close", close)
    if refusal:
        async def download_http(client, doc):
            return {**doc, "status": "captcha_required"}

        async def download_browser(browser, doc):
            browser.failures = 3
            return {**doc, "status": "browser_rejected_http_302", "rejection_reason": "site_verification_notice"}

        monkeypatch.setattr(benchmark.DocumentDownloader, "download", download_http)
        monkeypatch.setattr(NodriverDownloader, "download", download_browser)
    assert await benchmark.run(args) == int(bad_route or refusal)
    inventory.assert_awaited_once_with("", mode="direct", max_records=2)
    report = json.loads((args.output / "benchmark.json").read_text())
    expected_sessions = 1 if refusal and not bad_route else 2
    assert len(report["proxy_sessions"]) == expected_sessions
    if bad_route:
        expected_order = ["verify", "close"] * expected_sessions
    else:
        expected_order = ["verify", "source", "close"] * expected_sessions
    assert order == expected_order
    assert discover.await_count == (0 if bad_route else (1 if refusal else 2))
    assert all(s["jobs"] <= 1 for s in report["proxy_sessions"])
    if refusal and not bad_route:
        assert report["jobs"] == {"partial": 1, "pending": 1}
        with sqlite3.connect(args.output / "state.sqlite3") as db:
            maximum = db.execute("SELECT max(n) FROM (SELECT count(*) n FROM benchmark_attempts GROUP BY link)").fetchone()
            assert maximum == (1,)
    assert "secret" not in (args.output / "benchmark.json").read_text()


@pytest.mark.asyncio
async def test_nodriver_parallel_workers_use_distinct_verified_proxies(tmp_path, monkeypatch):
    from src.sites.pep.nodriver_download import NodriverDownloader

    args = make_args(tmp_path)
    args.network, args.browser_engine, args.headed = "webshare", "nodriver", True
    args.workers, args.proxy_sessions, args.session_links = 3, 0, 2
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setenv("WEBSHARE_API_TOKEN", "")
    monkeypatch.setattr(benchmark, "load_webshare_candidates", AsyncMock(return_value=[
        SimpleNamespace(url=f"http://user:secret@192.0.2.{i}:8000") for i in (1, 2, 3)
    ]))

    original_init = benchmark.DiscoveryClient.__init__
    clients = []

    def initialize(client, **kwargs):
        original_init(client, **kwargs)
        if kwargs.get("proxy"):
            clients.append(client)
            egress = "198.51.100." + kwargs["proxy"].split("@", 1)[1].split(":", 1)[0].rsplit(".", 1)[1]
            client.session.get = AsyncMock(return_value=SimpleNamespace(
                status_code=200, json=lambda: {"ip": egress}
            ))

    monkeypatch.setattr(benchmark.DiscoveryClient, "__init__", initialize)
    monkeypatch.setattr(benchmark.DiscoveryClient, "preflight", AsyncMock())
    monkeypatch.setattr(benchmark.DiscoveryClient, "discover", AsyncMock(return_value=("discovered", [])))
    verified = []

    async def verify(browser, address):
        verified.append((browser.proxy["server"], address))

    monkeypatch.setattr(NodriverDownloader, "verify_egress", verify)
    monkeypatch.setattr(NodriverDownloader, "close", AsyncMock())

    assert await benchmark.run(args) == 0
    report = json.loads((args.output / "benchmark.json").read_text())
    assert report["workers_available"] == 3
    assert len(report["proxy_sessions"]) == 3
    assert {address for _, address in verified} == {f"198.51.100.{i}" for i in (1, 2, 3)}
    assert len({proxy for proxy, _ in verified}) == 3
    assert all(item["routing"] == "verified" for item in report["proxy_sessions"])
    assert "secret" not in (args.output / "benchmark.json").read_text()


@pytest.mark.asyncio
async def test_undetected_worker_verifies_route_and_keeps_explicit_ca_bundle(tmp_path, monkeypatch):
    from src.sites.pep.undetected_download import UndetectedDownloader

    args = make_args(tmp_path)
    args.network, args.browser_engine, args.headed, args.workers = "webshare", "undetected", True, 1
    args.ca_bundle = tmp_path / "custom-ca.pem"
    args.page_settle_min, args.page_settle_max, args.browser_diagnostics = 4, 8, True
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setattr(benchmark, "load_webshare_candidates", AsyncMock(return_value=[
        SimpleNamespace(url="http://user:secret@192.0.2.1:8000"),
    ]))
    original_init = benchmark.DiscoveryClient.__init__

    def initialize(client, **kwargs):
        original_init(client, **kwargs)
        client.session.get = AsyncMock(return_value=SimpleNamespace(
            status_code=200, json=lambda: {"ip": "198.51.100.1"}))

    seen = []

    async def verify(browser, address):
        assert browser.ca_bundle == args.ca_bundle.resolve()
        assert (browser.page_settle_min, browser.page_settle_max) == (4, 8)
        seen.append(address)

    monkeypatch.setattr(benchmark.DiscoveryClient, "__init__", initialize)
    monkeypatch.setattr(benchmark.DiscoveryClient, "preflight", AsyncMock())
    monkeypatch.setattr(benchmark.DiscoveryClient, "discover", AsyncMock(return_value=("discovered", [])))
    monkeypatch.setattr(UndetectedDownloader, "verify_egress", verify)
    monkeypatch.setattr(UndetectedDownloader, "close", AsyncMock())
    monkeypatch.setattr(UndetectedDownloader, "save_environment", AsyncMock(return_value=tmp_path / "environment.json"))
    assert await benchmark.run(args) == 0
    assert seen == ["198.51.100.1"]
    report = json.loads((args.output / "benchmark.json").read_text())
    assert report["browser_adapter_revision"] == UndetectedDownloader.ADAPTER_REVISION
    assert report["proxy_sessions"][0]["routing"] == "verified"
    assert report["browser_experiment"]["page_settle_min"] == 4
    events = [json.loads(line) for line in (args.output / "events.jsonl").read_text().splitlines()]
    assert next(event for event in events if event["event"] == "browser_environment_saved")["diagnostics_file"] == (
        "environment.json"
    )
    args.page_settle_max = 9
    with pytest.raises(ValueError, match="new --output"):
        await benchmark.run(args)


@pytest.mark.asyncio
@pytest.mark.parametrize(("headed", "workers", "rotating"), [(False, 1, 0), (True, 3, 0), (True, 1, 2)])
async def test_undetected_rejects_unsupported_launch_modes(tmp_path, headed, workers, rotating):
    args = make_args(tmp_path)
    args.browser_engine, args.headed, args.workers, args.proxy_sessions = "undetected", headed, workers, rotating
    with pytest.raises(ValueError, match="undetected"):
        await benchmark.run(args)


@pytest.mark.asyncio
@pytest.mark.parametrize(("engine", "low", "high", "scroll"), [
    ("native", 4, 8, False), ("undetected", -1, 8, False), ("undetected", 8, 4, False),
    ("undetected", 0, float("nan"), False), ("undetected", 0, 16, False), ("undetected", 0, 0, True),
])
async def test_invalid_browser_experiment_rejected_before_network(tmp_path, monkeypatch, engine, low, high, scroll):
    args = make_args(tmp_path)
    args.browser_engine, args.headed, args.workers = engine, True, 1
    args.page_settle_min, args.page_settle_max, args.page_settle_scroll = low, high, scroll
    preflight = AsyncMock()
    monkeypatch.setattr(benchmark.DiscoveryClient, "preflight", preflight)
    with pytest.raises(ValueError, match="page-settle"):
        await benchmark.run(args)
    preflight.assert_not_awaited()


@pytest.mark.asyncio
async def test_shared_profiles_reused_across_reports_but_refuse_changed_egress(tmp_path, monkeypatch):
    from src.sites.pep.undetected_download import UndetectedDownloader

    args = make_args(tmp_path)
    args.network, args.browser_engine, args.headed, args.workers = "webshare", "undetected", True, 1
    args.browser_profile_root = tmp_path / "shared-profiles"
    args.browser_language = "es-CO"
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setattr(benchmark, "load_webshare_candidates", AsyncMock(return_value=[
        SimpleNamespace(url="http://user:secret@192.0.2.1:8000"),
    ]))
    original_init = benchmark.DiscoveryClient.__init__
    egress = {"ip": "198.51.100.1"}

    def initialize(client, **kwargs):
        original_init(client, **kwargs)
        expected = "en-US,en;q=0.9" if args.browser_language == "en-US" else "es-CO,es;q=0.9"
        assert kwargs.get("accept_language") == expected
        client.session.get = AsyncMock(return_value=SimpleNamespace(status_code=200, json=lambda: dict(egress)))

    profiles = []

    async def verify(browser, address):
        profiles.append(browser.profile)

    discover = AsyncMock(return_value=("discovered", []))
    monkeypatch.setattr(benchmark.DiscoveryClient, "__init__", initialize)
    monkeypatch.setattr(benchmark.DiscoveryClient, "preflight", AsyncMock())
    monkeypatch.setattr(benchmark.DiscoveryClient, "discover", discover)
    monkeypatch.setattr(UndetectedDownloader, "verify_egress", verify)
    monkeypatch.setattr(UndetectedDownloader, "close", AsyncMock())
    assert await benchmark.run(args) == 0
    args.output = tmp_path / "second-report"
    assert await benchmark.run(args) == 0
    assert profiles[0] == profiles[1]
    assert profiles[0].is_relative_to(args.browser_profile_root)
    raw = (profiles[0] / "pep-session-binding.json").read_text()
    assert "secret" not in raw and "198.51.100.1" not in raw

    egress["ip"] = "198.51.100.2"
    args.output = tmp_path / "changed-egress"
    calls = discover.await_count
    assert await benchmark.run(args) == 1
    assert discover.await_count == calls
    report = json.loads((args.output / "benchmark.json").read_text())
    assert "egress IP changed" in report["failure_details"][0]["message"]
    args.output = tmp_path / "changed-locale"
    args.browser_language = "en-US"
    # The profile configuration is checked before launch or egress verification.
    assert await benchmark.run(args) == 1
    assert len(profiles) == 3
    report = json.loads((args.output / "benchmark.json").read_text())
    assert "different renderer/locale" in report["failure_details"][0]["message"]


@pytest.mark.asyncio
async def test_shared_profile_root_locked_across_outputs(tmp_path, monkeypatch):
    import fcntl

    args = make_args(tmp_path)
    args.browser_profile_root = tmp_path / "shared-profiles"
    args.browser_profile_root.mkdir()
    inventory = AsyncMock()
    monkeypatch.setattr(benchmark, "load_webshare_candidates", inventory)
    with (args.browser_profile_root / "benchmark-profiles.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="profile root"):
            await benchmark.run(args)
    inventory.assert_not_awaited()


@pytest.mark.asyncio
async def test_time_limit_checkpoints_without_leaving_processing_jobs(tmp_path, monkeypatch) -> None:
    args = make_args(tmp_path)
    args.max_minutes = 0.001
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setattr(benchmark, "load_webshare_candidates", AsyncMock(return_value=[
        SimpleNamespace(url="http://user:secret@127.0.0.1:8000"),
    ]))
    monkeypatch.setattr(benchmark.DiscoveryClient, "preflight", AsyncMock())

    async def discover(self, url):
        await asyncio.sleep(60)

    monkeypatch.setattr(benchmark.DiscoveryClient, "discover", discover)
    assert await benchmark.run(args) == 1
    report = json.loads((args.output / "benchmark.json").read_text())
    assert report["status"] == "TIME_LIMIT"
    assert report["jobs"].get("processing", 0) == 0
    assert report["unique_documents_downloaded"] == 0
    assert (args.output / "records.json").exists()


@pytest.mark.asyncio
async def test_verification_refusal_logs_reason_and_leaves_unattempted_links_pending(tmp_path, monkeypatch):
    args = make_args(tmp_path)
    args.network = "direct"
    args.headed = True
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setattr(benchmark.DiscoveryClient, "preflight", AsyncMock())
    monkeypatch.setattr(benchmark.DiscoveryClient, "discover", AsyncMock(return_value=("discovered", [
        {"id": "123", "source_page": "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?private=person",
         "status": "discovered"},
    ])))
    monkeypatch.setattr(benchmark.DocumentDownloader, "download", AsyncMock(return_value={
        "id": "123", "status": "captcha_required",
    }))

    async def rejected(browser, document):
        browser.failures = 3
        return {**document, "status": "browser_rejected_http_302", "failure_stage": "download_response",
                "rejection_reason": "site_verification_notice", "browser_http_status": 302, "captchatoken": "secret"}

    monkeypatch.setattr(benchmark.BrowserDownloader, "download", rejected)
    assert await benchmark.run(args) == 1
    report = json.loads((args.output / "benchmark.json").read_text())
    assert report["status"] == "INCOMPLETE"
    assert report["jobs"] == {"partial": 1, "pending": 1}
    assert report["rejection_reasons"] == {"site_verification_notice": 1}
    assert report["failure_stages"] == {"download_response": 1}
    raw = (args.output / "events.jsonl").read_text()
    assert "private=person" not in raw
    assert "secret" not in raw
    quarantine = next(json.loads(line) for line in raw.splitlines() if json.loads(line)["event"] == "worker_quarantined")
    assert quarantine["reason"] == "site_verification_notice"


def test_event_log_only_persists_allowlisted_history_and_fields(tmp_path):
    report = benchmark.Report(None, tmp_path, 50)
    report.event("document_finished", link="https://example.org/?private=person", document={
        "id": "123", "status": "downloaded", "captchatoken": "private-token", "browser_history": [
            {"status": "browser_rejected_http_302", "cookie": "private-cookie"}, {"status": "downloaded"},
        ],
    }, cookie="extra-cookie")
    raw = (tmp_path / "events.jsonl").read_text()
    assert "private" not in raw
    assert "cookie" not in raw
    event = json.loads(raw)
    assert event["browser_history"] == [{"status": "browser_rejected_http_302"}, {"status": "downloaded"}]


@pytest.mark.asyncio
async def test_error_pass_only_recovers_failed_link_and_preserves_attempt_history(tmp_path, monkeypatch):
    args = make_args(tmp_path)
    args.network = "direct"
    args.headed = True
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setattr(benchmark.DiscoveryClient, "preflight", AsyncMock())
    seen = []

    async def discover(client, url):
        seen.append(url)
        if url.endswith("1") and seen.count(url) == 1:
            return "discovery_failed_network", []
        return "discovered", [{"id": url[-1], "source_page": url, "requires_captcha": False}]

    async def download(client, document):
        return {**document, **save_payload(b"%PDF-1.4\nexample\n%%EOF", args.output, document["id"])}

    monkeypatch.setattr(benchmark.DiscoveryClient, "discover", discover)
    monkeypatch.setattr(benchmark.DocumentDownloader, "download", download)
    assert await benchmark.run(args) == 1
    first = json.loads((args.output / "benchmark.json").read_text())
    assert first["completed_links"] == 1
    assert first["retryable_links"] == 1
    args.pass_kind = "errors"
    assert await benchmark.run(args) == 0
    latest = json.loads((args.output / "benchmark.json").read_text())
    assert latest["completed_links"] == 2
    assert latest["new_downloads_this_run"] == 1
    assert latest["job_attempts_total"] == 3
    assert len(seen) == 3
    assert seen[0] == seen[2]
    with sqlite3.connect(args.output / "state.sqlite3") as db:
        assert dict(db.execute("SELECT status,count(*) FROM benchmark_attempts GROUP BY status")) == {
            "completed": 2, "discovery_failed_network": 1,
        }


@pytest.mark.asyncio
async def test_persisted_attempt_cap_prevents_third_network_pass(tmp_path, monkeypatch):
    args = make_args(tmp_path)
    args.network = "direct"
    args.headed = True
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    preflight = AsyncMock()
    discover = AsyncMock(return_value=("discovery_failed_network", []))
    monkeypatch.setattr(benchmark.DiscoveryClient, "preflight", preflight)
    monkeypatch.setattr(benchmark.DiscoveryClient, "discover", discover)
    assert await benchmark.run(args) == 1
    args.pass_kind = "errors"
    assert await benchmark.run(args) == 1
    network_calls = preflight.await_count
    assert await benchmark.run(args) == 1
    assert discover.await_count == 4  # Two links, maximum two attempts each.
    assert preflight.await_count == network_calls
    report = json.loads((args.output / "benchmark.json").read_text())
    assert report["job_attempts_total"] == 4
    assert report["retryable_links"] == 0
    assert report["stop_reason"] == "no_eligible_jobs"


@pytest.mark.asyncio
async def test_corrupt_completed_file_is_not_success_at_attempt_cap(tmp_path, monkeypatch):
    from src.sites.pep.retry_queue import begin_attempt, finish_attempt

    args = make_args(tmp_path)
    args.network = "direct"
    args.headed = True
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setattr(benchmark.DiscoveryClient, "preflight", AsyncMock())

    async def discover(client, url):
        return "discovered", [{"id": url[-1], "source_page": url, "requires_captcha": False}]

    async def download(client, document):
        return {**document, **save_payload(b"%PDF-1.4\nexample\n%%EOF", args.output, document["id"])}

    monkeypatch.setattr(benchmark.DiscoveryClient, "discover", discover)
    monkeypatch.setattr(benchmark.DocumentDownloader, "download", download)
    assert await benchmark.run(args) == 0
    with sqlite3.connect(args.output / "state.sqlite3") as db:
        link = db.execute("SELECT link FROM jobs ORDER BY rowid LIMIT 1").fetchone()[0]
        begin_attempt(db, link, "second-attempt")
        finish_attempt(db, link, "second-attempt", "completed")
    (args.output / "pdf" / "1.pdf").unlink()
    assert await benchmark.run(args) == 1
    report = json.loads((args.output / "benchmark.json").read_text())
    assert report["completed_links"] == 1
    assert report["document_statuses"]["local_file_invalid"] == 1
    assert report["new_downloads_this_run"] == 0
