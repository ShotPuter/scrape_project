"""No live proxies/browsers: durable leases, streaming dispatch and the five-slot cap."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from scripts import res_free_pilot as pilot
from src.proxy.reputation import ReputationDenied
from src.sites.res import free_stream as stream
from src.sites.res.free_queue import FreeQueue
from src.sites.res.tracking import TrackingStore


def company(index):
    return {"company_id": f"company-{index:03d}", "company_name": f"Example {index}", "rut": f"760000{index:02d}-0"}


def ready(queue, index, ttl=600):
    endpoint = f"http://203.0.113.{index}:80"
    queue.add_candidates([(endpoint, "fixture")], capacity=100)
    assert queue.finish_probe(endpoint, exit_ip=f"198.51.100.{index}", ttl=ttl)
    return endpoint


@pytest.fixture
def queue(tmp_path):
    result = FreeQueue(tmp_path / "pool.sqlite3", "test")
    yield result
    result.close()


def test_jobs_and_proxy_leases_are_exclusive_and_durable(queue, tmp_path):
    queue.add_jobs([company(index) for index in range(4)])
    ready(queue, 1)
    ready(queue, 2)
    first = queue.claim_batch("first", 2)
    second = queue.claim_batch("second", 2)
    assert first and second
    assert first[0]["endpoint"] != second[0]["endpoint"]
    assert {row["company_id"] for row in first[1]}.isdisjoint(row["company_id"] for row in second[1])
    assert queue.claim_batch("third", 1) is None
    for row in first[1]:
        queue.finish_job(row["company_id"], "first", complete=True, attempted=True, limit=3, retry=0)
    queue.recover()
    assert queue.counts()["jobs"] == {"done": 2, "pending": 2}
    reopened = FreeQueue(tmp_path / "pool.sqlite3", "test")
    try:
        assert reopened.counts()["jobs"] == {"done": 2, "pending": 2}
    finally:
        reopened.close()


def test_proxy_failure_without_company_attempt_does_not_exhaust_retry_budget(queue):
    queue.add_jobs([company(1)])
    ready(queue, 1)
    proxy, jobs = queue.claim_batch("owner", 10)
    queue.finish_job(jobs[0]["company_id"], "owner", complete=False, attempted=False, limit=1, retry=0)
    queue.release_proxy(proxy["endpoint"], "owner", 0, "preflight failed")
    assert queue.counts()["jobs"] == {"pending": 1}
    assert queue.database.execute("SELECT attempts FROM free_jobs").fetchone()[0] == 0


def test_duplicate_exit_and_expired_ready_cannot_stall_pool(queue, monkeypatch):
    import time

    queue.add_jobs([company(1)])
    ready(queue, 1, ttl=10)
    queue.add_candidates([("http://203.0.113.2:80", "fixture")], capacity=100)
    assert not queue.finish_probe("http://203.0.113.2:80", exit_ip="198.51.100.1")
    now = time.time()
    monkeypatch.setattr("src.sites.res.free_queue.time.time", lambda: now + 11)
    assert queue.claim_batch("worker", 1) is None
    assert queue.counts()["proxies"].get("ready", 0) == 0
    assert queue.claim_probe(fresh_for=3600) is not None


def test_attempt_budget_and_explicit_error_retry(queue):
    queue.add_jobs([company(1)])
    endpoint = ready(queue, 1)
    for attempt in range(2):
        if attempt:
            assert queue.finish_probe(endpoint, exit_ip="198.51.100.1")
        proxy, jobs = queue.claim_batch("owner", 1)
        queue.finish_job(jobs[0]["company_id"], "wrong-owner", complete=True, attempted=False, limit=2, retry=0)
        queue.finish_job(jobs[0]["company_id"], "owner", complete=False, attempted=True, limit=2, retry=0)
        queue.release_proxy(endpoint, "owner", 0)
    assert queue.counts()["jobs"] == {"error": 1}
    queue.add_jobs([company(1)])
    assert queue.counts()["jobs"] == {"error": 1}
    queue.add_jobs([company(1)], retry_errors=True)
    assert queue.counts()["jobs"] == {"pending": 1}


async def test_proxies_start_workers_immediately_and_never_exceed_five(queue):
    args = stream.parse_args(["--browsers", "5", "--batch-size", "1", "--retry-seconds", "0"])
    queue.add_jobs([company(index) for index in range(10)])
    changed, stop = asyncio.Event(), asyncio.Event()
    events = asyncio.Queue()
    gates = {}
    active = 0
    peak = 0
    store = SimpleNamespace(cooldown_state=lambda: {"active": False, "halted": False})

    async def worker(slot, proxy, companies, owner):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        gate = asyncio.Event()
        gates[owner] = gate
        await events.put((slot, owner))
        try:
            await gate.wait()
            return stream.BatchResult(completed={companies[0]["company_id"]}, attempted={companies[0]["company_id"]})
        finally:
            active -= 1

    dispatcher = stream.Dispatcher(args, queue, worker, store, changed, stop)
    tasks = [asyncio.create_task(dispatcher.consume(slot)) for slot in range(5)]
    try:
        ready(queue, 1)
        changed.set()
        first = await asyncio.wait_for(events.get(), 1)
        assert active == 1  # Does not wait for all sources, verification batch, or five proxies.
        for index in range(2, 8):
            ready(queue, index)
        changed.set()
        for _ in range(4):
            await asyncio.wait_for(events.get(), 1)
        assert peak == 5 and active == 5
        gates[first[1]].set()
        await asyncio.wait_for(events.get(), 1)  # A queued proxy fills the released slot.
        assert peak == 5 and active == 5
    finally:
        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert not dispatcher.active and active == 0
    assert queue.counts()["jobs"].get("leased", 0) == 0


async def test_failed_worker_returns_company_for_next_proxy(queue):
    args = stream.parse_args(["--browsers", "1", "--batch-size", "1", "--retry-seconds", "0"])
    queue.add_jobs([company(1)])
    ready(queue, 1)
    ready(queue, 2)
    changed, stop = asyncio.Event(), asyncio.Event()
    used = []
    store = SimpleNamespace(cooldown_state=lambda: {"active": False, "halted": False})

    async def worker(slot, proxy, companies, owner):
        used.append(proxy["endpoint"])
        identifier = companies[0]["company_id"]
        if len(used) == 1:
            return stream.BatchResult(attempted={identifier}, error="proxy lost")
        stop.set()
        return stream.BatchResult(completed={identifier}, attempted={identifier})

    dispatcher = stream.Dispatcher(args, queue, worker, store, changed, stop)
    await asyncio.wait_for(dispatcher.consume(0), 1)
    assert len(used) == 2 and used[0] != used[1]
    assert queue.counts()["jobs"] == {"done": 1}


async def test_shared_site_limit_stops_dispatch_without_rotating(queue):
    args = stream.parse_args(["--browsers", "1", "--batch-size", "1"])
    queue.add_jobs([company(1)])
    ready(queue, 1)
    ready(queue, 2)
    control = {"active": False, "halted": False}
    changed, stop = asyncio.Event(), asyncio.Event()
    calls = []
    store = SimpleNamespace(cooldown_state=lambda: control)

    async def worker(*args):
        calls.append(1)
        control["halted"] = True
        return stream.BatchResult(error="daily limit")

    await stream.Dispatcher(args, queue, worker, store, changed, stop).consume(0)
    assert len(calls) == 1 and stop.is_set()
    assert queue.counts()["jobs"] == {"pending": 1}


@pytest.mark.parametrize(
    "endpoint,allowed",
    [
        ("http://1.1.1.1:80", True),
        ("socks5://1.1.1.1:443", True),
        ("http://1.1.1.1:3128", False),
        ("http://8.213.1.1:80", False),
        ("http://127.0.0.1:80", False),
        ("http://user:secret@1.1.1.1:80", False),
        ("socks4://1.1.1.1:80", False),
        ("http://proxy.example:80", False),
    ],
)
def test_admission_matches_firewall_and_rejects_untrusted_endpoint_types(endpoint, allowed):
    assert stream.endpoint_allowed(endpoint, set()) is allowed
    assert stream.endpoint_allowed("http://1.1.1.1:3128", {("1.1.1.1", 3128)})


async def test_denied_proxy_is_never_contacted(tmp_path, queue, monkeypatch):
    args = stream.parse_args(["--no-telegram"])
    producer = stream.ProxyProducer(args, tmp_path, queue, asyncio.Event(), asyncio.Event())
    monkeypatch.setattr(stream, "screen_proxy", AsyncMock(side_effect=ReputationDenied("listed")))
    client = Mock(side_effect=AssertionError("No candidate connections"))
    monkeypatch.setattr(stream.httpx, "AsyncClient", client)
    with pytest.raises(ReputationDenied):
        await producer.verify("http://1.1.1.1:80")
    client.assert_not_called()


async def test_worker_command_has_one_browser_one_proxy_and_expected_exit(tmp_path, monkeypatch):
    args = stream.parse_args(["--output", str(tmp_path), "--no-telegram"])
    args.runtime_lock_fd = 99
    root = tmp_path / args.run_id
    root.mkdir()
    companies = [company(1)]
    store = TrackingStore(tmp_path / "tracking.sqlite3", args.run_id)
    store.register_run({}, companies)
    calls = []

    class Process:
        pid = 123456
        returncode = 0

        async def wait(self):
            return 0

    async def spawn(*command, **kwargs):
        calls.append((command, kwargs))
        return Process()

    monkeypatch.setattr(stream.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(stream.os, "killpg", Mock(side_effect=ProcessLookupError))
    try:
        result = await stream.BrowserWorker(args, root, store)(
            0, {"endpoint": "http://1.1.1.1:80", "exit_ip": "1.1.1.1"}, companies, "lease"
        )
        command, options = calls[0]
        child = pilot.parse_args(command[command.index("--db") :])
        assert child.proxy_limit == 1 and child.company_attempts == 1 and child.profile_slots == 1
        assert child.expected_proxy_ip == "1.1.1.1" and child.captcha_timeout == 130
        assert child.site_limit_scope == "proxy"
        assert options["start_new_session"] is True and options["pass_fds"] == (99,)
        assert not result.attempted
    finally:
        store.close()


@pytest.mark.parametrize(
    "flags",
    [
        ["--browsers", "6"],
        ["--browsers", "0"],
        ["--proxy-rate-per-minute", "0"],
        ["--worker-timeout", "nan"],
        ["--run-id", "../escape"],
    ],
)
def test_invalid_resource_configuration_rejected(flags):
    with pytest.raises(SystemExit):
        stream.parse_args(flags)


def test_site_limited_exit_remains_paused_across_restart_and_new_endpoints(queue, tmp_path, monkeypatch):
    import time

    queue.add_jobs([company(1)])
    endpoint = ready(queue, 1)
    proxy, jobs = queue.claim_batch("owner", 1)
    queue.quarantine_proxy(endpoint, "owner", 86400, "daily limit")
    queue.finish_job(jobs[0]["company_id"], "owner", complete=False, attempted=False, limit=1, retry=0)
    reopened = FreeQueue(tmp_path / "pool.sqlite3", "test")
    try:
        reopened.recover()
        assert reopened.claim_probe(fresh_for=172800) is None
        other = "http://203.0.113.2:80"
        reopened.add_candidates([(other, "new-feed")], capacity=10)
        assert not reopened.finish_probe(other, exit_ip=proxy["exit_ip"])
        assert reopened.claim_batch("next", 1) is None
        now = time.time()
        monkeypatch.setattr("src.sites.res.free_queue.time.time", lambda: now + 86401)
        assert reopened.finish_probe(other, exit_ip=proxy["exit_ip"])
        assert reopened.claim_batch("next", 1)
    finally:
        reopened.close()


async def test_site_limit_pauses_only_that_proxy_and_continues_company_on_another(queue):
    args = stream.parse_args(["--browsers", "1", "--batch-size", "1", "--retry-seconds", "0", "--company-attempts", "1"])
    queue.add_jobs([company(1)])
    ready(queue, 1)
    ready(queue, 2)
    changed, stop = asyncio.Event(), asyncio.Event()
    used = []
    store = SimpleNamespace(cooldown_state=lambda: {"active": False, "halted": False})

    async def worker(slot, proxy, companies, owner):
        used.append(proxy["endpoint"])
        identifier = companies[0]["company_id"]
        if len(used) == 1:
            return stream.BatchResult(
                attempted={identifier}, quota_companies={identifier}, quota_limited=True, error="daily limit"
            )
        stop.set()
        return stream.BatchResult(completed={identifier}, attempted={identifier})

    dispatcher = stream.Dispatcher(args, queue, worker, store, changed, stop)
    await asyncio.wait_for(dispatcher.consume(0), 1)
    assert len(used) == 2 and used[0] != used[1]
    assert queue.counts()["jobs"] == {"done": 1}
    assert len(dispatcher.site_limits) == 1
    assert queue.database.execute("SELECT COUNT(*) FROM free_exit_cooldowns").fetchone()[0] == 1
    assert queue.database.execute("SELECT attempts FROM free_jobs").fetchone()[0] == 1


async def test_continuous_run_resumes_without_touching_ready_pdfs_or_network(tmp_path, monkeypatch):
    from src.sites.res import free_stream
    from src.utils.atomic import atomic_write_json

    args = stream.parse_args(["--output", str(tmp_path), "--limit", "1", "--run-id", "resume", "--no-telegram"])
    root = tmp_path / args.run_id
    root.mkdir()
    companies = [company(1)]
    identifier = companies[0]["company_id"]
    cve = "ACuKDHhqsvAd"
    atomic_write_json(root / "selection.json", {"config": {"year": 2025, "month": 1, "limit": 1}, "companies": companies})
    store = TrackingStore(tmp_path / "tracking.sqlite3", args.run_id)
    directory = root / "workers" / "w00"
    directory.mkdir(parents=True)
    cached = {"status": "completed", "cves": [cve]}
    store.register_run({}, companies)
    store.search_result(identifier, cached)
    saved = pilot.persist_pdf(
        directory,
        cve,
        b"%PDF-1.4\nfixture\n%%EOF",
        f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={cve}&codigo=valid",
    )
    store.file_result(saved, directory, identifier)
    store.close()
    before = (directory / saved["local_path"]).stat().st_mtime_ns
    monkeypatch.setattr(free_stream, "select_companies", Mock(side_effect=AssertionError("No company source requests")))
    monkeypatch.setattr(
        free_stream.ProxyProducer, "run", AsyncMock(side_effect=AssertionError("No proxy collection needed"))
    )
    monkeypatch.setattr(free_stream.BrowserWorker, "__call__", AsyncMock(side_effect=AssertionError("No browsers needed")))
    monkeypatch.setenv("ALLOW_PUBLIC_PROXY_COLLECTION", "0")
    assert await stream.execute(args, root) == 0
    assert (directory / saved["local_path"]).stat().st_mtime_ns == before
    summary = __import__("json").loads((root / "summary.json").read_text())
    assert summary["state"] == "finished" and summary["new_pdfs"] == 0
    assert summary["total_pdfs"] == 1 and summary["companies_with_pdf"] == 1
    assert summary["telegram"]["enabled"] is False
    assert (root / "events.jsonl").exists()


@pytest.mark.parametrize(
    "error,key",
    [(ReputationDenied("blocked by feed"), "reputation_denied"), (TimeoutError("connection timeout"), "connection_failed")],
)
async def test_proxy_statistics_distinguish_reputation_from_connection_failure(queue, tmp_path, error, key):
    args = stream.parse_args(["--no-telegram"])
    queue.add_candidates([("http://203.0.113.1:80", "fixture")], capacity=100)
    stop = asyncio.Event()
    producer = stream.ProxyProducer(args, tmp_path, queue, asyncio.Event(), stop)

    async def verify(endpoint):
        stop.set()
        raise error

    producer.verify = verify
    await producer.scan()
    assert producer.stats["checks"] == 1
    assert producer.stats[key] == 1
    assert producer.stats["ready"] == 0


def test_unlimited_candidate_pool_and_refresh_preserve_active_jobs_and_exit_pauses(queue):
    queue.add_candidates([(f"http://203.0.113.{index}:80", "fixture") for index in range(1, 40)], capacity=0)
    assert queue.database.execute("SELECT COUNT(*) FROM free_proxies").fetchone()[0] == 39
    queue.add_jobs([company(1)])
    queue.finish_probe("http://203.0.113.1:80", exit_ip="198.51.100.1")
    proxy, _ = queue.claim_batch("owner", 1)
    queue.database.execute("INSERT INTO free_exit_cooldowns VALUES ('198.51.100.2',9999999999,'site_document_limit')")
    queue.database.commit()
    assert queue.retain_candidates([]) == 38
    assert queue.database.execute("SELECT state FROM free_proxies").fetchone()[0] == "leased"
    assert queue.database.execute("SELECT COUNT(*) FROM free_exit_cooldowns").fetchone()[0] == 1
    assert queue.counts()["jobs"] == {"leased": 1}


async def test_catalog_screens_lists_before_ports_and_has_no_default_pool_truncation(queue, tmp_path, monkeypatch):
    from src.proxy.sources.catalog import CatalogLoadResult, ProxyCandidate

    args = stream.parse_args(["--sources", "monosans", "--no-telegram"])
    assert args.max_candidates == 0 and args.source_records == 0
    candidates = [
        ProxyCandidate(url=f"http://{host}:{port}", protocol="http", source="fixture")
        for host, port in [("11.0.0.1", 80), ("11.0.0.2", 3128), ("12.0.0.1", 80), ("8.211.1.1", 80)]
    ]
    loader = AsyncMock(return_value=CatalogLoadResult(candidates=candidates))
    monkeypatch.setattr(stream, "load_public_catalog", loader)
    seen = []

    def local_filter(rows):
        seen.extend(rows)
        return [(endpoint, source) for endpoint, source, host in rows if host != "12.0.0.1"], {"abuseipdb": 1}

    monkeypatch.setattr(stream, "filter_candidate_ips", local_filter)
    producer = stream.ProxyProducer(args, tmp_path, queue, asyncio.Event(), asyncio.Event())
    await producer.refresh()
    assert loader.call_args.kwargs["max_records"] is None
    assert any(row[2] == "11.0.0.2" for row in seen)  # Nonstandard port is still IP screened first.
    assert not any(row[2] == "8.211.1.1" for row in seen)
    assert producer.stats["local_rejected"]["blocked_networks"] == 1
    assert producer.stats["feed_rejected"] == {"abuseipdb": 1}
    assert producer.stats["port_rejected"] == 1 and producer.stats["selected_candidates"] == 1
    assert producer.stats["rejected_ports"] == {"3128": 1}
    assert stream.endpoint_allowed("http://11.0.0.2:3128", set(), stream.proxy_ports("80,443,3128"))
    assert not stream.endpoint_allowed("http://8.211.1.1:3128", set(), stream.proxy_ports("80,443,3128"))


@pytest.mark.parametrize(
    "error,expected",
    [
        (ImportError("Using SOCKS proxy, but socksio is not installed"), "missing_socks_dependency"),
        (TimeoutError(), "timeout"),
        (stream.httpx.ConnectError("certificate verify failed"), "tls_error"),
        (stream.httpx.ConnectError("connection refused"), "connect_error"),
        (stream.httpx.ProxyError("CONNECT failed"), "proxy_error"),
        (stream.httpx.RemoteProtocolError("Server disconnected"), "protocol_error"),
        (ValueError("RES form missing"), "res_form_missing"),
        (ValueError("Proxy exit changed during preflight"), "exit_changed"),
    ],
)
def test_probe_failures_have_specific_categories(error, expected):
    assert stream.probe_error_category(error) == expected


async def test_missing_socks_support_stops_before_scanning(queue, tmp_path, monkeypatch):
    import builtins

    original = builtins.__import__

    def without_socks(name, *args, **kwargs):
        if name == "socksio":
            raise ImportError("No socksio")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_socks)
    producer = stream.ProxyProducer(stream.parse_args(["--no-telegram"]), tmp_path, queue, asyncio.Event(), asyncio.Event())
    producer.scan = AsyncMock()
    producer.refresh = AsyncMock()
    with pytest.raises(RuntimeError, match=r"httpx\[socks\]"):
        await producer.run()
    producer.scan.assert_not_awaited()
    producer.refresh.assert_not_awaited()


async def test_missing_dependency_is_not_reported_as_a_network_failure(queue, tmp_path):
    queue.add_candidates([("socks5://13.0.0.1:1080", "fixture")], capacity=10)
    stop = asyncio.Event()
    producer = stream.ProxyProducer(stream.parse_args(["--no-telegram"]), tmp_path, queue, asyncio.Event(), stop)

    async def verify(endpoint):
        stop.set()
        raise ImportError("socksio is not installed")

    producer.verify = verify
    await producer.scan()
    assert producer.stats["configuration_failed"] == 1
    assert producer.stats["connection_failed"] == 0
    assert producer.stats["failure_reasons"] == {"missing_socks_dependency": 1}
    assert queue.database.execute("SELECT error FROM free_proxies").fetchone()[0].startswith("missing_socks_dependency:")


async def test_installed_socks_transport_constructs_without_contacting_candidate():
    stream.require_socks_support()
    # Construct HTTPX SOCKS transports only; no DNS, connect or HTTP calls.
    for scheme in ("socks5", "socks5h"):
        async with stream.httpx.AsyncClient(proxy=f"{scheme}://127.0.0.1:1080", trust_env=False):
            pass
