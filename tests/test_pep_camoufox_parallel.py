"""Parallel scheduling and proxy isolation without browser or external requests."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from scripts import pep_camoufox_batch as batch, pep_camoufox_parallel as parallel
from scripts.pep_camoufox_excel import ProgressReporter


def sample(size):
    return [
        {"n": i + 1, "url": f"https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?n={i}"} for i in range(size)
    ]


@pytest.mark.asyncio
async def test_fleet_pause_drains_then_one_probe_resumes_queue(monkeypatch, tmp_path, fake_browsers):
    initial = asyncio.Event()
    calls = 0
    probes = 0
    events = []

    async def fake_run(group, output, request_seconds, *, next_job, on_progress, **kwargs):
        nonlocal calls
        calls += 1
        fail = calls <= 2
        if calls == 2:
            initial.set()
        await initial.wait()
        jobs = []
        while (row := next_job()) is not None:
            jobs.append(
                {
                    **row,
                    "status": "failed" if fail else "completed",
                    "documents": [],
                    "navigation_stage": "waiting_result_html",
                    "error": {"type": "TimeoutError"} if fail else None,
                }
            )
            await on_progress({"status": "running", "jobs": jobs}, final=False)
            await asyncio.sleep(0.001)
            if fail:
                break
        await on_progress({"status": "finished", "jobs": jobs}, final=True)

    async def probe(*args):
        nonlocal probes
        probes += 1
        if probes == 1:
            raise TimeoutError("site unavailable")

    async def notify(report, *, final):
        if report.get("event"):
            events.append(report["event"])

    monkeypatch.setattr(parallel, "run", fake_run)
    monkeypatch.setattr(parallel, "navigate_results", probe)
    result = await parallel.run_parallel(
        sample(10),
        tmp_path / "run",
        workers=2,
        tabs=1,
        request_seconds=1,
        ca_bundle=tmp_path / "ca",
        proxy_ca=None,
        max_seconds=5,
        proxy=parallel.FixedProxyPool([{"server": f"http://192.0.2.{i}:80"} for i in (1, 2)]),
        notify=notify,
        site_pause_seconds=0.02,
    )
    assert events.count("site_paused") == 1
    assert events.count("site_probe_failed") == events.count("site_resumed") == 1
    assert probes == 2
    assert result["queue_remaining"] == 0
    assert len({job["n"] for job in result["jobs"]}) == 10
    assert result["site_pause"]["state"] == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type,expected_contexts", [("TimeoutError", 2), ("ValueError", 1), ("PersistentTimeout", 4)])
async def test_navigation_cooldown_only_recovers_timeouts(
    monkeypatch, tmp_path, fake_browsers, error_type, expected_contexts
):
    calls = 0

    async def fake_run(group, output, request_seconds, *, next_job, on_progress, **kwargs):
        nonlocal calls
        calls += 1
        jobs = []
        while (row := next_job()) is not None:
            failed = calls == 1 or error_type == "PersistentTimeout"
            jobs.append(
                {
                    **row,
                    "status": "failed" if failed else "completed",
                    "documents": [],
                    "error": {"type": "TimeoutError" if error_type == "PersistentTimeout" else error_type}
                    if failed
                    else None,
                }
            )
            if failed and len(jobs) == 3:
                break
        await on_progress(
            {
                "status": "stopped" if calls == 1 or error_type == "PersistentTimeout" else "finished",
                "jobs": jobs,
                "stop_reason": "consecutive_navigation_errors" if calls == 1 or error_type == "PersistentTimeout" else None,
            },
            final=True,
        )

    async def notify(report, *, final):
        pass

    monkeypatch.setattr(parallel, "run", fake_run)
    report = await parallel.run_parallel(
        sample(15 if error_type == "PersistentTimeout" else 5),
        tmp_path / "run",
        workers=1,
        request_seconds=10,
        ca_bundle=tmp_path / "ca",
        proxy_ca=None,
        max_seconds=10,
        proxy=None,
        notify=notify,
        navigation_backoff=0,
    )
    assert len(fake_browsers) == expected_contexts
    assert report["queue_remaining"] == (
        0 if error_type == "TimeoutError" else 3 if error_type == "PersistentTimeout" else 2
    )
    assert report["browser_restarts"] == expected_contexts - 1


@pytest.mark.asyncio
@pytest.mark.parametrize("age_limit", [False, True])
@pytest.mark.parametrize("sqlite_state", [False, True])
async def test_browser_rotation_drains_keeps_files_and_closes_old_context(
    monkeypatch, tmp_path, fake_browsers, age_limit, sqlite_state
):
    from src.sites.pep.documents import save_payload

    original = parallel.browser_context
    alive, maximum, closed = 0, 0, 0

    @asynccontextmanager
    async def tracked(options, lock):
        nonlocal alive, maximum, closed
        async with original(options, lock) as context:
            alive += 1
            maximum = max(maximum, alive)
            try:
                yield context
            finally:
                alive -= 1
                closed += 1

    async def fake_run(group, output, request_seconds, *, next_job, on_progress, **kwargs):
        jobs = []
        output.mkdir(parents=True, exist_ok=True)
        while (row := next_job()) is not None:
            await asyncio.sleep(0.02 if age_limit else 0.001)
            metadata = save_payload(b"%PDF-1.4\ntest\n%%EOF", output, str(row["n"]))
            jobs.append({**row, "status": "completed", "documents": [{"id": str(row["n"]), **metadata}]})
        await on_progress({"status": "finished", "jobs": jobs, "pdf_count": len(jobs)}, final=True)

    async def notify(report, *, final):
        pass

    monkeypatch.setattr(parallel, "browser_context", tracked)
    monkeypatch.setattr(parallel, "run", fake_run)
    output = tmp_path / "run"
    result = await parallel.run_parallel(
        sample(12),
        output,
        workers=2,
        tabs=3,
        request_seconds=10,
        ca_bundle=tmp_path / "ca",
        proxy_ca=None,
        max_seconds=10,
        proxy=parallel.FixedProxyPool([{"server": f"http://192.0.2.{i}:80"} for i in (1, 2)]),
        notify=notify,
        browser_max_seconds=0.01 if age_limit else 1000,
        browser_max_links=1000 if age_limit else 2,
        sqlite_state=sqlite_state,
    )
    assert result["status"] == "finished"
    assert result["browser_restarts"] >= 2
    assert result["tab_restarts"] == 0
    assert maximum <= 2 and alive == 0 and closed == len(fake_browsers)
    assert len({job["n"] for job in result["jobs"]}) == 12
    assert result["pdf_count"] == 12
    assert all((output / doc["local_path"]).exists() for job in result["jobs"] for doc in job["documents"])
    if sqlite_state:
        from scripts.pep_camoufox_db import read_report

        saved = read_report(output / "state.sqlite3")
        assert saved["jobs"] == result["jobs"]
        assert saved["status"] == "finished"
        assert not (output / "report.json").exists()
        assert not (output / "politicians.json").exists()


@pytest.mark.asyncio
async def test_resume_only_queues_missing_links(monkeypatch, tmp_path, fake_browsers):
    from scripts.pep_camoufox_resume import resume_history
    from src.sites.pep.documents import save_payload

    rows = sample(2)
    old = tmp_path / "run-old"
    metadata = save_payload(b"%PDF-1.4\ntest\n%%EOF", old, "1")
    (old / "report.json").write_text(
        json.dumps({"jobs": [{**rows[0], "status": "completed", "documents": [{"id": "1", **metadata}]}]})
    )
    assigned = []

    async def fake_run(group, output, request_seconds, *, next_job, on_state, **kwargs):
        jobs = []
        while row := next_job():
            assigned.append(row["n"])
            jobs.append({**row, "status": "completed", "documents": []})
        state = {"status": "finished", "jobs": jobs, "pdf_count": 0}
        on_state(state)
        return state

    async def notify(report, *, final):
        pass

    monkeypatch.setattr(parallel, "run", fake_run)
    output = tmp_path / "run-new"
    report = await parallel.run_parallel(
        rows,
        output,
        workers=1,
        request_seconds=60,
        ca_bundle=tmp_path / "ca",
        proxy_ca=None,
        max_seconds=60,
        proxy=None,
        notify=notify,
        resume_history=resume_history(tmp_path, rows),
    )
    assert assigned == [2]
    assert [job["n"] for job in report["jobs"]] == [1, 2]
    assert report["resumed_links"] == 1
    assert report["pdf_count"] == 1
    assert (output / report["jobs"][0]["documents"][0]["local_path"]).exists()


@pytest.mark.parametrize(
    ("size", "workers", "sizes"), [(100, 4, [25] * 4), (100, 3, [34, 33, 33]), (101, 4, [26, 25, 25, 25]), (2, 4, [1, 1])]
)
def test_balanced_unique_partition(size, workers, sizes):
    groups = parallel.split_sample(sample(size), workers)
    assert [len(group) for group in groups] == sizes
    assert sorted(row["n"] for group in groups for row in group) == list(range(1, size + 1))


def test_independent_stable_proxy_sessions():
    config = parallel.BrightDataProxy("http://brd.superproxy.io:44445", "brd-customer-test-zone-test", "test-password")
    proxies = [config.for_worker(i) for i in range(1, 5)]
    assert len({proxy["username"] for proxy in proxies}) == 4
    assert proxies[0] == config.for_worker(1)
    assert all(proxy["server"] == "http://brd.superproxy.io:44445" for proxy in proxies)
    assert "test-password" not in repr(config)


def test_reject_credentials_embedded_in_server():
    with pytest.raises(ValueError, match="Bright Data"):
        parallel.BrightDataProxy("http://user:password@brd.superproxy.io:44445", "user", "password")


def test_fixed_proxies_stay_assigned_and_do_not_leak_credentials():
    proxies = [{"server": f"http://192.0.2.{i}:8080", "username": "example", "password": "secret-password"} for i in (1, 2)]
    pool = parallel.FixedProxyPool(proxies)
    assigned = pool.for_worker(1)
    assigned["server"] = "http://changed.invalid"
    proxies[1]["server"] = "http://changed.invalid"
    assert pool.for_worker(1)["server"] == "http://192.0.2.1:8080"
    assert pool.for_worker(2)["server"] == "http://192.0.2.2:8080"
    assert "secret-password" not in repr(pool)
    with pytest.raises(ValueError, match="No assigned proxy"):
        pool.for_worker(3)


@pytest.fixture
def fake_browsers(monkeypatch):
    contexts = []

    class Page:
        async def goto(self, *args, **kwargs):
            return SimpleNamespace(status=200)

        def locator(self, selector):
            return self

        async def inner_text(self):
            return "192.0.2.1"

        async def close(self):
            pass

    class Context:
        async def new_page(self):
            return Page()

    @asynccontextmanager
    async def browser(options, lock):
        context = Context()
        contexts.append((context, options))
        yield context

    monkeypatch.setattr(parallel, "browser_context", browser)
    monkeypatch.setattr(parallel, "install_intermediate", lambda *args: None)
    monkeypatch.setattr(parallel, "install_proxy_ca", lambda *args: None)
    return contexts


@pytest.mark.asyncio
async def test_twenty_browsers_sixty_tabs_global_queue_and_progress(monkeypatch, tmp_path, fake_browsers):
    active, maximum = 0, 0
    assignments, context_users = [], {}
    all_tabs_started = asyncio.Event()

    async def fake_run(
        group, output, request_seconds, *, on_progress, on_state, proxy, context_override, next_job, **kwargs
    ):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        context_users.setdefault(context_override, []).append(proxy["username"])
        if active == 60:
            all_tabs_started.set()
        await asyncio.wait_for(all_tabs_started.wait(), timeout=5)
        state = {"status": "running", "pdf_count": 0, "archive_count": 0, "jobs": []}
        while (row := next_job()) is not None:
            assignments.append(row["n"])
            job = {**row, "status": "running", "documents": []}
            state["jobs"].append(job)
            on_state(state)
            await asyncio.sleep(0)
            job.update(
                status="completed",
                documents=[
                    {"status": "downloaded", "local_path": "pdf/example.pdf", "files": [{"local_path": "pdf/example.pdf"}]}
                ],
            )
            state["pdf_count"] += 1
            await on_progress(state, final=False)
        state["status"] = "finished"
        await on_progress(state, final=True)
        active -= 1
        return state

    monkeypatch.setattr(parallel, "run", fake_run)
    output = tmp_path / "run"
    result = await parallel.run_parallel(
        sample(100),
        output,
        workers=20,
        tabs=3,
        request_seconds=10,
        ca_bundle=tmp_path / "ca.pem",
        proxy_ca=tmp_path / "proxy.pem",
        max_seconds=100,
        proxy=parallel.BrightDataProxy("http://brd.superproxy.io:44445", "test-zone", "test-password"),
        notify=ProgressReporter(None, output),
    )
    assert maximum == 60
    assert len(fake_browsers) == len(context_users) == 20
    assert all(len(users) == 3 and len(set(users)) == 1 for users in context_users.values())
    assert len({users[0] for users in context_users.values()}) == 20
    assert sorted(assignments) == list(range(1, 101))
    assert result["job_counts"] == {"completed": 100}
    assert result["pdf_count"] == 100
    assert result["worker_count"] == 20 and result["tab_count"] == 60
    assert result["queue_remaining"] == 0
    progress = [json.loads(line) for line in (output / "progress.jsonl").read_text().splitlines()]
    counts = [row["processed"] for row in progress if row.get("event") != "download_started"]
    # Threaded persistence allows multiple tabs to finish while a snapshot is
    # being written; notifications can skip intermediate ten-link boundaries.
    assert counts == sorted(counts) and counts[-1] == 100
    assert all(count >= 10 for count in counts)
    assert sum(row["final"] for row in progress) == 1
    for job in result["jobs"]:
        expected = f"worker-{job['worker']}/tab-{job['tab']}/pdf/example.pdf"
        assert job["documents"][0]["local_path"] == expected
        assert job["documents"][0]["files"][0]["local_path"] == expected
    assert json.loads((output / "incomplete-documents.json").read_text()) == []


@pytest.mark.asyncio
async def test_fast_browser_takes_remaining_work_and_failed_browser_does_not_claim(monkeypatch, tmp_path, fake_browsers):
    slow_started = asyncio.Event()
    release_slow = asyncio.Event()

    async def fake_run(group, output, request_seconds, *, next_job, on_progress, **kwargs):
        state = {"status": "running", "jobs": []}
        slow = "worker-1" in str(output)
        if not slow:
            await slow_started.wait()
        while (row := next_job()) is not None:
            if slow:
                slow_started.set()
                await release_slow.wait()
            state["jobs"].append({**row, "status": "completed", "documents": []})
            await on_progress(state, final=False)
        if not slow:
            release_slow.set()
        state["status"] = "finished"
        await on_progress(state, final=True)

    monkeypatch.setattr(parallel, "run", fake_run)
    result = await parallel.run_parallel(
        sample(10),
        tmp_path / "run",
        workers=2,
        request_seconds=10,
        ca_bundle=tmp_path / "ca",
        proxy_ca=None,
        max_seconds=10,
        proxy=parallel.FixedProxyPool([{"server": f"http://192.0.2.{i}:80"} for i in (1, 2)]),
        notify=ProgressReporter(None, tmp_path / "run"),
    )
    counts = {i: sum(job["worker"] == i for job in result["jobs"]) for i in (1, 2)}
    assert counts == {1: 1, 2: 9}
    assert result["job_counts"] == {"completed": 10}


@pytest.mark.asyncio
async def test_multiple_workers_never_fall_back_to_direct(tmp_path):
    with pytest.raises(ValueError, match="credentials"):
        await parallel.run_parallel(
            sample(10),
            tmp_path,
            workers=4,
            request_seconds=10,
            ca_bundle=tmp_path / "ca",
            proxy_ca=tmp_path / "proxy",
            max_seconds=100,
            proxy=None,
            notify=None,
        )


@pytest.mark.asyncio
async def test_launch_exception_redacts_proxy_credentials(monkeypatch, tmp_path):
    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    class BrokenBrowser:
        async def __aenter__(self):
            raise RuntimeError("Launch failed for base-zone-session-id and base-zone with secret-password")

        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr(batch.asyncio, "to_thread", inline)
    monkeypatch.setattr(batch, "install_intermediate", lambda *args: None)
    monkeypatch.setattr(batch, "install_proxy_ca", lambda *args: None)
    monkeypatch.setattr(batch, "AsyncCamoufox", lambda **kwargs: BrokenBrowser())
    output = tmp_path / "worker"
    result = await batch.run(
        sample(1),
        output,
        10,
        proxy_ca=tmp_path / "proxy",
        proxy={"username": "base-zone-session-id", "password": "secret-password"},
    )
    assert result["status"] == "error"
    saved = (output / "report.json").read_text()
    assert "secret-password" not in saved
    assert "base-zone" not in saved
    log = (output / "run.log").read_text()
    assert "secret-password" not in log
    assert "base-zone" not in log
    assert '"event": "worker_finished"' in log


@pytest.mark.asyncio
async def test_native_launch_is_serial_but_browsers_stay_concurrent(monkeypatch):
    initializing, max_initializing, alive, max_alive, entered = 0, 0, 0, 0, 0
    all_entered = asyncio.Event()

    class Browser:
        async def __aenter__(self):
            nonlocal initializing, max_initializing, alive, max_alive
            initializing += 1
            max_initializing = max(max_initializing, initializing)
            await asyncio.sleep(0)
            initializing -= 1
            alive += 1
            max_alive = max(max_alive, alive)
            return self

        async def __aexit__(self, *args):
            nonlocal alive
            alive -= 1

    monkeypatch.setattr(batch, "AsyncCamoufox", lambda **kwargs: Browser())
    lock = asyncio.Lock()

    async def worker():
        nonlocal entered
        async with batch.browser_context({}, lock):
            entered += 1
            if entered == 4:
                all_entered.set()
            await all_entered.wait()

    await asyncio.gather(*(worker() for _ in range(4)))
    assert max_initializing == 1
    assert max_alive == 4
    assert alive == 0


def test_ip_report_does_not_assume_distinct_exits(tmp_path):
    rows = sample(4)
    aggregate = parallel.AggregateReport(rows, parallel.split_sample(rows, 4), tmp_path, None)
    for index in range(1, 5):
        aggregate.workers[index]["proxy_egress_ip"] = "192.0.2.1" if index < 4 else "192.0.2.2"
    snapshot = aggregate.snapshot()
    assert snapshot["proxy_egress_ips"] == ["192.0.2.1", "192.0.2.2"]
    assert snapshot["proxy_observed_browsers"] == 4
    aggregate.log.close()


@pytest.mark.asyncio
async def test_webshare_chooses_distinct_addresses_and_decodes_auth(monkeypatch):
    from src.proxy.sources import webshare

    async def inventory(token, *, mode):
        assert token == "test-api-key" and mode == "direct"
        return [
            SimpleNamespace(url=url)
            for url in (
                "http://user:p%40ss@192.0.2.1:8000",
                "http://user:p%40ss@192.0.2.1:8001",
                "http://user:p%40ss@192.0.2.2:8000",
            )
        ]

    monkeypatch.setattr(webshare, "load_webshare_candidates", inventory)
    pool = await parallel.webshare_pool("test-api-key", 2)
    assert pool.for_worker(1)["password"] == "p@ss"
    assert pool.for_worker(2)["server"] == "http://192.0.2.2:8000"
    with pytest.raises(ValueError, match="only 2.*requested 20"):
        await parallel.webshare_pool("test-api-key", 20)


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("duplicate_rows", [False, True])
async def test_actual_tabs_share_context_but_keep_downloads_separate(monkeypatch, tmp_path, legacy, duplicate_rows):
    original_sleep = asyncio.sleep
    pages, contexts_closed = [], []
    claimed = iter(sample(6))

    async def fast_sleep(seconds):
        await original_sleep(0)

    class Page:
        def __init__(self):
            self.url = ""
            self.closed = False
            self.handlers = {}

        def on(self, event, handler):
            self.handlers[event] = handler

        async def goto(self, url, **kwargs):
            self.url = url
            return SimpleNamespace(status=200)

        def locator(self, selector):
            return self

        async def wait_for(self, **kwargs):
            pass

        async def content(self):
            return "test"

        async def wait_for_function(self, *args, **kwargs):
            pass

        async def evaluate_all(self, *args):
            return 0

        def nth(self, index):
            return self

        async def evaluate(self, *args):
            pass

        async def close(self):
            self.closed = True

    class Context:
        async def new_page(self):
            page = Page()
            pages.append(page)
            return page

        async def close(self):
            contexts_closed.append(True)

    def parse(content, url):
        doc_id = str(100 + int(url.rsplit("=", 1)[1]))
        return [
            {
                "id": doc_id,
                "requires_captcha": not legacy,
                "download_url": "https://www.funcionpublica.gov.co" + batch.LEGACY_PATH + "?action=download",
            }
        ] * (2 if duplicate_rows else 1), None

    async def download(page, element, endpoint, timeout_ms, **kwargs):
        # An empty pageerror must not break the event callback or PDF processing.
        page.handlers["pageerror"](batch.BrowserError(""))
        page.handlers["pageerror"](batch.BrowserError("first line\nsecond line"))
        if legacy:
            assert kwargs["expected_url"].startswith("https://")
            return None, 404
        # A per-page URL identifies its own native download.
        doc_id = str(100 + int(page.url.rsplit("=", 1)[1]))

        class Download:
            async def save_as(self, filename):
                from pathlib import Path

                await asyncio.to_thread(Path(filename).write_bytes, b"%PDF-1.4\n" + doc_id.encode() + b"\n%%EOF")

        await original_sleep(0)
        return Download(), None

    monkeypatch.setattr(batch.asyncio, "sleep", fast_sleep)
    monkeypatch.setattr(batch, "parse_results", parse)
    monkeypatch.setattr(batch, "download_once", download)
    context = Context()
    results = await asyncio.gather(
        *(
            batch.run([], tmp_path / f"tab-{i}", 10, context_override=context, next_job=lambda: next(claimed, None))
            for i in range(3)
        )
    )
    jobs = [job for report in results for job in report["jobs"]]
    assert sorted(job["n"] for job in jobs) == list(range(1, 7))
    assert all(job["status"] == ("completed_with_unavailable" if legacy else "completed") for job in jobs)
    assert all(job["documents"][0]["javascript_errors"] == ["Error", "first line"] for job in jobs)
    assert sum(report["pdf_count"] for report in results) == (0 if legacy else 6)
    assert len(pages) == 3 and all(page.closed for page in pages)
    assert not contexts_closed
    for path in tmp_path.glob("tab-*/pdf/*.pdf"):
        assert path.stem.encode() in path.read_bytes()


@pytest.mark.asyncio
async def test_explicit_rejection_stops_peer_tabs_and_leaves_queue(monkeypatch, tmp_path):
    original_sleep = asyncio.sleep
    claimed = iter(sample(10))
    stop = asyncio.Event()

    class Page:
        url = ""

        def on(self, *args):
            pass

        async def goto(self, url, **kwargs):
            self.url = url
            return SimpleNamespace(status=200)

        def locator(self, selector):
            return self

        async def wait_for(self, **kwargs):
            pass

        async def content(self):
            return "Recaptcha did not pass validation"

        async def wait_for_function(self, *args, **kwargs):
            pass

        async def close(self):
            pass

    class Context:
        async def new_page(self):
            return Page()

    async def sleep(seconds):
        await original_sleep(0)

    async def rejected(*args):
        await original_sleep(0)
        return None, 403

    monkeypatch.setattr(batch.asyncio, "sleep", sleep)
    monkeypatch.setattr(batch, "parse_results", lambda *args: ([{"id": "123", "requires_captcha": True}], None))
    monkeypatch.setattr(batch, "download_once", rejected)
    results = await asyncio.gather(
        *(
            batch.run(
                [],
                tmp_path / f"tab-{i}",
                10,
                context_override=Context(),
                next_job=lambda: next(claimed, None),
                stop_event=stop,
            )
            for i in range(3)
        )
    )
    assert stop.is_set()
    assert sum(len(report["jobs"]) for report in results) <= 3
    assert all(report["status"] == "stopped" for report in results)
    assert next(claimed) is not None


@pytest.mark.asyncio
async def test_failed_proxy_does_not_lose_queued_links_or_expose_password(monkeypatch, tmp_path, fake_browsers):
    @asynccontextmanager
    async def browser(options, lock):
        if options["proxy"]["server"] == "http://192.0.2.1:80":
            raise RuntimeError("Connect failed: example-user secret-password")

        class Page:
            async def goto(self, *args, **kwargs):
                return SimpleNamespace(status=200)

            def locator(self, *args):
                return self

            async def inner_text(self):
                return "192.0.2.2"

            async def close(self):
                pass

        class Context:
            async def new_page(self):
                return Page()

        yield Context()

    async def consume(group, output, request_seconds, *, next_job, on_progress, **kwargs):
        state = {"status": "finished", "jobs": []}
        while (row := next_job()) is not None:
            state["jobs"].append({**row, "status": "completed", "documents": []})
        await on_progress(state, final=True)

    monkeypatch.setattr(parallel, "browser_context", browser)
    monkeypatch.setattr(parallel, "run", consume)
    output = tmp_path / "run"
    result = await parallel.run_parallel(
        sample(10),
        output,
        workers=2,
        tabs=3,
        request_seconds=10,
        ca_bundle=tmp_path / "ca",
        proxy_ca=None,
        max_seconds=10,
        proxy=parallel.FixedProxyPool(
            [{"server": f"http://192.0.2.{i}:80", "username": "example-user", "password": "secret-password"} for i in (1, 2)]
        ),
        notify=ProgressReporter(None, output),
    )
    assert result["status"] == "error"
    assert result["job_counts"] == {"completed": 10}
    assert all(job["worker"] == 2 for job in result["jobs"])
    for path in (output / "report.json", output / "run.log", output / "worker-1/report.json"):
        text = path.read_text()
        assert "example-user" not in text and "secret-password" not in text


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["goto", "crash", "download", "timeout_diagnostics"])
async def test_dead_tab_never_drains_queue_or_stops_healthy_peers(monkeypatch, tmp_path, failure):
    original_sleep = asyncio.sleep
    queue = iter(sample(10))
    shared_stop = asyncio.Event()

    class TargetClosedError(Exception):
        pass

    class Page:
        url = ""
        handlers = {}
        closed = False

        def on(self, event, handler):
            self.handlers[event] = handler

        def is_closed(self):
            return self.closed

        async def goto(self, url, **kwargs):
            self.url = url
            if failure == "goto":
                raise TargetClosedError("Page.goto: Target page, context or browser has been closed")
            return SimpleNamespace(status=200)

        def locator(self, selector):
            return self

        async def wait_for(self, **kwargs):
            if failure == "crash":
                self.handlers["crash"](self)
                raise batch.BrowserError("Locator.wait_for: Target crashed")

        async def wait_for_function(self, *args, **kwargs):
            await self.wait_for()

        async def content(self):
            if failure == "timeout_diagnostics":
                raise TargetClosedError("Page.content: Target page, context or browser has been closed")
            return "test"

        async def title(self):
            return "test"

        async def inner_text(self, **kwargs):
            return "test"

        async def screenshot(self, **kwargs):
            pass

        async def close(self):
            self.closed = True
            self.handlers["close"](self)

    class Context:
        async def new_page(self):
            return Page()

    async def sleep(seconds):
        await original_sleep(0)

    async def download(*args):
        if failure == "timeout_diagnostics":
            raise batch.BrowserTimeout("Download timed out")
        raise TargetClosedError("Locator.click: Target page, context or browser has been closed")

    # Let discovery succeed before the diagnostic content call fails.
    original_content = Page.content
    content_calls = 0

    async def content(self):
        nonlocal content_calls
        content_calls += 1
        if content_calls == 1:
            return "test"
        return await original_content(self)

    monkeypatch.setattr(Page, "content", content)
    monkeypatch.setattr(batch.asyncio, "sleep", sleep)
    monkeypatch.setattr(batch, "parse_results", lambda *args: ([{"id": "123", "requires_captcha": True}], None))
    monkeypatch.setattr(batch, "download_once", download)
    result = await batch.run(
        [], tmp_path / "tab", 10, context_override=Context(), next_job=lambda: next(queue, None), stop_event=shared_stop
    )
    assert len(result["jobs"]) == 1
    assert next(queue)["n"] == 2
    assert result["status"] == "stopped"
    assert result["stop_reason"] == ("page_crashed" if failure == "crash" else "page_closed")
    assert not shared_stop.is_set()
    if failure == "timeout_diagnostics":
        doc = result["jobs"][0]["documents"][0]
        assert doc["status"] == "timeout"
        assert doc["error"]["type"] == "TimeoutError"
        assert doc["diagnostic_error"]["type"] == "TargetClosedError"
    assert result["page_events"][-1]["kind"] == "close"
    assert result["page_events"][-1]["expected"] is True


@pytest.mark.asyncio
async def test_queue_waits_for_every_browser_startup_result(monkeypatch, tmp_path, fake_browsers):
    last_starting, release_last = asyncio.Event(), asyncio.Event()
    started_tabs = []
    contexts = []

    @asynccontextmanager
    async def browser(options, lock):
        if options["proxy"]["server"] == "http://192.0.2.2:80":
            last_starting.set()
            await release_last.wait()

        class Page:
            async def goto(self, *args, **kwargs):
                return SimpleNamespace(status=200)

            def locator(self, *args):
                return self

            async def inner_text(self):
                return "192.0.2.1"

            async def close(self):
                pass

        class Context:
            async def new_page(self):
                return Page()

        context = Context()
        contexts.append(context)
        yield context

    async def consume(group, output, request_seconds, *, next_job, on_progress, **kwargs):
        started_tabs.append(str(output))
        state = {"status": "finished", "jobs": []}
        while (row := next_job()) is not None:
            state["jobs"].append({**row, "status": "completed", "documents": []})
        await on_progress(state, final=True)

    monkeypatch.setattr(parallel, "browser_context", browser)
    monkeypatch.setattr(parallel, "run", consume)
    output = tmp_path / "run"
    task = asyncio.create_task(
        parallel.run_parallel(
            sample(10),
            output,
            workers=2,
            tabs=3,
            request_seconds=10,
            ca_bundle=tmp_path / "ca",
            proxy_ca=None,
            max_seconds=10,
            proxy=parallel.FixedProxyPool([{"server": f"http://192.0.2.{i}:80"} for i in (1, 2)]),
            notify=ProgressReporter(None, output),
        )
    )
    try:
        await asyncio.wait_for(last_starting.wait(), timeout=5)
        await asyncio.sleep(0)
        assert not started_tabs
        release_last.set()
        result = await asyncio.wait_for(task, timeout=5)
        assert len(contexts) == 2
        assert len(started_tabs) == 6
        assert result["job_counts"] == {"completed": 10}
        events = [json.loads(line) for line in (output / "run.log").read_text().splitlines()]
        assert next(event for event in events if event["event"] == "queue_released")["ready_browsers"] == 2
    finally:
        release_last.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_replacement_tab_keeps_context_proxy_and_previous_results(monkeypatch, tmp_path, fake_browsers):
    calls = []

    async def fake_run(group, output, request_seconds, *, context_override, proxy, next_job, on_progress, **kwargs):
        calls.append((context_override, dict(proxy), output))
        jobs = []
        while (row := next_job()) is not None:
            jobs.append(
                {
                    **row,
                    "status": "completed",
                    "documents": [
                        {
                            "id": str(row["n"]),
                            "status": "downloaded",
                            "local_path": "pdf/test.pdf",
                            "files": [{"local_path": "pdf/test.pdf"}],
                        }
                    ],
                }
            )
            if len(calls) == 1:
                # Preserve a successful download and a separate failed link before replacing the page.
                broken = next_job()
                jobs.append(
                    {
                        **broken,
                        "status": "failed",
                        "documents": [],
                        "error": {"type": "TargetClosedError", "message": "page closed"},
                    }
                )
                break
        report = {
            "status": "stopped" if len(calls) == 1 else "finished",
            "jobs": jobs,
            "stop_reason": "page_closed" if len(calls) == 1 else None,
            "pdf_count": sum(job["status"] == "completed" for job in jobs),
        }
        await on_progress(report, final=True)

    monkeypatch.setattr(parallel, "run", fake_run)
    output = tmp_path / "run"
    proxy = parallel.FixedProxyPool([{"server": "http://192.0.2.1:80", "username": "test-user", "password": "test-pass"}])
    result = await parallel.run_parallel(
        sample(5),
        output,
        workers=1,
        tabs=1,
        request_seconds=10,
        ca_bundle=tmp_path / "ca",
        proxy_ca=None,
        max_seconds=10,
        proxy=proxy,
        notify=ProgressReporter(None, output),
    )
    assert len(calls) == 2
    assert calls[0][0] is calls[1][0]
    assert calls[0][1] == calls[1][1]
    assert len(fake_browsers) == 1
    assert result["job_counts"] == {"completed": 4, "failed": 1}
    assert result["tab_restarts"] == 1
    assert result["pdf_count"] == 4
    assert [job["n"] for job in result["jobs"]] == [1, 2, 3, 4, 5]
    assert result["jobs"][0]["documents"][0]["local_path"] == "worker-1/tab-1/pdf/test.pdf"
    assert result["jobs"][2]["documents"][0]["local_path"] == "worker-1/tab-1/restart-1/pdf/test.pdf"
    tab = result["workers"][0]["tabs"][0]
    assert tab["status"] == "finished"
    assert tab["recovery_history"][0]["stop_reason"] == "page_closed"
    summary = json.loads((output / "worker-1/tab-1/summary.json").read_text())
    assert len(summary["jobs"]) == 5


@pytest.mark.asyncio
@pytest.mark.parametrize(("reason", "expected_calls"), [("page_crashed", 3), ("recaptcha_notice", 1)])
async def test_replacement_limit_and_site_refusal(monkeypatch, tmp_path, fake_browsers, reason, expected_calls):
    calls = []

    async def fake_run(group, output, request_seconds, *, next_job, on_progress, **kwargs):
        row = next_job()
        calls.append(row["n"])
        await on_progress(
            {"status": "stopped", "stop_reason": reason, "jobs": [{**row, "status": "failed", "documents": []}]}, final=True
        )

    monkeypatch.setattr(parallel, "run", fake_run)
    output = tmp_path / "run"
    result = await parallel.run_parallel(
        sample(10),
        output,
        workers=1,
        tabs=1,
        tab_restarts=2,
        request_seconds=10,
        ca_bundle=tmp_path / "ca",
        proxy_ca=None,
        max_seconds=10,
        proxy=parallel.FixedProxyPool([{"server": "http://192.0.2.1:80"}]),
        notify=ProgressReporter(None, output),
    )
    assert len(calls) == expected_calls
    assert result["tab_restarts"] == expected_calls - 1
    assert result["queue_remaining"] == 10 - expected_calls
    assert result["status"] == "stopped"


@pytest.mark.asyncio
@pytest.mark.parametrize("timeouts", [1, 2])
async def test_proxy_preflight_retries_timeout_in_same_context_and_closes_pages(tmp_path, timeouts):
    pages, attempts = [], []
    log = parallel.EventLog(tmp_path)

    class Page:
        def __init__(self, number):
            self.number = number
            self.closed = False

        async def goto(self, url, **kwargs):
            assert url == "https://api.ipify.org"
            if self.number <= timeouts:
                raise parallel.BrowserTimeout("test timeout")
            return SimpleNamespace(status=200)

        def locator(self, selector):
            return self

        async def inner_text(self):
            return "192.0.2.1"

        async def close(self):
            self.closed = True

    class Context:
        async def new_page(self):
            page = Page(len(pages) + 1)
            pages.append(page)
            return page

    context = Context()
    try:
        if timeouts == 2:
            with pytest.raises(parallel.BrowserTimeout):
                await parallel.proxy_preflight(context, 60, on_attempt=attempts.append, log=log, worker=1)
        else:
            assert await parallel.proxy_preflight(context, 60, on_attempt=attempts.append, log=log, worker=1) == "192.0.2.1"
    finally:
        log.close()
    assert attempts == [1, 2]
    assert all(page.closed for page in pages)
    events = [json.loads(line) for line in (tmp_path / "run.log").read_text().splitlines()]
    assert [event["will_retry"] for event in events] == ([True, False] if timeouts == 2 else [True])


@pytest.mark.asyncio
async def test_webshare_failure_replaces_proxy_without_duplicate_assignment(monkeypatch, tmp_path, fake_browsers):
    launches = []

    class Page:
        def __init__(self, server):
            self.server = server

        async def goto(self, *args, **kwargs):
            if self.server.endswith(".1:80"):
                raise parallel.BrowserTimeout("timeout")
            return SimpleNamespace(status=200)

        def locator(self, selector):
            return self

        async def inner_text(self):
            return "192.0.2.10"

        async def close(self):
            pass

    class Context:
        def __init__(self, server):
            self.server = server

        async def new_page(self):
            return Page(self.server)

    @asynccontextmanager
    async def browser(options, lock):
        launches.append(options["proxy"]["server"])
        yield Context(options["proxy"]["server"])

    async def run(group, output, request_seconds, *, next_job, on_progress, **kwargs):
        jobs = []
        while (row := next_job()) is not None:
            jobs.append({**row, "status": "completed", "documents": []})
        await on_progress({"status": "finished", "jobs": jobs}, final=True)

    monkeypatch.setattr(parallel, "browser_context", browser)
    monkeypatch.setattr(parallel, "run", run)
    output = tmp_path / "run"
    pool = parallel.WebshareProxyPool([{"server": f"http://192.0.2.{i}:80"} for i in (1, 2, 3)], 2)
    result = await parallel.run_parallel(
        sample(6),
        output,
        workers=2,
        tabs=1,
        request_seconds=1,
        ca_bundle=tmp_path / "ca",
        proxy_ca=None,
        max_seconds=10,
        proxy=pool,
        notify=ProgressReporter(None, output),
    )
    assert sorted(launches) == [f"http://192.0.2.{i}:80" for i in (1, 2, 3)]
    assert result["job_counts"] == {"completed": 6}
    assert result["workers"][0]["proxy_replacements"] == 1
    assert result["workers"][0]["proxy_failures"] == [{"type": "TimeoutError", "attempts": 2}]
    assert result["workers"][0]["proxy_preflight_attempts"] == 1
    assert result["status"] == "finished"
    assert pool.replacement() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [404, 200])
async def test_legacy_popup_response_or_download_is_observed_without_other_tab_events(status):
    class Emitter:
        def __init__(self):
            self.listeners = {}
            self.closed = False

        def on(self, name, callback):
            self.listeners[name] = callback

        def remove_listener(self, name, callback):
            del self.listeners[name]

        async def close(self):
            self.closed = True

    context, page, popup, unrelated = Emitter(), Emitter(), Emitter(), Emitter()
    page.context = context

    async def opener():
        return page

    async def no_opener():
        return None

    popup.opener = opener
    unrelated.opener = no_opener
    download = object()

    class Element:
        async def click(self, **kwargs):
            handler = context.listeners["response"]
            await handler(
                SimpleNamespace(
                    url="https://example.com" + batch.LEGACY_PATH, status=500, frame=SimpleNamespace(page=unrelated)
                )
            )
            page.listeners["popup"](popup)
            if status == 200:
                popup.listeners["download"](download)
            else:
                await handler(
                    SimpleNamespace(
                        url="https://example.com" + batch.LEGACY_PATH, status=404, frame=SimpleNamespace(page=popup)
                    )
                )

    result, code = await batch.download_once(page, Element(), batch.LEGACY_PATH, 1000)
    assert (result is download and code is None) if status == 200 else (result is None and code == 404)
    assert not page.listeners and not popup.listeners and not context.listeners
    assert popup.closed is (status == 404)


@pytest.mark.asyncio
async def test_legacy_expected_url_handles_popup_response_without_frame():
    class Emitter:
        def __init__(self):
            self.listeners = {}

        def on(self, name, handler):
            self.listeners[name] = handler

        def remove_listener(self, name, handler):
            del self.listeners[name]

    page, context = Emitter(), Emitter()
    page.context = context
    target = "https://example.com" + batch.LEGACY_PATH + "?file=one"

    class Element:
        async def click(self, **kwargs):
            handler = context.listeners["response"]
            await handler(SimpleNamespace(url=target.replace("one", "other"), status=500))
            # Provisional popup responses can lack a usable frame.
            await handler(SimpleNamespace(url=target, status=404))

    assert await batch.download_once(page, Element(), batch.LEGACY_PATH, 1000, expected_url=target) == (None, 404)
    assert not context.listeners
