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
    assert [row["processed"] for row in progress] == [*range(10, 101, 10), 100]
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
async def test_actual_tabs_share_context_but_keep_downloads_separate(monkeypatch, tmp_path):
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
        return [{"id": doc_id, "requires_captcha": True}], None

    async def download(page, element, endpoint, timeout_ms):
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
    assert all(job["status"] == "completed" for job in jobs)
    assert sum(report["pdf_count"] for report in results) == 6
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
