"""Parallel scheduling and proxy isolation without browser or external requests."""

import asyncio
import json

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


@pytest.mark.asyncio
async def test_four_concurrent_browsers_global_progress_and_paths(monkeypatch, tmp_path):
    active, maximum = 0, 0
    assignments, users = [], []

    async def fake_run(group, output, request_seconds, *, on_progress, proxy, **kwargs):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        assignments.append(group)
        users.append(proxy["username"])
        await asyncio.sleep(0)
        state = {
            "status": "running",
            "pdf_count": 0,
            "archive_count": 0,
            "jobs": [{**row, "status": "pending", "documents": []} for row in group],
        }
        for job in state["jobs"]:
            job.update(
                status="completed",
                documents=[
                    {"status": "downloaded", "local_path": "pdf/example.pdf", "files": [{"local_path": "pdf/example.pdf"}]}
                ],
            )
            state["pdf_count"] += 1
            await on_progress(state, final=False)
            await asyncio.sleep(0)
        state["status"] = "finished"
        await on_progress(state, final=True)
        active -= 1
        return state

    monkeypatch.setattr(parallel, "run", fake_run)
    output = tmp_path / "run"
    notify = ProgressReporter(None, output)
    result = await parallel.run_parallel(
        sample(100),
        output,
        workers=4,
        request_seconds=10,
        ca_bundle=tmp_path / "ca.pem",
        proxy_ca=tmp_path / "proxy.pem",
        max_seconds=100,
        proxy=parallel.BrightDataProxy("http://brd.superproxy.io:44445", "test-zone", "test-password"),
        notify=notify,
    )
    assert maximum == 4
    assert [len(group) for group in assignments] == [25] * 4
    assert len(set(users)) == 4
    assert result["job_counts"] == {"completed": 100}
    assert result["pdf_count"] == 100
    progress = [json.loads(line) for line in (output / "progress.jsonl").read_text().splitlines()]
    assert [row["processed"] for row in progress] == [*range(10, 101, 10), 100]
    assert sum(row["final"] for row in progress) == 1
    assert result["jobs"][0]["documents"][0]["local_path"] == "worker-1/pdf/example.pdf"
    assert result["jobs"][1]["documents"][0]["files"][0]["local_path"] == "worker-2/pdf/example.pdf"


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
