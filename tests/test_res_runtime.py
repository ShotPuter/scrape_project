import argparse
import asyncio
import json
import sys
from types import SimpleNamespace

import pytest

from src.sites.dequienes.queue import QueueStore
from src.sites.res import __main__ as search, download
from src.sites.res.pdf import persist_pdf
from src.sites.res.runtime import BrowserLayout, BrowserPool, browser_layout, checkpoints, validate_runtime, wait_turn
from src.sites.res.source import SearchIssue


class FakeBrowser:
    def __init__(self, launches, **options):
        self.options = options
        self.closed = False
        self.pages = []
        launches.append(self)

    async def __aenter__(self):
        await asyncio.sleep(0)
        return self

    async def __aexit__(self, *args):
        self.closed = True

    async def new_page(self):
        assert not self.closed
        page = SimpleNamespace(browser=self, tab=len(self.pages))
        self.pages.append(page)
        return page


def fake_browsers(monkeypatch):
    launches = []
    monkeypatch.setitem(
        sys.modules, "camoufox.async_api", SimpleNamespace(AsyncCamoufox=lambda **kwargs: FakeBrowser(launches, **kwargs))
    )
    return launches


def args(output, **overrides):
    values = {
        "output": output,
        "profile": output / "profile",
        "action": "collect",
        "company_id": None,
        "cve": None,
        "limit": 100,
        "run_id": "pool-test",
        "retry_errors": False,
        "max_attempts": 3,
        "workers": None,
        "browsers": 5,
        "tabs_per_browser": 2,
        "checkpoint_interval": 15,
        "rate_per_minute": 6,
        "timeout": 60,
        "captcha_wait": 30,
        "headed": False,
    }
    return argparse.Namespace(**(values | overrides))


def seed(output, count):
    store = QueueStore(output / "companies.sqlite3")
    with store.transaction():
        for index in range(count):
            key = f"company-{index:03}"
            store.db.execute("INSERT INTO companies VALUES (?,?,?,?,?)", (key, key, key, None, "name_candidate"))
    store.close()


def test_layout_defaults_legacy_and_invalid_combinations():
    assert browser_layout(argparse.Namespace()) == BrowserLayout(1, 1)
    assert browser_layout(argparse.Namespace(workers=4)) == BrowserLayout(1, 4)
    assert browser_layout(argparse.Namespace(browsers=5, workers=2)).workers == 10
    for values in [{"browsers": 0}, {"tabs_per_browser": -1}, {"workers": 2, "tabs_per_browser": 2}]:
        with pytest.raises(ValueError):
            browser_layout(argparse.Namespace(**values))
    for interval in [0, float("nan"), float("inf")]:
        with pytest.raises(ValueError):
            validate_runtime(argparse.Namespace(checkpoint_interval=interval))


@pytest.mark.parametrize("stage", [search, download])
async def test_five_browsers_two_tabs_share_limit_and_resume_without_duplicates(tmp_path, monkeypatch, stage):
    seed(tmp_path, 120)
    if stage is download:
        store = QueueStore(tmp_path / "companies.sqlite3")
        with store.transaction():
            for index in range(120):
                store.db.execute("INSERT INTO documents(cve) VALUES (?)", (f"CODE{index:06}",))
        store.close()
    launches = fake_browsers(monkeypatch)
    monkeypatch.setattr(QueueStore, "request_delay", lambda *args: 0)
    active = set()
    max_active = 0
    barrier = asyncio.Event()
    seen = []

    async def process(page, value, *rest, **kwargs):
        nonlocal max_active
        key = value["company_id"] if stage is search else value
        assert key not in active
        active.add(key)
        max_active = max(max_active, len(active))
        if len(active) == 10:
            barrier.set()
        await asyncio.wait_for(barrier.wait(), 2)
        await asyncio.sleep(0)
        active.remove(key)
        seen.append(key)
        if stage is search:
            return {
                **value,
                "status": "not_found",
                "events": [],
                "source_url": "https://www.registrodeempresasysociedades.cl/BuscarActuaciones2.aspx",
            }
        return persist_pdf(
            tmp_path,
            value,
            b"%PDF-1.4\n%%EOF\n",
            f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={value}",
        )

    monkeypatch.setattr(stage, "lookup" if stage is search else "download_certificate", process)
    options = args(tmp_path, action="collect" if stage is search else "download")
    assert await stage.run(options) == 0
    assert len(launches) == 5
    assert all(len(browser.pages) == 2 and browser.closed for browser in launches)
    assert len({browser.options["user_data_dir"] for browser in launches}) == 5
    assert max_active == 10
    assert len(seen) == len(set(seen)) == 100
    # Same run ID: the 20 unselected jobs must remain unselected, even if the pool size changes.
    options.browsers, options.tabs_per_browser = 2, 3
    assert await stage.run(options) == 0
    assert len(launches) == 5
    store = QueueStore(tmp_path / "companies.sqlite3")
    try:
        queue = "res_company_lookup" if stage is search else "certificate_download"
        expected_status = "not_found" if stage is search else "completed"
        assert store.status()["queues"][queue] == {expected_status: 100, "pending": 20}
        assert store.db.execute("SELECT claimed FROM runs WHERE run_id='pool-test'").fetchone() == (100,)
        assert store.db.execute("SELECT sum(attempts) FROM jobs WHERE queue=?", (queue,)).fetchone() == (100,)
    finally:
        store.close()


async def test_pool_keeps_persistent_tabs_alive_and_reuses_stable_profiles(tmp_path, monkeypatch):
    launches = fake_browsers(monkeypatch)
    profile = tmp_path / "profile"
    async with BrowserPool(profile, BrowserLayout(2, 2)) as pool:
        pages = await asyncio.gather(*(pool.page(b, t) for b, t in pool.layout.slots()))
        assert len(launches) == 2
        assert await pool.page(0, 0) is pages[0]
        expected = [profile, tmp_path / "profile-browser-2"]
        assert pool.profiles == expected
        assert all(not context.closed for context in launches)
    async with BrowserPool(profile, BrowserLayout(2, 2)) as resumed:
        assert resumed.profiles == expected
        await resumed.page(1, 0)
    assert all(context.closed for context in launches)


async def test_periodic_checkpoint_records_running_jobs_and_interrupt_preserves_results(tmp_path, monkeypatch):
    seed(tmp_path, 12)
    fake_browsers(monkeypatch)
    monkeypatch.setattr(QueueStore, "request_delay", lambda *args: 0)
    first_saved = asyncio.Event()
    others_started = asyncio.Event()

    async def lookup(page, company, **kwargs):
        if company["company_id"] == "company-000":
            first_saved.set()
            return {
                **company,
                "status": "not_found",
                "source_url": "https://www.registrodeempresasysociedades.cl/BuscarActuaciones2.aspx",
                "events": [],
            }
        others_started.set()
        await asyncio.Event().wait()
        return None

    monkeypatch.setattr(search, "lookup", lookup)
    # Faster timer for this test; the public CLI still validates intervals >= 1 second.
    from src.sites.res.runtime import checkpoints as actual_checkpoints

    monkeypatch.setattr(search, "checkpoints", lambda save, interval: actual_checkpoints(save, 0.01))
    options = args(tmp_path, limit=10)
    task = asyncio.create_task(search.run(options))
    await asyncio.wait_for(first_saved.wait(), 2)
    await asyncio.wait_for(others_started.wait(), 2)
    await asyncio.sleep(0.04)
    running = read_report(tmp_path / "res_search_report.json")
    assert running["state"] == "running"
    assert running["progress"]["statuses"]["running"] > 0
    assert running["summary"] == {"not_found": 1}
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    stopped = read_report(tmp_path / "res_search_report.json")
    assert stopped["state"] == "interrupted"
    assert not stopped["progress"]["statuses"].get("running")
    assert stopped["summary"] == {"not_found": 1}
    store = QueueStore(tmp_path / "companies.sqlite3")
    with store.transaction():
        store.db.execute("UPDATE jobs SET available_at=0 WHERE status='retry_wait'")
    store.close()

    async def resumed_lookup(page, company, **kwargs):
        assert company["company_id"] != "company-000"
        return {
            **company,
            "status": "not_found",
            "source_url": "https://www.registrodeempresasysociedades.cl/BuscarActuaciones2.aspx",
            "events": [],
        }

    monkeypatch.setattr(search, "lookup", resumed_lookup)
    assert await search.run(options) == 0
    resumed = read_report(tmp_path / "res_search_report.json")
    assert resumed["summary"] == {"not_found": 10}
    assert resumed["progress"]["claimed"] == 10


def read_report(path):
    return json.loads(path.read_text())


async def test_stop_interrupts_long_global_rate_wait(monkeypatch):
    stop = asyncio.Event()
    store = SimpleNamespace(request_delay=lambda *args: 600)
    task = asyncio.create_task(wait_turn(store, "res-search", 6, stop))
    await asyncio.sleep(0)
    stop.set()
    assert await asyncio.wait_for(task, 0.2) is False


async def test_checkpoint_failure_propagates_instead_of_silently_stopping_updates():
    calls = 0

    def save():
        nonlocal calls
        calls += 1
        if calls > 1:
            raise OSError("disk full")

    with pytest.raises(ExceptionGroup, match="TaskGroup"):
        async with checkpoints(save, 0.01):
            await asyncio.sleep(1)


@pytest.mark.parametrize("stage", [search, download])
async def test_captcha_stops_all_browsers_and_resume_keeps_saved_results(tmp_path, monkeypatch, stage):
    seed(tmp_path, 20)
    if stage is download:
        store = QueueStore(tmp_path / "companies.sqlite3")
        with store.transaction():
            for index in range(20):
                store.db.execute("INSERT INTO documents(cve) VALUES (?)", (f"CODE{index:06}",))
        store.close()
    launches = fake_browsers(monkeypatch)
    monkeypatch.setattr(QueueStore, "request_delay", lambda *args: 0)
    started = 0
    ready, captcha = asyncio.Event(), asyncio.Event()
    seen = []
    resuming = False

    async def process(page, value, *rest, **kwargs):
        nonlocal started
        key = value["company_id"] if stage is search else value
        seen.append(key)
        if not resuming:
            started += 1
            if started == 10:
                ready.set()
            await asyncio.wait_for(ready.wait(), 2)
            if key.endswith("009"):
                captcha.set()
                raise SearchIssue("captcha_required", "Operator needed")
            await asyncio.wait_for(captcha.wait(), 2)
        if stage is search:
            return {
                **value,
                "status": "not_found",
                "events": [],
                "source_url": "https://www.registrodeempresasysociedades.cl/BuscarActuaciones2.aspx",
            }
        return persist_pdf(
            tmp_path, value, b"%PDF-1.4\n%%EOF\n", f"https://www.registrodeempresasysociedades.cl/pdf?cve={value}"
        )

    monkeypatch.setattr(stage, "lookup" if stage is search else "download_certificate", process)
    options = args(tmp_path, limit=20, action="collect" if stage is search else "download")
    assert await stage.run(options) == 1
    assert len(seen) == 10
    assert len(launches) == 5
    queue = "res_company_lookup" if stage is search else "certificate_download"
    success = "not_found" if stage is search else "completed"
    store = QueueStore(tmp_path / "companies.sqlite3")
    assert store.status()["queues"][queue] == {success: 9, "captcha_required": 1, "pending": 10}
    store.close()
    first_profiles = {browser.options["user_data_dir"] for browser in launches}
    resuming = True
    options.retry_errors = True
    assert await stage.run(options) == 0
    assert len(seen) == 21
    assert len(set(seen)) == 20
    assert {browser.options["user_data_dir"] for browser in launches} == first_profiles
    store = QueueStore(tmp_path / "companies.sqlite3")
    try:
        assert store.status()["queues"][queue] == {success: 20}
        assert store.db.execute("SELECT claimed FROM runs WHERE run_id='pool-test'").fetchone() == (20,)
    finally:
        store.close()
