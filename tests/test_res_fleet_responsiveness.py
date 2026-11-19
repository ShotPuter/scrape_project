"""Large catalog writes must not stop browser supervision or checkpoints."""

import asyncio
import sqlite3
import time
from types import SimpleNamespace

from src.proxy.sources.catalog import ProxyCandidate
from src.sites.res import fleet


async def test_catalog_import_yields_to_browser_supervision(tmp_path, monkeypatch):
    args = fleet.parse_args(["--providers", "free", "--sources", "monosans"])
    queue = fleet.FleetQueue(tmp_path / "fleet.sqlite3", "test")
    producer = fleet.FleetProducer(args, tmp_path, queue, asyncio.Event(), asyncio.Event())
    catalog = SimpleNamespace(
        candidates=[ProxyCandidate(f"http://11.0.{i // 250}.{i % 250 + 1}:80", "http", "monosans") for i in range(600)],
        errors={},
    )

    async def load(*args, **kwargs):
        return catalog

    def screen(rows):
        return [(endpoint, source) for endpoint, source, address in rows], {}

    monkeypatch.setattr(fleet, "load_public_catalog", load)
    monkeypatch.setattr(fleet, "filter_candidate_ips", screen)

    # Emulate a slow storage device. A real SQLite writer is still used.
    class SlowConnection(sqlite3.Connection):
        def execute(self, *args, **kwargs):
            if args[0].startswith("INSERT INTO fleet_routes"):
                time.sleep(0.001)
            return super().execute(*args, **kwargs)

        def executemany(self, *args, **kwargs):
            time.sleep(0.08)
            return super().executemany(*args, **kwargs)

    connect = sqlite3.connect

    def slow_connect(*args, **kwargs):
        kwargs["factory"] = SlowConnection
        return connect(*args, **kwargs)

    monkeypatch.setattr(fleet.sqlite3, "connect", slow_connect)
    original = queue.add_route

    def slow_add(*args, **kwargs):
        time.sleep(0.001)
        return original(*args, **kwargs)

    monkeypatch.setattr(queue, "add_route", slow_add)
    heartbeats = []
    done = asyncio.Event()

    async def supervise():
        while not done.is_set():
            heartbeats.append(time.monotonic())
            await asyncio.sleep(0.01)

    watcher = asyncio.create_task(supervise())
    try:
        await producer.refresh()
        await asyncio.sleep(0.01)
        assert producer.stats["queue_inserted"] == 600
        assert queue.counts()["routes"] == {"pending": 600}
        assert max(b - a for a, b in zip(heartbeats, heartbeats[1:], strict=False)) < 0.15
    finally:
        done.set()
        await watcher
        queue.close()


async def test_catalog_writer_preserves_existing_leases_and_reports_only_insertions(tmp_path):
    queue = fleet.FleetQueue(tmp_path / "fleet.sqlite3", "test")
    queue.add_route("free", "http://9.9.9.9:80", state="ready")
    route = queue.claim_route("browser", ["free"], {}, fleet.current_day(), 95)
    try:
        inserted = await asyncio.to_thread(
            queue.add_free_candidates,
            [("http://9.9.9.9:80", "catalog"), ("http://8.8.8.8:80", "catalog"), ("http://8.8.8.8:80", "catalog")],
        )
        assert inserted == 1
        assert queue.counts()["routes"] == {"leased": 1, "pending": 1}
        live = queue.database.execute("SELECT owner FROM fleet_routes WHERE route_id=?", (route["route_id"],)).fetchone()
        assert live[0] == "browser"
        queue.finish_route(route, documents=1)
        assert queue.provider_spent("free", fleet.current_day()) == 1
    finally:
        queue.close()
