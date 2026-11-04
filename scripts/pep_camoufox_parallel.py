"""Independent Camoufox browsers with balanced work and serialized global reports."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import math
import secrets
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

from scripts.pep_camoufox_batch import run
from scripts.pep_camoufox_logging import EventLog, report_totals
from src.utils.atomic import atomic_write_json


def split_sample(sample: list[dict], workers: int) -> list[list[dict]]:
    if not 1 <= workers <= 4:
        raise ValueError("workers must be between 1 and 4")
    return [sample[index::workers] for index in range(min(workers, len(sample)))]


class BrightDataProxy:
    def __init__(self, server: str, username: str, password: str):
        try:
            parsed = urlsplit(server)
            valid = (
                parsed.scheme == "http"
                and parsed.hostname == "brd.superproxy.io"
                and parsed.port == 44445
                and not parsed.username
                and not parsed.password
                and not parsed.query
                and not parsed.fragment
                and parsed.path in {"", "/"}
            )
        except ValueError:
            valid = False
        if not valid or not username or not password or "-session-" in username:
            raise ValueError("Set the HTTP Bright Data 44445 server and base zone credentials without a session suffix")
        self.server, self.username, self.password = server.rstrip("/"), username, password
        self.session_prefix = "pep" + secrets.token_hex(8)

    def for_worker(self, number: int) -> dict:
        return {
            "server": self.server,
            "username": f"{self.username}-session-{self.session_prefix}{number}",
            "password": self.password,
        }


class AggregateReport:
    def __init__(self, sample: list[dict], groups: list[list[dict]], output: Path, notify):
        self.output, self.notify, self.started = output, notify, time.monotonic()
        self.lock = asyncio.Lock()
        self.log = EventLog(output)
        self.worker_updated = {}
        self.sample = sample
        self.workers = {
            index: {"status": "pending", "jobs": [{**row, "status": "pending", "documents": []} for row in group]}
            for index, group in enumerate(groups, 1)
        }

    def observe(self, index: int, report: dict) -> None:
        self.workers[index] = copy.deepcopy(report)
        self.worker_updated[index] = time.monotonic()

    def snapshot(self, status: str = "running") -> dict:
        jobs, workers, reasons = [], [], []
        for index, worker in self.workers.items():
            folder = f"worker-{index}"
            idle_seconds = (
                time.monotonic() - self.worker_updated.get(index, time.monotonic()) if worker["status"] == "running" else 0
            )
            elapsed = worker.get("seconds", 0) + idle_seconds
            stats = report_totals(worker)
            for job in copy.deepcopy(worker["jobs"]):
                job["worker"] = index
                for doc in job["documents"]:
                    for key in ("local_path", "archive_path"):
                        if doc.get(key):
                            doc[key] = f"{folder}/{doc[key]}"
                    for item in doc.get("files", []):
                        item["local_path"] = f"{folder}/{item['local_path']}"
                jobs.append(job)
            workers.append(
                {
                    "worker": index,
                    "status": worker["status"],
                    "links": len(worker["jobs"]),
                    "pdf_count": worker.get("pdf_count", 0),
                    "stop_reason": worker.get("stop_reason"),
                    "proxy_egress_ip": worker.get("proxy_egress_ip"),
                    "activity": worker.get("activity", {"stage": "pending"}),
                    "activity_seconds": round(max(0, elapsed - worker.get("activity", {}).get("started_seconds", 0)), 2),
                    "elapsed_seconds": round(elapsed, 2),
                    "links_per_minute": round(stats["processed"] * 60 / elapsed, 2) if elapsed else 0,
                    "declarations_per_minute": round(stats["downloaded"] * 60 / elapsed, 2) if elapsed else 0,
                    **stats,
                }
            )
            if worker.get("stop_reason") or worker["status"] == "error":
                reasons.append(f"{folder}:{worker.get('stop_reason', 'error')}")
        addresses = sorted({worker["proxy_egress_ip"] for worker in workers if worker.get("proxy_egress_ip")})
        return {
            "engine": "camoufox",
            "status": status,
            "jobs": sorted(jobs, key=lambda job: job["n"]),
            "workers": workers,
            "worker_count": len(workers),
            "proxy_egress_ips": addresses,
            "proxy_observed_browsers": sum(bool(worker.get("proxy_egress_ip")) for worker in workers),
            "seconds": round(time.monotonic() - self.started, 2),
            "job_counts": dict(Counter(job["status"] for job in jobs)),
            "pdf_count": sum(worker.get("pdf_count", 0) for worker in self.workers.values()),
            "archive_count": sum(worker.get("archive_count", 0) for worker in self.workers.values()),
            "sample_sha256": hashlib.sha256(json.dumps(self.sample, sort_keys=True).encode()).hexdigest(),
            "stop_reason": "; ".join(reasons) or None,
        }

    async def update(self, index: int, report: dict) -> None:
        # Store copies: other browsers cannot mutate a snapshot while Telegram is sending it.
        async with self.lock:
            self.observe(index, report)
            combined = self.snapshot()
            atomic_write_json(self.output / "report.json", combined)
            self.log.summary("worker_progress", combined, worker=index)
        await self.notify(combined, final=False)

    async def heartbeat(self) -> None:
        async with self.lock:
            combined = self.snapshot()
            atomic_write_json(self.output / "report.json", combined)
            self.log.summary("heartbeat", combined, interval=True, workers=combined["workers"])

    async def finish(self, status: str | None = None) -> dict:
        async with self.lock:
            if status is None:
                statuses = {worker["status"] for worker in self.workers.values()}
                status = "error" if "error" in statuses else "stopped" if "stopped" in statuses else "finished"
            combined = self.snapshot(status)
            atomic_write_json(self.output / "report.json", combined)
            self.log.summary("run_finished", combined, workers=combined["workers"])
            self.log.close()
        await self.notify(combined, final=True)
        return combined


async def run_parallel(
    sample: list[dict],
    output: Path,
    *,
    workers: int,
    request_seconds: float,
    ca_bundle: Path,
    proxy_ca: Path,
    max_seconds: float,
    proxy: BrightDataProxy | None,
    notify,
    log_interval: float = 30,
) -> dict:
    if not math.isfinite(log_interval) or log_interval <= 0:
        raise ValueError("log_interval must be finite and positive")
    if workers > 1 and proxy is None:
        raise ValueError("Multiple browsers require Bright Data credentials; use --workers 1 for a direct connection")
    groups = split_sample(sample, workers)
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False, mode=0o700)
    atomic_write_json(output / "sample.json", sample)
    aggregate = AggregateReport(sample, groups, output, notify)
    launch_lock = asyncio.Lock()
    atomic_write_json(output / "report.json", aggregate.snapshot())
    aggregate.log.summary("run_started", aggregate.snapshot(), worker_count=len(groups), log_interval=log_interval)
    stop_heartbeat = asyncio.Event()
    finished_workers = 0

    async def heartbeat():
        while not stop_heartbeat.is_set():
            try:
                await asyncio.wait_for(stop_heartbeat.wait(), timeout=log_interval)
            except TimeoutError:
                await aggregate.heartbeat()

    async def worker(index, group):
        async def progress(report, *, final):
            await aggregate.update(index, report)

        nonlocal finished_workers
        try:
            await run(
                group,
                output / f"worker-{index}",
                request_seconds,
                ca_bundle=ca_bundle,
                max_seconds=max_seconds,
                on_progress=progress,
                on_state=lambda state: aggregate.observe(index, state),
                proxy=proxy.for_worker(index) if proxy else None,
                proxy_ca=proxy_ca if proxy else None,
                launch_lock=launch_lock,
            )
        finally:
            finished_workers += 1
            if finished_workers == len(groups):
                stop_heartbeat.set()

    try:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(heartbeat())
            for index, group in enumerate(groups, 1):
                tasks.create_task(worker(index, group))
    except BaseException as exc:
        status = "interrupted" if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt)) else "error"
        await aggregate.finish(status)
        raise
    return await aggregate.finish()
