"""Independent Camoufox browsers with balanced work and serialized global reports."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import ipaddress
import json
import math
import secrets
import time
from collections import Counter
from contextlib import AsyncExitStack, suppress
from pathlib import Path
from urllib.parse import unquote, urlsplit

from playwright.async_api import Error as BrowserError, TimeoutError as BrowserTimeout

from scripts.pep_camoufox_batch import browser_context, run
from scripts.pep_camoufox_db import StateDB
from scripts.pep_camoufox_export import write_export
from scripts.pep_camoufox_logging import EventLog, report_totals, resource_snapshot
from scripts.pep_camoufox_pilot import install_intermediate, install_proxy_ca
from scripts.pep_camoufox_resume import materialize_history
from src.utils.atomic import atomic_write_json


async def proxy_preflight(context, request_seconds, *, on_attempt, log, worker):
    """Retry a timed-out IP check once, within the same browser and proxy session."""
    for attempt in (1, 2):
        on_attempt(attempt)
        page = None
        try:
            page = await context.new_page()
            response = await page.goto(
                "https://api.ipify.org", wait_until="domcontentloaded", timeout=int(request_seconds * 1000)
            )
            if response is None or response.status != 200:
                raise ValueError("Proxy browser preflight failed")
            address = (await page.locator("body").inner_text()).strip()
            return str(ipaddress.ip_address(address))
        except (BrowserTimeout, TimeoutError):
            log.emit("proxy_preflight_timeout", worker=worker, attempt=attempt, will_retry=attempt == 1)
            if attempt == 2:
                raise
        finally:
            if page is not None:
                with suppress(Exception):
                    await page.close()
    raise AssertionError("Unreachable")


class FixedProxyPool:
    """Assign one provider proxy per browser without rotating on failures."""

    def __init__(self, proxies: list[dict]):
        self._proxies = [dict(proxy) for proxy in proxies]

    def for_worker(self, number: int) -> dict:
        if not 1 <= number <= len(self._proxies):
            raise ValueError("No assigned proxy for this browser")
        return dict(self._proxies[number - 1])


class WebshareProxyPool(FixedProxyPool):
    def __init__(self, proxies: list[dict], workers: int):
        super().__init__(proxies)
        self._next = workers

    def replacement(self) -> dict | None:
        if self._next >= len(self._proxies):
            return None
        proxy = dict(self._proxies[self._next])
        self._next += 1
        return proxy


async def webshare_pool(token: str, workers: int) -> FixedProxyPool:
    from src.proxy.sources.webshare import load_webshare_candidates

    candidates = await load_webshare_candidates(token, mode="direct")
    proxies, addresses = [], set()
    for candidate in candidates:
        parsed = urlsplit(candidate.url)
        if parsed.hostname in addresses:
            continue
        addresses.add(parsed.hostname)
        host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
        proxies.append(
            {
                "server": f"http://{host}:{parsed.port}",
                "username": unquote(parsed.username or ""),
                "password": unquote(parsed.password or ""),
            }
        )
    if len(proxies) < workers:
        raise ValueError(f"Webshare supplied only {len(proxies)} distinct valid proxy addresses; requested {workers}")
    return WebshareProxyPool(proxies, workers)


def split_sample(sample: list[dict], workers: int) -> list[list[dict]]:
    if not 1 <= workers <= 100:
        raise ValueError("workers must be between 1 and 100")
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
    def __init__(self, sample: list[dict], groups: list[list[dict]], output: Path, notify, *, dynamic=False, tabs=1):
        self.output, self.notify, self.started = output, notify, time.monotonic()
        self.lock = asyncio.Lock()
        self.log = EventLog(output)
        self.worker_updated = {}
        self.last_flush = float("-inf")
        self.flush_interval = 15.0
        self.last_notified_count = 0
        self.database = None
        self.sample = sample
        self.resumed_jobs = []
        self.dynamic, self.tabs = dynamic, tabs
        self.initial_resources = resource_snapshot()
        self.workers = {
            index: {
                "status": "pending",
                "jobs": [] if dynamic else [{**row, "status": "pending", "documents": []} for row in group],
            }
            for index, group in enumerate(groups, 1)
        }

    def observe(self, index: int, report: dict) -> None:
        self.workers[index] = copy.deepcopy(report)
        self.worker_updated[index] = time.monotonic()

    def snapshot(self, status: str = "running") -> dict:
        jobs, workers, reasons = copy.deepcopy(self.resumed_jobs), [], []
        for index, worker in self.workers.items():
            folder = f"worker-{index}"
            idle_seconds = (
                time.monotonic() - self.worker_updated.get(index, time.monotonic()) if worker["status"] == "running" else 0
            )
            elapsed = worker.get("seconds", 0) + idle_seconds
            stats = report_totals(worker)
            tab_snapshots = copy.deepcopy(worker.get("tabs", []))
            for tab in tab_snapshots:
                if tab["status"] == "running":
                    tab["activity_seconds"] = round(tab.get("activity_seconds", 0) + idle_seconds, 2)
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
                    "browser_restarts": worker.get("browser_restarts", 0),
                    "pdf_count": worker.get("pdf_count", 0),
                    "stop_reason": worker.get("stop_reason"),
                    "error": worker.get("error"),
                    "failure_stage": worker.get("failure_stage"),
                    "proxy_preflight_attempts": worker.get("proxy_preflight_attempts", 0),
                    "proxy_replacements": worker.get("proxy_replacements", 0),
                    "proxy_failures": worker.get("proxy_failures", []),
                    "proxy_egress_ip": worker.get("proxy_egress_ip"),
                    "tabs": tab_snapshots,
                    "tab_restarts": sum(tab.get("restarts", 0) for tab in tab_snapshots),
                    "activity": worker.get("activity", {"stage": "pending"}),
                    "activity_seconds": round(max(0, elapsed - worker.get("activity", {}).get("started_seconds", 0)), 2),
                    "elapsed_seconds": round(elapsed, 2),
                    "links_per_minute": round(stats["processed"] * 60 / elapsed, 2) if elapsed else 0,
                    "declarations_per_minute": round(stats["downloaded"] * 60 / elapsed, 2) if elapsed else 0,
                    **stats,
                }
            )
            if worker.get("stop_reason") or worker["status"] == "error":
                reasons.append(f"{folder}:{worker.get('stop_reason') or 'error'}")
        if self.dynamic:
            assigned = {job["n"] for job in jobs}
            jobs.extend({**row, "status": "pending", "documents": []} for row in self.sample if row["n"] not in assigned)
        addresses = sorted({worker["proxy_egress_ip"] for worker in workers if worker.get("proxy_egress_ip")})
        resources = resource_snapshot()
        initial_events = self.initial_resources.get("memory.events", {})
        resources["memory_events_since_start"] = {
            key: max(0, value - initial_events.get(key, 0)) for key, value in resources.get("memory.events", {}).items()
        }
        return {
            "engine": "camoufox",
            "status": status,
            "jobs": sorted(jobs, key=lambda job: job["n"]),
            "workers": workers,
            "worker_count": len(workers),
            "resumed_links": len(self.resumed_jobs),
            "tabs_per_browser": self.tabs,
            "tab_count": len(workers) * self.tabs,
            "tab_restarts": sum(worker["tab_restarts"] for worker in workers),
            "browser_restarts": sum(worker["browser_restarts"] for worker in workers),
            "queue_remaining": sum(job["status"] == "pending" for job in jobs),
            "resources": resources,
            "proxy_egress_ips": addresses,
            "proxy_observed_browsers": sum(bool(worker.get("proxy_egress_ip")) for worker in workers),
            "seconds": round(time.monotonic() - self.started, 2),
            "job_counts": dict(Counter(job["status"] for job in jobs)),
            "pdf_count": sum(worker.get("pdf_count", 0) for worker in self.workers.values())
            + sum(
                len(doc.get("files") or [doc])
                for job in self.resumed_jobs
                for doc in job["documents"]
                if doc["status"] == "downloaded"
            ),
            "archive_count": sum(worker.get("archive_count", 0) for worker in self.workers.values()),
            "sample_sha256": hashlib.sha256(json.dumps(self.sample, sort_keys=True).encode()).hexdigest(),
            "stop_reason": "; ".join(reasons) or None,
        }

    async def update(self, index: int, report: dict) -> None:
        # Store copies: other browsers cannot mutate a snapshot while Telegram is sending it.
        async with self.lock:
            self.observe(index, report)
            if self.database is not None:
                jobs = copy.deepcopy(report["jobs"])
                for job in jobs:
                    job["worker"] = index
                    for doc in job["documents"]:
                        for item in [doc, *doc.get("files", [])]:
                            for key in ("local_path", "archive_path"):
                                if item.get(key):
                                    item[key] = f"worker-{index}/{item[key]}"
                self.database.stage(jobs)
                if len(self.database.pending) >= self.database.batch_size:
                    await asyncio.to_thread(self.database.flush)
            # Per-tab checkpoints retain each result. Do not rewrite the entire
            # workbook/export for every completed link in a large run.
            flush_due = time.monotonic() - self.last_flush >= self.flush_interval
            processed = sum(report_totals(worker)["processed"] for worker in self.workers.values()) + len(self.resumed_jobs)
            count_notice = (
                not getattr(self.notify, "interval_seconds", 0) and processed // 10 > self.last_notified_count // 10
            )
            if not flush_due and not count_notice:
                return
            combined = self.snapshot()
            if flush_due:
                await asyncio.to_thread(self.persist, combined)
                self.last_flush = time.monotonic()
                self.log.summary("worker_progress", combined, worker=index)
            self.last_notified_count = processed
        await self.notify(combined, final=False)

    def persist(self, report):
        if self.database is not None:
            self.database.stage(report["jobs"])
            self.database.flush(
                force=report["status"] != "running" or time.monotonic() - self.database.last_commit >= 60, metadata=report
            )
            return
        atomic_write_json(self.output / "report.json", report)
        write_export(self.output, report)

    async def heartbeat(self) -> None:
        async with self.lock:
            combined = self.snapshot()
            await asyncio.to_thread(self.persist, combined)
            self.last_flush = time.monotonic()
            self.log.summary(
                "heartbeat", combined, interval=True, workers=combined["workers"], resources=combined["resources"]
            )
        await self.notify(combined, final=False)

    async def finish(self, status: str | None = None) -> dict:
        async with self.lock:
            if status is None:
                statuses = {worker["status"] for worker in self.workers.values()}
                status = "error" if "error" in statuses else "stopped" if "stopped" in statuses else "finished"
            combined = self.snapshot(status)
            if status == "finished" and combined["queue_remaining"]:
                combined["status"] = "stopped"
                combined["stop_reason"] = "queue_unprocessed"
            incomplete = [
                {
                    "job": job["n"],
                    "worker": job.get("worker"),
                    "tab": job.get("tab"),
                    "document": doc.get("id"),
                    "status": doc["status"],
                    "http_status": doc.get("http_status", doc.get("observed_http_status")),
                    "error": doc.get("error"),
                    "recaptcha_notice": doc.get("recaptcha_notice", False),
                }
                for job in combined["jobs"]
                for doc in job["documents"]
                if doc["status"] not in {"downloaded", "unavailable_http_404"}
            ]
            combined["incomplete_document_counts"] = dict(Counter(doc["status"] for doc in incomplete))
            if self.database is None:
                atomic_write_json(self.output / "incomplete-documents.json", incomplete)
            await asyncio.to_thread(self.persist, combined)
            self.log.summary("run_finished", combined, workers=combined["workers"], resources=combined["resources"])
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
    proxy_ca: Path | None,
    max_seconds: float,
    proxy: BrightDataProxy | FixedProxyPool | None,
    notify,
    log_interval: float = 30,
    tabs: int = 1,
    tab_restarts: int = 3,
    reusable=None,
    resume_history=None,
    browser_max_seconds: float = 1200,
    browser_max_links: int = 40,
    sqlite_state: bool = False,
) -> dict:
    if not math.isfinite(browser_max_seconds) or browser_max_seconds <= 0 or browser_max_links < 1:
        raise ValueError("Browser lifetime and link limit must be positive")
    if not math.isfinite(log_interval) or log_interval <= 0:
        raise ValueError("log_interval must be finite and positive")
    if workers > 1 and proxy is None and not resume_history:
        raise ValueError("Multiple browsers require proxy credentials; use --workers 1 for a direct connection")
    if not 1 <= tabs <= 10:
        raise ValueError("tabs must be between 1 and 10")
    if not 0 <= tab_restarts <= 100:
        raise ValueError("tab_restarts must be between 0 and 100")
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False, mode=0o700)
    resumed, copied, known_unavailable = await asyncio.to_thread(materialize_history, resume_history or {}, sample, output)
    reusable = {**(reusable or {}), **copied}
    resumed_urls = {job["url"] for job in resumed}
    pending_sample = [row for row in sample if row["url"] not in resumed_urls]
    if pending_sample and workers > 1 and proxy is None:
        raise ValueError("Multiple browsers require proxy credentials; use --workers 1 for a direct connection")
    print(
        f"Resume: {len(resumed)} links complete; {len(copied)} declarations reused; {len(pending_sample)} links queued",
        flush=True,
    )
    groups = split_sample(pending_sample, workers) if pending_sample else []
    queue = asyncio.Queue()
    for row in pending_sample:
        queue.put_nowait(row)

    def next_job():
        try:
            return queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    if not sqlite_state:
        atomic_write_json(output / "sample.json", sample)
    aggregate = AggregateReport(sample, groups, output, notify, dynamic=True, tabs=tabs)
    aggregate.resumed_jobs = resumed
    if sqlite_state:
        aggregate.database = await asyncio.to_thread(StateDB, output / "state.sqlite3")
        initial = aggregate.snapshot()
        aggregate.database.stage(initial["jobs"])
        await asyncio.to_thread(aggregate.database.flush, force=True, metadata=initial)
    launch_lock = asyncio.Lock()
    if not sqlite_state:
        atomic_write_json(output / "report.json", aggregate.snapshot())
    aggregate.log.summary("run_started", aggregate.snapshot(), worker_count=len(groups), log_interval=log_interval)
    stop_heartbeat = asyncio.Event()
    if not groups:
        return await aggregate.finish()
    finished_workers = 0
    startup_gate = asyncio.Event()
    startup_finished, ready_workers = set(), set()
    start_notice_lock = asyncio.Lock()
    start_notified = False

    def startup_done(index, *, ready=False):
        if index in startup_finished:
            return
        startup_finished.add(index)
        if ready:
            ready_workers.add(index)
        aggregate.log.emit(
            "browser_startup", worker=index, ready=ready, resolved=len(startup_finished), expected=len(groups)
        )
        if len(startup_finished) == len(groups):
            aggregate.log.emit(
                "queue_released", ready_browsers=len(ready_workers), failed_browsers=len(groups) - len(ready_workers)
            )
            startup_gate.set()

    async def heartbeat():
        while not stop_heartbeat.is_set():
            try:
                await asyncio.wait_for(stop_heartbeat.wait(), timeout=log_interval)
            except TimeoutError:
                await aggregate.heartbeat()

    async def worker(index, group):
        nonlocal finished_workers, start_notified
        folder = output / f"worker-{index}"
        assigned_proxy = proxy.for_worker(index) if proxy else None
        state = {"status": "running", "jobs": [], "tabs": [], "pdf_count": 0, "archive_count": 0}
        tab_states = {}
        tab_updated = {}
        tab_segments = {}
        browser_generation = 0
        rotation_reason = None
        cycle_claimed = 0
        cycle_deadline = float("inf")
        browser_started = time.monotonic()
        stop_browser = asyncio.Event()
        context_closing = False

        def browser_next_job():
            nonlocal rotation_reason, cycle_claimed
            if rotation_reason or stop_browser.is_set():
                return None
            if not queue.empty() and (time.monotonic() >= cycle_deadline or cycle_claimed >= browser_max_links):
                rotation_reason = "age_limit" if time.monotonic() >= cycle_deadline else "link_limit"
                aggregate.log.emit(
                    "browser_draining",
                    worker=index,
                    reason=rotation_reason,
                    claimed=cycle_claimed,
                    generation=browser_generation,
                )
                return None
            row = next_job()
            if row is not None:
                cycle_claimed += 1
            return row

        def activity(stage):
            state["activity"] = {"stage": stage, "started_seconds": time.monotonic() - browser_started}
            combine()

        def combine():
            state["seconds"] = round(time.monotonic() - browser_started, 2)
            state["jobs"], state["tabs"] = [], []
            state["pdf_count"] = state["archive_count"] = 0
            for number, tab in sorted(tab_states.items()):
                stats = report_totals(tab)
                state["tabs"].append(
                    {
                        "tab": number,
                        "restarts": tab.get("restarts", 0),
                        "recovery_history": tab.get("recovery_history", []),
                        "status": tab["status"],
                        "activity": tab.get("activity"),
                        "activity_seconds": round(
                            max(
                                0,
                                tab.get("seconds", 0)
                                + (time.monotonic() - tab_updated[number] if tab["status"] == "running" else 0)
                                - tab.get("activity", {}).get("started_seconds", 0),
                            ),
                            2,
                        ),
                        "stop_reason": tab.get("stop_reason"),
                        **stats,
                    }
                )
                state["pdf_count"] += tab.get("pdf_count", 0)
                state["archive_count"] += tab.get("archive_count", 0)
                for job in copy.deepcopy(tab["jobs"]):
                    job["tab"] = number
                    for doc in job["documents"]:
                        for key in ("local_path", "archive_path"):
                            if doc.get(key):
                                doc[key] = f"tab-{number}/{doc[key]}"
                        for item in doc.get("files", []):
                            item["local_path"] = f"tab-{number}/{item['local_path']}"
                    state["jobs"].append(job)
            aggregate.observe(index, state)

        async def tab_worker(number, context):
            generations = tab_segments.setdefault(number, {})
            generation = max(generations, default=-1) + 1
            crash_restarts = 0
            slot_started = time.monotonic()
            generation_offset = 0.0
            tab_folder = folder / f"tab-{number}"

            def observe(report):
                generations[generation] = copy.deepcopy(report)
                merged = copy.deepcopy(report)
                merged.update(
                    jobs=[],
                    pdf_count=0,
                    archive_count=0,
                    restarts=sum(
                        segment.get("stop_reason") in {"page_closed", "page_crashed"}
                        for segment_index, segment in generations.items()
                        if segment_index < generation
                    ),
                    recovery_history=[],
                    seconds=round(time.monotonic() - slot_started, 2),
                )
                if merged.get("activity"):
                    merged["activity"]["started_seconds"] += generation_offset
                for segment_index, segment in sorted(generations.items()):
                    merged["pdf_count"] += segment.get("pdf_count", 0)
                    merged["archive_count"] += segment.get("archive_count", 0)
                    if segment_index < generation:
                        merged["recovery_history"].append(
                            {
                                "generation": segment_index,
                                "stop_reason": segment.get("stop_reason"),
                                "error": segment.get("error"),
                                "seconds": segment.get("seconds", 0),
                            }
                        )
                    for job in copy.deepcopy(segment["jobs"]):
                        job["tab_generation"] = segment_index
                        if segment_index:
                            for doc in job["documents"]:
                                for key in ("local_path", "archive_path"):
                                    if doc.get(key):
                                        doc[key] = f"restart-{segment_index}/{doc[key]}"
                                for item in doc.get("files", []):
                                    item["local_path"] = f"restart-{segment_index}/{item['local_path']}"
                        merged["jobs"].append(job)
                tab_states[number] = merged
                tab_updated[number] = time.monotonic()
                combine()

            async def progress(report, *, final):
                observe(report)
                await aggregate.update(index, state)

            try:
                while True:
                    generation_offset = time.monotonic() - slot_started
                    await run(
                        [],
                        tab_folder if generation == 0 else tab_folder / f"restart-{generation}",
                        request_seconds,
                        ca_bundle=ca_bundle,
                        max_seconds=max_seconds,
                        on_progress=progress,
                        on_state=observe,
                        proxy=assigned_proxy,
                        proxy_ca=proxy_ca if proxy else None,
                        context_override=context,
                        next_job=browser_next_job,
                        stop_event=stop_browser,
                        egress_ip=state.get("proxy_egress_ip"),
                        reusable=reusable,
                        known_unavailable=known_unavailable,
                        write_json=not sqlite_state,
                    )
                    report = generations.get(generation, {})
                    if (
                        report.get("stop_reason") not in {"page_closed", "page_crashed"}
                        or stop_browser.is_set()
                        or queue.empty()
                    ):
                        break
                    if crash_restarts >= tab_restarts:
                        aggregate.log.emit(
                            "tab_restart_limit",
                            worker=index,
                            tab=number,
                            restarts=generation,
                            reason=report.get("stop_reason"),
                        )
                        break
                    aggregate.log.emit(
                        "tab_restarting",
                        worker=index,
                        tab=number,
                        generation=generation + 1,
                        reason=report.get("stop_reason"),
                    )
                    try:
                        await asyncio.wait_for(stop_browser.wait(), timeout=1)
                        break
                    except TimeoutError:
                        pass
                    if queue.empty():
                        break
                    generation += 1
                    crash_restarts += 1
            finally:
                if number in tab_states and not sqlite_state:
                    atomic_write_json(tab_folder / "summary.json", tab_states[number])

        try:
            folder.mkdir(mode=0o700)
            async with asyncio.timeout(max_seconds):
                activity("certificates")
                await asyncio.to_thread(install_intermediate, folder / "profile", ca_bundle)
                if (
                    assigned_proxy
                    and urlsplit(assigned_proxy["server"]).hostname == "brd.superproxy.io"
                    and proxy_ca is None
                ):
                    raise ValueError("Bright Data proxy CA is required")
                if assigned_proxy and proxy_ca is not None:
                    await asyncio.to_thread(install_proxy_ca, folder / "profile", proxy_ca)
                state["proxy_replacements"] = 0
                state["proxy_failures"] = []
                while True:
                    activity("browser_start")
                    context_closing = False
                    retry_proxy = False
                    async with AsyncExitStack() as browser_stack:
                        context = await browser_stack.enter_async_context(
                            browser_context(
                                {
                                    "headless": "virtual",
                                    "os": "linux",
                                    "locale": "es-CO",
                                    "persistent_context": True,
                                    "user_data_dir": str(folder / "profile"),
                                    "accept_downloads": True,
                                    "ignore_https_errors": False,
                                    "proxy": assigned_proxy,
                                },
                                launch_lock,
                            )
                        )

                        def context_closed(*_):
                            if not context_closing:  # noqa: B023 - closed before the next iteration
                                stop_browser.set()
                                ready_workers.discard(index)
                            state["context_closed"] = True
                            aggregate.log.emit(
                                "browser_context_closed",
                                worker=index,
                                expected=context_closing,  # noqa: B023
                                activity=state.get("activity"),
                            )

                        if hasattr(context, "on"):
                            context.on("close", context_closed)
                        try:
                            if assigned_proxy:
                                activity("proxy_preflight")

                                def record_attempt(attempt):
                                    state["proxy_preflight_attempts"] = attempt
                                    combine()

                                state["proxy_egress_ip"] = await proxy_preflight(
                                    context, request_seconds, on_attempt=record_attempt, log=aggregate.log, worker=index
                                )
                                aggregate.log.emit("proxy_ready", worker=index, egress_ip=state["proxy_egress_ip"])
                        except (BrowserError, TimeoutError, ValueError) as exc:
                            state["proxy_failures"].append(
                                {"type": type(exc).__name__, "attempts": state.get("proxy_preflight_attempts", 0)}
                            )
                            replacement = (
                                proxy.replacement()
                                if isinstance(proxy, WebshareProxyPool) and state["proxy_replacements"] < 3
                                else None
                            )
                            if replacement is None:
                                raise
                            state["proxy_replacements"] += 1
                            aggregate.log.emit(
                                "proxy_replaced",
                                worker=index,
                                replacement=state["proxy_replacements"],
                                failure_type=type(exc).__name__,
                            )
                            next_proxy = replacement
                            context_closing = True
                            retry_proxy = True
                        if not retry_proxy:
                            activity("waiting_for_browsers")
                            startup_done(index, ready=True)
                            await startup_gate.wait()
                            async with start_notice_lock:
                                if not start_notified:
                                    start_notified = True
                                    notice = aggregate.snapshot()
                                    notice.update(event="download_started", ready_browsers=len(ready_workers))
                                    await aggregate.notify(notice, final=False)
                            activity("tabs_running")
                            rotation_reason = None
                            cycle_claimed = 0
                            # Stagger rotations so the fleet does not drain simultaneously.
                            cycle_deadline = time.monotonic() + browser_max_seconds * (0.8 + 0.2 * index / len(groups))
                            async with asyncio.TaskGroup() as tab_tasks:
                                for number in range(1, tabs + 1):
                                    tab_tasks.create_task(tab_worker(number, context))
                            context_closing = True
                    if not retry_proxy:
                        if rotation_reason and not queue.empty() and not stop_browser.is_set():
                            browser_generation += 1
                            state["browser_restarts"] = browser_generation
                            aggregate.log.emit(
                                "browser_restarting",
                                worker=index,
                                generation=browser_generation,
                                reason=rotation_reason,
                                resources=resource_snapshot(),
                            )
                            await aggregate.update(index, state)
                            continue
                        break
                    assigned_proxy = next_proxy
                    stop_browser.clear()
                statuses = {tab["status"] for tab in tab_states.values()}
                state["status"] = "error" if "error" in statuses else "stopped" if "stopped" in statuses else "finished"
                reasons = {tab["stop_reason"] for tab in tab_states.values() if tab.get("stop_reason")}
                state["stop_reason"] = "; ".join(sorted(reasons)) or None
        except TimeoutError:
            state.update(status="stopped", stop_reason="time_budget")
        except BaseException as exc:
            message = str(exc)
            for value in sorted((assigned_proxy or {}).values(), key=len, reverse=True):
                if value:
                    message = message.replace(value, "[redacted]")
            message = aggregate.log.clean(message)
            state.update(
                status="error",
                stop_reason=f"browser_error:{type(exc).__name__}",
                failure_stage=state.get("activity", {}).get("stage"),
                error={"type": type(exc).__name__, "message": message[:800]},
            )
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt)):
                state["status"] = "interrupted"
                raise
        finally:
            startup_done(index)
            activity(state["status"])
            if not sqlite_state:
                atomic_write_json(folder / "report.json", state)
            await aggregate.update(index, state)
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
