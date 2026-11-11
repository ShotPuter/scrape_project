"""Resumable bounded Webshare experiment with durable metrics and Telegram."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import hashlib
import ipaddress
import json
import math
import os
import time
import uuid
from collections import Counter
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import aiohttp
from dotenv import load_dotenv

from src.proxy.sources.webshare import WebshareError, load_webshare_candidates
from src.services.notifications import send_telegram
from src.sites.pep.__main__ import export_json, setup_database
from src.sites.pep.browser_discovery import BrowserDiscoveryClient
from src.sites.pep.browser_download import BrowserDownloader
from src.sites.pep.browser_options import BrowserEnvironmentError, BrowserOptions
from src.sites.pep.discovery import DiscoveryClient, TLSPreflightError
from src.sites.pep.documents import DocumentDownloader, inspect_saved
from src.sites.pep.pacing import RequestPacer
from src.sites.pep.retry_queue import attempts_for, begin_attempt, eligible_links, finish_attempt, prepare_attempts
from src.utils.atomic import atomic_write_json


class Report:
    def __init__(self, db, output: Path, target: int) -> None:
        self.db = db
        self.output = output
        self.target = target
        self.started = time.monotonic()
        self.run_id = uuid.uuid4().hex[:12]
        self.started_at = datetime.now(UTC).isoformat()
        self.new_ids: set[str] = set()
        self.reused_ids: set[str] = set()
        self.worker_errors: Counter = Counter()
        self.status = "STARTING"
        self.stop_reason = None
        self.workers_requested = 0
        self.workers_available = 0
        self.network = "webshare"
        self.headed = False
        self.failure_details = []
        self.browser_engine = "native"
        self.pass_kind = "all"
        self.max_job_attempts = 2
        self.sample_sha256 = ""
        self.adapter_revision = None
        self.proxy_sessions = []
        self.browser_experiment = {}
        self.source_mode = "http-browser"

    def event(self, event: str, *, link: str | None = None, document: dict | None = None, **fields) -> None:
        """Append bounded application diagnostics, never response bodies or credentials."""
        record = {"timestamp": datetime.now(UTC).isoformat(), "run_id": self.run_id,
                  "elapsed_seconds": round(time.monotonic() - self.started, 3), "event": event}
        if link:
            record["link_ref"] = hashlib.sha256(link.encode()).hexdigest()[:16]
        if document:
            record["document_ref"] = hashlib.sha256(str(document["id"]).encode()).hexdigest()[:16]
            for key in ("status", "failure_stage", "error_type", "rejection_reason", "redirect_path",
                        "download_method", "browser_seconds", "browser_attempts", "browser_http_status",
                        "native_exit_code", "browser_warmup_http_status", "browser_button_in_view",
                         "browser_button_receives_pointer", "browser_settle_seconds", "browser_page_age_at_click_seconds",
                         "browser_mouse_moves"):
                if key in document:
                    record[key] = document[key]
            if "browser_history" in document:
                allowed = ("status", "failure_stage", "error_type", "rejection_reason",
                           "browser_http_status", "browser_seconds")
                record["browser_history"] = [{key: item[key] for key in allowed if key in item}
                                             for item in document["browser_history"][:2]]
        for key in ("worker", "status", "seconds", "documents", "scope", "error_type", "curl_code", "reason", "reused_file",
                    "diagnostics_file"):
            if key in fields:
                record[key] = fields[key]
        fd = os.open(self.output / "events.jsonl", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        if event in {"job_finished", "worker_quarantined", "exception"}:
            print(f"PEP {event}: link={record.get('link_ref', '-')} "
                  f"status={record.get('status', '-')} reason={record.get('reason', record.get('error_type', '-'))}",
                  flush=True)

    def failure(self, scope: str, exc: Exception) -> None:
        self.worker_errors[type(exc).__name__] += 1
        detail = {"scope": scope, "type": type(exc).__name__}
        # Only these locally defined exceptions guarantee credential-free messages.
        if isinstance(exc, (TLSPreflightError, WebshareError, BrowserEnvironmentError)):
            detail["message"] = str(exc)
            print(f"PEP {scope}: {exc}", flush=True)
        self.failure_details.append(detail)
        self.event("exception", scope=scope, error_type=type(exc).__name__)

    def snapshot(self) -> dict:
        jobs = Counter()
        documents = {}
        empty = 0
        for status, raw in self.db.execute(
            "SELECT jobs.status,jobs.result FROM jobs JOIN benchmark_selection s ON s.link=jobs.link"
        ):
            jobs[status] += 1
            docs = json.loads(raw)
            if status == "completed" and not docs:
                empty += 1
            for doc in docs:
                old = documents.get(doc["id"])
                if not old or doc["status"] == "downloaded":
                    documents[doc["id"]] = doc
        states = Counter(d["status"] for d in documents.values())
        elapsed = time.monotonic() - self.started
        downloaded = [d for d in documents.values() if d["status"] == "downloaded"]
        unresolved = [d for d in documents.values() if d["status"] != "downloaded"]
        selected = sum(jobs.values())
        return {
            "run_id": self.run_id, "status": self.status, "started_at": self.started_at,
            "elapsed_seconds": round(elapsed, 2), "target_unique_links": self.target,
            "selected_unique_links": sum(jobs.values()), "jobs": dict(jobs),
            "finished_links": sum(n for status, n in jobs.items() if status not in {"pending", "processing"}),
            "completed_links": jobs["completed"], "empty_links": empty,
            "unique_documents_discovered": len(documents), "unique_documents_downloaded": len(downloaded),
            "document_statuses": dict(states), "unresolved_documents": len(documents) - len(downloaded),
            "pending_links": jobs["pending"],
            "completed_links_percent": round(100 * jobs["completed"] / selected, 2) if selected else 0,
            "failure_stages": dict(Counter(d.get("failure_stage", "unspecified") for d in unresolved)),
            "rejection_reasons": dict(Counter(d["rejection_reason"] for d in unresolved if d.get("rejection_reason"))),
            "pdf_files": sum(len(d.get("files") or [d]) for d in downloaded),
            "downloaded_bytes": sum(sum(f.get("size_bytes", 0) for f in d.get("files") or [d]) for d in downloaded),
            "new_downloads_this_run": len(self.new_ids), "reused_documents_this_run": len(self.reused_ids),
            "new_documents_per_minute": round(len(self.new_ids) * 60 / elapsed, 2) if elapsed else 0,
            "worker_errors": dict(self.worker_errors),
            "stop_reason": self.stop_reason,
            "workers_requested": self.workers_requested, "workers_available": self.workers_available,
            "network": self.network, "headed": self.headed,
            "browser_engine": self.browser_engine, "pass_kind": self.pass_kind,
            "source_mode": self.source_mode,
            "browser_adapter_revision": self.adapter_revision,
            "browser_experiment": self.browser_experiment,
            "proxy_sessions": self.proxy_sessions,
            "max_job_attempts": self.max_job_attempts, "sample_sha256": self.sample_sha256,
            "retryable_links": len(eligible_links(self.db, "errors", self.max_job_attempts)),
            "job_attempts_total": self.db.execute(
                "SELECT count(*) FROM benchmark_attempts a JOIN benchmark_selection s ON s.link=a.link"
            ).fetchone()[0],
            "browser_warmup_enabled": True,
            "failure_details": self.failure_details,
            "coverage_note": "Counts cover discovered documents only; failed discovery may hide additional documents.",
        }

    def save(self) -> dict:
        result = self.snapshot()
        atomic_write_json(self.output / "benchmark.json", result)
        return result

    def text(self, report: dict) -> str:
        jobs = report["jobs"]
        errors = sum(n for status, n in jobs.items() if status not in {"completed", "pending", "processing"})
        return (
            f"PEP test {self.run_id}: {self.status}\n"
            f"Ссылки: {report['finished_links']}/{report['selected_unique_links']}; "
            f"полностью: {report['completed_links']}; с ошибками: {errors}\n"
            f"Документы: {report['unique_documents_downloaded']}/{report['unique_documents_discovered']}; "
            f"новых: {len(self.new_ids)}; не скачано: {report['unresolved_documents']}\n"
            f"PDF: {report['pdf_files']}; время: {report['elapsed_seconds'] / 60:.1f} мин; "
            f"новых/мин: {report['new_documents_per_minute']}\n"
            f"Ожидают: {jobs.get('pending', 0)}; ошибок воркеров: {sum(self.worker_errors.values())}"
        )


async def run(args) -> int:
    args.output.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Shared profile roots also need a lock across different output directories.
    with ExitStack() as locks:
        lock = locks.enter_context((args.output / "benchmark.lock").open("a"))
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("A benchmark already uses this output directory") from None
        profile_root = getattr(args, "browser_profile_root", None)
        if profile_root:
            profile_root.mkdir(parents=True, exist_ok=True, mode=0o700)
            profile_lock = locks.enter_context((profile_root / "benchmark-profiles.lock").open("a"))
            try:
                fcntl.flock(profile_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError("A benchmark already uses this browser profile root") from None
        return await run_locked(args)


async def run_locked(args) -> int:
    # Benchmark is also an executable entry point; load the repo's .env before
    # reading Webshare/notification credentials. Existing process env wins.
    load_dotenv()
    engine = getattr(args, "browser_engine", "native")
    owned_engine = engine in {"nodriver", "undetected"}
    browser_only = getattr(args, "browser_only", False)
    if browser_only and not owned_engine:
        raise ValueError("browser-only requires nodriver or undetected")
    settle_min = getattr(args, "page_settle_min", 0.0)
    settle_max = getattr(args, "page_settle_max", 0.0)
    settle_scroll = getattr(args, "page_settle_scroll", False)
    browser_options = BrowserOptions(
        render_mode=getattr(args, "browser_renderer", "native"),
        browser_language=getattr(args, "browser_language", None),
        browser_timezone=getattr(args, "browser_timezone", None),
        mouse_motion=getattr(args, "mouse_motion", False), require_webgl=getattr(args, "require_webgl", False),
    )
    profile_root = getattr(args, "browser_profile_root", None)
    if profile_root and getattr(args, "network", "webshare") != "webshare":
        raise ValueError("Shared profiles require a verified Webshare proxy route")
    diagnostics = getattr(args, "browser_diagnostics", False) or browser_options.require_webgl
    if (not math.isfinite(settle_min) or not math.isfinite(settle_max)
            or not 0 <= settle_min <= settle_max <= 15):
        raise ValueError("page-settle range must satisfy 0 <= min <= max <= 15 seconds")
    if settle_scroll and settle_max == 0:
        raise ValueError("page-settle-scroll requires a positive page-settle range")
    configured = settle_max or settle_scroll or diagnostics or profile_root or browser_options != BrowserOptions()
    if configured and not owned_engine:
        raise ValueError("page-settle/browser-diagnostics require nodriver or undetected")
    settle_options = {"page_settle_min": float(settle_min), "page_settle_max": float(settle_max),
                      "page_settle_scroll": settle_scroll}
    experiment = {**settle_options, **browser_options.snapshot(), "browser_only": browser_only, "profile_storage": (
        hashlib.sha256(str(profile_root.resolve()).encode()).hexdigest()[:16] if profile_root else "output-local"
    )}
    rotating = getattr(args, "proxy_sessions", 0)
    if owned_engine and not getattr(args, "headed", False):
        raise ValueError(f"{engine} requires --headed (Xvfb supported)")
    if engine == "undetected" and (args.workers != 1 or rotating):
        raise ValueError("experimental undetected mode requires --workers 1 and no proxy-sessions")
    if owned_engine and getattr(args, "network", "webshare") == "direct" and args.workers != 1:
        raise ValueError("parallel nodriver workers require one distinct Webshare proxy per browser")
    if engine == "nodriver" and not rotating and args.workers > 3:
        raise ValueError("parallel nodriver mode supports at most 3 proxy-backed browsers")
    if rotating and (engine != "nodriver" or getattr(args, "network", "webshare") != "webshare"
                     or not 1 <= rotating <= 5 or not 1 <= getattr(args, "session_links", 2) <= 20):
        raise ValueError("proxy-sessions requires nodriver/Webshare, 1..5 sessions and 1..20 links/session")
    browser_class = BrowserDownloader
    if engine == "nodriver":
        from src.sites.pep.nodriver_download import NodriverDownloader

        browser_class = NodriverDownloader
    elif engine == "undetected":
        from src.sites.pep.undetected_download import UndetectedDownloader

        browser_class = UndetectedDownloader
    print("PEP: importing/checking workbook and preparing the fixed sample...", flush=True)
    db = setup_database(args.output / "state.sqlite3", args.workbook)
    db.execute("CREATE TABLE IF NOT EXISTS benchmark_selection (link TEXT PRIMARY KEY)")
    prepare_attempts(db)
    selected = db.execute("SELECT count(*) FROM benchmark_selection").fetchone()[0]
    if selected and selected != min(args.count, db.execute("SELECT count(*) FROM jobs").fetchone()[0]):
        db.close()
        raise ValueError("This directory has a different sample size; choose a new --output")
    with db:
        db.execute("INSERT OR IGNORE INTO benchmark_selection SELECT link FROM jobs ORDER BY rowid LIMIT ?", (args.count,))
    print(f"PEP: sample ready; unique_links={db.execute('SELECT count(*) FROM benchmark_selection').fetchone()[0]}",
          flush=True)
    report = Report(db, args.output, args.count)
    report.workers_requested = args.workers
    report.network = getattr(args, "network", "webshare")
    report.headed = getattr(args, "headed", False)
    report.browser_engine = engine
    report.source_mode = "browser-only" if browser_only else "http-browser"
    report.adapter_revision = browser_class.ADAPTER_REVISION
    report.browser_experiment = {**experiment, "diagnostics": diagnostics}
    report.pass_kind = getattr(args, "pass_kind", "all")
    report.max_job_attempts = getattr(args, "max_job_attempts", 2)
    sample = [row[0] for row in db.execute("SELECT link FROM benchmark_selection ORDER BY rowid")]
    report.sample_sha256 = hashlib.sha256("\n".join(sample).encode()).hexdigest()
    previous_engine = db.execute("SELECT value FROM meta WHERE key='benchmark_browser_engine'").fetchone()
    if previous_engine and previous_engine[0] != engine:
        db.close()
        raise ValueError("Use a new --output when changing --browser-engine")
    previous_network = db.execute("SELECT value FROM meta WHERE key='benchmark_network'").fetchone()
    if previous_network and previous_network[0] != report.network:
        db.close()
        raise ValueError("Use a new --output when changing --network for a fair comparison")
    previous_experiment = db.execute("SELECT value FROM meta WHERE key='benchmark_browser_experiment'").fetchone()
    old_experiment = json.loads(previous_experiment[0]) if previous_experiment else {
        "page_settle_min": 0.0, "page_settle_max": 0.0, "page_settle_scroll": False,
    }
    old_experiment = {**BrowserOptions().snapshot(), "profile_storage": "output-local", "browser_only": False,
                      **old_experiment}
    if previous_engine and old_experiment != experiment:
        db.close()
        raise ValueError("Use a new --output when changing browser experiment settings for a fair comparison")
    with db:
        db.execute("INSERT OR IGNORE INTO meta VALUES ('benchmark_network', ?)", (report.network,))
        db.execute("INSERT OR IGNORE INTO meta VALUES ('benchmark_browser_engine', ?)", (engine,))
        db.execute("INSERT OR IGNORE INTO meta VALUES ('benchmark_browser_experiment', ?)", (json.dumps(experiment),))
    telegram = {"token": os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
                "chat": os.getenv("TELEGRAM_CHAT_ID", "").strip()}
    token = os.getenv("WEBSHARE_API_TOKEN", "").strip()
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
    heartbeat = None

    async def notify() -> None:
        snapshot = report.save()
        print(report.text(snapshot), flush=True)
        if telegram["token"] and telegram["chat"]:
            try:
                await send_telegram(session, telegram["token"], telegram["chat"], report.text(snapshot))
            except RuntimeError:
                print("PEP: Telegram unavailable (credentials hidden); collection continues", flush=True)

    async def periodic() -> None:
        while True:
            await asyncio.sleep(args.notify_interval)
            await notify()

    try:
        report.status = "RUNNING"
        report.event("run_started", status=report.status)
        if not (telegram["token"] and telegram["chat"]):
            print("PEP: Telegram disabled; set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID", flush=True)
        await notify()
        queue = asyncio.Queue()
        for link in eligible_links(db, report.pass_kind, report.max_job_attempts):
            queue.put_nowait(link)
        if queue.empty():
            state = report.snapshot()
            report.status = "COMPLETED" if state["completed_links"] == state["selected_unique_links"] else "INCOMPLETE"
            report.stop_reason = "no_eligible_jobs"
            return int(report.status != "COMPLETED")
        heartbeat = asyncio.create_task(periodic())
        proxies = (await load_webshare_candidates(token, mode="direct", max_records=rotating or 100)
                   if report.network == "webshare" else [SimpleNamespace(url=None)])
        # Rotation is explicit, finite, sequential, and never retries a refused job.
        unique = {}
        for candidate in proxies:
            from urllib.parse import urlsplit

            unique.setdefault(urlsplit(candidate.url).hostname if candidate.url else "direct", candidate)
        proxies = list(unique.values())[:rotating or args.workers]
        report.workers_available = len(proxies)
        if not proxies:
            raise ValueError("Webshare returned no usable proxy endpoints")
        if owned_engine and report.network == "webshare" and len(proxies) < args.workers:
            raise ValueError("Webshare did not provide one distinct proxy per nodriver browser")
        ca_bundle = getattr(args, "ca_bundle", None)
        http_options = {"accept_language": browser_options.accept_language} if browser_options.accept_language else {}
        if not rotating and not browser_only:
            bootstrap = DiscoveryClient(workers=1, delay=args.delay, timeout=args.timeout, ca_bundle=ca_bundle,
                                        proxy=proxies[0].url, **http_options)
            try:
                await bootstrap.preflight(args.output)
                ca_bundle = bootstrap.ca_bundle
            finally:
                await bootstrap.close()
        limiter = RequestPacer(args.delay)
        stop_after_verification = asyncio.Event()
        verified_egresses: set[str] = set()

        async def worker(index, proxy) -> None:
            fingerprint = hashlib.sha256(proxy.url.encode()).hexdigest()[:16] if proxy.url else "direct"
            profile = profile_root / engine / fingerprint if profile_root else args.output / "profiles" / fingerprint
            browser = browser_class(
                output=args.output, profile=profile, channel="chrome",
                headed=report.headed, cdp_url=None, timeout_ms=args.timeout * 1000, pace=limiter.pace,
                headless=not report.headed, native=not bool(proxy.url), proxy=proxy.url,
                warmup=True,
                retry_transient_redirect=not getattr(args, "single_browser_attempt", False),
                **({"ca_bundle": ca_bundle, **settle_options, "browser_options": browser_options} if owned_engine else {}),
            )
            client = (BrowserDiscoveryClient(browser) if browser_only else
                      DiscoveryClient(workers=1, delay=args.delay, timeout=args.timeout,
                                      ca_bundle=ca_bundle, proxy=proxy.url, **http_options))
            if not browser_only:
                client.pace = limiter.pace  # One global site budget, not N independent rates.
            downloader = None if browser_only else DocumentDownloader(client, args.output)
            session_state = {"session": index + 1, "proxy_ref": fingerprint, "routing": "not_checked", "jobs": 0}
            if rotating or (owned_engine and proxy.url):
                report.proxy_sessions.append(session_state)
            try:
                identity = {"engine": engine, "render_mode": browser_options.render_mode,
                            "browser_language": browser_options.browser_language,
                            "browser_timezone": browser_options.browser_timezone}
                binding_path = profile / "pep-session-binding.json"
                binding = json.loads(binding_path.read_text()) if profile_root and binding_path.is_file() else None
                if binding and binding.get("identity") != identity:
                    raise BrowserEnvironmentError("Shared profile has different renderer/locale settings; choose a new root")
                report.event("worker_starting", worker=index + 1)
                if proxy.url and (rotating or owned_engine):
                    if browser_only:
                        address = str(ipaddress.ip_address(await browser.observe_egress()))
                    else:
                        # Both worker clients must prove the route before processing jobs.
                        response = await client.session.get("https://api.ipify.org?format=json", allow_redirects=False)
                        if response.status_code != 200:
                            raise ValueError("Proxy egress check failed")
                        address = str(ipaddress.ip_address(response.json()["ip"]))
                        await browser.verify_egress(address)
                    egress_ref = hashlib.sha256(address.encode()).hexdigest()[:16]
                    if binding and binding.get("egress_ref") != egress_ref:
                        raise BrowserEnvironmentError("Shared profile egress IP changed; refusing to reuse this identity")
                    if not rotating and egress_ref in verified_egresses:
                        raise ValueError("Proxy browsers share the same public egress IP")
                    verified_egresses.add(egress_ref)
                    session_state.update(routing="browser_verified" if browser_only else "verified", egress_ref=egress_ref)
                    report.event("proxy_route_verified", worker=index + 1)
                if profile_root:
                    profile.mkdir(parents=True, exist_ok=True, mode=0o700)
                    await asyncio.to_thread(atomic_write_json, binding_path,
                                            {"identity": identity, "egress_ref": session_state.get("egress_ref")})
                if diagnostics:
                    path = await browser.save_environment()
                    report.event("browser_environment_saved", worker=index + 1, diagnostics_file=path.name)
                await client.preflight(args.output)
                report.event("worker_ready", worker=index + 1)
                while True:
                    if stop_after_verification.is_set():
                        return
                    if rotating and session_state["jobs"] >= getattr(args, "session_links", 2):
                        return
                    try:
                        link = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    try:
                        job_started = time.monotonic()
                        report.event("job_started", link=link, worker=index + 1)
                        old_status, raw = db.execute("SELECT status,result FROM jobs WHERE link=?", (link,)).fetchone()
                        previous = json.loads(raw)
                        if old_status == "completed":
                            validations = [
                                await asyncio.to_thread(inspect_saved, args.output, doc["id"]) for doc in previous
                            ]
                            if all(validations):
                                report.reused_ids.update(doc["id"] for doc in previous)
                                report.event("job_finished", link=link, worker=index + 1, status="completed",
                                             reason="validated_local_files", documents=len(previous))
                                report.save()
                                continue
                            # Never leave a corrupt completed document counted as success,
                            # including when its persisted network-attempt budget is spent.
                            previous = [
                                {**doc, **metadata} if metadata else
                                {**doc, "status": "local_file_invalid", "failure_stage": "local_validation"}
                                for doc, metadata in zip(previous, validations, strict=True)
                            ]
                            old_status = "partial"
                            with db:
                                db.execute("UPDATE jobs SET status='partial',result=? WHERE link=?",
                                           (json.dumps(previous, ensure_ascii=False), link))
                        if attempts_for(db, link) >= report.max_job_attempts:
                            report.event("job_deferred", link=link, reason="job_attempt_limit")
                            continue
                        begin_attempt(db, link, report.run_id)
                        session_state["jobs"] += 1
                        with db:
                            db.execute("UPDATE jobs SET status='processing' WHERE link=?", (link,))
                        if old_status in {"completed", "partial", "discovered"}:
                            status, docs = "discovered", previous
                        else:
                            report.event("discovery_started", link=link, worker=index + 1)
                            status, docs = await client.discover(link)
                        if client.last_failure.get("reason") == "site_verification_notice":
                            stop_after_verification.set()
                        report.event("discovery_finished", link=link, worker=index + 1, status=status, documents=len(docs),
                                     **getattr(client, "last_failure", {}))
                        merged = {d["id"]: d for d in previous}
                        merged.update({d["id"]: d for d in docs})
                        with db:
                            db.execute("UPDATE jobs SET result=? WHERE link=?",
                                       (json.dumps(list(merged.values()), ensure_ascii=False), link))
                        for doc_id, doc in merged.items():
                            if stop_after_verification.is_set():
                                break
                            document_started = time.monotonic()
                            report.event("document_started", link=link, document=doc, worker=index + 1)
                            reused = await asyncio.to_thread(inspect_saved, args.output, doc_id)
                            if browser_only:
                                result = {**doc, **reused} if reused else await browser.download(doc)
                            else:
                                result = await downloader.download(doc)
                                if result["status"] == "captcha_required":
                                    result = await browser.download(result)
                            merged[doc_id] = result
                            if result["status"] == "downloaded":
                                if reused:
                                    if doc_id not in report.new_ids:
                                        report.reused_ids.add(doc_id)
                                else:
                                    report.new_ids.add(doc_id)
                                    report.reused_ids.discard(doc_id)
                            with db:
                                db.execute("UPDATE jobs SET result=? WHERE link=?",
                                           (json.dumps(list(merged.values()), ensure_ascii=False), link))
                            diagnostic = result
                            if reused and result["status"] == "downloaded":
                                diagnostic = {**result, "download_method": "local_reuse", "browser_attempts": 0,
                                              "browser_seconds": 0, "browser_history": []}
                            report.event("document_finished", link=link, document=diagnostic, worker=index + 1,
                                         reused_file=bool(reused and result["status"] == "downloaded"),
                                         seconds=round(time.monotonic() - document_started, 3))
                            if result.get("rejection_reason") == "site_verification_notice":
                                stop_after_verification.set()
                                break
                        if status == "discovered":
                            status = "completed" if all(d["status"] == "downloaded" for d in merged.values()) else "partial"
                        with db:
                            db.execute("UPDATE jobs SET status=?,result=? WHERE link=?",
                                        (status, json.dumps(list(merged.values()), ensure_ascii=False), link))
                        finish_attempt(db, link, report.run_id, status)
                        report.save()
                        report.event("job_finished", link=link, worker=index + 1, status=status,
                                     seconds=round(time.monotonic() - job_started, 3), documents=len(merged))
                        if (browser.failures >= 3 or status in {"discovery_failed_tls", "discovery_failed_http_403"}
                                or stop_after_verification.is_set()):
                            report.worker_errors["worker_quarantined"] += 1
                            reason = next((d.get("rejection_reason") for d in merged.values()
                                            if d.get("rejection_reason") == "site_verification_notice"),
                                           client.last_failure.get("reason"))
                            report.event("worker_quarantined", link=link, worker=index + 1, status=status,
                                         reason=reason or (status if status.startswith("discovery_failed")
                                                            else "browser_failure_threshold"))
                            session_state["stop_reason"] = reason or "worker_quarantined"
                            return
                    except asyncio.CancelledError:
                        with db:
                            db.execute("UPDATE jobs SET status='interrupted' WHERE link=?", (link,))
                        finish_attempt(db, link, report.run_id, "interrupted")
                        report.event("job_interrupted", link=link, worker=index + 1, status="interrupted")
                        raise
                    except Exception as exc:  # noqa: BLE001 - sanitized item failure must not destroy a sample
                        report.failure("item", exc)
                        with db:
                            db.execute("UPDATE jobs SET status='worker_error' WHERE link=?", (link,))
                        finish_attempt(db, link, report.run_id, "worker_error")
                        report.event("job_finished", link=link, worker=index + 1, status="worker_error",
                                     error_type=type(exc).__name__)
                        return
                    finally:
                        queue.task_done()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - never expose proxy credentials from exceptions
                session_state["error_type"] = type(exc).__name__
                report.failure(f"worker-{index + 1}", exc)
            finally:
                try:
                    await browser.close()
                finally:
                    await client.close()
                    report.event("worker_stopped", worker=index + 1)

        try:
            async with asyncio.timeout(args.max_minutes * 60):
                if rotating:
                    for index, proxy in enumerate(proxies):
                        if queue.empty() or stop_after_verification.is_set():
                            break
                        if index:
                            await asyncio.sleep(args.delay)
                        await worker(index, proxy)
                else:
                    async with asyncio.TaskGroup() as tasks:
                        for index, proxy in enumerate(proxies):
                            tasks.create_task(worker(index, proxy))
        except TimeoutError:
            report.status = "TIME_LIMIT"
            report.stop_reason = "time_budget_exhausted"
        if report.status == "RUNNING":
            state = report.snapshot()
            report.status = "COMPLETED" if state["completed_links"] == state["selected_unique_links"] else "INCOMPLETE"
            if not queue.empty():
                report.stop_reason = "workers_unavailable_or_quarantined"
            elif state["pending_links"] > 0:
                report.stop_reason = "sample_has_pending_links"
            elif report.status == "INCOMPLETE":
                report.stop_reason = "sample_contains_unresolved_errors"
        return int(report.status != "COMPLETED")
    except asyncio.CancelledError:
        report.status = "INTERRUPTED"
        report.stop_reason = "cancelled_or_keyboard_interrupt"
        raise
    except Exception as exc:  # noqa: BLE001 - report failure without leaking API tokens or proxy URLs
        report.status = "FAILED"
        report.stop_reason = "startup_or_runtime_error"
        report.failure("startup_or_runtime", exc)
        return 1
    finally:
        if heartbeat:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
        try:
            export_json(db, args.output / "records.json")
            await notify()
            atomic_write_json(args.output / f"run-{report.run_id}.json", report.snapshot())
            report.event("run_finished", status=report.status, reason=report.stop_reason)
        finally:
            await session.close()
            db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workbook", type=Path)
    parser.add_argument("--output", type=Path, default=Path("data/pep-benchmark-100"))
    parser.add_argument("--count", type=int, default=100, help="Fixed first N unique consultation links, not Excel rows")
    parser.add_argument(
        "--workers", type=int, default=2,
        help="Concurrent workers; nodriver/Webshare uses one verified proxy-backed browser per worker",
    )
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--delay", type=float, default=1.0, help="Global minimum interval between source requests")
    parser.add_argument("--notify-interval", type=int, default=120, help="Console/JSON/Telegram interval in seconds")
    parser.add_argument("--max-minutes", type=float, default=60)
    parser.add_argument("--network", choices=["webshare", "direct"], default="webshare")
    parser.add_argument("--headed", action="store_true", help="Visible Chrome for a controlled comparison")
    parser.add_argument("--ca-bundle", type=Path, help="Reuse an existing trusted CA bundle")
    parser.add_argument("--browser-engine", choices=["native", "nodriver", "undetected"], default="native")
    parser.add_argument("--browser-only", action="store_true",
                        help="Parse and download through Chrome; no HTTP portal client")
    parser.add_argument("--browser-diagnostics", action="store_true", help="Save local UA/screen/WebGL observations")
    parser.add_argument("--page-settle-min", type=float, default=0, help="Minimum pre-click dwell in seconds (0..15)")
    parser.add_argument("--page-settle-max", type=float, default=0, help="Maximum pre-click dwell in seconds (min..15)")
    parser.add_argument("--page-settle-scroll", action="store_true", help="Small scroll and restore during pre-click dwell")
    parser.add_argument("--browser-renderer", choices=["native", "swiftshader"], default="native")
    parser.add_argument("--require-webgl", action="store_true", help="Check WebGL before processing jobs; save diagnostics")
    parser.add_argument("--mouse-motion", action="store_true", help="15..25 CDP mouseMoved events before clicking")
    parser.add_argument("--browser-language", help="Explicit locale/Accept-Language, e.g. es-CO; no automatic IP inference")
    parser.add_argument("--browser-timezone", help="Explicit IANA timezone; match it to your proxy, not the site's country")
    parser.add_argument("--browser-profile-root", type=Path,
                        help="Shared persistent profiles outside run reports; root locked")
    parser.add_argument("--pass-kind", choices=["all", "errors"], default="all")
    parser.add_argument("--max-job-attempts", type=int, default=2, help="Persisted upper bound on attempts per link")
    parser.add_argument("--single-browser-attempt", action="store_true", help="Retry only in a later error pass")
    parser.add_argument("--proxy-sessions", type=int, default=0, help="Sequential nodriver/Webshare sessions (1..5)")
    parser.add_argument("--session-links", type=int, default=2, help="Maximum new jobs per proxy session (1..20)")
    args = parser.parse_args()
    if args.ca_bundle and not args.ca_bundle.is_file():
        parser.error("--ca-bundle must be an existing PEM file")
    if not 1 <= args.workers <= 5 or not 1 <= args.count <= 10000:
        parser.error("workers must be 1..5 and count 1..10000")
    if not 1 <= args.max_job_attempts <= 10:
        parser.error("max-job-attempts must be 1..10")
    if args.proxy_sessions < 0:
        parser.error("proxy-sessions must be 0..5")
    if args.browser_engine in {"nodriver", "undetected"} and not args.headed:
        parser.error(f"{args.browser_engine} requires --headed (use Xvfb without a physical monitor)")
    if args.browser_engine == "undetected" and (args.workers != 1 or args.proxy_sessions):
        parser.error("experimental undetected mode requires --workers 1 and no proxy-sessions")
    if args.browser_engine == "nodriver" and args.network == "direct" and args.workers != 1:
        parser.error("parallel nodriver workers require distinct Webshare proxies")
    if args.browser_engine == "nodriver" and not args.proxy_sessions and args.workers > 3:
        parser.error("parallel nodriver mode supports at most 3 proxy-backed browsers")
    if (args.timeout <= 0 or not math.isfinite(args.delay) or args.delay < 0 or args.notify_interval < 30
            or not math.isfinite(args.max_minutes) or args.max_minutes <= 0):
        parser.error("timeout/max-minutes must be positive, delay non-negative, notify-interval >= 30")
    try:
        raise SystemExit(asyncio.run(run(args)))
    except ValueError as exc:
        parser.error(str(exc))
    except KeyboardInterrupt:
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
