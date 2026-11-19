"""Continuous HTTP proxy discovery feeding up to ten isolated RES browser slots."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import ipaddress
import json
import math
import os
import re
import shutil
import signal
import time
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from scripts import res_free_pilot as pilot
from src.proxy.candidate_filter import filter_candidate_ips
from src.proxy.reputation import (
    ReputationDenied,
    check_ip,
    public_collection_allowed,
    reputation_mode,
    screen_proxy,
    validate_ip,
)
from src.proxy.sources.catalog import load_public_catalog
from src.proxy.sources.free_proxy import load_free_proxy_candidates
from src.proxy.sources.registry import resolve_sources
from src.sites.res import benchmark
from src.sites.res.free_queue import FreeQueue
from src.sites.res.http_flow import USER_AGENT
from src.sites.res.open_data import select_archive, select_companies
from src.sites.res.stream_notifications import StreamNotifications
from src.sites.res.tracking import TrackingStore, import_run
from src.utils.atomic import atomic_write_json
from src.utils.error_details import safe_message

TERMINAL = {"downloaded", "not_found", "no_cve_found"}


@dataclass
class BatchResult:
    completed: set[str] = field(default_factory=set)
    attempted: set[str] = field(default_factory=set)
    terminal_errors: set[str] = field(default_factory=set)
    quota_companies: set[str] = field(default_factory=set)
    quota_limited: bool = False
    fleet_limit: str | None = None
    retry_connection: bool = False
    new_pdfs: int = 0
    error: str | None = None


def endpoint_allowed(endpoint: str, exceptions: set[tuple[str, int]], ports: set[int] | None = None) -> bool:
    """Match the host firewall policy before spending any network requests."""
    try:
        parts = urlsplit(endpoint)
        if parts.scheme not in {"http", "socks5", "socks5h"} or parts.username or parts.password:
            return False
        address = ipaddress.ip_address(validate_ip(parts.hostname or ""))
        return address.version == 4 and (
            parts.port in (ports if ports is not None else {80, 443}) or (str(address), parts.port) in exceptions
        )
    except ValueError:
        return False


def endpoint_exceptions(value: str) -> set[tuple[str, int]]:
    result = set()
    for item in value.split():
        host, port_text = item.rsplit(":", 1)
        address = validate_ip(host)
        port = int(port_text)
        if ipaddress.ip_address(address).version != 4 or not 1 <= port <= 65535:
            raise ValueError("Proxy exception must be an exact public IPv4:port")
        result.add((address, port))
    return result


def proxy_ports(value: str) -> set[int]:
    """Explicit application policy; does not grant corresponding host firewall access."""
    ports = {int(item.strip()) for item in value.split(",") if item.strip()}
    if not ports or any(port < 1 or port > 65535 for port in ports):
        raise ValueError("Proxy ports must be a comma-separated list of ports 1..65535")
    return ports


def probe_error_category(error: Exception) -> str:
    """Distinguish local setup errors from candidate connectivity and RES refusals."""
    if isinstance(error, ReputationDenied):
        return "reputation_denied"
    if isinstance(error, ImportError):
        return "missing_socks_dependency" if "socksio" in str(error).lower() else "missing_dependency"
    if isinstance(error, (httpx.TimeoutException, TimeoutError)):
        return "timeout"
    if isinstance(error, httpx.HTTPStatusError):
        return f"http_{error.response.status_code}"
    if isinstance(error, httpx.ProxyError):
        return "proxy_error"
    if isinstance(error, httpx.ConnectError):
        return "tls_error" if "ssl" in str(error).lower() or "certificate" in str(error).lower() else "connect_error"
    if isinstance(error, httpx.RemoteProtocolError):
        return "protocol_error"
    if "RES form missing" in str(error):
        return "res_form_missing"
    if "exit changed" in str(error):
        return "exit_changed"
    return type(error).__name__


def require_socks_support() -> None:
    """Fail before scanning if the image cannot construct a SOCKS transport."""
    try:
        import socksio  # noqa: F401 - HTTPX optional transport dependency
    except ImportError:
        raise RuntimeError("RES requires SOCKS support: rebuild the image with httpx[socks]") from None


class ProxyProducer:
    """Downloads only public list data; all target probing is HTTP, never a browser."""

    def __init__(self, args, root: Path, queue: FreeQueue, changed: asyncio.Event, stop: asyncio.Event):
        self.args, self.root, self.queue, self.changed, self.stop = args, root, queue, changed, stop
        self.exceptions = endpoint_exceptions(args.allowed_proxy_endpoints)
        self.ports = proxy_ports(args.proxy_ports)
        self.stats: dict[str, Any] = {
            "checks": 0,
            "ready": 0,
            "denied_or_failed": 0,
            "catalog_refreshes": 0,
            "reputation_denied": 0,
            "connection_failed": 0,
            "configuration_failed": 0,
            "connection_rechecks": 0,
            "connection_recovered": 0,
            "failure_reasons": {},
            "not_admitted": 0,
            "recent_ready": [],
        }
        self.notifications: StreamNotifications | None = None
        self.next_catalog = 0.0
        self.next_probe = 0.0
        self.next_pool_count = 0.0
        self.pool_counts: dict[str, int] = {}
        self.rate_lock = asyncio.Lock()
        self.security_pause_until = 0.0

    async def refresh(self) -> None:
        sources = resolve_sources(self.args.sources)
        public = [source for source in sources if source != "free-proxy"]
        catalog = await load_public_catalog(
            public,
            timeout=self.args.proxy_timeout,
            cache_dir=self.root / "source-cache",
            download_concurrency=3,
            max_records=self.args.source_records or None,
            max_response_bytes=16 * 1024 * 1024,
        )
        raw = [(candidate.url, candidate.source) for candidate in sorted(catalog.candidates, key=lambda row: row.priority)]
        if "free-proxy" in sources:
            try:
                extra = await asyncio.wait_for(load_free_proxy_candidates(request_timeout=10), 35)
                raw.extend((endpoint, "free-proxy") for endpoint in extra)
            except Exception as error:
                catalog.errors["free-proxy"] = type(error).__name__
        seen: set[str] = set()
        local_rejected: Counter[str] = Counter()
        ipv4_rows: list[tuple[str, str, str]] = []
        for endpoint, source in raw:
            if endpoint in seen:
                local_rejected["duplicates"] += 1
                continue
            seen.add(endpoint)
            try:
                parts = urlsplit(endpoint)
                if parts.scheme not in {"http", "socks5", "socks5h"} or parts.username or parts.password or not parts.port:
                    local_rejected["unsupported_endpoint"] += 1
                    continue
                address = ipaddress.ip_address(parts.hostname or "")
                if address.version != 4 or not address.is_global:
                    local_rejected["non_public_ipv4"] += 1
                    continue
                try:
                    validate_ip(str(address))
                except ReputationDenied:
                    local_rejected["blocked_networks"] += 1
                    continue
                ipv4_rows.append((endpoint, source, str(address)))
            except ValueError:
                local_rejected["invalid_endpoint"] += 1
        self.stats.update(
            source_counts=catalog.source_counts,
            source_errors=catalog.errors,
            catalog_candidates=len(raw),
            catalog_unique=len(seen),
            local_rejected=dict(local_rejected),
            prefilter_status="screening",
        )
        # Download cached lists once; all IP membership tests are local and off-loop.
        try:
            screened, feed_rejected = await asyncio.to_thread(filter_candidate_ips, ipv4_rows)
        except Exception:
            self.stats["prefilter_status"] = "blocked"
            raise
        eligible = [
            (endpoint, source) for endpoint, source in screened if endpoint_allowed(endpoint, self.exceptions, self.ports)
        ]
        rejected_ports = Counter(
            str(urlsplit(endpoint).port)
            for endpoint, _ in screened
            if not endpoint_allowed(endpoint, self.exceptions, self.ports)
        )
        candidates = eligible[: self.args.max_candidates] if self.args.max_candidates else eligible
        self.queue.prune(unseen_for=86400)
        removed = self.queue.retain_candidates(endpoint for endpoint, _ in candidates)
        inserted = self.queue.add_candidates(candidates, capacity=self.args.max_candidates)
        self.stats.update(
            feed_rejected=feed_rejected,
            blacklist_passed=len(screened),
            port_rejected=len(screened) - len(eligible),
            rejected_ports=dict(rejected_ports.most_common(10)),
            port_eligible=len(eligible),
            selected_candidates=len(candidates),
            capacity_rejected=len(eligible) - len(candidates),
            queue_inserted=inserted,
            queue_archived=removed,
            queue_removed=0,
            prefilter_status="ready",
        )
        if self.notifications:
            self.notifications.activity(
                "catalog_filtered",
                f"🧹 RES {self.args.run_id}: каталог отфильтрован\n"
                f"Получено: {len(raw)}; уникальных endpoint: {len(seen)}\n"
                f"Запрещённые сети (8 диапазонов): {local_rejected['blocked_networks']}; "
                f"блоклисты: {sum(feed_rejected.values())}\n"
                f"Прошло IP-фильтры: {len(screened)}; отклонено по портам: {len(screened) - len(eligible)}\n"
                f"Частые исключённые порты: {dict(rejected_ports.most_common(5))}\n"
                f"В выбранном пуле: {len(candidates)}; добавлено новых: {inserted}; лимит пула: "
                f"{self.args.max_candidates or 'не задан'}",
            )
        self.stats.pop("catalog_error", None)
        self.stats["catalog_refreshes"] += 1
        # Bound raw download cache independently from the small durable queue.
        cached = sorted((self.root / "source-cache").glob("*"), key=lambda path: path.stat().st_mtime)
        retained = sum(path.stat().st_size for path in cached if path.is_file())
        for path in cached:
            if retained <= self.args.source_cache_mib * 1024 * 1024:
                break
            if path.is_file():
                retained -= path.stat().st_size
                path.unlink(missing_ok=True)
        self.changed.set()

    async def verify(self, endpoint: str) -> str:
        pinned = await screen_proxy(endpoint)
        async with httpx.AsyncClient(
            proxy=pinned,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(self.args.proxy_timeout, connect=min(8, self.args.proxy_timeout)),
            headers={"User-Agent": USER_AGENT},
        ) as client:

            async def get(url: str, limit: int = 2 * 1024 * 1024) -> bytes:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > limit:
                            raise ValueError("Proxy probe response too large")
                    return bytes(body)

            exit_ip = str(ipaddress.ip_address(json.loads(await get("https://api.ipify.org?format=json", 65536))["ip"]))
            await asyncio.to_thread(check_ip, exit_ip)
            for url, marker in ((pilot.FORM_URL, b"ddlTipoBusqueda"), (pilot.RES_URL, b"txtCve")):
                if marker not in await get(url):
                    raise ValueError("RES form missing")
            after = str(ipaddress.ip_address(json.loads(await get("https://api.ipify.org?format=json", 65536))["ip"]))
            if after != exit_ip:
                raise ValueError("Proxy exit changed during preflight")
            return exit_ip

    async def scan(self) -> None:
        while not self.stop.is_set():
            if time.monotonic() >= self.next_pool_count:
                self.pool_counts = self.queue.proxy_counts()
                self.next_pool_count = time.monotonic() + 1
            counts = self.pool_counts
            if (
                counts.get("ready", 0) + counts.get("leased", 0) >= self.args.ready_capacity
                or time.monotonic() < self.security_pause_until
            ):
                await asyncio.sleep(0.5)
                continue
            candidate = self.queue.claim_probe(fresh_for=max(3600, self.args.catalog_seconds * 3))
            if not candidate:
                await asyncio.sleep(0.5)
                continue
            cancelled = False
            if candidate.get("connection_retry"):
                self.stats["connection_rechecks"] += 1
            try:
                async with self.rate_lock:
                    await asyncio.sleep(max(0, self.next_probe - time.monotonic()))
                    self.next_probe = time.monotonic() + 60 / self.args.proxy_rate_per_minute
                if (candidate.get("connection_retry") or candidate.get("last_success", 0)) and not endpoint_allowed(
                    candidate["endpoint"], self.exceptions, self.ports
                ):
                    raise ReputationDenied("Proxy no longer allowed by endpoint policy")
                async with asyncio.timeout(self.args.proxy_timeout * 5 + 30):
                    exit_ip = await self.verify(candidate["endpoint"])
                if self.queue.finish_probe(
                    candidate["endpoint"], exit_ip=exit_ip, ttl=self.args.proxy_ttl, retry=self.args.proxy_retry_seconds
                ):
                    self.stats["ready"] += 1
                    if candidate.get("connection_retry"):
                        self.stats["connection_recovered"] += 1
                        if self.notifications:
                            self.notifications.activity(
                                "connection_recovered", f"✅ RES: прокси восстановлен: {candidate['endpoint']}"
                            )
                    self.stats["recent_ready"] = [*self.stats["recent_ready"][-4:], exit_ip]
                    if self.notifications:
                        self.notifications.activity(
                            "proxy_ready",
                            f"✅ RES {self.args.run_id}: прокси допущен\n"
                            f"Endpoint: {candidate['endpoint']}; выходной IP: {exit_ip}\n"
                            f"Репутация и обе страницы RES проверены; проверок: {self.stats['checks'] + 1}",
                        )
                    self.changed.set()  # Dispatch immediately, not after a catalog/verification batch.
                else:
                    self.stats["not_admitted"] += 1
            except asyncio.CancelledError:
                cancelled = True
                self.queue.finish_probe(candidate["endpoint"], retry=0, error="probe interrupted")
                raise
            except Exception as error:
                category = probe_error_category(error)
                message = f"{category}: {safe_message(error) or type(error).__name__}"
                self.stats["failure_reasons"][category] = self.stats["failure_reasons"].get(category, 0) + 1
                self.queue.finish_probe(
                    candidate["endpoint"],
                    retry=self.args.proxy_retry_seconds,
                    error=message,
                    retryable=category in {"timeout", "connect_error", "proxy_error", "protocol_error", "ReadError"},
                    connection_retry_delay=self.args.connection_retry_delay,
                )
                self.stats["denied_or_failed"] += 1
                counter = (
                    "reputation_denied"
                    if isinstance(error, ReputationDenied)
                    else "configuration_failed"
                    if isinstance(error, ImportError)
                    else "connection_failed"
                )
                self.stats[counter] += 1
                if "feed" in message.lower() and any(
                    word in message.lower() for word in ("unavailable", "expired", "invalid")
                ):
                    self.security_pause_until = time.monotonic() + 60
                self.stats["last_error"] = message
            finally:
                if not cancelled:
                    self.stats["checks"] += 1

    async def run(self) -> None:
        require_socks_support()
        scanners = [asyncio.create_task(self.scan()) for _ in range(self.args.proxy_check_concurrency)]
        try:
            while not self.stop.is_set():
                if any(scanner.done() and not scanner.cancelled() and scanner.exception() for scanner in scanners):
                    raise RuntimeError("A proxy verifier task failed")
                if time.monotonic() >= self.next_catalog:
                    try:
                        await self.refresh()
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        self.stats["catalog_error"] = safe_message(error)
                    self.next_catalog = time.monotonic() + self.args.catalog_seconds
                await asyncio.sleep(0.5)
        finally:
            for scanner in scanners:
                scanner.cancel()
            await asyncio.gather(*scanners, return_exceptions=True)


async def stop_child(process: asyncio.subprocess.Process) -> None:
    """A replacement is never started until the previous worker has exited."""
    if process.returncode is None:
        with suppress(ProcessLookupError):
            process.send_signal(signal.SIGINT)
        try:
            await asyncio.wait_for(process.wait(), 45)
        except TimeoutError:
            process.kill()
            await process.wait()
    # Also terminate surviving browser/Xvfb children in this worker's own session.
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    await asyncio.sleep(0.1)
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)


class BrowserWorker:
    def __init__(self, args, root: Path, store: TrackingStore):
        self.args, self.root, self.store = args, root, store
        self.active: dict[int, dict[str, Any]] = {}

    async def __call__(self, slot: int, proxy: dict[str, Any], companies: list[dict[str, Any]], owner: str) -> BatchResult:
        directory = self.root / "workers" / f"w{slot:02d}"
        directory.mkdir(parents=True, exist_ok=True)
        self.store.restore_worker(directory, companies)
        benchmark.write_database(directory / "queue.sqlite3", companies)
        (directory / "proxy.txt").write_text(proxy["endpoint"] + "\n")
        prior = {str(path) for path in directory.glob("report-*.json")}
        command = benchmark.child_command(self.args, directory, len(companies))

        def option(name: str, value: str) -> None:
            command[command.index(name) + 1] = value

        option("--proxy-provider", "residential")
        option("--proxy-limit", "1")
        option("--company-attempts", "1")
        option("--max-failures", "1")
        option("--error-cooldown-minutes", "0")
        option("--max-cooldowns", "0")
        option("--solves-per-proxy", "0")
        command.extend(
            [
                "--proxy-file",
                str(directory / "proxy.txt"),
                "--expected-proxy-ip",
                proxy["exit_ip"],
                "--site-limit-scope",
                "proxy",
                "--stream-db",
                str(self.args.output / "free-runtime.sqlite3"),
                "--stream-owner",
                owner,
                "--stream-company-attempts",
                str(self.args.company_attempts),
                "--stream-network-retries",
                str(self.args.session_network_retries),
            ]
        )
        batch_started_at = time.time()
        log = directory / (owner + ".log")
        result = BatchResult()
        process = None
        self.active[slot] = {
            "slot": slot,
            "endpoint": proxy["endpoint"],
            "exit_ip": proxy["exit_ip"],
            "owner": owner,
            "directory": str(directory),
            "prior_reports": list(prior),
            "log": str(log),
            "ready": False,
        }
        try:
            with log.open("w") as output:
                process = await asyncio.create_subprocess_exec(
                    *command,
                    stdout=output,
                    stderr=asyncio.subprocess.STDOUT,
                    start_new_session=True,
                    pass_fds=(self.args.runtime_lock_fd,),
                )
                self.active[slot]["pid"] = process.pid
                if getattr(self, "notifications", None):
                    self.notifications.activity(
                        "browser_started",
                        f"🖥 RES {self.args.run_id}: запущен процесс слота {slot + 1}\n"
                        f"Прокси: {proxy['endpoint']}; ожидаемый IP: {proxy['exit_ip']}\n"
                        "Непрерывная сессия: компании по одной, без лимита количества; подготовка браузера",
                    )
                try:
                    waiter = asyncio.create_task(process.wait())
                    last_activity = time.monotonic()
                    seen_report_time = 0
                    try:
                        while not waiter.done():
                            report_time = max(
                                (
                                    path.stat().st_mtime_ns
                                    for path in directory.glob("report-*.json")
                                    if str(path) not in prior
                                ),
                                default=0,
                            )
                            if report_time > seen_report_time:
                                seen_report_time = report_time
                                last_activity = time.monotonic()
                            if time.monotonic() - last_activity >= self.args.worker_timeout:
                                raise TimeoutError
                            with suppress(TimeoutError):
                                await asyncio.wait_for(asyncio.shield(waiter), 1)
                    finally:
                        if not waiter.done():
                            waiter.cancel()
                        await asyncio.gather(waiter, return_exceptions=True)
                except TimeoutError:
                    result.error = "Browser worker inactivity timeout"
                except asyncio.CancelledError:
                    result.error = "Dispatcher stopping"
                finally:
                    await stop_child(process)
        except Exception as error:
            result.error = safe_message(error)
        finally:
            # This slot owns exactly one profile and the prior process has been reaped.
            if process is not None and process.returncode is None:
                await stop_child(process)
            shutil.rmtree(directory / "profile", ignore_errors=True)
            self.active.pop(slot, None)
        state = pilot.load_state(directory / "state.json")
        new_reports = [benchmark.read_json(path, {}) for path in directory.glob("report-*.json") if str(path) not in prior]
        latest = {row["company_id"]: row for report in new_reports for row in report.get("companies", [])}
        result.attempted = {attempt["company_id"] for report in new_reports for attempt in report.get("attempts", [])}
        # A killed process may have begun an attempt before its first JSON checkpoint.
        result.attempted.update(
            row[0]
            for row in self.store.db.execute(
                "SELECT company_id FROM attempts WHERE run_id=? AND worker=? AND started_at>=?",
                (self.args.run_id, directory.name, batch_started_at),
            )
        )
        with self.store.db:
            self.store.db.execute(
                "UPDATE attempts SET status='interrupted',finished_at=? WHERE run_id=? AND worker=? AND "
                "started_at>=? AND status='running'",
                (time.time(), self.args.run_id, directory.name, batch_started_at),
            )
        result.new_pdfs = sum(
            report.get("totals", {}).get("downloaded", report.get("progress", {}).get("downloaded", 0))
            for report in new_reports
        )
        # A continuous worker receives only its first company from the parent;
        # its final report includes all later companies claimed from SQLite.
        result.completed.update(identifier for identifier, row in latest.items() if row.get("status") in TERMINAL)
        result.terminal_errors.update(
            identifier for identifier, row in latest.items() if row.get("status") == "ambiguous_match"
        )
        for company in companies:
            cached = pilot.cached_company_result(company, state, self.args, directory)
            row = cached or latest.get(company["company_id"], {})
            if row.get("status") in TERMINAL:
                result.completed.add(company["company_id"])
                self.store.company_status(company["company_id"], row["status"])
            elif row.get("status") == "ambiguous_match":
                result.terminal_errors.add(company["company_id"])
            if row.get("error"):
                result.error = safe_message(row["error"])
        result.quota_companies = {
            attempt["company_id"]
            for report in new_reports
            for attempt in report.get("attempts", [])
            if attempt["result"].get("status") == "daily_limit"
        }
        result.quota_companies.update(
            row[0]
            for row in self.store.db.execute(
                "SELECT company_id FROM attempts WHERE run_id=? AND worker=? AND started_at>=? AND status='daily_limit'",
                (self.args.run_id, directory.name, batch_started_at),
            )
        )
        result.retry_connection = not result.attempted and any(
            proxy.get("status") == "proxy_failed"
            and not proxy.get("http_ip")
            and any(word in proxy.get("error", "") for word in ("ConnectTimeout", "ConnectError", "ReadTimeout"))
            and not any(word in proxy.get("error", "").lower() for word in ("ssl", "certificate", "reputation"))
            for report in new_reports
            for proxy in report.get("proxies", [])
        )
        result.quota_limited = bool(result.quota_companies) or any(
            report.get("stop_reason", {}).get("code") == "daily_document_limit" for report in new_reports
        )
        explicit_reason = next(
            (
                report.get("stop_reason", {}).get("error")
                for report in reversed(new_reports)
                if report.get("stop_reason", {}).get("error")
            ),
            None,
        )
        if explicit_reason and any(report.get("stop_reason", {}).get("code") != "queue_drained" for report in new_reports):
            result.error = safe_message(explicit_reason)
        if not result.error and len(result.completed) < len(companies):
            result.error = next(
                (
                    safe_message(proxy["error"])
                    for report in new_reports
                    for proxy in report.get("proxies", [])
                    if proxy.get("error")
                ),
                "Сессия завершена; незавершённые компании вернутся в очередь",
            )
        if getattr(self, "notifications", None):
            self.notifications.activity(
                "browser_finished",
                f"🔁 RES {self.args.run_id}: сессия слота {slot + 1} завершена\n"
                f"Компаний завершено: {len(result.completed)}; новых PDF: {result.new_pdfs}\n"
                + (
                    "Причина: лимит RES; прокси уйдёт на паузу"
                    if result.quota_limited
                    else f"Причина: {result.error}"
                    if result.error
                    else "Порция обработана; слот доступен следующему прокси"
                ),
            )
        return result


class Dispatcher:
    """Each consumer serially owns one browser slot; producer never owns a slot."""

    def __init__(
        self,
        args,
        queue: FreeQueue,
        worker: Callable[..., Awaitable[BatchResult]],
        store: TrackingStore,
        changed: asyncio.Event,
        stop: asyncio.Event,
    ):
        if not 1 <= args.browsers <= 10:
            raise ValueError("Free dispatcher supports at most ten browsers")
        self.args, self.queue, self.worker, self.store, self.changed, self.stop = args, queue, worker, store, changed, stop
        self.active: dict[int, str] = {}
        self.proxy_failures = 0
        self.total_new_pdfs = 0
        self.site_limits: list[dict[str, Any]] = []

    async def consume(self, slot: int) -> None:
        while not self.stop.is_set():
            control = self.store.cooldown_state()
            if control["halted"]:
                self.stop.set()
                return
            lease = None if control["active"] else self.queue.claim_batch(uuid.uuid4().hex, 1)
            if not lease:
                with suppress(TimeoutError):
                    await asyncio.wait_for(self.changed.wait(), 0.5)
                self.changed.clear()
                continue
            proxy, companies = lease
            owner = proxy["owner"]
            self.active[slot] = owner
            result = BatchResult(error="Worker interrupted before processing")
            recovery_queued = False
            try:
                result = await self.worker(slot, proxy, companies, owner)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                result.error = safe_message(error)
            finally:
                # Persist the exhausted route before returning its jobs to the queue.
                if result.quota_limited:
                    self.queue.quarantine_proxy(proxy["endpoint"], owner, self.args.site_limit_hours * 3600, result.error)
                elif result.retry_connection:
                    recovery_queued = self.queue.schedule_connection_retry(
                        proxy["endpoint"],
                        owner,
                        attempts=self.args.connection_retries,
                        delay=self.args.connection_retry_delay,
                        error=result.error,
                    )
                    if recovery_queued and getattr(self.worker, "notifications", None):
                        self.worker.notifications.activity(
                            "connection_retry",
                            f"🔄 RES: прокси {proxy['endpoint']} в очереди повторной проверки; "
                            f"пауза {self.args.connection_retry_delay:g} с, максимум {self.args.connection_retries} попытки",
                        )
                for company in self.queue.owned_companies(owner):
                    identifier = company["company_id"]
                    self.queue.finish_job(
                        identifier,
                        owner,
                        complete=identifier in result.completed,
                        attempted=identifier in result.attempted and identifier not in result.quota_companies,
                        limit=1 if identifier in result.terminal_errors else self.args.company_attempts,
                        retry=self.args.retry_seconds,
                        error=result.error,
                    )
                if result.quota_limited:
                    self.site_limits.append(
                        {
                            "endpoint": proxy["endpoint"],
                            "exit_ip": proxy["exit_ip"],
                            "paused_until": time.time() + self.args.site_limit_hours * 3600,
                        }
                    )
                elif not recovery_queued:
                    self.queue.release_proxy(proxy["endpoint"], owner, self.args.proxy_retry_seconds, result.error)
                self.active.pop(slot, None)
                self.changed.set()
            self.total_new_pdfs += result.new_pdfs
            self.proxy_failures = (
                0 if result.completed or result.quota_limited or recovery_queued else self.proxy_failures + 1
            )
            if self.proxy_failures >= self.args.max_failures and self.args.error_cooldown_minutes > 0:
                self.store.request_cooldown(self.args.error_cooldown_minutes * 60, self.args.max_cooldowns or None)
                self.proxy_failures = 0


async def execute(args, root: Path) -> int:
    store = TrackingStore(args.tracking_db or args.output / "tracking.sqlite3", args.run_id)
    queue = FreeQueue(args.output / "free-runtime.sqlite3", args.run_id)
    stop, changed = asyncio.Event(), asyncio.Event()
    producer = ProxyProducer(args, root, queue, changed, stop)
    worker = BrowserWorker(args, root, store)
    notifications = StreamNotifications(args, root)
    producer.notifications = notifications
    worker.notifications = notifications
    dispatcher = Dispatcher(args, queue, worker, store, changed, stop)
    started = time.monotonic()
    invocation_started_at = time.time()
    children: list[asyncio.Task] = []
    interrupted = False
    summary: dict[str, Any] = {}
    fatal_error = None
    initial_pdf_count = 0

    def pdf_count() -> int:
        return store.db.execute(
            "SELECT COUNT(DISTINCT f.cve) FROM document_files f JOIN documents d USING(cve) JOIN "
            "company_documents cd USING(cve) JOIN run_companies rc USING(company_id) WHERE rc.run_id=? AND "
            "d.status='downloaded'",
            (args.run_id,),
        ).fetchone()[0]

    def attempt_counts() -> dict[str, int]:
        statuses = dict(
            store.db.execute(
                "SELECT status,COUNT(*) FROM attempts WHERE run_id=? AND started_at>=? GROUP BY status",
                (args.run_id, invocation_started_at),
            ).fetchall()
        )
        return {"attempts": sum(statuses.values()), "captcha_attempts": statuses.get("captcha_required", 0)}

    await notifications.start()
    notifications.publish("preparing", f"🚀 RES {args.run_id}: подготовка выборки и восстановление сохранённых PDF")
    try:
        config = {"year": args.year, "month": args.month, "limit": args.limit}
        if args.archive_from_year:
            config["archive_from_year"] = args.archive_from_year
        selection = benchmark.read_json(root / "selection.json")
        if selection and any(selection["config"].get(key) != value for key, value in config.items()):
            raise ValueError("Selection settings changed; choose a new --run-id")
        if not selection:
            selection = (
                await asyncio.to_thread(select_archive, args.archive_from_year, args.limit)
                if args.archive_from_year
                else (await asyncio.to_thread(select_companies, args.year, args.month, args.limit))
            )
            selection["config"] = config
            atomic_write_json(root / "selection.json", selection)
        companies = selection["companies"]
        import_run(store, root, max_documents=args.max_documents, selection=selection)
        queue.recover()
        queue.add_jobs(companies, retry_errors=args.retry_errors)
        # Reconcile all selected companies from the durable ledger, even after a
        # crash between the final PDF write and the scheduler acknowledgement.
        recovery = root / "recovery"
        known = {
            row[0]
            for row in store.db.execute(
                "SELECT c.company_id FROM run_companies rc JOIN companies c USING(company_id) "
                "WHERE rc.run_id=? AND c.search_status NOT IN ('pending','searching')",
                (args.run_id,),
            )
        }
        known_companies = [company for company in companies if company["company_id"] in known]
        store.restore_worker(recovery, known_companies)
        recovered = pilot.load_state(recovery / "state.json")
        complete = set()
        for company in known_companies:
            cached = pilot.cached_company_result(company, recovered, args, recovery)
            if cached and cached["status"] in TERMINAL:
                complete.add(company["company_id"])
                queue.mark_complete(company["company_id"])
        for identifier in queue.completed_company_ids() - complete:
            queue.reset_job(identifier)
        initial_pdf_count = pdf_count()
        control = store.cooldown_state()
        if control["reason"] == "daily_document_limit" and control["halted"] and not args.resume_after_site_limit:
            raise ValueError("Site daily limit is still recorded; resume only after reset with --resume-after-site-limit")
        store.prepare_cooldown(enabled=args.error_cooldown_minutes > 0)
        counts = queue.counts()["jobs"]
        if counts.get("pending", 0) and not public_collection_allowed():
            raise ValueError("Explicit free mode requires ALLOW_PUBLIC_PROXY_COLLECTION=1")
        store.run_status("running")
        notifications.publish(
            "started",
            f"🚀 RES {args.run_id} — запуск\n"
            f"Выборка: {selection.get('newest_period', f'{args.year}-{args.month:02d}')}, компаний: {len(companies)}\n"
            f"В очереди: {counts.get('pending', 0)}; уже завершено: {counts.get('done', 0)}\n"
            f"Браузеров максимум: {args.browsers}; CAPTCHA: {args.captcha_timeout:g} с\n"
            f"Проверка репутации: {reputation_mode()}; полная сводка каждые {args.telegram_minutes:g} мин",
        )
        if counts.get("pending", 0):
            children = [
                asyncio.create_task(producer.run()),
                *(asyncio.create_task(dispatcher.consume(slot)) for slot in range(args.browsers)),
            ]
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        next_checkpoint = 0.0
        last_work = time.monotonic()
        last_completed = counts.get("done", 0)
        ready_notified: set[str] = set()
        site_limit_cursor = 0
        while children and not stop.is_set():
            if not dispatcher.active and not queue.has_unfinished_jobs():
                break
            for task in children:
                if task.done() and not task.cancelled() and (failure := task.exception()):
                    raise RuntimeError(f"Dispatcher background task failed: {safe_message(failure)}") from failure
            control = store.cooldown_state()
            if control["halted"]:
                break
            if dispatcher.active:
                last_work = time.monotonic()
            if args.idle_minutes and time.monotonic() - last_work > args.idle_minutes * 60:
                summary["stop_reason"] = "no_working_proxies_timeout"
                break
            for active in list(worker.active.values()):
                current = await asyncio.to_thread(
                    lambda directory=active["directory"], prior=active["prior_reports"]: [
                        path for path in Path(directory).glob("report-*.json") if str(path) not in prior
                    ]
                )
                if current and active["owner"] in dispatcher.active.values():
                    report = benchmark.read_json(sorted(current)[-1], {})
                    active["ready"] = any(
                        proxy.get("http_ip") and proxy.get("http_ip") == proxy.get("browser_ip")
                        for proxy in report.get("proxies", [])
                    )
                    if active["ready"] and active["owner"] not in ready_notified:
                        ready_notified.add(active["owner"])
                        notifications.activity(
                            "browser_ready",
                            f"🌐 RES {args.run_id}: браузер в слоте {active['slot'] + 1} готов\n"
                            f"Выходной IP: {active['exit_ip']}; HTTP и браузер совпадают\n"
                            f"Активных слотов: {len(dispatcher.active)}/{args.browsers}",
                        )
            if len(dispatcher.site_limits) > site_limit_cursor:
                for event in dispatcher.site_limits[site_limit_cursor:]:
                    notifications.activity(
                        "site_limit",
                        f"⏸ RES {args.run_id}: прокси {event['endpoint']} достиг лимита; "
                        f"пауза {args.site_limit_hours:g} ч, очередь продолжает работу через другие прокси",
                    )
                site_limit_cursor = len(dispatcher.site_limits)
            if time.monotonic() >= next_checkpoint:
                counts = queue.counts()
                if counts["jobs"].get("done", 0) > last_completed:
                    last_work = time.monotonic()
                    last_completed = counts["jobs"]["done"]
                total_pdfs = pdf_count()
                summary.update(
                    run_id=args.run_id,
                    state="cooldown" if control["active"] else "running",
                    elapsed_seconds=round(time.monotonic() - started, 2),
                    counts=counts,
                    active_browsers=len(dispatcher.active),
                    browser_limit=args.browsers,
                    ready_browsers=sum(bool(active.get("ready")) for active in worker.active.values()),
                    new_pdfs=max(0, total_pdfs - initial_pdf_count),
                    producer=producer.stats,
                    subnets=queue.subnet_status(),
                    cooldown=control,
                    proxy_reputation_mode=reputation_mode(),
                    site_limit_pauses=dispatcher.site_limits,
                    total_pdfs=total_pdfs,
                    companies_with_pdf=store.db.execute(
                        "SELECT COUNT(DISTINCT cd.company_id) FROM company_documents cd JOIN "
                        "run_companies rc USING(company_id) JOIN document_files f USING(cve) WHERE rc.run_id=?",
                        (args.run_id,),
                    ).fetchone()[0],
                    telegram=dict(notifications.state),
                    security_paused=time.monotonic() < producer.security_pause_until,
                )
                summary.update(attempt_counts())
                atomic_write_json(root / "progress.json", summary)
                print(
                    f"RES stream: jobs={counts['jobs']}; proxies={counts['proxies']}; "
                    f"browser slots={len(dispatcher.active)}/{args.browsers}",
                    flush=True,
                )
                next_checkpoint = time.monotonic() + args.checkpoint_seconds
            notifications.update(summary)
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), 0.5)
        interrupted = stop.is_set() and not store.cooldown_state()["halted"]
    except (asyncio.CancelledError, KeyboardInterrupt):
        interrupted = True
        raise
    except Exception as error:
        fatal_error = safe_message(error)
        raise
    finally:
        stop.set()
        for task in children:
            task.cancel()
        if children:
            await asyncio.gather(*children, return_exceptions=True)
        for signum in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_running_loop().remove_signal_handler(signum)
        counts = queue.counts()
        summary.update(
            run_id=args.run_id,
            counts=counts,
            active_browsers=0,
            browser_limit=args.browsers,
            elapsed_seconds=round(time.monotonic() - started, 2),
            new_pdfs=max(0, pdf_count() - initial_pdf_count),
            producer=producer.stats,
            subnets=queue.subnet_status(),
            cooldown=store.cooldown_state(),
        )
        summary.update(attempt_counts())
        summary["site_limit_pauses"] = dispatcher.site_limits
        summary["error"] = fatal_error
        summary["state"] = (
            "failed"
            if fatal_error
            else "interrupted"
            if interrupted
            else "finished"
            if not counts["jobs"].get("pending", 0) and not counts["jobs"].get("error", 0)
            else "partial"
        )
        summary["companies"] = store.rows()
        summary["tracking_database"] = str(store.path)
        summary["documents"] = [
            dict(row)
            for row in store.db.execute(
                "SELECT DISTINCT d.cve,d.status,f.local_path,f.sha256,f.size_bytes FROM documents d JOIN "
                "company_documents cd USING(cve) JOIN run_companies rc USING(company_id) LEFT JOIN "
                "document_files f USING(cve) WHERE rc.run_id=?",
                (args.run_id,),
            )
        ]
        store.run_status(summary["state"])
        summary["ready_browsers"] = 0
        summary["total_pdfs"] = pdf_count()
        summary["companies_with_pdf"] = len({row["company_id"] for row in store.rows() if row.get("pdfs_saved", 0)})
        await notifications.finish(
            notifications.format_summary(summary, "🏁 Итог")
            + (f"\nПричина ошибки: {fatal_error}" if fatal_error else "")
            + f"\nОтчёт: {root / 'summary.json'}"
        )
        summary["telegram"] = dict(notifications.state)
        atomic_write_json(root / "summary.json", summary)
        atomic_write_json(root / "progress.json", {key: value for key, value in summary.items() if key != "companies"})
        queue.close()
        store.close()
    return 130 if interrupted else 0 if summary["state"] == "finished" else 2


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-from-year", type=int, default=0)
    parser.add_argument("--telegram-events", choices=("summary", "details"), default="summary")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--month", type=int, default=1)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--browsers", type=int, default=5)
    parser.add_argument("--session-network-retries", type=int, choices=(0, 1, 2), default=2)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--max-documents", type=int, default=0)
    parser.add_argument("--company-attempts", type=int, default=3)
    parser.add_argument("--sources", default="all")
    parser.add_argument("--max-candidates", type=int, default=0, help="0 = no candidate pool truncation")
    parser.add_argument("--source-records", type=int, default=0, help="0 = parse all records within source byte limit")
    parser.add_argument("--source-cache-mib", type=int, default=128)
    parser.add_argument("--catalog-seconds", type=float, default=600)
    parser.add_argument("--proxy-check-concurrency", type=int, default=12)
    parser.add_argument("--proxy-rate-per-minute", type=float, default=120)
    parser.add_argument("--proxy-timeout", type=float, default=15)
    parser.add_argument("--proxy-ttl", type=float, default=600)
    parser.add_argument("--connection-retries", type=int, default=2)
    parser.add_argument("--connection-retry-delay", type=float, default=30)
    parser.add_argument("--proxy-retry-seconds", type=float, default=300)
    parser.add_argument("--retry-seconds", type=float, default=30)
    parser.add_argument("--ready-capacity", type=int, default=20)
    parser.add_argument("--proxy-ports", default=os.getenv("RES_FREE_PROXY_PORTS", "80,443"))
    parser.add_argument("--allowed-proxy-endpoints", default=os.getenv("RES_FREE_ALLOWED_PROXY_ENDPOINTS", ""))
    parser.add_argument("--worker-timeout", type=float, default=1800)
    parser.add_argument(
        "--idle-minutes", type=float, default=0, help="Stop after no active worker/progress; 0 waits indefinitely"
    )
    parser.add_argument("--tokens-per-proxy", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=40)
    parser.add_argument("--captcha-timeout", type=float, default=130)
    parser.add_argument("--audio-model", default="base")
    parser.add_argument("--delay", type=float, default=3)
    parser.add_argument("--max-failures", type=int, default=20)
    parser.add_argument("--error-cooldown-minutes", type=float, default=10)
    parser.add_argument("--max-cooldowns", type=int, default=0, help="0 = unlimited error cooldown cycles")
    parser.add_argument("--site-limit-hours", type=float, default=24, help="Pause only a site-limited proxy/exit")
    parser.add_argument("--checkpoint-seconds", type=float, default=15)
    parser.add_argument("--telegram-minutes", type=float, default=30)
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--retry-errors", action="store_true", default=os.getenv("RES_RETRY_ERRORS", "0") == "1")
    parser.add_argument("--resume-after-site-limit", action="store_true")
    parser.add_argument("--run-id", default="res-free-2025-01-100")
    parser.add_argument("--output", type=Path, default=Path("data/res-benchmark"))
    parser.add_argument("--tracking-db", type=Path)
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}", args.run_id):
        parser.error("Invalid run id")
    if (
        not 1 <= args.browsers <= 10
        or not 1 <= args.month <= 12
        or not (args.limit >= 0 if args.archive_from_year else 1 <= args.limit <= 10000)
    ):
        parser.error("browsers: 1..10; month: 1..12; limit: 1..10000")
    if (
        any(
            value < 1
            for value in (
                args.batch_size,
                args.company_attempts,
                args.source_cache_mib,
                args.proxy_check_concurrency,
                args.ready_capacity,
                args.tokens_per_proxy,
                args.max_failures,
            )
        )
        or args.proxy_check_concurrency > 64
        or args.ready_capacity < args.browsers
        or not 0 <= args.connection_retries <= 2
        or (args.archive_from_year != 0 and not 2013 <= args.archive_from_year <= 2100)
        or args.max_candidates < 0
        or args.source_records < 0
        or args.max_documents < 0
        or args.max_cooldowns < 0
    ):
        parser.error("Invalid queue or concurrency bounds")
    timing = (
        args.catalog_seconds,
        args.proxy_rate_per_minute,
        args.proxy_timeout,
        args.proxy_ttl,
        args.connection_retry_delay,
        args.site_limit_hours,
        args.proxy_retry_seconds,
        args.worker_timeout,
        args.timeout,
        args.captcha_timeout,
        args.checkpoint_seconds,
        args.telegram_minutes,
    )
    if any(not math.isfinite(value) or value <= 0 for value in timing) or any(
        not math.isfinite(value) or value < 0
        for value in (args.idle_minutes, args.retry_seconds, args.delay, args.error_cooldown_minutes)
    ):
        parser.error("Invalid timing bounds")
    resolve_sources(args.sources)
    proxy_ports(args.proxy_ports)
    endpoint_exceptions(args.allowed_proxy_endpoints)
    args.refresh = False
    args.proxy_provider = "residential"
    args.proxy_sessions = 1
    return args


def main(argv=None):
    from dotenv import load_dotenv

    for name in (".env.res", ".env"):
        load_dotenv(name)
    args = parse_args(argv)
    args.output.mkdir(parents=True, exist_ok=True)
    root = args.output / args.run_id
    root.mkdir(exist_ok=True)
    with (args.output / ".free-dispatcher.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("A free dispatcher or its browser workers already holds the runtime lock", flush=True)
            return 1
        args.runtime_lock_fd = lock.fileno()
        try:
            return asyncio.run(execute(args, root))
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("RES free dispatcher interrupted; progress saved", flush=True)
            return 130
        except Exception as error:
            print("RES free dispatcher stopped: " + safe_message(error), flush=True)
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
