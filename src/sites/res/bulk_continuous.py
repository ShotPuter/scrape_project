"""Continuous RES queue with concurrent saved-link downloads and durable quota waits."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import math
import os
import ssl
import time
from contextlib import suppress
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx

from scripts import res_free_pilot as pilot
from src.proxy.reputation import screen_proxy
from src.sites.res import http_flow
from src.sites.res.audio import load_local_audio_solver
from src.sites.res.bulk import iso_day, santiago_today
from src.sites.res.bulk_pipeline import discover_dates
from src.sites.res.burst_verifier import burst_verify, fetch_pending_cves, mint_recaptcha_token
from src.sites.res.engines import launch_context
from src.sites.res.ready_download import download_ready
from src.sites.res.source import SearchIssue
from src.sites.res.stream_notifications import StreamNotifications
from src.sites.res.tracking import TrackingStore
from src.utils.atomic import atomic_write_json
from src.utils.error_details import safe_message


def next_day(store, start, end, kind):
    """Discover one missing completed day at a time, newest first."""
    done = {
        row[0]
        for row in store.db.execute(
            "SELECT day FROM date_searches WHERE run_id=? AND kind=? AND complete=1", (store.run_id, kind)
        )
    }
    day = min(end or santiago_today() - timedelta(days=1), santiago_today() - timedelta(days=1))
    while day >= start:
        if day.isoformat() not in done:
            return day
        day -= timedelta(days=1)
    return None


def result_action(status):
    """Only transport failures permit route failover. Restrictions always wait."""
    if status == "proxy_failed":
        return "failover"
    if status == "daily_limit":
        return "quota_wait"
    if status in {"captcha_required", "blocked"}:
        return "restriction_wait"
    if status in {"ok", "completed"}:
        return "continue"
    return "error_wait"


def issue_status(error):
    """A concurrent restriction takes precedence over a transport failure."""
    if isinstance(error, BaseExceptionGroup):
        statuses = [issue_status(item) for item in error.exceptions]
        for status in ("daily_limit", "blocked", "captcha_required", "failed", "proxy_failed"):
            if status in statuses:
                return status
        return statuses[0]
    return getattr(error, "status", "proxy_failed" if isinstance(error, httpx.TransportError) else "failed")


class ContinuousBulk:
    def __init__(self, args, store, notifications):
        self.args, self.store, self.notifications = args, store, notifications
        self.path = store.base / "continuous-state.json"
        self.state = json.loads(self.path.read_text()) if self.path.exists() else {}
        self.state.setdefault("tokens", 0)
        self.state.setdefault("network_failovers", 0)
        self.started = time.monotonic()
        self.initial_pdfs = self.counts()["pdfs"]
        self.last_pdfs = self.initial_pdfs
        self.last_notice = self.started
        self.notice_pdfs = self.initial_pdfs
        self.status = "starting"
        self.stop = asyncio.Event()
        self.endpoint = None
        self.endpoints = []
        self.solver = None

    def counts(self):
        statuses = dict(self.store.db.execute("SELECT status,count(*) FROM documents GROUP BY status"))
        files, size = self.store.db.execute("SELECT count(*),coalesce(sum(size_bytes),0) FROM document_files").fetchone()
        return {
            "pdfs": files,
            "bytes": size,
            "cves": sum(statuses.values()),
            "pending": statuses.get("pending", 0),
            "errors": statuses.get("verify_error", 0),
        }

    def snapshot(self):
        counts = self.counts()
        elapsed = max(0.001, time.monotonic() - self.started)
        return {
            "run_id": self.args.run_id,
            "state": self.status,
            **counts,
            "new_pdfs": counts["pdfs"] - self.initial_pdfs,
            "pdfs_per_minute": round((counts["pdfs"] - self.initial_pdfs) * 60 / elapsed, 2),
            "elapsed_seconds": round(elapsed, 2),
            "tokens": self.state["tokens"],
            "network_failovers": self.state["network_failovers"],
            "download_failures": self.state.get("download_failures", 0),
            "failures": self.state.get("failures", 0),
            "resume_at": self.state.get("resume_at", 0),
            "pause_reason": self.state.get("pause_reason"),
            "proxy": self.endpoint.label if self.endpoint else None,
            "telegram": dict(self.notifications.state),
        }

    def message(self, title):
        s = self.snapshot()
        resume = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(s["resume_at"])) if s["resume_at"] else "нет"
        status = {
            "starting": "подготовка",
            "running": "сбор идёт",
            "discovering": "поиск CVE",
            "verifying_and_downloading": "верификация и загрузка PDF",
            "downloading_saved_links": "докачивание PDF",
            "archive_current": "ожидание новых дней",
            "quota_wait": "ожидание сброса квоты",
            "restriction_wait": "пауза после ограничения",
            "error_wait": "пауза после ошибки",
            "failover": "смена прокси после сетевого сбоя",
            "stopped": "остановлено",
        }.get(s["state"], s["state"])
        return (
            f"{title} — RES {s['run_id']}\nСостояние: {status}\n"
            f"PDF всего: {s['pdfs']}; новых: {s['new_pdfs']} (+{s['pdfs'] - self.notice_pdfs} за период); "
            f"размер: {s['bytes'] / 1048576:.1f} МБ\n"
            f"CVE: {s['cves']}; в очереди: {s['pending']}; ошибки: {s['errors']}; "
            f"ошибок загрузки: {s['download_failures']}\n"
            f"Сбоев за всё время: {s['failures']}\n"
            f"Скорость: {s['pdfs_per_minute']:.2f} PDF/мин; токенов: {s['tokens']}\n"
            f"Смен прокси при сетевых сбоях: {s['network_failovers']}\n"
            f"Ожидание до: {resume}\n"
            f"Прошло: {s['elapsed_seconds'] / 60:.1f} мин"
        )

    def save(self):
        atomic_write_json(self.path, self.state)
        atomic_write_json(self.store.base / "progress.json", self.snapshot())

    def pause(self, status):
        seconds = (
            self.args.quota_hours * 3600
            if status == "daily_limit"
            else self.args.network_retry_seconds
            if status == "proxy_failed"
            else self.args.retry_seconds
        )
        self.state.update(resume_at=time.time() + seconds, pause_reason=status)
        self.status = result_action(status)
        self.save()
        self.notifications.publish("paused", self.message("⏸ Пауза"))

    async def monitor(self):
        while not self.stop.is_set():
            self.save()
            now = time.monotonic()
            if now - self.last_notice >= self.args.telegram_minutes * 60:
                self.notifications.publish("progress", self.message("📊 Статистика"))
                self.last_notice = now
                self.notice_pdfs = self.counts()["pdfs"]
            if self.last_pdfs == self.initial_pdfs and self.counts()["pdfs"] > self.last_pdfs:
                self.notifications.publish("first_pdf", self.message("📄 Первые PDF сохранены"))
            self.last_pdfs = self.counts()["pdfs"]
            with suppress(TimeoutError):
                await asyncio.wait_for(self.stop.wait(), 15)

    async def load_endpoints(self):
        endpoints = []
        errors = []
        for provider in self.args.providers.split(","):
            if provider == "webshare" and not os.getenv("WEBSHARE_API_TOKEN"):
                continue
            if provider == "brightdata" and not os.getenv("BRIGHTDATA_PROXY_USERNAME"):
                continue
            if provider == "residential" and not self.args.proxy_file.is_file():
                continue
            try:
                options = SimpleNamespace(
                    proxy_provider=provider,
                    proxy_limit=0 if provider == "webshare" else 1,
                    lazy_proxies=False,
                    proxy_file=self.args.proxy_file,
                    output=self.store.base,
                )
                endpoints.extend(await pilot.load_proxy_endpoints(options))
            except Exception as exc:
                errors.append(safe_message(exc))
        if not endpoints:
            raise SearchIssue("configuration_error", "; ".join(errors) or "Configure a proxy provider or use direct")
        self.endpoints = endpoints
        self.endpoint = endpoints[self.state.get("route_index", 0) % len(endpoints)]

    async def downloads(self, client, finished):
        """Consume committed links while serial verification keeps the token busy."""
        while True:
            was_finished = finished.is_set()
            result = await download_ready(
                self.store, client, limit=None, workers=self.args.workers, interval=self.args.interval
            )
            self.state["download_failures"] = self.state.get("download_failures", 0) + result["failed"]
            if result["stop_reason"]:
                reason = result["stop_reason"]
                raise SearchIssue("daily_limit" if "daily" in reason.lower() else "blocked", reason)
            if result.get("network_failures"):
                raise SearchIssue("proxy_failed", "PDF download transport failed; issued links remain queued")
            if result["failed"]:
                raise SearchIssue("retry_wait", "PDF response was invalid; issued links remain queued")
            if was_finished:
                return
            with suppress(TimeoutError):
                await asyncio.wait_for(finished.wait(), 0.5)

    async def cycle(self):
        endpoint = self.endpoint
        pinned = await screen_proxy(endpoint.url)
        browser_proxy = pilot.endpoint_from_url(pinned).browser if pinned else None
        verify = True
        if endpoint.ca_file:
            verify = ssl.create_default_context()
            verify.load_verify_locations(cafile=str(endpoint.ca_file))
        transport = httpx.AsyncHTTPTransport(proxy=pinned, verify=verify, trust_env=False, retries=1)
        async with httpx.AsyncClient(timeout=self.args.timeout, transport=transport, trust_env=False) as client:
            # Recover issued links before requesting any new tokens.
            self.status = "downloading_saved_links"
            initial = await download_ready(
                self.store, client, limit=None, workers=self.args.workers, interval=self.args.interval
            )
            if initial["stop_reason"]:
                raise SearchIssue(
                    "daily_limit" if "daily" in initial["stop_reason"].lower() else "blocked", initial["stop_reason"]
                )
            if initial.get("network_failures"):
                raise SearchIssue("proxy_failed", "Saved PDF link transport failed")
            if initial["failed"]:
                self.state["download_failures"] = self.state.get("download_failures", 0) + initial["failed"]
                raise SearchIssue("retry_wait", "Saved PDF response was invalid")
            # Only confirmed expired server links can be reissued. Good links survive outages.
            with self.store.db:
                self.store.db.execute(
                    "UPDATE documents SET status='pending',download_url=NULL,codigo=NULL "
                    "WHERE status IN ('expired_saved_link','expired_codigo')"
                )
            day = next_day(self.store, self.args.start, self.args.end, self.args.kind)
            if not fetch_pending_cves(self.store, 1) and day is None:
                self.status = "archive_current"
                return "idle"
            if self.solver is None:
                self.solver = await asyncio.to_thread(load_local_audio_solver, self.args.audio_model)
            with http_flow.new_session(timeout=self.args.timeout, proxy=pinned, verify=verify) as session:
                await asyncio.to_thread(http_flow.prepare, session)
                async with launch_context(
                    self.store.base / "profile",
                    proxy=browser_proxy,
                    engine="patchright",
                    headed=False,
                    proxy_ca=endpoint.ca_file,
                    proxy_ip=endpoint.expected_ip,
                ) as context:
                    page = await context.new_page()

                    async def mint(url=None):
                        from src.sites.dequienes.source import RES_URL

                        token = await mint_recaptcha_token(
                            page, self.solver, timeout_seconds=self.args.captcha_timeout, form_url=url or RES_URL
                        )
                        self.state["tokens"] += 1
                        return token

                    if not fetch_pending_cves(self.store, 1) and day is not None:
                        self.status = "discovering"
                        await discover_dates(
                            self.store, session, mint, SimpleNamespace(start=day, end=day, kind=self.args.kind)
                        )
                    self.status = "verifying_and_downloading"
                    finished = asyncio.Event()
                    async with asyncio.TaskGroup() as group:
                        group.create_task(self.downloads(client, finished))
                        try:
                            result = await burst_verify(
                                self.store, session, mint, limit=self.args.batch_size or None, max_tokens_per_session=None
                            )
                        finally:
                            finished.set()
                    atomic_write_json(self.store.base / "last-cycle.json", result)
                    self.state["cycles"] = self.state.get("cycles", 0) + 1
                    return result["status"]

    async def run(self):
        await self.notifications.start()
        self.notifications.publish("started", self.message("🚀 Непрерывный сбор запущен"))
        monitor = asyncio.create_task(self.monitor())
        cycles = 0
        try:
            while not self.stop.is_set():
                deadline = self.state.get("resume_at", 0)
                if deadline > time.time():
                    self.status = result_action(self.state.get("pause_reason"))
                    await asyncio.sleep(min(30, deadline - time.time()))
                    continue
                if deadline:
                    self.state.update(resume_at=0, pause_reason=None)
                    self.status = "running"
                    self.notifications.publish("resumed", self.message("▶️ Сбор возобновлён"))
                try:
                    if self.endpoint is None:
                        await self.load_endpoints()
                    status = await self.cycle()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    status = issue_status(exc)
                    self.notifications.record("error", safe_message(exc))
                cycles += 1
                if status not in {"ok", "completed", "idle"}:
                    self.state["failures"] = self.state.get("failures", 0) + 1
                action = result_action(status)
                if action == "failover":
                    self.state["route_index"] = self.state.get("route_index", 0) + 1
                    self.state["network_failovers"] += 1
                    self.endpoint = (
                        self.endpoints[self.state["route_index"] % len(self.endpoints)] if self.endpoints else None
                    )
                    self.notifications.publish("network_failover", self.message("🔄 Сетевой сбой, смена прокси"))
                    self.pause(status)
                elif status == "idle":
                    self.status = "archive_current"
                    if not self.args.max_cycles or cycles < self.args.max_cycles:
                        await asyncio.sleep(self.args.poll_seconds)
                elif action != "continue":
                    self.pause(status)
                self.save()
                if self.args.max_cycles and cycles >= self.args.max_cycles:
                    return 0 if status in {"ok", "completed", "idle"} else 2
        finally:
            self.stop.set()
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
            self.status = "stopped"
            self.save()
            await self.notifications.finish(self.message("⏹ Сбор остановлен, очередь сохранена"))
            self.save()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("data/res-bulk/continuous/tracking.sqlite3"))
    parser.add_argument("--run-id", default="res-continuous")
    parser.add_argument("--start", type=iso_day, default=iso_day("2013-01-01"))
    parser.add_argument("--end", type=iso_day)
    parser.add_argument("--kind", choices=("action", "registration"), default="action")
    parser.add_argument("--providers", default="webshare,brightdata,residential")
    parser.add_argument("--proxy-file", type=Path, default=Path("data/residential-proxies.txt"))
    parser.add_argument("--batch-size", type=int, default=0, help="Diagnostic per-cycle bound; 0 drains pending CVEs")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--interval", type=float, default=0.1)
    parser.add_argument("--quota-hours", type=float, default=24)
    parser.add_argument("--retry-seconds", type=float, default=300)
    parser.add_argument("--network-retry-seconds", type=float, default=10)
    parser.add_argument("--poll-seconds", type=float, default=600)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--captcha-timeout", type=int, default=130)
    parser.add_argument("--audio-model", default="base")
    parser.add_argument("--telegram-minutes", type=float, default=5)
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--max-cycles", type=int, default=0, help="Diagnostic bounded run; 0 runs continuously")
    args = parser.parse_args(argv)
    args.telegram_events = "details"
    if (
        min(
            args.workers,
            args.quota_hours,
            args.retry_seconds,
            args.network_retry_seconds,
            args.poll_seconds,
            args.timeout,
            args.captcha_timeout,
            args.telegram_minutes,
        )
        <= 0
        or args.interval < 0
        or args.batch_size < 0
        or args.max_cycles < 0
    ):
        parser.error("Budgets must be positive; interval and max-cycles must be nonnegative")
    if not all(
        math.isfinite(value)
        for value in (
            args.quota_hours,
            args.retry_seconds,
            args.network_retry_seconds,
            args.poll_seconds,
            args.timeout,
            args.interval,
            args.telegram_minutes,
        )
    ):
        parser.error("Budgets must be finite")
    if set(args.providers.split(",")) - {"webshare", "brightdata", "residential", "direct"}:
        parser.error("Unknown proxy provider")
    if (
        args.start < iso_day("2013-01-01")
        or args.start > (args.end or santiago_today())
        or (args.end and args.end > santiago_today())
    ):
        parser.error("Invalid date range")
    args.db.parent.mkdir(parents=True, exist_ok=True)
    with args.db.with_suffix(".bulk.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("Another bulk worker already uses this database")
        store = TrackingStore(args.db, args.run_id)
        try:
            store.register_run({"mode": "continuous", "start": str(args.start), "end": str(args.end)}, [])
            notifications = StreamNotifications(args, store.base)
            result = asyncio.run(ContinuousBulk(args, store, notifications).run())
        finally:
            store.close()
    return result


if __name__ == "__main__":
    raise SystemExit(main())
