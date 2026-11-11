"""Bounded, asynchronous lifecycle and periodic RES notifications."""

from __future__ import annotations

import asyncio
import json
import math
import os
import time
from pathlib import Path
from types import SimpleNamespace

import aiohttp

from src.services.telegram import send_telegram
from src.utils.proxy_url import redact_proxy_credentials


def notification_settings(args):
    # Old programmatic callers and tests remain offline unless they opt in.
    if getattr(args, "no_telegram", False) or not hasattr(args, "telegram_interval_minutes"):
        return "", "", 300.0
    from dotenv import load_dotenv

    load_dotenv()
    minutes = args.telegram_interval_minutes
    if minutes is None:
        try:
            minutes = float(os.getenv("RES_TELEGRAM_INTERVAL_MINUTES", "5"))
        except ValueError as exc:
            raise ValueError("RES_TELEGRAM_INTERVAL_MINUTES must be a positive number") from exc
    if not math.isfinite(minutes) or minutes < 1:
        raise ValueError("Telegram interval must be finite and at least 1 minute")
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if bool(token) != bool(chat):
        print("RES Telegram disabled: set both TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID", flush=True)
        return "", "", minutes * 60
    return token, chat, minutes * 60


def format_message(snapshot):
    event = snapshot["event"]
    headings = {
        "started": "🚀 RES — запуск",
        "browsers_ready": "🌐 RES — браузеры подготовлены",
        "progress": "📊 RES — статус сбора",
        "finished": "🏁 RES — завершение",
    }
    counts = snapshot["statuses"]
    terminal = {"completed", "not_found", "no_cve_found", "ambiguous_match", "failed", "blocked", "captcha_required"}
    processed = sum(counts.get(status, 0) for status in terminal)
    errors = sum(counts.get(status, 0) for status in ("failed", "blocked", "captcha_required", "retry_wait"))
    target = snapshot["target"] if snapshot["target"] is not None else "все найденные"
    stages = {"search": "поиск компаний", "download": "скачивание PDF", "collect": "сбор компаний из DeQuienes"}
    if snapshot["stage"] == "collect":
        processed = snapshot.get("companies", 0)
    document_label = {"search": "Найдено CVE", "download": "PDF сохранено", "collect": "Компаний в БД"}
    text = (
        f"{headings[event]}\n"
        f"Этап: {stages[snapshot['stage']]}\n"
        f"Запуск: {snapshot['run_id']}\n"
        f"Обработано: {processed}/{target}; выбрано: {snapshot['claimed']}\n"
        f"Успешно: {counts.get('completed', 0)}; без записей: {counts.get('not_found', 0)}\n"
        f"Без CVE: {counts.get('no_cve_found', 0)}; неоднозначно: {counts.get('ambiguous_match', 0)}\n"
        f"Активны: {counts.get('running', 0)}; ошибки/ожидание повтора: {errors}\n"
        f"CAPTCHA: {counts.get('captcha_required', 0)}\n"
        f"{document_label[snapshot['stage']]}: {snapshot['documents']}\n"
        f"Подготовлено браузеров: {snapshot['ready_browsers']}/{snapshot['browsers']}; "
        f"вкладок на браузер: {snapshot['tabs_per_browser']}\n"
        f"Прокси: {snapshot['proxy_provider']}; проверено: {snapshot['verified_proxies']}\n"
        f"Прошло: {snapshot['elapsed_seconds'] / 60:.1f} мин\n"
        f"Состояние: {snapshot['state']}"
    )
    if snapshot.get("stop_reason"):
        text += f"\nПричина остановки: {snapshot['stop_reason']}"
    return text


def append_event(path: Path, event):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False) + "\n")


class RunNotifications:
    def __init__(self, args, report, *, stage, refresh, clock=time.monotonic, sleep=asyncio.sleep):
        self.token, self.chat, self.interval = notification_settings(args)
        self.report, self.stage, self.refresh = report, stage, refresh
        self.clock, self.started = clock, clock()
        self.sleep = sleep
        self.ready = set()
        self.queue = asyncio.Queue()
        self.sender = self.timer = self.session = None
        directory = {"search": "res_searches", "download": "res_downloads", "collect": "public_collection"}[stage]
        self.path = args.output / directory / f"{report['run_id']}.telegram.jsonl"
        self.state = {
            "enabled": bool(self.token and self.chat),
            "interval_minutes": self.interval / 60,
            "sent": 0,
            "failed": 0,
            "dropped": 0,
        }
        report["telegram"] = self.state

    def snapshot(self, event):
        self.refresh()
        progress = self.report.get("progress", {})
        statuses = dict(progress.get("statuses", {}))
        reason = redact_proxy_credentials(str(self.report.get("stop_reason") or ""))
        # Even an unexpected error containing raw credentials cannot leak to Telegram/history.
        for secret in (self.token, self.chat):
            if secret:
                reason = reason.replace(secret, "***")
        return {
            "at": time.time(),
            "event": event,
            "stage": self.stage,
            "run_id": self.report["run_id"],
            "target": self.report["target"],
            "claimed": progress.get("claimed", 0),
            "statuses": statuses,
            "documents": (
                self.report.get("companies", 0)
                if self.stage == "collect"
                else len(self.report.get("cves", []))
                if self.stage == "search"
                else statuses.get("completed", 0)
            ),
            "companies": self.report.get("companies", 0),
            "browsers": self.report["browsers"],
            "tabs_per_browser": self.report["tabs_per_browser"],
            "ready_browsers": len(self.ready),
            "proxy_provider": self.report["proxy_provider"],
            "verified_proxies": sum(bool(route.get("verified")) for route in self.report.get("proxy_routes", [])),
            "elapsed_seconds": round(self.clock() - self.started, 2),
            "state": self.report["state"],
            "stop_reason": reason[:400],
        }

    def publish(self, event):
        if self.state["enabled"]:
            try:
                self.queue.put_nowait(self.snapshot(event))
            except Exception:
                self.state["dropped"] += 1
                print("RES Telegram snapshot unavailable; collection continues", flush=True)

    def browser_ready(self, browser):
        if browser in self.ready:
            return
        self.ready.add(browser)
        if len(self.ready) == self.report["browsers"]:
            self.publish("browsers_ready")

    async def start(self):
        if not self.state["enabled"]:
            return
        try:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10), trust_env=False)
        except Exception:
            self.state["enabled"] = False
            print("RES Telegram session unavailable; collection continues", flush=True)
            return
        self.sender = asyncio.create_task(self._send_loop())
        self.timer = asyncio.create_task(self._periodic())
        self.publish("started")

    async def _periodic(self):
        while True:
            await self.sleep(self.interval)
            self.publish("progress")

    async def _send_loop(self):
        while True:
            event = await self.queue.get()
            delivered = False
            try:
                await send_telegram(self.session, self.token, self.chat, format_message(event))
                delivered = True
                self.state["sent"] += 1
            except Exception:
                self.state["failed"] += 1
                print("RES Telegram delivery failed; collection and local reports continue", flush=True)
            finally:
                try:
                    append_event(self.path, {**event, "delivered": delivered})
                except OSError:
                    print("RES Telegram history could not be saved", flush=True)
                self.queue.task_done()

    async def finish(self):
        if self.session is None:
            return
        self.timer.cancel()
        await asyncio.gather(self.timer, return_exceptions=True)
        try:
            self.publish("finished")
            try:
                await asyncio.wait_for(self.queue.join(), timeout=35)
            except TimeoutError:
                self.state["dropped"] = self.queue.qsize()
                print("RES Telegram shutdown timeout; saved collection results are unchanged", flush=True)
        finally:
            self.sender.cancel()
            await asyncio.gather(self.sender, return_exceptions=True)
            await self.session.close()


async def test_delivery():
    """Send one explicit configuration check without opening the queue or browser."""
    try:
        token, chat, seconds = notification_settings(SimpleNamespace(telegram_interval_minutes=None, no_telegram=False))
        if not token or not chat:
            raise ValueError("Telegram credentials missing")
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10), trust_env=False) as session:
            await send_telegram(
                session,
                token,
                chat,
                "✅ RES — проверка Telegram.\n"
                "Настроены уведомления: запуск, готовность браузеров, "
                f"статус каждые {seconds / 60:g} мин. и завершение.\n"
                "Это проверка связи; сбор и браузеры не запускаются.",
            )
    except Exception:
        print("RES Telegram test failed; check credentials, chat access and network", flush=True)
        return 1
    print("RES Telegram test delivered", flush=True)
    return 0
