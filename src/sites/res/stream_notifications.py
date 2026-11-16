"""Bounded Telegram delivery and a local event journal for the free-proxy dispatcher."""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import aiohttp

from src.services.telegram import send_telegram
from src.utils.proxy_url import redact_proxy_credentials


class StreamNotifications:
    """Record every lifecycle event locally; aggregate recurring events in Telegram."""

    def __init__(
        self,
        args,
        root: Path,
        *,
        clock: Callable[[], float] = time.monotonic,
        deliver: Callable[[str], Awaitable[None]] | None = None,
    ):
        self.args, self.root, self.clock = args, root, clock
        self.path = root / "events.jsonl"
        self.deliver = deliver or self._deliver
        self.enabled = not args.no_telegram and bool(os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID"))
        self.queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue(maxsize=32)
        self.sender: asyncio.Task | None = None
        self.state = {"enabled": self.enabled, "sent": 0, "failed": 0, "dropped": 0}
        self.first_events: set[str] = set()
        self.pending: Counter[str] = Counter()
        self.started = self.last_activity = clock()
        self.next_summary = self.started + args.telegram_minutes * 60
        self.previous: dict[str, Any] = {}
        self.cooldown = False
        self.health = ""
        self.last_progress = self.started
        self.last_pdfs = 0

    def clean(self, text: str) -> str:
        text = redact_proxy_credentials(text)
        for name in (
            "TELEGRAM_BOT_TOKEN",
            "TELEGRAM_CHAT_ID",
            "ABUSEIPDB_API_KEY",
            "WEBSHARE_API_TOKEN",
            "BRIGHTDATA_PROXY_PASSWORD",
        ):
            secret = os.getenv(name, "")
            if secret:
                text = text.replace(secret, "***")
        return text[:3900]

    def record(self, event: str, text: str) -> None:
        """Keep a bounded local history even when Telegram is disabled or unavailable."""
        text = self.clean(text)
        print(f"RES [{event}] {text}", flush=True)
        try:
            if self.path.exists() and self.path.stat().st_size >= 5 * 1024 * 1024:
                self.path.replace(self.path.with_suffix(".previous.jsonl"))
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({"at": time.time(), "event": event, "text": text}, ensure_ascii=False) + "\n")
        except OSError:
            print("RES event journal unavailable; collection continues", flush=True)

    def publish(self, event: str, text: str) -> None:
        text = self.clean(text)
        self.record(event, text)
        if self.enabled:
            try:
                self.queue.put_nowait((event, text))
            except asyncio.QueueFull:
                self.state["dropped"] += 1

    def activity(self, event: str, text: str) -> None:
        """Send the first occurrence immediately and batch subsequent rotations every five minutes."""
        self.pending[event] += 1
        if event not in self.first_events:
            self.first_events.add(event)
            self.publish("first_" + event, text)
        else:
            self.record(event, text)

    async def start(self) -> None:
        if self.enabled:
            self.sender = asyncio.create_task(self._send_loop())

    async def _deliver(self, text: str) -> None:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10), trust_env=False) as session:
            await send_telegram(session, os.environ["TELEGRAM_BOT_TOKEN"], os.environ["TELEGRAM_CHAT_ID"], text)

    async def _send_loop(self) -> None:
        while True:
            event, text = await self.queue.get()
            try:
                await self.deliver(text)
                self.state["sent"] += 1
            except Exception:
                self.state["failed"] += 1
                self.record("telegram_failed", f"Не доставлено уведомление {event}; сбор продолжается")
            finally:
                self.queue.task_done()

    def format_summary(self, snapshot: dict[str, Any], title: str = "📊 Статистика") -> str:
        jobs, proxies = snapshot["counts"]["jobs"], snapshot["counts"]["proxies"]
        stats = snapshot.get("producer", {})
        elapsed = max(0.001, snapshot["elapsed_seconds"])
        new_pdfs = snapshot["new_pdfs"]
        delta = new_pdfs - self.previous.get("new_pdfs", 0)
        interval = max(0.001, elapsed - self.previous.get("elapsed_seconds", 0))
        errors = stats.get("source_errors", {})
        state = {
            "running": "сбор идёт",
            "cooldown": "общая пауза",
            "finished": "завершено",
            "partial": "частичный результат",
            "interrupted": "остановлено",
            "failed": "ошибка",
        }.get(snapshot.get("state"), "сбор идёт")
        return (
            f"{title} — RES {self.args.run_id}\n"
            f"Состояние: {state}\n"
            f"Завершено компаний: {jobs.get('done', 0)}/{sum(jobs.values())}; "
            f"в работе: {jobs.get('leased', 0)}; в очереди: {jobs.get('pending', 0)}; "
            f"ошибки: {jobs.get('error', 0)}\n"
            f"PDF у компаний: {snapshot.get('companies_with_pdf', 0)}; "
            f"всего PDF: {snapshot.get('total_pdfs', new_pdfs)}; новых: {new_pdfs} (+{delta} за период)\n"
            f"Скорость: {new_pdfs * 60 / elapsed:.2f} PDF/мин; "
            f"за период: {delta * 60 / interval:.2f} PDF/мин\n"
            f"Каталог: {stats.get('catalog_unique', stats.get('catalog_candidates', 0))}; "
            f"прошло IP-фильтры: {stats.get('blacklist_passed', 0)}; "
            f"исключено по портам: {stats.get('port_rejected', 0)}; "
            f"в пуле: {stats.get('selected_candidates', 0)}\n"
            f"Проверок прокси: {stats.get('checks', 0)}; допусков: {stats.get('ready', 0)}; "
            f"отклонено репутацией: {stats.get('reputation_denied', 0)}; "
            f"ошибок связи/проверки: {stats.get('connection_failed', 0)}; "
            f"дублей/пауз выхода: {stats.get('not_admitted', 0)}\n"
            f"Прокси готово: {proxies.get('ready', 0)}; проверяются: {proxies.get('probing', 0)}; "
            f"на паузе: {proxies.get('cooldown', 0)}\n"
            f"Браузеры: {snapshot['active_browsers']}/{snapshot['browser_limit']}; "
            f"подтверждён IP: {snapshot.get('ready_browsers', 0)}\n"
            f"Попыток компаний за запуск: {snapshot.get('attempts', 0)}; "
            f"из них ошибок CAPTCHA: {snapshot.get('captcha_attempts', 0)}\n"
            f"Источники с ошибками: {len(errors)}; "
            f"лимитов RES за запуск: {len(snapshot.get('site_limit_pauses', []))}\n"
            f"Прошло: {elapsed / 60:.1f} мин"
        )

    def update(self, snapshot: dict[str, Any]) -> None:
        now = self.clock()
        control = snapshot.get("cooldown", {})
        active = bool(control.get("active"))
        if active != self.cooldown:
            self.publish(
                "cooldown" if active else "resumed",
                f"{'⏸ Общая пауза после серии ошибок' if active else '▶️ Работа возобновлена'} — RES {self.args.run_id}",
            )
            self.cooldown = active
        stats = snapshot.get("producer", {})
        if stats.get("catalog_refreshes", 0) and "catalog" not in self.first_events:
            self.activity(
                "catalog",
                f"🔎 RES {self.args.run_id}: источники загружены\n"
                f"Кандидатов: {stats.get('catalog_candidates', 0)}; "
                f"подходят по портам и предварительным ограничениям: {stats.get('port_eligible', 0)}\n"
                f"Источников с ошибками: {len(stats.get('source_errors', {}))}; начаты проверки репутации и связи",
            )
        if snapshot.get("new_pdfs", 0) > self.last_pdfs:
            if not self.last_pdfs:
                self.activity(
                    "first_pdf",
                    f"📄 RES {self.args.run_id}: первые PDF сохранены на диск; новых файлов: {snapshot['new_pdfs']}",
                )
            self.last_progress, self.last_pdfs = now, snapshot["new_pdfs"]
        health = (
            "security"
            if snapshot.get("security_paused")
            else "catalog"
            if stats.get("catalog_error") and not stats.get("catalog_refreshes")
            else "stalled"
            if now - self.last_progress >= 600
            else ""
        )
        if health != self.health:
            if health or self.health:
                messages = {
                    "security": (
                        "🛡 Проверки прокси приостановлены: обязательные репутационные списки недоступны. "
                        "Непроверенные IP не используются."
                    ),
                    "catalog": (
                        "⚠️ Не удалось загрузить источники прокси. Повтор запланирован; "
                        "уже готовые маршруты могут продолжить работу."
                    ),
                    "stalled": (
                        "⚠️ Новых PDF нет 10 минут. Проверки и очередь продолжаются; подробные счётчики будут в сводке."
                    ),
                    "": "✅ Предыдущая проблема больше не наблюдается; обработка продолжается.",
                }
                self.publish("health", f"RES {self.args.run_id}\n{messages[health]}")
            self.health = health
        if now - self.last_activity >= 300:
            if self.pending or stats.get("checks", 0) != self.previous.get("checks", 0):
                self.publish(
                    "activity",
                    f"🔄 RES {self.args.run_id}: проверено прокси {stats.get('checks', 0)}, "
                    f"допусков {stats.get('ready', 0)}; "
                    f"запусков браузеров за 5 мин {self.pending['browser_started']}, "
                    f"подтверждено {self.pending['browser_ready']}; "
                    f"сессий завершено {self.pending['browser_finished']}; "
                    f"лимитов RES {self.pending['site_limit']}\n"
                    f"Сетевых ошибок: {stats.get('connection_failed', 0)}; "
                    f"отказов репутации при проверке: {stats.get('reputation_denied', 0)}\n"
                    f"Пул после IP-фильтров: {stats.get('blacklist_passed', 0)}; "
                    f"отсечено портами: {stats.get('port_rejected', 0)}; "
                    f"частые порты: {stats.get('rejected_ports', {})}\n"
                    f"Работают: {snapshot['active_browsers']}/{snapshot['browser_limit']}; "
                    f"новых PDF: {snapshot['new_pdfs']}\n"
                    f"Последние допущенные: {', '.join(stats.get('recent_ready', [])) or 'нет'}",
                )
            self.pending.clear()
            self.last_activity = now
            self.previous["checks"] = stats.get("checks", 0)
        if now >= self.next_summary:
            self.publish("progress", self.format_summary(snapshot))
            self.previous.update(new_pdfs=snapshot["new_pdfs"], elapsed_seconds=snapshot["elapsed_seconds"])
            self.next_summary = now + self.args.telegram_minutes * 60

    async def finish(self, text: str) -> None:
        # Prefer the complete final report over stale queued progress after an outage.
        if self.queue.qsize() > 2:
            while not self.queue.empty():
                self.queue.get_nowait()
                self.queue.task_done()
                self.state["dropped"] += 1
        self.publish("finished", text)
        if self.sender is not None:
            try:
                await asyncio.wait_for(self.queue.join(), 35)
            except TimeoutError:
                self.state["dropped"] += self.queue.qsize()
            finally:
                self.sender.cancel()
                await asyncio.gather(self.sender, return_exceptions=True)
