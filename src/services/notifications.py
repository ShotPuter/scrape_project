"""Send periodic Telegram statistics using outbound requests only."""

from __future__ import annotations

import argparse
import asyncio
import logging
import math
import os
import time
from pathlib import Path

import aiohttp

from config import settings
from src.services.run_statistics import read_json, statistics
from src.utils.atomic import atomic_write_json

logger = logging.getLogger(__name__)


def report_identity(report: dict) -> str:
    return str(report.get("run_id") or report.get("started_at") or "current")


def should_notify(report: dict, state: dict, now: float, interval: float) -> bool:
    if not report:
        return False
    identity = report_identity(report)
    status = report.get("status", "unknown")
    if identity != state.get("run") or status != state.get("status"):
        return True
    return status == "RUNNING" and now - state.get("sent_at", 0) >= interval


async def send_telegram(session: aiohttp.ClientSession, token: str, chat_id: str, text: str) -> None:
    # Never propagate a token-bearing URL or Telegram response into logs.
    try:
        async with session.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text[:4000]},
        ) as response:
            result = await response.json()
            if response.status != 200 or not result.get("ok"):
                raise RuntimeError("Telegram rejected notification; check credentials and chat access")
    except (aiohttp.ClientError, TimeoutError, ValueError):
        raise RuntimeError("Telegram notification unavailable") from None


async def watch(directory: Path, interval: float) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise ValueError("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")
    state_path = directory / "notification-state.json"
    state = read_json(state_path)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        startup_pending = True
        while True:
            if startup_pending:
                try:
                    await send_telegram(
                        session, token, chat_id,
                        "✅ Сервис уведомлений запущен. Telegram работает.\n"
                        f"Статистика будет приходить каждые {interval / 60:g} мин. и при завершении запуска.\n"
                        "Если скрапер ещё не запущен, ожидаю его статистику.",
                    )
                except RuntimeError:
                    logger.warning("Telegram startup message failed; retrying in 30 seconds (credentials hidden)")
                    await asyncio.sleep(30)
                    continue
                startup_pending = False
            report = await asyncio.to_thread(read_json, directory / "scraper-status.json")
            if not report:
                report = await asyncio.to_thread(read_json, directory / "stress-report.json")
            if should_notify(report, state, time.time(), interval):
                text = await asyncio.to_thread(statistics, directory)
                errors = await asyncio.to_thread(statistics, directory, errors=True)
                try:
                    await send_telegram(session, token, chat_id, f"{text}\n\n{errors}")
                except RuntimeError:
                    logger.warning("Telegram delivery failed; retrying in 30 seconds (credentials hidden)")
                else:
                    state = {"run": report_identity(report), "status": report.get("status"), "sent_at": time.time()}
                    try:
                        await asyncio.to_thread(atomic_write_json, state_path, state)
                    except OSError:
                        logger.warning("Could not save notification state; restart may repeat last notification")
            await asyncio.sleep(30)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=settings.log_file.parent)
    parser.add_argument("--interval", type=float, default=float(os.getenv("NOTIFY_INTERVAL_SECONDS", "1800")))
    parser.add_argument("--preview", action="store_true", help="Print statistics locally without sending")
    args = parser.parse_args()
    if not math.isfinite(args.interval) or args.interval < 30:
        parser.error("--interval must be finite and >= 30 seconds")
    if args.preview:
        print(statistics(args.data_dir))
        print(statistics(args.data_dir, errors=True))
        return
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(watch(args.data_dir, args.interval))
    except ValueError as exc:
        parser.error(str(exc))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
