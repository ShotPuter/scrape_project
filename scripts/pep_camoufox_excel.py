"""Server test: read local Excel, download in Camoufox, notify every ten links."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import aiohttp

from scripts.pep_camoufox_logging import report_totals as totals
from scripts.pep_camoufox_parallel import BrightDataProxy, run_parallel, webshare_pool
from src.services.notifications import send_telegram
from src.sites.pep.source import RESULT_PATH, safe_url
from src.sites.pep.workbook import LINK_COLUMN, read_rows


def collect_sample(workbook: Path, limit: int, offset: int) -> list[dict]:
    """Keep first occurrence order; count unique consultation links, not duplicate Excel rows."""
    seen = set()
    sample = []
    selected = {}
    rows = read_rows(workbook)
    try:
        for number, row in rows:
            if not row.get(LINK_COLUMN):
                continue
            url = safe_url(str(row[LINK_COLUMN]))
            if urlsplit(url).path != RESULT_PATH:
                raise ValueError(f"Unexpected consultation URL in Excel row {number}")
            if url in seen:
                if url in selected:
                    selected[url]["source_records"].append({"excel_row": number, "data": row})
                continue
            seen.add(url)
            if len(seen) <= offset:
                continue
            if limit and len(sample) >= limit:
                continue
            item = {
                "n": len(sample) + 1,
                "source_row": number,
                "url": url,
                "source_records": [{"excel_row": number, "data": row}],
            }
            sample.append(item)
            selected[url] = item
    finally:
        rows.close()
    if not sample:
        raise ValueError("No consultation links in the selected Excel range")
    return sample


class ProgressReporter:
    def __init__(self, session, output: Path, token: str = "", chat_id: str = ""):
        self.session, self.output, self.token, self.chat_id = session, output, token, chat_id
        self.last_count = 0
        self.send_lock = asyncio.Lock()

    async def __call__(self, report: dict, *, final: bool) -> None:
        stats = totals(report)
        processed = stats["processed"]
        if not final and (processed == 0 or processed // 10 <= self.last_count // 10):
            return
        self.last_count = processed
        minutes = report.get("seconds", 0) / 60
        speed = stats["downloaded"] / minutes if minutes else 0
        text = (
            f"PEP Camoufox {'— итог' if final else '— прогресс'}\n"
            f"Запуск: {self.output.name}\n"
            f"Обработано ссылок: {processed}/{stats['total']}\n"
            f"Полностью скачаны: {stats['completed']}\n"
            f"Без деклараций: {stats['empty']}\n"
            f"Частично: {stats['partial']}; ошибки: {stats['failed']}; ожидают: {stats['pending']}\n"
            f"Деклараций скачано: {stats['downloaded']}; не скачано из найденных: {stats['not_downloaded']}\n"
            f"PDF сохранено: {stats['pdf']}\n"
            f"Браузеров: {report.get('worker_count', 1)}; прошло: {minutes:.1f} мин; темп: {speed:.1f} деклараций/мин\n"
            f"Вкладок: {report.get('tab_count', 1)}; активных ссылок: {stats['running']}; в очереди: {stats['pending']}\n"
            f"Попыток восстановления вкладок: {report.get('tab_restarts', 0)}\n"
            f"Разных прокси IP при проверке: {len(report.get('proxy_egress_ips', []))}\n"
            f"Статус: {report['status']}; причина остановки: {report.get('stop_reason') or 'нет'}"
        )
        print(text, flush=True)
        with (self.output / "progress.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(
                json.dumps(
                    {
                        "at": datetime.now(UTC).isoformat(),
                        "final": final,
                        **stats,
                        "elapsed_seconds": report.get("seconds", 0),
                        "declarations_per_minute": round(speed, 2),
                        "links_per_minute": round(processed / minutes, 2) if minutes else 0,
                        "worker_count": report.get("worker_count", 1),
                        "tab_count": report.get("tab_count", 1),
                        "tab_restarts": report.get("tab_restarts", 0),
                        "proxy_egress_ips": report.get("proxy_egress_ips", []),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
        if self.token:
            try:
                async with self.send_lock:
                    await send_telegram(self.session, self.token, self.chat_id, text)
            except RuntimeError:
                # A notification failure must not discard browser results or expose credentials.
                print("Telegram notification failed; statistics are saved locally", flush=True)


async def execute(args, sample: list[dict]) -> dict:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if bool(token) != bool(chat_id):
        raise ValueError("Set both TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
    if not token:
        print("Telegram is not configured; reports will be written to logs and progress.jsonl", flush=True)
    output = args.output / datetime.now(UTC).strftime("run-%Y%m%dT%H%M%S%fZ")
    username = os.getenv("BRIGHTDATA_PROXY_USERNAME", "").strip()
    password = os.getenv("BRIGHTDATA_PROXY_PASSWORD", "")
    provider = os.getenv("PEP_PROXY_PROVIDER", "auto").strip().lower()
    if provider == "auto":
        provider = (
            "webshare" if os.getenv("WEBSHARE_API_TOKEN", "").strip() else "brightdata" if username or password else "direct"
        )
    proxy = None
    proxy_ca = None
    if provider == "webshare":
        proxy = await webshare_pool(os.getenv("WEBSHARE_API_TOKEN", ""), min(args.workers, len(sample)))
    elif provider == "brightdata":
        proxy = BrightDataProxy(os.getenv("BRIGHTDATA_PROXY_SERVER", "http://brd.superproxy.io:44445"), username, password)
        proxy_ca = args.proxy_ca
    elif provider != "direct":
        raise ValueError("PEP_PROXY_PROVIDER must be webshare, brightdata, direct or auto")
    print(
        f"Proxy provider: {provider}; browsers: {min(args.workers, len(sample))}; tabs per browser: {args.tabs}", flush=True
    )
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        return await run_parallel(
            sample,
            output,
            workers=args.workers,
            request_seconds=args.timeout,
            ca_bundle=args.ca_bundle,
            max_seconds=args.max_minutes * 60,
            proxy_ca=proxy_ca,
            proxy=proxy,
            notify=ProgressReporter(session, output, token, chat_id),
            log_interval=args.log_interval,
            tabs=args.tabs,
            tab_restarts=args.tab_restarts,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workbook", type=Path)
    parser.add_argument("--limit", type=int, default=100, help="Unique links; 0 means all")
    parser.add_argument("--offset", type=int, default=0, help="Skip this many unique links")
    parser.add_argument("--output", type=Path, default=Path("/app/data"))
    parser.add_argument("--ca-bundle", type=Path, default=Path("/opt/pep-ca-bundle.pem"))
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--workers", type=int, default=20, help="1 to 100 independent browsers")
    parser.add_argument("--tabs", type=int, default=3, help="Concurrent tabs per browser (1..10)")
    parser.add_argument("--tab-restarts", type=int, default=3, help="Replacement tabs after crashes per slot (0..100)")
    parser.add_argument("--proxy-ca", type=Path, default=Path("/opt/brightdata-proxy-ca.crt"))
    parser.add_argument("--max-minutes", type=float, default=120)
    parser.add_argument("--log-interval", type=float, default=30, help="Heartbeat interval in seconds (5..300)")
    parser.add_argument("--preview", action="store_true", help="Read Excel only; no browser or Telegram requests")
    args = parser.parse_args()
    if not 1 <= args.workers <= 100:
        parser.error("workers must be between 1 and 100")
    if not 1 <= args.tabs <= 10:
        parser.error("tabs must be between 1 and 10")
    if not 0 <= args.tab_restarts <= 100:
        parser.error("tab-restarts must be between 0 and 100")
    if not math.isfinite(args.log_interval) or not 5 <= args.log_interval <= 300:
        parser.error("log-interval must be finite and between 5 and 300 seconds")
    if args.limit < 0 or args.offset < 0:
        parser.error("limit and offset must be nonnegative")
    if not math.isfinite(args.timeout) or not 5 <= args.timeout <= 120:
        parser.error("timeout must be finite and between 5 and 120 seconds")
    if not math.isfinite(args.max_minutes) or args.max_minutes <= 0:
        parser.error("max-minutes must be finite and positive")
    sample = collect_sample(args.workbook, args.limit, args.offset)
    print(f"Selected {len(sample)} unique links from {args.workbook.name}, offset {args.offset}", flush=True)
    if args.preview:
        return 0
    report = asyncio.run(execute(args, sample))
    return 0 if report["status"] == "finished" and all(job["status"] == "completed" for job in report["jobs"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
