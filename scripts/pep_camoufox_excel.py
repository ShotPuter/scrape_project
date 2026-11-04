"""Server test: read local Excel, download in Camoufox, notify every ten links."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import aiohttp

from scripts.pep_camoufox_batch import run
from src.services.notifications import send_telegram
from src.sites.pep.source import RESULT_PATH, safe_url
from src.sites.pep.workbook import LINK_COLUMN, read_rows


def collect_sample(workbook: Path, limit: int, offset: int) -> list[dict]:
    """Keep first occurrence order; count unique consultation links, not duplicate Excel rows."""
    seen = set()
    sample = []
    rows = read_rows(workbook)
    try:
        for number, row in rows:
            if not row.get(LINK_COLUMN):
                continue
            url = safe_url(str(row[LINK_COLUMN]))
            if urlsplit(url).path != RESULT_PATH:
                raise ValueError(f"Unexpected consultation URL in Excel row {number}")
            if url in seen:
                continue
            seen.add(url)
            if len(seen) <= offset:
                continue
            sample.append({"n": len(sample) + 1, "source_row": number, "url": url})
            if limit and len(sample) >= limit:
                break
    finally:
        rows.close()
    if not sample:
        raise ValueError("No consultation links in the selected Excel range")
    return sample


def totals(report: dict) -> dict:
    jobs = report["jobs"]
    counts = Counter(job["status"] for job in jobs)
    documents = [doc for job in jobs for doc in job["documents"]]
    downloaded = sum(doc["status"] == "downloaded" for doc in documents)
    empty = sum(job["status"] == "completed" and not job["documents"] for job in jobs)
    return {
        "total": len(jobs),
        "processed": sum(counts.get(s, 0) for s in ("completed", "partial", "failed", "interrupted")),
        "completed": counts.get("completed", 0) - empty,
        "empty": empty,
        "partial": counts.get("partial", 0),
        "failed": counts.get("failed", 0) + counts.get("interrupted", 0),
        "pending": counts.get("pending", 0),
        "downloaded": downloaded,
        "not_downloaded": len(documents) - downloaded,
        "pdf": report.get("pdf_count", 0),
    }


class ProgressReporter:
    def __init__(self, session, output: Path, token: str = "", chat_id: str = ""):
        self.session, self.output, self.token, self.chat_id = session, output, token, chat_id
        self.last_count = 0

    async def __call__(self, report: dict, *, final: bool) -> None:
        stats = totals(report)
        processed = stats["processed"]
        if not final and (processed in (self.last_count, 0) or processed % 10):
            return
        self.last_count = processed
        text = (
            f"PEP Camoufox {'— итог' if final else '— прогресс'}\n"
            f"Запуск: {self.output.name}\n"
            f"Обработано ссылок: {processed}/{stats['total']}\n"
            f"Полностью скачаны: {stats['completed']}\n"
            f"Без деклараций: {stats['empty']}\n"
            f"Частично: {stats['partial']}; ошибки: {stats['failed']}; ожидают: {stats['pending']}\n"
            f"Деклараций скачано: {stats['downloaded']}; не скачано из найденных: {stats['not_downloaded']}\n"
            f"PDF сохранено: {stats['pdf']}\n"
            f"Статус: {report['status']}; причина остановки: {report.get('stop_reason') or 'нет'}"
        )
        print(text, flush=True)
        with (self.output / "progress.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(
                json.dumps({"at": datetime.now(UTC).isoformat(), "final": final, **stats}, ensure_ascii=False) + "\n"
            )
        if self.token:
            try:
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
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        return await run(
            sample,
            output,
            args.timeout,
            ca_bundle=args.ca_bundle,
            max_seconds=args.max_minutes * 60,
            on_progress=ProgressReporter(session, output, token, chat_id),
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workbook", type=Path)
    parser.add_argument("--limit", type=int, default=100, help="Unique links; 0 means all")
    parser.add_argument("--offset", type=int, default=0, help="Skip this many unique links")
    parser.add_argument("--output", type=Path, default=Path("/app/data"))
    parser.add_argument("--ca-bundle", type=Path, default=Path("/opt/pep-ca-bundle.pem"))
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--max-minutes", type=float, default=120)
    parser.add_argument("--preview", action="store_true", help="Read Excel only; no browser or Telegram requests")
    args = parser.parse_args()
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
    return 0 if all(job["status"] == "completed" for job in report["jobs"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
