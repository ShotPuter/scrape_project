"""Official company list -> isolated Camoufox/HTTP workers -> PDFs and speed report."""

from __future__ import annotations

import argparse
import asyncio
import csv
import fcntl
import hashlib
import json
import math
import os
import re
import signal
import sqlite3
import statistics
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import aiohttp

from scripts import res_free_pilot as pilot
from src.services.telegram import send_telegram
from src.sites.res.open_data import select_companies
from src.utils.atomic import atomic_write_json
from src.utils.error_details import safe_message

ERRORS = {
    "failed",
    "blocked",
    "captcha_required",
    "proxy_failed",
    "proxy_ip_mismatch",
    "proxy_auth_failed",
    "documents_failed",
    "retry_wait",
}


def worker_index(company_id, browsers):
    return int(hashlib.sha256(company_id.encode()).hexdigest(), 16) % browsers


def read_json(path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


def latest_results(directory):
    latest = {}
    for path in sorted(directory.glob("report-*.json")):
        report = read_json(path, {})
        # Interrupted attempts may not yet be included in the final companies list.
        for attempt in report.get("attempts", []):
            latest[attempt["company_id"]] = attempt["result"]
        for row in report.get("companies", []):
            latest[row["company_id"]] = row
    return latest


def write_database(path, companies):
    temporary = path.with_suffix(".tmp")
    temporary.unlink(missing_ok=True)
    with sqlite3.connect(temporary) as db:
        db.execute("CREATE TABLE companies(company_id TEXT PRIMARY KEY,company_name TEXT NOT NULL,rut TEXT NOT NULL)")
        db.executemany(
            "INSERT INTO companies VALUES (?,?,?)", [(c["company_id"], c["company_name"], c["rut"]) for c in companies]
        )
    temporary.replace(path)


def prepare_queues(args, root, selection):
    queues, baseline = [], []
    for index in range(args.browsers):
        directory = root / "workers" / f"w{index:02d}"
        directory.mkdir(parents=True, exist_ok=True)
        assigned = [c for c in selection if worker_index(c["company_id"], args.browsers) == index]
        state = pilot.load_state(directory / "state.json")
        previous = latest_results(directory)
        pending = []
        for company in assigned:
            complete = pilot.cached_company_result(company, state, args, directory)
            old = previous.get(company["company_id"], {})
            if complete:
                baseline.append({**complete, "worker": index})
            elif not args.retry_errors and old.get("status") in ERRORS:
                baseline.append({**old, "worker": index})
            else:
                pending.append(company)
        write_database(directory / "queue.sqlite3", pending)
        if pending:
            queues.append((index, directory, len(pending)))
    return queues, baseline


def child_command(args, directory, count):
    return [
        sys.executable,
        "-u",
        "-m",
        "scripts.res_free_pilot",
        "--db",
        str(directory / "queue.sqlite3"),
        "--limit",
        "0",
        "--output",
        str(directory),
        "--profile",
        str(directory / "profile"),
        "--engine",
        "camoufox",
        "--proxy-provider",
        args.proxy_provider,
        "--proxy-limit",
        str(args.proxy_sessions or count * args.company_attempts + 5),
        "--lazy-proxies",
        "--profile-slots",
        "1",
        "--solves-per-proxy",
        str(args.tokens_per_proxy),
        "--company-attempts",
        str(args.company_attempts),
        "--challenges-per-proxy",
        "1",
        "--max-documents",
        str(args.max_documents),
        "--timeout",
        str(args.timeout),
        "--delay",
        str(args.delay),
        "--audio-model",
        args.audio_model,
        "--audio-rounds",
        "2",
        "--audio-seconds",
        "90",
        "--max-failures",
        str(args.max_failures),
    ]


def collect_rows(args, root, companies):
    workers = {}
    for index in range(args.browsers):
        directory = root / "workers" / f"w{index:02d}"
        workers[index] = (directory, pilot.load_state(directory / "state.json"), latest_results(directory))
    rows = []
    for company in companies:
        index = worker_index(company["company_id"], args.browsers)
        directory, state, previous = workers[index]
        result = pilot.cached_company_result(company, state, args, directory) or previous.get(company["company_id"])
        row = {**company, **(result or {"status": "pending", "documents": []}), "worker": index}
        row["cves"] = state["companies"].get(company["company_id"], {}).get("cves", [])
        documents = []
        for cve in row["cves"]:
            saved = pilot.cached_pdf(directory, cve)
            if saved:
                documents.append({**saved, "local_path": str((directory / saved["local_path"]).relative_to(root))})
        row["documents"] = documents
        rows.append(row)
    return rows


def statistics_report(args, root, companies, invocation):
    rows = collect_rows(args, root, companies)
    current = []
    for path in root.glob("workers/w*/report-*.json"):
        if str(path) not in invocation["prior_reports"]:
            current.append(read_json(path, {}))
    attempts = [a for report in current for a in report.get("attempts", [])]
    for row in rows:
        related = [a for a in attempts if a["company_id"] == row["company_id"]]
        row["attempts_this_run"] = len(related)
        row["seconds_this_run"] = round(sum(a.get("seconds", 0) for a in related), 3)
        for document in row["documents"]:
            document["reused"] = document["local_path"] in invocation["prior_pdfs"]
            document["new_this_run"] = not document["reused"]
    elapsed = max(0.001, invocation["elapsed_seconds"])
    new_documents = [doc for row in rows for doc in row["documents"] if doc["local_path"] not in invocation["prior_pdfs"]]
    downloaded = len(new_documents)
    seconds = sorted(a["seconds"] for a in attempts if "seconds" in a)
    http = Counter()
    for report in current:
        http.update(report.get("http_metrics", {}))
    summary = {
        **invocation,
        "rows": rows,
        "statuses": dict(Counter(r["status"] for r in rows)),
        "selected_companies": len(companies),
        "companies_with_pdf": sum(bool(r["documents"]) for r in rows),
        "pdf_count": sum(len(r["documents"]) for r in rows),
        "new_pdfs": downloaded,
        "new_pdf_bytes": sum(doc["size_bytes"] for doc in new_documents),
        "company_attempts": len(attempts),
        "attempt_statuses": dict(Counter(a["result"]["status"] for a in attempts)),
        "accepted_tokens": sum(len(report.get("tokens", [])) for report in current),
        "token_stages": dict(Counter(t["stage"] for report in current for t in report.get("tokens", []))),
        "proxy_session_statuses": dict(Counter(p["status"] for report in current for p in report.get("proxies", []))),
        "http_metrics": dict(http),
        "new_pdfs_per_minute": round(downloaded * 60 / elapsed, 3),
        "company_attempts_per_minute": round(len(attempts) * 60 / elapsed, 3),
        "company_attempt_seconds_mean": round(statistics.mean(seconds), 3) if seconds else None,
        "company_attempt_seconds_p50": round(statistics.median(seconds), 3) if seconds else None,
        "company_attempt_seconds_p95": seconds[max(0, math.ceil(len(seconds) * 0.95) - 1)] if seconds else None,
        "worker_reports": [str(p.relative_to(root)) for p in sorted(root.glob("workers/w*/report-*.json"))],
        "browsers": args.browsers,
        "proxy_provider": args.proxy_provider,
        "tabs_per_browser": 1,
        "max_documents_per_company": args.max_documents,
    }
    summary.pop("prior_reports", None)
    summary.pop("prior_pdfs", None)
    return summary


def export(root, summary):
    atomic_write_json(root / "summary.json", summary)
    with (root / "results.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "company_name",
                "rut",
                "status",
                "cves_found",
                "pdfs_saved",
                "attempts_this_run",
                "seconds_this_run",
                "pdf_paths",
                "error",
            ]
        )
        for row in summary["rows"]:
            writer.writerow(
                [
                    row["company_name"],
                    row["rut"],
                    row["status"],
                    len(row["cves"]),
                    len(row["documents"]),
                    row["attempts_this_run"],
                    row["seconds_this_run"],
                    " | ".join(d["local_path"] for d in row["documents"]),
                    row.get("error", ""),
                ]
            )


async def notify(args, text):
    token, chat = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if args.no_telegram or not token or not chat:
        return
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10), trust_env=False) as session:
            await send_telegram(session, token, chat, text)
    except Exception:
        print("Telegram unavailable; collection continues", flush=True)


def progress_message(args, progress, total, active):
    seconds = max(0.001, progress["elapsed_seconds"])
    statuses = progress["statuses"]
    return (
        f"📊 RES {args.run_id} — прогресс\n"
        f"Обработано компаний: {sum(statuses.values())}/{total}\n"
        f"Новых PDF: {progress['new_pdfs']}; скорость: {progress['new_pdfs'] * 60 / seconds:.2f} PDF/мин\n"
        f"Попыток: {progress['attempts']}; ошибочных: {progress['error_attempts']}; "
        f"CAPTCHA: {progress['captcha_attempts']}\n"
        f"Подготовлено браузеров: {progress['ready_browsers']}; активных процессов: {active}\n"
        f"Прошло: {seconds / 60:.1f} мин"
    )


def final_message(args, summary):
    state = {"finished": "завершён", "partial": "частичный результат", "interrupted": "остановлен"}[summary["state"]]
    unfinished = sum(
        count for status, count in summary["statuses"].items() if status not in {"downloaded", "not_found", "no_cve_found"}
    )
    return (
        f"🏁 RES {args.run_id} — {state}\n"
        f"PDF у {summary['companies_with_pdf']}/{len(summary['rows'])} компаний; не завершено: {unfinished}\n"
        f"Всего PDF: {summary['pdf_count']}; новых: {summary['new_pdfs']}\n"
        f"Скорость: {summary['new_pdfs_per_minute']:.2f} PDF/мин; прошло: {summary['elapsed_seconds'] / 60:.1f} мин\n"
        f"Отчёт: {args.output / args.run_id / 'summary.json'}"
    )


async def execute(args, root):
    started = time.monotonic()
    invocation = {
        "run_id": args.run_id,
        "year": args.year,
        "month": args.month,
        "invocation_id": datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ"),
        "started_at": datetime.now(UTC).isoformat(),
        "state": "preparing",
        "workers": {},
        "prior_pdfs": [str(p.relative_to(root)) for p in root.glob("workers/w*/pdf/**/*.pdf")],
        "prior_reports": [str(p) for p in root.glob("workers/w*/report-*.json")],
    }
    config = {key: getattr(args, key) for key in ("year", "month", "limit", "browsers")}
    previous = read_json(root / "selection.json")
    if previous and previous["config"] != config:
        raise ValueError("Selection settings changed; choose a new --run-id or restore the original settings")
    print(f"RES benchmark {args.run_id}: {config}", flush=True)
    await notify(args, f"🚀 RES {args.run_id}: подготовка {args.limit} компаний; браузеров: {args.browsers}")
    seed_started = time.monotonic()
    if previous:
        selection = previous
    else:
        selection = await asyncio.to_thread(select_companies, args.year, args.month, args.limit)
        selection["config"] = config
        atomic_write_json(root / "selection.json", selection)
    invocation["seed_seconds"] = round(time.monotonic() - seed_started, 3)
    invocation["source_url"] = selection.get("source_url")
    invocation["selection_reused"] = bool(previous)
    companies = selection["companies"]
    write_database(root / "companies.sqlite3", companies)
    queues, baseline = prepare_queues(args, root, companies)
    if queues:
        required = (
            ("BRIGHTDATA_PROXY_USERNAME", "BRIGHTDATA_PROXY_PASSWORD")
            if args.proxy_provider == "brightdata"
            else ("WEBSHARE_API_TOKEN",)
        )
        missing = [key for key in required if not os.getenv(key, "").strip()]
        if missing:
            raise ValueError("Missing proxy environment variables: " + ", ".join(missing))
    invocation["skipped_companies"] = len(baseline)
    invocation["scheduled_companies"] = sum(c for _, _, c in queues)
    children, interrupted = [], False
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)
    invocation["state"] = "running"

    async def launch(index, directory, count):
        log = directory / f"worker-{invocation['invocation_id']}.log"
        try:
            with log.open("w") as output:
                process = await asyncio.create_subprocess_exec(
                    *child_command(args, directory, count), stdout=output, stderr=asyncio.subprocess.STDOUT
                )
                children.append(process)
                invocation["workers"][str(index)] = {
                    "pid": process.pid,
                    "selected": count,
                    "log": str(log.relative_to(root)),
                }
                invocation["workers"][str(index)]["exit_code"] = await process.wait()
        except OSError as exc:
            invocation["workers"][str(index)] = {"exit_code": -1, "error": safe_message(exc)}

    tasks = [asyncio.create_task(launch(*queue)) for queue in queues]
    next_telegram = time.monotonic() + args.telegram_minutes * 60
    ready_sent = False
    try:
        while tasks and not all(task.done() for task in tasks):
            if stop.is_set():
                interrupted = True
                for child in children:
                    if child.returncode is None:
                        child.send_signal(signal.SIGINT)
                break
            invocation["elapsed_seconds"] = round(time.monotonic() - started, 3)
            progress = {k: v for k, v in invocation.items() if k not in {"prior_reports", "prior_pdfs"}}
            # Child checkpoints are small JSON files; PDF integrity is checked at final export.
            statuses, docs, tokens, attempts, ready = Counter(r["status"] for r in baseline), 0, 0, 0, set()
            error_attempts = captcha_attempts = 0
            for _, directory, _ in queues:
                for path in directory.glob("report-*.json"):
                    if str(path) in invocation["prior_reports"]:
                        continue
                    report = read_json(path, {})
                    statuses.update(report.get("statuses", {}))
                    docs += report.get("progress", {}).get("downloaded", 0)
                    tokens += len(report.get("tokens", []))
                    attempts += len(report.get("attempts", []))
                    for attempt in report.get("attempts", []):
                        result = attempt["result"]
                        error_attempts += result["status"] in ERRORS
                        captcha_attempts += result["status"] == "captcha_required" or any(
                            doc.get("status") == "captcha_required" for doc in result.get("documents", [])
                        )
                    if any(p.get("http_ip") and p.get("http_ip") == p.get("browser_ip") for p in report.get("proxies", [])):
                        ready.add(str(directory))
            progress.update(
                statuses=dict(statuses),
                new_pdfs=docs,
                tokens=tokens,
                attempts=attempts,
                error_attempts=error_attempts,
                captcha_attempts=captcha_attempts,
                ready_browsers=len(ready),
            )
            atomic_write_json(root / "progress.json", progress)
            print(
                f"RES progress: {sum(statuses.values())}/{len(companies)} companies; {docs} new PDFs; "
                f"{attempts} attempts; {invocation['elapsed_seconds'] / 60:.1f} min",
                flush=True,
            )
            if ready and len(ready) == len(queues) and not ready_sent:
                ready_sent = True
                await notify(
                    args, f"🌐 RES {args.run_id}: подготовлено браузеров: {len(ready)}; IP браузера и HTTP совпадают"
                )
            if time.monotonic() >= next_telegram:
                completed = sum(t.done() for t in tasks)
                await notify(args, progress_message(args, progress, len(companies), len(tasks) - completed))
                next_telegram = time.monotonic() + args.telegram_minutes * 60
            waiter = asyncio.create_task(stop.wait())
            await asyncio.wait(
                [t for t in tasks if not t.done()] + [waiter],
                timeout=args.checkpoint_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=45 if interrupted else None)
            except TimeoutError:
                for child in children:
                    if child.returncode is None:
                        child.kill()
                await asyncio.gather(*(c.wait() for c in children))
    finally:
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(signum)
    invocation["elapsed_seconds"] = round(time.monotonic() - started, 3)
    invocation["finished_at"] = datetime.now(UTC).isoformat()
    invocation["state"] = "interrupted" if interrupted else "finished"
    summary = statistics_report(args, root, companies, invocation)
    # Include final cache/PDF integrity checks in the wall-clock measurement.
    summary["elapsed_seconds"] = round(time.monotonic() - started, 3)
    summary["finished_at"] = datetime.now(UTC).isoformat()
    summary["new_pdfs_per_minute"] = round(summary["new_pdfs"] * 60 / max(0.001, summary["elapsed_seconds"]), 3)
    summary["company_attempts_per_minute"] = round(
        summary["company_attempts"] * 60 / max(0.001, summary["elapsed_seconds"]), 3
    )
    incomplete = any(r["status"] not in {"downloaded", "not_found", "no_cve_found"} for r in summary["rows"])
    if incomplete and not interrupted:
        summary["state"] = "partial"
    export(root, summary)
    atomic_write_json(root / f"benchmark-{invocation['invocation_id']}.json", summary)
    atomic_write_json(root / "progress.json", {k: v for k, v in summary.items() if k != "rows"})
    print(
        f"RES {summary['state']}: {summary['companies_with_pdf']}/{len(companies)} companies with PDFs, "
        f"{summary['new_pdfs']} new PDFs, {summary['elapsed_seconds']:.1f}s, "
        f"{summary['new_pdfs_per_minute']:.2f} new PDFs/min; report: {root / 'summary.json'}",
        flush=True,
    )
    await notify(args, final_message(args, summary))
    return 130 if interrupted else (2 if incomplete else 0)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--month", type=int, default=1)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--browsers", type=int, default=2)
    parser.add_argument("--max-documents", type=int, default=0, help="PDFs per company; 0 = all found CVEs")
    parser.add_argument("--proxy-provider", choices=["brightdata", "webshare"], default="brightdata")
    parser.add_argument(
        "--proxy-sessions", type=int, default=0, help="Max sessions per worker; 0 = bounded automatic budget"
    )
    parser.add_argument("--tokens-per-proxy", type=int, default=20)
    parser.add_argument("--company-attempts", type=int, default=2)
    parser.add_argument("--max-failures", type=int, default=20)
    parser.add_argument("--audio-model", default="base")
    parser.add_argument("--timeout", type=float, default=40)
    parser.add_argument("--delay", type=float, default=3)
    parser.add_argument("--checkpoint-seconds", type=float, default=15)
    parser.add_argument("--telegram-minutes", type=float, default=5)
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument(
        "--retry-errors", action="store_true", default=os.getenv("RES_RETRY_ERRORS", "0").lower() in {"1", "true", "yes"}
    )
    parser.add_argument("--refresh", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--output", type=Path, default=Path("data/res-benchmark"))
    parser.add_argument("--run-id", default="res-2025-01-100")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}", args.run_id):
        parser.error("run-id must be a simple directory name")
    if not 1 <= args.month <= 12 or not 1 <= args.limit <= 10000 or not 1 <= args.browsers <= 16:
        parser.error("month: 1..12; limit: 1..10000; browsers: 1..16")
    if (
        min(args.max_documents, args.proxy_sessions) < 0
        or min(args.company_attempts, args.tokens_per_proxy, args.max_failures) < 1
    ):
        parser.error("Invalid document, proxy, or attempt limits")
    if any(not math.isfinite(v) or v <= 0 for v in (args.timeout, args.checkpoint_seconds, args.telegram_minutes)):
        parser.error("Timing values must be finite and positive")
    if not math.isfinite(args.delay) or args.delay < 0:
        parser.error("delay must be finite and non-negative")
    return args


def main(argv=None):
    from dotenv import load_dotenv

    for file in (".env.res", ".env.camoufox", ".env"):
        load_dotenv(file)
    args = parse_args(argv)
    root = args.output / args.run_id
    root.mkdir(parents=True, exist_ok=True)
    try:
        with (root / ".lock").open("w") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                print("This run is already active; stop it before starting another instance", flush=True)
                return 1
            return asyncio.run(execute(args, root))
    except Exception as exc:
        print(f"RES benchmark stopped: {safe_message(exc)}", flush=True)
        failure = {
            "state": "failed",
            "error_type": type(exc).__name__,
            "error": safe_message(exc),
            "at": datetime.now(UTC).isoformat(),
        }
        atomic_write_json(root / "progress.json", failure)
        atomic_write_json(root / f"failure-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}.json", failure)
        asyncio.run(
            notify(
                args,
                f"⛔ RES {args.run_id} — ошибка\n{failure['error_type']}: {failure['error']}\n"
                f"Диагностика: {root / 'progress.json'}",
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
