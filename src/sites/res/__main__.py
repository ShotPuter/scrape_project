"""Queue-backed direct RES company search; PDF verification is a separate stage."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
from collections import Counter
from contextlib import suppress
from pathlib import Path
from time import monotonic

from src.sites.dequienes.pipeline import export_pipeline
from src.sites.dequienes.queue import LostLease, QueueStore
from src.sites.res.browser import lookup
from src.sites.res.proxies import proxy_provider
from src.sites.res.runtime import (
    BrowserPool,
    BrowserStartError,
    add_browser_arguments,
    browser_layout,
    checkpoints,
    progress_snapshot,
    run_incomplete,
    validate_runtime,
    wait_turn,
)
from src.sites.res.source import FORM_URL, SearchIssue
from src.sites.res.telegram import RunNotifications
from src.utils.atomic import atomic_text_writer, atomic_write_json


def save_report(store, output, report, *, from_queue=False):
    if from_queue:
        report["results"] = [
            json.loads(row[0])
            for row in store.db.execute(
                "SELECT result_json FROM res_lookup_results r JOIN run_jobs j ON r.job_id=j.job_id "
                "WHERE j.run_id=? ORDER BY r.job_id",
                (report["run_id"],),
            )
        ]
    progress_snapshot(store, report)
    report["summary"] = dict(Counter(r["status"] for r in report["results"]))
    report["cves"] = sorted({e["cve"] for r in report["results"] for e in r.get("events", []) if e.get("cve")})
    for target in [output / "res_search_report.json", output / "res_searches" / f"{report['run_id']}.json"]:
        atomic_write_json(target, report)
    for target in [output / "res_search_report.csv", output / "res_searches" / f"{report['run_id']}.csv"]:
        with atomic_text_writer(target, newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "company_id",
                    "company_name",
                    "rut",
                    "matched_rut",
                    "status",
                    "action_count",
                    "cves",
                    "error",
                    "elapsed_seconds",
                    "worker",
                ]
            )
            for row in report["results"]:
                writer.writerow(
                    [
                        row["company_id"],
                        row["company_name"],
                        row.get("rut") or "",
                        ";".join(sorted({event["rut"] for event in row.get("events", []) if event.get("rut")})),
                        row["status"],
                        len(row.get("events", [])),
                        ";".join(e["cve"] for e in row.get("events", []) if e.get("cve")),
                        row.get("error", ""),
                        row.get("elapsed_seconds", ""),
                        row.get("worker", ""),
                    ]
                )
    export_pipeline(store, output)


async def run(args):
    validate_runtime(args)
    layout = browser_layout(args)
    store = QueueStore(args.output / "companies.sqlite3")
    report = None
    notifier = None
    try:
        if args.action in {"status", "export"}:
            if args.action == "export":
                export_pipeline(store, args.output)
            print(json.dumps(store.status(), ensure_ascii=False, indent=2))
            return 0
        run_id = store.start_run("res_company_lookup", args.limit, run_id=args.run_id)
        if args.retry_errors:
            print(f"Reset RES error jobs: {store.retry_res_jobs(args.run_id)}", flush=True)
        store.seed_res_jobs(args.company_id, args.max_attempts)
        provider = proxy_provider(args)
        pool = BrowserPool(args.profile, layout, headed=args.headed, proxy_provider=provider)
        report = {
            "run_id": run_id,
            "target": args.limit,
            "browsers": layout.browsers,
            "tabs_per_browser": layout.tabs_per_browser,
            "workers": layout.workers,
            "profiles": [str(profile) for profile in pool.profiles],
            "proxy_provider": provider,
            "proxy_routes": pool.routes.public,
            "checkpoint_interval": getattr(args, "checkpoint_interval", 15),
            "state": "running",
            "results": [],
        }
        print(
            f"RES run: {run_id}; company limit: {args.limit}; "
            f"browsers: {layout.browsers}; tabs/browser: {layout.tabs_per_browser}",
            flush=True,
        )
        save_report(store, args.output, report, from_queue=True)
        notifier = RunNotifications(args, report, stage="search", refresh=lambda: progress_snapshot(store, report))
        pool.on_ready = notifier.browser_ready
        await notifier.start()
        if args.limit == 0:
            report["state"] = "completed"
            return 0

        stop = asyncio.Event()
        failure_streak = 0

        async def heartbeat(job):
            while True:
                await asyncio.sleep(30)
                store.heartbeat(job, 180)

        async def worker(browser, tab):
            nonlocal failure_streak
            owner = f"res-browser-{browser + 1}/tab-{tab + 1}"
            page = None
            while not stop.is_set():
                job = store.claim(run_id, owner, lease_seconds=180, max_in_flight=layout.workers, keys=args.company_id)
                if job is None:
                    return
                name, rut = store.db.execute(
                    "SELECT company_name,rut FROM companies WHERE company_id=?", (job["key"],)
                ).fetchone()
                company = {"company_id": job["key"], "company_name": name, "rut": rut}
                task = asyncio.create_task(heartbeat(job))
                started = monotonic()
                try:
                    try:
                        if page is None:
                            page = await pool.page(browser, tab, stop=stop)
                        if page is None or not await wait_turn(store, "res-search", args.rate_per_minute, stop):
                            store.finish(job, "retry_wait", error="RES worker stopped before search")
                            return
                        result = await lookup(
                            page, company, timeout_ms=args.timeout * 1000, captcha_seconds=args.captcha_wait
                        )
                    except (SearchIssue, BrowserStartError) as exc:
                        result = {
                            **company,
                            "source_url": FORM_URL,
                            "events": [],
                            "status": exc.status if isinstance(exc, SearchIssue) else "retry_wait",
                            "error": str(exc),
                        }
                        if isinstance(exc, BrowserStartError):
                            report["stop_reason"] = str(exc)
                            stop.set()
                    except Exception as exc:
                        result = {
                            **company,
                            "source_url": FORM_URL,
                            "events": [],
                            "status": "retry_wait",
                            "error": f"{type(exc).__name__}: no confirmed search result",
                        }
                    result["elapsed_seconds"] = round(monotonic() - started, 2)
                    result["worker"] = owner
                    store.finish_res_lookup(job, result)
                except asyncio.CancelledError:
                    with suppress(LostLease):
                        store.finish(job, "retry_wait", error="RES worker cancelled")
                    raise
                except LostLease:
                    report["stop_reason"] = "RES job lease lost; result was not committed"
                    stop.set()
                    return
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                print(f"[{owner}] {name}: {result['status']}; CVE: {len(result.get('res_cves', []))}", flush=True)
                failure_streak = failure_streak + 1 if result["status"] in {"retry_wait", "failed"} else 0
                if result["status"] in {"captcha_required", "blocked"} or failure_streak >= 3:
                    report["stop_reason"] = result.get("error", result["status"])
                    stop.set()
                save_report(store, args.output, report, from_queue=True)

        async with (
            pool,
            checkpoints(lambda: save_report(store, args.output, report, from_queue=True), report["checkpoint_interval"]),
            asyncio.TaskGroup() as group,
        ):
            for browser, tab in layout.slots():
                group.create_task(worker(browser, tab))
        progress_snapshot(store, report)
        incomplete = run_incomplete(report)
        report["state"] = "stopped" if incomplete else "completed"
        return int(incomplete)
    finally:
        if report is not None:
            if report["state"] == "running":
                report["state"] = "interrupted"
            save_report(store, args.output, report, from_queue=True)
            if notifier is not None:
                await notifier.finish()
                save_report(store, args.output, report, from_queue=True)
        store.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/dequienes-companies-2025"))
    parser.add_argument("--profile", type=Path, default=Path("data/res-search-profile"))
    parser.add_argument("--limit", type=int, default=50)
    add_browser_arguments(parser)
    parser.add_argument("--run-id")
    parser.add_argument("--company-id", action="append", help="Restrict to existing company IDs; repeat as needed")
    parser.add_argument("--rate-per-minute", type=float, default=6)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--captcha-wait", type=int, default=30)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--retry-errors", action="store_true")
    parser.add_argument("--headed", action="store_true", help="Visible browser for operator CAPTCHA interaction")
    parser.add_argument("--action", choices=["collect", "status", "export"], default="collect")
    args = parser.parse_args()
    try:
        validate_runtime(args)
    except ValueError as exc:
        parser.error(str(exc))
    if args.limit < 0 or min(args.timeout, args.captcha_wait, args.max_attempts) < 1:
        parser.error("limit must be non-negative; workers/timeouts/attempts must be positive")
    if not math.isfinite(args.rate_per_minute) or args.rate_per_minute <= 0:
        parser.error("rate must be finite and positive")
    if not (args.output / "companies.sqlite3").is_file():
        parser.error("An existing companies.sqlite3 is required; run the company collector first")
    try:
        raise SystemExit(asyncio.run(run(args)))
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception as exc:
        print(f"RES stopped: {type(exc).__name__}; check profile availability and report.", flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
