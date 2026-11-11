"""Consume certificate_download jobs and save verified RES PDFs."""

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
from src.sites.dequienes.source import RES_URL, validate_cve
from src.sites.res.certificates import download_certificate
from src.sites.res.pdf import cached_pdf
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
from src.sites.res.source import SearchIssue
from src.sites.res.telegram import RunNotifications
from src.utils.atomic import atomic_text_writer, atomic_write_json


def document_limit(value):
    if value == "all":
        return None
    try:
        limit = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("limit must be a non-negative integer or 'all'") from exc
    if limit < 0:
        raise argparse.ArgumentTypeError("limit must be non-negative")
    return limit


def selected_cves(store, args):
    source_run = getattr(args, "from_search_run", None)
    if source_run is None:
        return args.cve
    row = store.db.execute("SELECT queue FROM runs WHERE run_id=?", (source_run,)).fetchone()
    if row != ("res_company_lookup",):
        raise ValueError("from-search-run must identify an existing RES company search run")
    found = {
        row[0]
        for row in store.db.execute(
            "WITH sources AS (SELECT cve,company_id FROM res_document_sources "
            "UNION SELECT cve,company_id FROM document_sources "
            "UNION SELECT cve,company_id FROM account_document_sources) "
            "SELECT DISTINCT s.cve FROM sources s JOIN jobs j "
            "ON j.job_key=s.company_id AND j.queue='res_company_lookup' "
            "JOIN run_jobs r ON r.job_id=j.id WHERE r.run_id=?",
            (source_run,),
        )
    }
    return sorted(found if args.cve is None else found.intersection(args.cve))


def save_report(store, output, report):
    progress_snapshot(store, report)
    report["results"] = [
        json.loads(row[0])
        for row in store.db.execute(
            "SELECT result_json FROM certificate_download_results r JOIN run_jobs j ON r.job_id=j.job_id "
            "WHERE j.run_id=? ORDER BY r.job_id",
            (report["run_id"],),
        )
    ]
    report["summary"] = dict(Counter(row["status"] for row in report["results"]))
    for target in [output / "res_download_report.json", output / "res_downloads" / f"{report['run_id']}.json"]:
        atomic_write_json(target, report)
    fields = ["cve", "status", "local_path", "sha256", "size_bytes", "reused", "error", "elapsed_seconds", "worker"]
    for target in [output / "res_download_report.csv", output / "res_downloads" / f"{report['run_id']}.csv"]:
        with atomic_text_writer(target, newline="") as handle:
            writer = csv.DictWriter(handle, fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(report["results"])
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
        keys = selected_cves(store, args)
        store.prepare_certificate_jobs(keys, args.max_attempts)
        run_id = store.start_run("certificate_download", args.limit, run_id=args.run_id)
        if args.retry_errors:
            reset = store.retry_stage_jobs("certificate_download", run_id=args.run_id, keys=keys)
            print(f"Reset certificate error jobs: {reset}", flush=True)
        provider = proxy_provider(args)
        pool = BrowserPool(args.profile, layout, headed=args.headed, pdf=True, proxy_provider=provider)
        report = {
            "run_id": run_id,
            "target": args.limit,
            "from_search_run": getattr(args, "from_search_run", None),
            "workers": layout.workers,
            "browsers": layout.browsers,
            "tabs_per_browser": layout.tabs_per_browser,
            "profiles": [str(profile) for profile in pool.profiles],
            "proxy_provider": provider,
            "proxy_routes": pool.routes.public,
            "checkpoint_interval": getattr(args, "checkpoint_interval", 15),
            "state": "running",
            "results": [],
        }
        print(
            f"RES PDF run: {run_id}; document limit: {args.limit}; "
            f"browsers: {layout.browsers}; tabs/browser: {layout.tabs_per_browser}",
            flush=True,
        )
        save_report(store, args.output, report)
        notifier = RunNotifications(args, report, stage="download", refresh=lambda: progress_snapshot(store, report))
        pool.on_ready = notifier.browser_ready
        await notifier.start()
        stop = asyncio.Event()
        failure_streak = 0

        async with pool, checkpoints(lambda: save_report(store, args.output, report), report["checkpoint_interval"]):

            async def heartbeat(job):
                while True:
                    await asyncio.sleep(30)
                    store.heartbeat(job, 180)

            async def worker(browser, tab):
                nonlocal failure_streak
                page = None
                owner = f"res-pdf-browser-{browser + 1}/tab-{tab + 1}"
                while not stop.is_set():
                    job = store.claim(run_id, owner, lease_seconds=180, max_in_flight=layout.workers, keys=keys)
                    if job is None:
                        return
                    started = monotonic()
                    task = asyncio.create_task(heartbeat(job))
                    try:
                        result = cached_pdf(args.output, job["key"])
                        if result is None:
                            try:
                                if page is None:
                                    page = await pool.page(browser, tab, stop=stop)
                                if page is None or not await wait_turn(
                                    store, "res-certificates", args.rate_per_minute, stop
                                ):
                                    store.finish(job, "retry_wait", error="Certificate worker stopped before verification")
                                    return
                                result = await download_certificate(
                                    page,
                                    job["key"],
                                    args.output,
                                    timeout_ms=args.timeout * 1000,
                                    captcha_seconds=args.captcha_wait,
                                )
                            except (SearchIssue, BrowserStartError) as exc:
                                result = {
                                    "cve": job["key"],
                                    "status": exc.status if isinstance(exc, SearchIssue) else "retry_wait",
                                    "error": str(exc),
                                }
                                if isinstance(exc, BrowserStartError):
                                    report["stop_reason"] = str(exc)
                                    stop.set()
                            except Exception as exc:
                                result = {
                                    "cve": job["key"],
                                    "status": "retry_wait",
                                    "error": (
                                        f"{type(exc).__name__}: "
                                        f"{(str(exc).splitlines() or ['no confirmed PDF download'])[0][:250]}"
                                    ),
                                }
                        result["source_url"] = RES_URL
                        result["elapsed_seconds"] = round(monotonic() - started, 2)
                        result["worker"] = owner
                        store.finish_certificate(job, result)
                    except asyncio.CancelledError:
                        with suppress(LostLease):
                            store.finish(job, "retry_wait", error="Certificate worker cancelled")
                        raise
                    except LostLease:
                        report["stop_reason"] = "Certificate job lease lost; result was not committed"
                        stop.set()
                        return
                    finally:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                    print(f"[{owner}] {job['key']}: {result['status']}; bytes: {result.get('size_bytes', 0)}", flush=True)
                    failure_streak = failure_streak + 1 if result["status"] in {"retry_wait", "failed"} else 0
                    if result["status"] in {"captcha_required", "blocked"} or failure_streak >= 3:
                        report["stop_reason"] = result.get("error", result["status"])
                        stop.set()
                    save_report(store, args.output, report)

            async with asyncio.TaskGroup() as group:
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
            save_report(store, args.output, report)
            if notifier is not None:
                await notifier.finish()
                save_report(store, args.output, report)
        store.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/dequienes-companies-2025"))
    parser.add_argument("--profile", type=Path, default=Path("data/res-certificates-profile"))
    parser.add_argument("--limit", type=document_limit, default=10, help="Maximum distinct CVEs, or 'all' for no cap")
    parser.add_argument("--from-search-run", help="Download only CVEs found for companies of this search run")
    add_browser_arguments(parser)
    parser.add_argument("--run-id", help="Reuse this ID with the same limit to resume")
    parser.add_argument("--cve", action="append", type=validate_cve, help="Restrict to a CVE already in the DB; repeatable")
    parser.add_argument("--rate-per-minute", type=float, default=6)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--captcha-wait", type=int, default=30)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--retry-errors", action="store_true")
    parser.add_argument("--headed", action="store_true", help="Visible browser for operator CAPTCHA interaction")
    parser.add_argument("--action", choices=["download", "status", "export"], default="download")
    args = parser.parse_args()
    try:
        validate_runtime(args)
    except ValueError as exc:
        parser.error(str(exc))
    if (args.limit is not None and args.limit < 0) or min(args.timeout, args.captcha_wait, args.max_attempts) < 1:
        parser.error("limit must be non-negative; workers/timeouts/attempts must be positive")
    if not math.isfinite(args.rate_per_minute) or args.rate_per_minute <= 0:
        parser.error("rate must be finite and positive")
    if not (args.output / "companies.sqlite3").is_file():
        parser.error("An existing companies.sqlite3 is required; run the company collector/search first")
    try:
        raise SystemExit(asyncio.run(run(args)))
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception as exc:
        print(f"RES PDF stopped: {type(exc).__name__}; check profile availability and report.", flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
