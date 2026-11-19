"""Newest completed day first, with a shared CVE queue and bounded browser fleet."""

from __future__ import annotations

import asyncio
import json
import signal
import time
from contextlib import suppress
from pathlib import Path

from src.proxy.reputation import public_collection_allowed
from src.sites.res import fleet
from src.sites.res.daily_queue import DailyQueue
from src.sites.res.fleet_import import import_bulk, legacy_sources, recover_worker_files
from src.sites.res.stream_notifications import StreamNotifications
from src.sites.res.tracking import TrackingStore
from src.utils.atomic import atomic_write_json
from src.utils.error_details import safe_message


def restore_day(queue_path, tracking_path, run_id, day, kind):
    queue = DailyQueue(queue_path, run_id, initialize=False)
    store = TrackingStore(tracking_path, run_id)
    try:
        row = store.db.execute(
            "SELECT actions_json FROM date_searches WHERE run_id=? AND day=? AND kind=? AND complete=1", (run_id, day, kind)
        ).fetchone()
        if row:
            queue.expand_day(day, kind, json.loads(row[0]), store)
            with queue.database:
                queue.database.execute(
                    "UPDATE fleet_day_jobs SET state='done' WHERE run_id=? AND company_id=? AND state='recovering'",
                    (run_id, f"date:{kind}:{day}"),
                )
    finally:
        queue.close()
        store.close()


async def advance(queue, store, args):
    while True:
        day = queue.schedule_day(args.start, args.end, args.kind)
        if not day:
            return None
        if store.day_done(day, args.kind):
            with queue.database:
                changed = queue.database.execute(
                    "UPDATE fleet_day_jobs SET state='recovering' WHERE run_id=? AND company_id=? AND state='pending'",
                    (args.run_id, f"date:{args.kind}:{day}"),
                ).rowcount
            if changed:
                await asyncio.to_thread(restore_day, queue.path, store.path, args.run_id, day, args.kind)
        if queue.has_unfinished_jobs():
            return day


def audit_daily_files(queue_path, tracking_path, run_id):
    """Reopen completed CVEs whose file disappeared, without discarding day discovery."""
    queue = DailyQueue(queue_path, run_id, initialize=False)
    store = TrackingStore(tracking_path, run_id)
    reopened = 0
    try:
        after = ""
        while True:
            rows = queue.database.execute(
                "SELECT company_id,payload,day FROM fleet_day_jobs WHERE run_id=? AND state='done' "
                "AND company_id LIKE 'pdf:%' AND company_id>? ORDER BY company_id LIMIT 256",
                (run_id, after),
            ).fetchall()
            if not rows:
                break
            after = rows[-1][0]
            for task_id, payload, day in rows:
                job = json.loads(payload)
                if not store.existing_file(job["cve"]):
                    with queue.database:
                        queue.database.execute(
                            "UPDATE fleet_day_jobs SET state='pending',owner=NULL,next_at=0 WHERE run_id=? AND company_id=?",
                            (run_id, task_id),
                        )
                        queue.database.execute(
                            "UPDATE fleet_days SET state='draining' WHERE run_id=? AND day=? AND kind=?",
                            (run_id, day, job["kind"]),
                        )
                        queue.database.execute("UPDATE fleet_daily_control SET archive_current=0 WHERE run_id=?", (run_id,))
                    reopened += 1
    finally:
        store.close()
        queue.close()
    return reopened


async def execute_daily(args, root):
    config = {"mode": "days", "start": str(args.start), "end": str(args.end) if args.end else None, "kind": args.kind}
    path = root / "daily-config.json"
    if path.exists() and json.loads(path.read_text()) != config:
        raise ValueError("Daily date settings changed; choose a new --run-id")
    if not path.exists():
        for name in ("summary.json", "progress.json"):
            old = root / name
            if old.exists():
                old.rename(root / ("companies-" + name))
        atomic_write_json(path, config)
    store = TrackingStore(args.tracking_db or args.output / "tracking.sqlite3", args.run_id)
    queue = DailyQueue(args.output / "fleet-runtime.sqlite3", args.run_id)
    stop, changed = asyncio.Event(), asyncio.Event()
    worker = fleet.FleetBrowserWorker(args, root, store, queue)
    notifications = StreamNotifications(args, root)
    worker.notifications = notifications
    dispatcher = fleet.FleetDispatcher(args, queue, worker, store, changed, stop)
    producer = fleet.FleetProducer(args, root, queue, changed, stop) if "free" in args.provider_set else None
    if producer:
        producer.notifications = notifications
    children = []
    started = time.monotonic()
    initial = 0
    imports = []
    current = None
    error = None
    ready = set()
    summary = {}

    def snapshot(state):
        total = store.db.execute("SELECT COUNT(*) FROM document_files").fetchone()[0]
        return {
            "run_id": args.run_id,
            "mode": "days",
            "state": state,
            "current_date": current,
            "days_completed": queue.days_done(),
            "job_unit": "заданий дня/CVE",
            "counts": queue.counts(),
            "active_browsers": len(dispatcher.active),
            "browser_limit": args.browsers,
            "ready_browsers": sum(bool(r.get("ready")) for r in worker.active.values()),
            "total_pdfs": total,
            "companies_with_pdf": store.db.execute(
                "SELECT COUNT(DISTINCT cd.company_id) FROM document_files f CROSS JOIN company_documents cd ON cd.cve=f.cve"
            ).fetchone()[0],
            "new_pdfs": max(0, total - initial),
            "elapsed_seconds": round(time.monotonic() - started, 2),
            "producer": producer.stats if producer else {},
            "site_limit_pauses": dispatcher.site_limits,
            "tracking_database": str(store.path),
            "imports": imports,
        }

    await notifications.start()
    notifications.publish(
        "preparing", f"🚀 RES daily fleet: сохранение PDF и восстановление дневной очереди ({args.run_id})"
    )
    try:
        store.register_run(config, [])
        queue.recover()
        recovered = await asyncio.to_thread(recover_worker_files, store.path, args.run_id)
        for source in legacy_sources(args):
            counts = await asyncio.to_thread(import_bulk, source, store.path, args.run_id)
            imports.append({"source": str(source), **counts})
            notifications.record("import", f"RES daily import {source}: {counts}")
        reopened = await asyncio.to_thread(audit_daily_files, queue.path, store.path, args.run_id)
        if reopened:
            notifications.record(
                "recovery", f"RES daily: восстановлено заданий с отсутствующим/повреждённым PDF: {reopened}"
            )
        initial = store.db.execute("SELECT COUNT(*) FROM document_files").fetchone()[0]
        store.prepare_cooldown(enabled=args.error_cooldown_minutes > 0)
        dispatcher.rollover_day()
        queue.wake_parked(time.time())
        current = await advance(queue, store, args)
        notifications.publish(
            "started",
            f"🚀 RES daily fleet: {args.browsers} браузеров; провайдеры {args.providers}\n"
            f"Порядок: вчера → {args.start}; день {current or 'архив обработан'}\n"
            f"Уже сохранено PDF: {initial}; восстановлено из манифестов: {recovered}",
        )
        store.run_status("running")
        children = [asyncio.create_task(dispatcher.consume(slot)) for slot in range(args.browsers)]
        children.append(asyncio.create_task(fleet.wake_routes(queue, stop)))
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        next_checkpoint = 0.0
        next_poll = 0.0
        seeded = False
        last_day = None
        while not stop.is_set():
            for task in children:
                if task.done() and not task.cancelled() and task.exception():
                    raise RuntimeError("Daily fleet background task failed") from task.exception()
            if store.cooldown_state()["halted"]:
                break
            if current is not None or time.monotonic() >= next_poll:
                current = await advance(queue, store, args)
                next_poll = time.monotonic() + args.poll_seconds
                changed.set()
            if current != last_day:
                notifications.record(
                    "day", f"RES daily fleet: день {current or 'архив обработан'}; завершено дней {queue.days_done()}"
                )
                last_day = current
            if current and queue.has_available_job() and not seeded:
                if producer and not public_collection_allowed():
                    raise ValueError("Fleet free provider requires ALLOW_PUBLIC_PROXY_COLLECTION=1")
                await fleet.seed_routes(args, queue)
                if producer:
                    children.append(asyncio.create_task(producer.run()))
                seeded = True
                changed.set()
            for active in list(worker.active.values()):
                paths = await asyncio.to_thread(
                    lambda a=active: [
                        p for p in Path(a["directory"]).glob("report-*.json") if str(p) not in a["prior_reports"]
                    ]
                )
                if paths:
                    report = fleet.benchmark.read_json(max(paths, key=lambda p: p.stat().st_mtime), {})
                    pair = next(
                        (
                            p
                            for p in report.get("proxies", [])
                            if p.get("http_ip") and p.get("http_ip") == p.get("browser_ip")
                        ),
                        None,
                    )
                    active["ready"] = bool(pair)
                    if pair and active["owner"] not in ready:
                        ready.add(active["owner"])
                        notifications.activity(
                            "browser_ready", f"🌐 Слот {active['slot'] + 1}: HTTP и браузер {pair['http_ip']}"
                        )
            if time.monotonic() >= next_checkpoint:
                summary = snapshot("running" if current else "archive_current")
                atomic_write_json(root / "progress.json", summary)
                print(
                    f"RES daily fleet: date={current}; days_done={summary['days_completed']}; "
                    f"jobs={summary['counts']['jobs']}; browsers={len(dispatcher.active)}/{args.browsers}; "
                    f"PDF total={summary['total_pdfs']}, new={summary['new_pdfs']}",
                    flush=True,
                )
                notifications.update(summary)
                next_checkpoint = time.monotonic() + args.checkpoint_seconds
            if current is None and args.end and not dispatcher.active:
                break
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), 0.5)
    except (asyncio.CancelledError, KeyboardInterrupt):
        stop.set()
        raise
    except Exception as exc:
        error = safe_message(exc)
        raise
    finally:
        interrupted = stop.is_set()
        stop.set()
        for task in children:
            task.cancel()
        if children:
            await asyncio.gather(*children, return_exceptions=True)
        for signum in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_running_loop().remove_signal_handler(signum)
        summary = snapshot(
            "failed" if error else "interrupted" if interrupted else "finished" if current is None else "partial"
        )
        summary.update(active_browsers=0, ready_browsers=0)
        if error:
            summary["error"] = error
        store.run_status(summary["state"])
        atomic_write_json(root / "progress.json", summary)
        atomic_write_json(root / "summary.json", summary)
        await notifications.finish(notifications.format_summary(summary, "🏁 RES daily fleet"))
        queue.close()
        store.close()
    return 130 if interrupted else 0 if summary["state"] == "finished" else 2
