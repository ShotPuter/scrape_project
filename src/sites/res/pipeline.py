"""Fresh server workflow: discover companies, search RES, then download all found CVEs."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import math
import re
from pathlib import Path

from src.sites.dequienes.companies import month_value
from src.sites.dequienes.pipeline import company_keys, run_public, selection_months
from src.sites.dequienes.queue import QueueStore
from src.sites.res import __main__ as search, download
from src.sites.res.proxies import proxy_provider
from src.sites.res.runtime import add_browser_arguments, checkpoints, progress_snapshot, validate_runtime
from src.sites.res.telegram import RunNotifications
from src.utils.atomic import atomic_write_json
from src.utils.error_details import exception_details, failure_state


def read_state(path):
    if path.exists():
        return json.loads(path.read_text())
    return None


async def collect(args, store, provider):
    public_id = args.run_id + "-public"
    profile = getattr(args, "collection_profile", None) or args.profile.with_name(args.profile.name + "-public")
    report = {
        "run_id": public_id,
        "target": args.limit,
        "browsers": 1,
        "tabs_per_browser": 1,
        "proxy_provider": provider,
        "proxy_routes": [],
        "state": "running",
        "companies": 0,
        "phase": "preparing_browser",
        "profile": str(profile),
    }

    def refresh():
        progress_snapshot(store, report)
        report["companies"] = len(company_keys(store, selection_months(args.year, args.month)))

    def save():
        refresh()
        atomic_write_json(args.output / "public_collection" / f"{public_id}.json", report)

    notifier = RunNotifications(args, report, stage="collect", refresh=refresh)

    def observer(_store, _run_id, routes, ready):
        report["proxy_routes"] = routes
        if ready:
            report["phase"] = "browser_proxy_verified" if provider == "webshare" else "browser_ready"
            notifier.browser_ready(0)

    def phase_observer(details):
        report["bootstrap"] = details
        for key in ("phase", "last_http_status", "last_url", "diagnostics"):
            if key in details:
                report[key] = details[key]

    public_args = argparse.Namespace(
        output=args.output,
        profile=profile,
        config=None,
        action="collect",
        year=args.year,
        month=args.month,
        limit=None,
        company_target=args.limit,
        run_id=public_id,
        workers=args.collect_workers,
        max_in_flight=args.collect_workers,
        transport="hybrid",
        proxy_provider=provider,
        rate_per_minute=args.collect_rate_per_minute,
        delay=0.3,
        timeout=args.timeout,
        max_attempts=args.max_attempts,
        retry_errors=args.retry_errors,
        on_collection_progress=observer,
        on_collection_phase=phase_observer,
    )
    try:
        save()
        await notifier.start()
        async with checkpoints(save, args.checkpoint_interval):
            code = await run_public(public_args)
        refresh()
        enough = report["companies"] >= args.limit
        report["state"] = "completed" if enough else "stopped"
        if not enough:
            report["stop_reason"] = f"Collected {report['companies']}/{args.limit} unique companies; collection incomplete"
        return 0 if enough else (code or 1)
    except BaseException as exc:
        report["state"] = failure_state(exc)
        report["error"] = exception_details(exc)
        report["stop_reason"] = report["error"]["summary"]
        print(f"Public collection {report['state']}: {report['stop_reason']}", flush=True)
        raise
    finally:
        if report["state"] == "running":
            report["state"] = "interrupted"
        save()
        await notifier.finish()
        save()


async def run(args):
    args.output.mkdir(parents=True, exist_ok=True)
    # One coordinator per output, separate from the persistent browser profile locks.
    with (args.output / ".pipeline.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("Another pipeline already uses this output") from exc
        return await run_locked(args)


async def run_locked(args):
    validate_runtime(args)
    store = QueueStore(args.output / "companies.sqlite3")
    path = args.output / "pipelines" / f"{args.run_id}.json"
    selection = {"year": args.year, "months": args.month, "limit": args.limit}
    state = {"run_id": args.run_id, "selection": selection, "company_ids": [], "phase": "collect", "state": "running"}
    save_state = False
    try:
        previous = read_state(path)
        if previous is not None:
            if previous.get("selection") != selection:
                raise ValueError("Existing pipeline run has a different year/month/limit; use its original selection")
            state.update(previous)
        save_state = True
        state.update(state="running", phase="collect")
        state.pop("error", None)
        provider = proxy_provider(args)
        collection_choice = getattr(args, "collect_proxy_provider", "inherit")
        collect_provider = (
            provider
            if collection_choice == "inherit"
            else proxy_provider(argparse.Namespace(proxy_provider=collection_choice))
        )
        state["proxy_providers"] = {"collect": collect_provider, "res": provider}
        atomic_write_json(path, state)
        if not state["company_ids"]:
            name = args.profile.name + "-public"
            if collect_provider != provider:
                name += "-" + collect_provider
            collection_args = argparse.Namespace(**{**vars(args), "collection_profile": args.profile.with_name(name)})
            if await collect(collection_args, store, collect_provider):
                state["state"] = "stopped"
                return 1
            # Freeze the selected company set so later public discoveries cannot change a resumed search.
            state["company_ids"] = company_keys(store, selection_months(args.year, args.month))[: args.limit]
            if len(state["company_ids"]) != args.limit:
                raise ValueError("Collection did not reach the requested number of unique companies")
        state["phase"] = "search"
        atomic_write_json(path, state)
        search_args = argparse.Namespace(
            **{**vars(args), "action": "collect", "company_id": state["company_ids"], "proxy_provider": provider}
        )
        search_code = await search.run(search_args)
        state.update(phase="download", search_exit_code=search_code)
        atomic_write_json(path, state)
        # Both stages use the same saved browser/proxy slots sequentially.
        pdf_args = argparse.Namespace(
            **{
                **vars(args),
                "action": "download",
                "limit": None,
                "cve": None,
                "from_search_run": args.run_id,
                "run_id": args.run_id + "-pdf",
                "proxy_provider": provider,
            }
        )
        pdf_code = await download.run(pdf_args)
        code = int(bool(search_code or pdf_code))
        state.update(phase="finished", state="stopped" if code else "completed", download_exit_code=pdf_code)
        return code
    except BaseException as exc:
        state.update(state=failure_state(exc), error=exception_details(exc))
        raise
    finally:
        if save_state:
            atomic_write_json(path, state)
        store.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/dequienes-companies-2025"))
    parser.add_argument("--profile", type=Path, default=Path("data/res-search-profile"))
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--month", action="append", type=month_value, default=None)
    parser.add_argument("--limit", type=int, default=100, help="Unique companies to discover and check")
    parser.add_argument("--run-id", default="res-companies-100")
    parser.add_argument("--collect-workers", type=int, default=4, help="Concurrent public HTTP workers")
    parser.add_argument("--collect-rate-per-minute", type=float, default=60)
    parser.add_argument(
        "--collect-proxy-provider",
        choices=["inherit", "auto", "direct", "webshare"],
        default="inherit",
        help="Route for DeQuienes only; inherit keeps the RES proxy provider",
    )
    parser.add_argument("--rate-per-minute", type=float, default=6)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--captcha-wait", type=int, default=30)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--retry-errors", action="store_true")
    parser.add_argument("--headed", action="store_true")
    add_browser_arguments(parser)
    args = parser.parse_args()
    args.month = args.month or ["1"]
    try:
        validate_runtime(args)
        if min(args.limit, args.collect_workers, args.timeout, args.captcha_wait, args.max_attempts) < 1:
            raise ValueError("company limit, workers and timeouts must be positive")
        if not 1 <= args.year <= 9999 or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", args.run_id):
            raise ValueError("Invalid year or run ID")
        for rate in [args.rate_per_minute, args.collect_rate_per_minute]:
            if not math.isfinite(rate) or rate <= 0:
                raise ValueError("Request rates must be finite and positive")
    except ValueError as exc:
        parser.error(str(exc))
    try:
        raise SystemExit(asyncio.run(run(args)))
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception as exc:
        details = exception_details(exc)
        print(f"RES pipeline stopped: {details['summary']}; progress saved in {args.output}", flush=True)
        for cause in details["causes"][:3]:
            print(f"Cause: {cause['type']}: {cause['message']}", flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
