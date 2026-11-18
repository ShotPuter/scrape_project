"""Automatic date discovery, token-reusing verification and persistent PDF download."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
from pathlib import Path

import httpx

from scripts import res_free_pilot as pilot
from src.sites.res import http_flow
from src.sites.res.audio import load_local_audio_solver
from src.sites.res.bulk import date_range, iso_day, santiago_today
from src.sites.res.burst_verifier import burst_verify, mint_recaptcha_token
from src.sites.res.engines import launch_context
from src.sites.res.ready_download import download_ready, log_progress
from src.sites.res.source import FORM_URL, SEARCH_PATH, search_link
from src.sites.res.tracking import TrackingStore
from src.utils.atomic import atomic_write_json
from src.utils.error_details import safe_message


async def discover_dates(store, session, minter, args):
    """Use RES's date PageMethod; commit each completed day before continuing."""
    for day in date_range(args.start, args.end):
        if store.day_done(day.isoformat(), args.kind):
            continue
        token = await minter(FORM_URL)
        payload = await asyncio.to_thread(
            http_flow._post,
            session,
            SEARCH_PATH,
            {
                "tipoRegistro": "1",
                "tipoBusqueda": "4" if args.kind == "action" else "6",
                "texto": day.isoformat(),
                "Response": token,
            },
        )
        actions = await asyncio.to_thread(http_flow.details, session, search_link(payload))
        store.day_result(day.isoformat(), args.kind, actions, complete=day < santiago_today())
        print(f"RES {day}: {len(actions)} actions saved", flush=True)


async def run(args):
    store = TrackingStore(args.db, args.run_id)
    report = {"run_id": args.run_id, "operator": False}
    try:
        store.register_run({"mode": "bulk", "start": str(args.start), "end": str(args.end)}, [])
        options = argparse.Namespace(
            proxy_provider=args.proxy_provider,
            proxy_limit=args.proxy_index + 1,
            lazy_proxies=False,
            proxy_file=args.proxy_file,
        )
        endpoint = (await pilot.load_proxy_endpoints(options))[args.proxy_index]
        report["proxy"] = endpoint.label
        solver = await asyncio.to_thread(load_local_audio_solver, args.audio_model)
        with http_flow.new_session(timeout=args.timeout, proxy=endpoint.url) as session:
            await asyncio.to_thread(http_flow.prepare, session)
            async with launch_context(
                store.base / "profile",
                proxy=endpoint.browser,
                engine=args.engine,
                headed=False,
                proxy_ca=endpoint.ca_file,
                proxy_ip=endpoint.expected_ip,
            ) as context:
                page = await context.new_page()

                async def minter(url=None):
                    if url:
                        return await mint_recaptcha_token(page, solver, timeout_seconds=args.captcha_timeout, form_url=url)
                    return await mint_recaptcha_token(page, solver, timeout_seconds=args.captcha_timeout)

                await discover_dates(store, session, minter, args)
                report["verification"] = await burst_verify(
                    store, session, minter, limit=args.limit, max_tokens_per_session=args.max_tokens
                )
        report["audio_events"] = solver.events
        # Preserve and download already-issued links even when generation hits its quota.
        proxy = None if args.direct_download else endpoint.url
        transport = httpx.AsyncHTTPTransport(proxy=proxy, trust_env=False, retries=1)
        async with httpx.AsyncClient(timeout=args.timeout, trust_env=False, transport=transport) as client:
            report["downloads"] = await download_ready(
                store, client, limit=args.limit, workers=args.workers, interval=args.interval, progress=log_progress
            )
        report["status"] = report["verification"]["status"]
        if report["downloads"]["stop_reason"] or report["downloads"]["failed"] or report["verification"]["failed"]:
            report["status"] = "incomplete"
    except Exception as exc:
        report.update(status=getattr(exc, "status", "failed"), error=safe_message(f"{type(exc).__name__}: {exc}"))
    finally:
        atomic_write_json(store.base / "bulk-report.json", report)
        store.close()
        print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0 if report["status"] in {"ok", "completed"} else 2


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("data/res-bulk/tracking.sqlite3"))
    parser.add_argument("--run-id", default="res-bulk")
    parser.add_argument("--start", type=iso_day, required=True)
    parser.add_argument("--end", type=iso_day)
    parser.add_argument("--kind", choices=("action", "registration"), default="action")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--max-tokens", type=int, default=4)
    parser.add_argument("--proxy-provider", choices=("webshare", "direct", "residential"), default="webshare")
    parser.add_argument("--proxy-index", type=int, default=0)
    parser.add_argument("--proxy-file", type=Path, default=Path("data/residential-proxies.txt"))
    parser.add_argument("--engine", choices=("patchright", "camoufox"), default="patchright")
    parser.add_argument("--audio-model", default="base")
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--captcha-timeout", type=int, default=130)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--interval", type=float, default=0.1)
    parser.add_argument("--direct-download", action="store_true")
    args = parser.parse_args(argv)
    args.end = args.end or args.start
    if not iso_day("2013-01-01") <= args.start <= args.end <= santiago_today():
        parser.error("Dates must be ordered and between 2013-01-01 and today in Santiago")
    if min(args.limit, args.max_tokens, args.timeout, args.captcha_timeout, args.workers) <= 0:
        parser.error("Limits, workers and timeouts must be positive")
    if args.interval < 0 or args.proxy_index < 0:
        parser.error("Interval and proxy index must be nonnegative")
    args.db.parent.mkdir(parents=True, exist_ok=True)
    with args.db.with_suffix(".bulk.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("Another bulk worker already uses this database")
        return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
