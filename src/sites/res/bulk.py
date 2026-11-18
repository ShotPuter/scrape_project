"""Collect RES actions by date using the site's search form."""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from src.proxy.reputation import screen_proxy
from src.sites.res.audio import load_local_audio_solver
from src.sites.res.browser import open_search_form, submit_search
from src.sites.res.source import SearchIssue
from src.sites.res.tracking import TrackingStore

DEFAULT_DB = Path("data/res-bulk/tracking.sqlite3")
DEFAULT_RUN = "res-bulk-discover"
DATE_FIELDS = {"action": ("3", "#txtFecha"), "registration": ("4", "#txtFechaRegistro")}


def santiago_today():
    return datetime.now(ZoneInfo("America/Santiago")).date()


def date_range(start, end):
    for offset in range((end - start).days + 1):
        yield start + timedelta(days=offset)


async def discover_day(page, day, kind, *, captcha_solver=None, captcha_wait_ms=60000):
    await open_search_form(page, timeout_ms=60000)
    option, field = DATE_FIELDS[kind]
    await page.locator("#ddlTipoBusqueda").select_option(option)
    await page.locator(field).fill(day)
    if captcha_solver is not None:
        await captcha_solver(page, timeout_ms=captcha_wait_ms, captcha_seconds=captcha_wait_ms / 1000)
    else:
        print(f"RES {day} — solve CAPTCHA in browser window. Search will submit automatically.", file=sys.stderr, flush=True)
        await (
            page.frame_locator('iframe[title="reCAPTCHA"]')
            .locator('#recaptcha-anchor[aria-checked="true"]')
            .wait_for(state="attached", timeout=captcha_wait_ms)
        )
    return await submit_search(page, timeout_ms=60000)


async def discover(args):
    from playwright.async_api import TimeoutError as BrowserTimeout, async_playwright

    store = TrackingStore(args.db, args.run_id)
    try:
        store.register_run({"mode": "date_discovery", "kind": args.kind, "start": str(args.start), "end": str(args.end)}, [])
        days = [day for day in date_range(args.start, args.end) if not store.day_done(day.isoformat(), args.kind)]
        if not days:
            print("All dates in this range are already processed.", flush=True)
            return 0

        proxy = await screen_proxy(args.proxy)
        solver = None
        if not args.operator:
            try:
                solver = await asyncio.to_thread(load_local_audio_solver, args.audio_model)
            except SearchIssue as exc:
                print(str(exc), file=sys.stderr)
                return 2

        async with async_playwright() as playwright:
            launch_kwargs = {"headless": not args.operator, "chromium_sandbox": True}
            if args.channel:
                launch_kwargs["channel"] = args.channel
            elif args.operator:
                launch_kwargs["channel"] = "chrome"

            if proxy:
                launch_kwargs["proxy"] = {"server": proxy}

            browser = await playwright.chromium.launch(**launch_kwargs)
            try:
                page = await browser.new_page(locale="es-CL")
                for day in days:
                    complete = day < santiago_today()
                    try:
                        actions = await discover_day(
                            page, day.isoformat(), args.kind, captcha_solver=solver, captcha_wait_ms=args.captcha_wait * 1000
                        )
                    except (SearchIssue, BrowserTimeout) as error:
                        reason = (
                            str(error) if isinstance(error, SearchIssue) else "timeout waiting for CAPTCHA or RES response"
                        )
                        print(f"RES {day} — {reason}", file=sys.stderr)
                        return 2
                    store.day_result(day.isoformat(), args.kind, actions, complete=complete)
                    cves = {action["cve"] for action in actions if action["cve"]}
                    print(f"{day} — actions: {len(actions)}, unique CVEs: {len(cves)}", flush=True)
            finally:
                await browser.close()
        return 0
    finally:
        store.close()


def iso_day(value):
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("Use YYYY-MM-DD") from error
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("Use YYYY-MM-DD")
    return parsed


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--run-id", default=DEFAULT_RUN)
    parser.add_argument("--start", type=iso_day, required=True)
    parser.add_argument("--end", type=iso_day, help="Inclusive end date, defaults to --start")
    parser.add_argument("--kind", choices=tuple(DATE_FIELDS), default="action")
    parser.add_argument("--captcha-wait", type=int, default=120, help="Seconds to wait for CAPTCHA solver or operator")
    parser.add_argument("--operator", action="store_true", help="Run headed and wait for manual operator to solve CAPTCHA")
    parser.add_argument("--audio-model", default="base", help="Whisper model name for audio CAPTCHA solver")
    parser.add_argument("--proxy", help="Optional proxy URL (e.g. http://user:pass@host:port)")
    parser.add_argument("--channel", help="Optional browser channel (e.g. chrome)")
    args = parser.parse_args(argv)
    args.end = args.end or args.start
    if not date(2013, 1, 1) <= args.start <= args.end <= santiago_today():
        parser.error("Date range must be ordered and fall between 2013-01-01 and today in Santiago")
    if args.captcha_wait < 1:
        parser.error("--captcha-wait must be positive")
    return asyncio.run(discover(args))


if __name__ == "__main__":
    raise SystemExit(main())
