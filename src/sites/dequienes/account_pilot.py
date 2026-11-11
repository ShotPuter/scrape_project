"""Bounded account pilot: match queued companies and collect RES CVEs from Legal."""

from __future__ import annotations

import argparse
import asyncio
import csv
import fcntl
import json
import re
import shutil
import unicodedata
from collections import Counter
from contextlib import AsyncExitStack, suppress
from pathlib import Path
from urllib.parse import urlencode

from bs4 import BeautifulSoup

from src.sites.dequienes.companies import exclusive_profile
from src.sites.dequienes.pipeline import export_pipeline
from src.sites.dequienes.queue import LostLease, QueueStore, normalized_rut
from src.sites.dequienes.source import parse_legal
from src.utils.atomic import atomic_text_writer, atomic_write_json


def name_key(name):
    return " ".join(unicodedata.normalize("NFKC", name).casefold().translate(str.maketrans("áéíóúü", "aeiouu")).split())


def search_cards(html):
    soup = BeautifulSoup(html, "html.parser")
    cards = []
    for index, heading in enumerate(soup.select("h3")):
        if not heading.parent.find("span", string="Empresa", recursive=False):
            continue
        card = next((p for p in heading.parents if "bg-card" in p.get("class", [])), None)
        if card is None:
            raise ValueError("Company card container changed")
        match = re.search(r"RUT:\s*([0-9.]+-[0-9Kk])", card.get_text(" ", strip=True))
        cards.append(
            {
                "index": index,
                "company_name": heading.get_text(" ", strip=True),
                "rut": normalized_rut(match[1]) if match else None,
            }
        )
    return cards


def choose_card(cards, company):
    if company.get("rut"):
        matches = [c for c in cards if c["rut"] == company["rut"]]
        if not matches and len(cards) == 1 and cards[0]["rut"] is None:
            return cards[0]  # Must verify RUT after expanding; never accept it on this basis alone.
    else:
        matches = [c for c in cards if name_key(c["company_name"]) == name_key(company["company_name"])]
    return matches[0] if len(matches) == 1 else None


def access_status(text):
    if re.search(
        r"has agotado las b[uú]squedas|l[ií]mite de consultas|sin cr[eé]ditos|cr[eé]ditos insuficientes", text, re.I
    ):
        return "blocked"
    if re.search(r"Podrás seguir buscando|Inicia sesión|Entra con Google", text, re.I):
        return "needs_login"
    return None


async def lookup(page, company, timeout_ms):
    query = company.get("rut") or company["company_name"]
    url = "https://dequienes.cl/?" + urlencode({"term": query, "sources": "company"})
    result = {**company, "query": query, "source_url": url, "status": "failed", "events": []}
    await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    await page.wait_for_function(
        """() => {
        const t=document.body.innerText;
        return document.querySelector('h3') || /Podrás seguir buscando|Inicia sesión|Entra con Google/i.test(t)
            || /no se encontraron|sin resultados|no encontramos|no hay resultados/i.test(t)
            || /has agotado las b[uú]squedas|l[ií]mite de consultas|sin cr[eé]ditos|cr[eé]ditos insuficientes/i.test(t);
    }""",
        timeout=timeout_ms,
    )
    html = await page.content()
    cards = search_cards(html)
    text = await page.locator("body").inner_text()
    gate = access_status(text)
    if gate == "blocked" or (not cards and gate):
        return {**result, "status": gate, "error": "account_query_limit" if gate == "blocked" else "login_required"}
    if not cards:
        if re.search(r"no se encontraron|sin resultados|no encontramos|no hay resultados", text, re.I):
            return {**result, "status": "not_found"}
        raise ValueError("Search layout is not recognized")
    selected = choose_card(cards, company)
    if selected is None:
        return {**result, "status": "ambiguous_match", "candidates": cards}
    heading = page.locator("h3").nth(selected["index"])
    card = heading.locator("xpath=ancestor::div[contains(concat(' ',normalize-space(@class),' '),' bg-card ')][1]")
    legal = card.get_by_role("tab", name="Legal", exact=True)
    if not await legal.count() or not await legal.is_visible():
        await heading.click(timeout=timeout_ms)
        await legal.wait_for(state="visible", timeout=timeout_ms)
    expanded = search_cards(await page.content())
    actual = next((c for c in expanded if c["index"] == selected["index"]), None)
    if not actual or (company.get("rut") and actual["rut"] != company["rut"]):
        return {**result, "status": "ambiguous_match", "error": "RUT does not match selected company"}
    result.update(found_company_name=actual["company_name"], found_rut=actual["rut"])
    await legal.click(timeout=timeout_ms)
    panel = card.locator("[role=tabpanel][data-state=active]").first
    await panel.get_by_text("Historial Legal", exact=True).wait_for(timeout=timeout_ms)
    events, seen = [], set()
    expected_count = None
    for _ in range(100):
        await page.wait_for_function(
            """element => element.querySelector('table tbody tr')
            || /0 registros? legales|sin eventos|no hay registros|no se encontraron eventos/i.test(element.innerText)""",
            arg=await panel.element_handle(),
            timeout=timeout_ms,
        )
        body = await panel.inner_text()
        count = re.search(r"(\d+)\s+registros?\s+legales", body)
        if count:
            expected_count = int(count[1])
        if await panel.locator("table").count():
            rows = parse_legal(await panel.inner_html(), actual["company_name"], actual["rut"] or "")
        else:
            rows = []
        for row in rows:
            key = tuple(row.get(k) for k in ["date", "event_type", "source", "cve", "description"])
            if key not in seen:
                seen.add(key)
                events.append(row)
        pager = re.search(r"Página\s+(\d+)\s+de\s+(\d+)", body)
        if not pager or pager[1] == pager[2]:
            break
        current = int(pager[1])
        button = panel.locator("button").filter(has=page.locator("svg.lucide-chevron-right"))
        if await button.count() != 1 or await button.is_disabled():
            raise ValueError("Legal pagination is not accessible")
        await button.click(timeout=timeout_ms)
        await page.wait_for_function(
            """([element,next]) => new RegExp('Página\\\\s+'+next+'\\\\s+de').test(element.innerText)""",
            arg=[await panel.element_handle(), current + 1],
            timeout=timeout_ms,
        )
    else:
        raise ValueError("Legal history exceeds pilot pagination limit")
    if expected_count is not None and len(events) != expected_count:
        raise ValueError("Legal event count does not match loaded rows")
    codes = sorted({e["cve"] for e in events if e["source"].strip().upper() == "RES"})
    return {**result, "status": "completed" if codes else "no_cve_found", "events": events, "res_cves": codes}


def save_report(output, report):
    report["summary"] = dict(Counter(r["status"] for r in report["results"]))
    report["confirmed_results"] = sum(
        r["status"] in {"completed", "no_cve_found", "not_found", "ambiguous_match"} for r in report["results"]
    )
    for path in [output / "account_pilot.json", output / "account_pilots" / f"{report['run_id']}.json"]:
        atomic_write_json(path, report)
    for path in [output / "account_pilot.csv", output / "account_pilots" / f"{report['run_id']}.csv"]:
        with atomic_text_writer(path, newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "company_name",
                    "requested_rut",
                    "found_company_name",
                    "found_rut",
                    "status",
                    "legal_event_count",
                    "res_cves",
                    "source_url",
                ]
            )
            for result in report["results"]:
                writer.writerow(
                    [
                        result["company_name"],
                        result.get("rut") or "",
                        result.get("found_company_name", ""),
                        result.get("found_rut") or "",
                        result["status"],
                        len(result.get("events", [])),
                        ";".join(result.get("res_cves", [])),
                        result.get("source_url", ""),
                    ]
                )


def prepare_profiles(source, run_id, count):
    if count == 1:
        return [source]
    profiles = []
    with exclusive_profile(source), (source / ".parentlock").open("a") as handle:
        try:
            fcntl.lockf(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("Close the source account browser before copying its profile") from exc
        for index in range(count):
            target = source.parent / (source.name + "-workers") / run_id / f"browser-{index + 1}"
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if target.exists():
                if not (target / "cookies.sqlite").is_file():
                    raise ValueError("Incomplete worker profile; select a new run ID")
                profiles.append(target)
                continue
            shutil.copytree(
                source,
                target,
                ignore=shutil.ignore_patterns(
                    "lock",
                    ".parentlock",
                    "parent.lock",
                    ".collector.lock",
                    "cache2",
                    "startupCache",
                    "shader-cache",
                    "crashes",
                    "minidumps",
                    "saved-telemetry-pings",
                ),
            )
            target.chmod(0o700)
            profiles.append(target)
    return profiles


async def run(args):
    from camoufox.async_api import AsyncCamoufox

    store = QueueStore(args.output / "companies.sqlite3")
    run_id = store.start_run("company_lookup", args.limit, run_id=getattr(args, "run_id", None))
    previous = store.db.execute(
        "SELECT r.result_json FROM company_lookup_results r JOIN run_jobs j ON j.job_id=r.job_id "
        "WHERE j.run_id=? ORDER BY r.job_id",
        (run_id,),
    ).fetchall()
    report = {
        "results": [json.loads(row[0]) for row in previous],
        "run_id": run_id,
        "browsers": args.browsers,
        "tabs_per_browser": args.tabs_per_browser,
    }
    stop = asyncio.Event()
    failure_streak = 0
    print(
        f"Account run: {run_id}; limit: {args.limit}; browsers: {args.browsers}; tabs each: {args.tabs_per_browser}",
        flush=True,
    )

    async def worker(page, owner):
        nonlocal failure_streak
        try:
            while not stop.is_set():
                job = store.claim(run_id, owner, lease_seconds=180, max_in_flight=args.browsers * args.tabs_per_browser)
                if job is None:
                    return
                company_id, name, rut = store.db.execute(
                    "SELECT company_id,company_name,rut FROM companies WHERE company_id=?", (job["key"],)
                ).fetchone()
                company = {"company_id": company_id, "company_name": name, "rut": rut}

                async def heartbeat(current):
                    while True:
                        await asyncio.sleep(30)
                        store.heartbeat(current, 180)

                task = asyncio.create_task(heartbeat(job))
                try:
                    await asyncio.sleep(store.request_delay("dequienes-account", args.rate_per_minute))
                    if stop.is_set():
                        store.finish(job, "retry_wait", error="another worker stopped account session")
                        return
                    result = await lookup(page, company, args.timeout * 1000)
                    store.finish_company_lookup(job, result)
                except asyncio.CancelledError:
                    with suppress(LostLease):
                        store.finish(job, "retry_wait", error="pilot cancelled")
                    raise
                except LostLease:
                    stop.set()
                    return
                except Exception as exc:
                    result = {**company, "status": "failed", "source_url": "", "error": type(exc).__name__, "events": []}
                    with suppress(LostLease):
                        store.finish_company_lookup(job, result)
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                report["results"] = [r for r in report["results"] if r["company_id"] != company_id]
                report["results"].append(result)
                print(
                    f"{len(report['results'])}/{args.limit} [{owner}]: {name}: {result['status']}; "
                    f"RES CVEs: {len(result.get('res_cves', []))}",
                    flush=True,
                )
                save_report(args.output, report)
                export_pipeline(store, args.output)
                failure_streak = failure_streak + 1 if result["status"] == "failed" else 0
                if result["status"] in {"needs_login", "blocked"} or failure_streak >= 3:
                    report["stop_reason"] = result.get("error") or result["status"]
                    stop.set()
        finally:
            await page.close()

    try:
        profiles = prepare_profiles(args.profile, run_id, args.browsers)
        report["profiles"] = [str(p) for p in profiles]
        async with AsyncExitStack() as stack:
            contexts = []
            for number, profile in enumerate(profiles, 1):
                stack.enter_context(exclusive_profile(profile))
                context = await stack.enter_async_context(
                    AsyncCamoufox(
                        headless="virtual",
                        os="linux",
                        locale="es-CL",
                        persistent_context=True,
                        user_data_dir=str(profile.resolve()),
                    )
                )
                for extra in list(context.pages)[1:]:
                    await extra.close()
                contexts.append(context)
                print(f"Account browser {number}/{len(profiles)} ready", flush=True)
            async with asyncio.TaskGroup() as group:
                for browser_index, context in enumerate(contexts, 1):
                    for tab_index in range(args.tabs_per_browser):
                        page = context.pages[0] if tab_index == 0 and context.pages else await context.new_page()
                        group.create_task(worker(page, f"browser-{browser_index}/tab-{tab_index + 1}"))
        return int(any(r["status"] in {"needs_login", "blocked", "failed"} for r in report["results"]))
    finally:
        save_report(args.output, report)
        export_pipeline(store, args.output)
        store.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/dequienes-companies-2025"))
    parser.add_argument("--profile", type=Path, default=Path("data/dequienes-account-profile"))
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--run-id", help="Resume an existing company pilot without selecting extra companies")
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--browsers", type=int, default=1)
    parser.add_argument("--tabs-per-browser", type=int, default=1)
    parser.add_argument("--rate-per-minute", type=float, default=30)
    args = parser.parse_args()
    if args.browsers < 1 or args.tabs_per_browser < 1 or not 0 < args.rate_per_minute < 10000:
        parser.error("browsers, tabs and rate must be positive (rate below 10000)")
    if args.limit < 1 or args.timeout < 1:
        parser.error("limit and timeout must be positive")
    if not (args.output / "companies.sqlite3").is_file():
        parser.error("Existing companies database is required")
    try:
        raise SystemExit(asyncio.run(run(args)))
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception as exc:
        print(f"Account pilot stopped: {type(exc).__name__}", flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
