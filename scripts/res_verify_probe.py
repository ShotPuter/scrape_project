"""Verify+download RES CVEs on one proxy with parallel tabs; report daily-limit behaviour.

Input is the ``actions.json`` produced by ``scripts.res_date_search_probe`` (or any
JSON list of objects with a ``cve`` field).  Every document costs exactly one
reCAPTCHA token; the probe measures how many documents one IP can generate before
RES answers with its daily limit.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv

from scripts import res_free_pilot as pilot
from src.sites.res import http_flow
from src.sites.res.engines import launch_context
from src.sites.res.source import SearchIssue


def load_cves(args):
    cves = []
    seen = set()
    for path in args.actions:
        rows = json.loads(Path(path).read_text(encoding="utf-8"))
        for row in rows:
            cve = (row.get("cve") or "").strip()
            if cve and cve not in seen:
                seen.add(cve)
                cves.append({"cve": cve, "company_id": row.get("rut") or "date-search"})
    return cves[: args.limit] if args.limit else cves


async def verify_one(cascade, http, cve, company_id, state, args, output, counters):
    return await pilot.ensure_document(cascade, http, cve, company_id, state, args, output, counters)


async def tab_worker(index, context, queue, args, audio, state, output, counters, endpoint, results, http):
    page = await context.new_page()
    cascade = pilot.Cascade(page, args, None, audio, counters, results["tokens"], f"{endpoint.label}#{index}")
    while True:
        try:
            item = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        cve = item["cve"]
        try:
            saved = await verify_one(cascade, http, cve, item["company_id"], state, args, output, counters)
            results["documents"].append(
                {"cve": cve, "status": saved.get("status", "downloaded"), "size": saved.get("size_bytes")}
            )
            pilot.say(f"tab{index} {cve} {saved.get('status', 'downloaded')} {saved.get('size_bytes')}")
        except SearchIssue as exc:
            results["documents"].append({"cve": cve, "status": exc.status, "error": str(exc)[:300]})
            pilot.say(f"tab{index} {cve} {exc.status}: {str(exc)[:120]}")
            if exc.status in {"daily_limit", "blocked", "proxy_failed"}:
                results["stop"] = exc.status
                while not queue.empty():
                    queue.get_nowait()
                    queue.task_done()
                break
        except Exception as exc:  # noqa: BLE001
            results["documents"].append({"cve": cve, "status": "error", "error": f"{type(exc).__name__}: {exc}"})
            pilot.say(f"tab{index} {cve} error: {type(exc).__name__}")
        finally:
            queue.task_done()
        if args.interval:
            await asyncio.sleep(args.interval)


async def probe(args):
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    items = load_cves(args)
    if not items:
        raise SystemExit("No CVEs to verify")
    pilot.say(f"{len(items)} unique CVEs, tabs={args.tabs}")

    args.proxy_limit = max(args.proxy_limit, args.proxy_index + 1)
    endpoints = await pilot.load_proxy_endpoints(args)
    endpoint = endpoints[args.proxy_index]
    audio, audio_error = pilot.load_audio(args)
    if audio_error:
        pilot.say(audio_error)

    state = {"companies": {}, "cves": {}}
    args.state_path = output / "state.json"
    counters = Counter(
        tokens=0,
        codigos=0,
        downloaded=0,
        reused=0,
        http_verify_seconds=0.0,
        http_verify_calls=0,
        http_download_seconds=0.0,
        http_download_calls=0,
    )
    results = {"tokens": [], "documents": [], "stop": None, "proxy": endpoint.label}
    started = time.monotonic()
    http = http_flow.new_session(timeout=args.timeout, proxy=endpoint.url)
    try:
        results["http_ip"] = pilot.http_exit_ip(http)
        http_flow.prepare(http)
        queue = asyncio.Queue()
        for item in items:
            queue.put_nowait(item)
        async with launch_context(
            output / "profile", proxy=endpoint.browser, engine=args.engine, headed=args.headed, proxy_ca=endpoint.ca_file
        ) as context:
            page = await context.new_page()
            results["browser_ip"] = await pilot.browser_exit_ip(page)
            await page.close()
            await asyncio.gather(
                *(
                    tab_worker(i, context, queue, args, audio, state, output, counters, endpoint, results, http)
                    for i in range(args.tabs)
                )
            )
    except Exception as exc:  # noqa: BLE001
        results["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        http.close()
        results["counters"] = dict(counters)
        results["elapsed_seconds"] = round(time.monotonic() - started, 2)
        results["tokens_accepted"] = len(results["tokens"])
        results["ok"] = sum(1 for d in results["documents"] if d["status"] in {"downloaded", "reused"})
        results["docs_per_minute"] = round(results["ok"] * 60 / max(results["elapsed_seconds"], 1), 2)
        (output / "result.json").write_text(json.dumps(results, ensure_ascii=False, indent=1), encoding="utf-8")
        pilot.say(json.dumps({k: v for k, v in results.items() if k != "documents"}, ensure_ascii=False, indent=1)[:3000])
    return 0


def main(argv=None):
    load_dotenv(".env")
    load_dotenv(".env.res")
    os.environ.setdefault("PROXY_REPUTATION_MODE", "blocklist_only")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--actions", type=Path, nargs="+", required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--tabs", type=int, default=2)
    parser.add_argument("--interval", type=float, default=0.0)
    parser.add_argument("--output", type=Path, default=Path("data/res-verify-probe"))
    parser.add_argument("--proxy-provider", choices=("webshare", "brightdata", "direct"), default="webshare")
    parser.add_argument("--proxy-limit", type=int, default=1)
    parser.add_argument("--proxy-index", type=int, default=0)
    parser.add_argument("--proxy-file", type=Path, default=Path("data/residential-proxies.txt"))
    parser.add_argument("--engine", choices=("patchright", "camoufox"), default="patchright")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--captcha-timeout", type=float, default=90.0)
    parser.add_argument("--audio-seconds", type=float, default=90.0)
    parser.add_argument("--audio-model", default="base")
    parser.add_argument("--audio-device", default="cpu")
    parser.add_argument("--audio-compute", default="int8")
    parser.add_argument("--audio-rounds", type=int, default=3)
    parser.add_argument("--native-wait", type=float, default=6.0)
    parser.add_argument("--operator-wait", type=float, default=0.0)
    parser.add_argument("--solves-per-proxy", type=int, default=0)
    parser.add_argument("--mint-attempts", type=int, default=1)
    parser.add_argument("--delay", type=float, default=0.0)
    parser.add_argument("--lazy-proxies", action="store_true")
    parser.add_argument("--expected-proxy-ip")
    args = parser.parse_args(argv)
    args.vlm_model = ""
    args.vlm_url = "http://127.0.0.1:11434"
    args.vlm_rounds = 0
    return asyncio.run(probe(args))


if __name__ == "__main__":
    raise SystemExit(main())
