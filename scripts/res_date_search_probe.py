"""Live probe: does RES accept a date-based search, and what does details return?

The browser is used only to mint a single reCAPTCHA token.  The search and the
details call are plain HTTP PageMethods, exactly like the production flow.
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
from src.sites.res.source import FORM_URL, SEARCH_PATH, search_link


async def probe(args):
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    args.proxy_limit = max(args.proxy_limit, args.proxy_index + 1)
    endpoints = await pilot.load_proxy_endpoints(args)
    endpoint = endpoints[args.proxy_index]
    pilot.say(f"proxy {endpoint.label} expected_ip={endpoint.expected_ip}")

    counters = Counter(tokens=0)
    records: list = []
    audio, audio_error = pilot.load_audio(args)
    if audio_error:
        pilot.say(audio_error)
    result = {"date": args.date, "tipo": args.tipo, "proxy": endpoint.label, "expected_ip": endpoint.expected_ip}
    started = time.monotonic()
    session = http_flow.new_session(timeout=args.timeout, proxy=endpoint.url)
    try:
        result["http_ip"] = pilot.http_exit_ip(session)
        http_flow.prepare(session)
        profile = output / "profile"
        async with launch_context(
            profile, proxy=endpoint.browser, engine=args.engine, headed=args.headed, proxy_ca=endpoint.ca_file
        ) as context:
            page = await context.new_page()
            result["browser_ip"] = await pilot.browser_exit_ip(page)
            cascade = pilot.Cascade(page, args, None, audio, counters, records, endpoint.label)
            token = await cascade.mint(FORM_URL)
            result["token_seconds"] = round(time.monotonic() - started, 2)
            result["token_records"] = records
            payload = http_flow._post(
                session,
                SEARCH_PATH,
                {"tipoRegistro": "1", "tipoBusqueda": str(args.tipo), "texto": args.date, "Response": token},
            )
            (output / "search-payload.json").write_text(json.dumps(payload)[:200_000], encoding="utf-8")
            link = search_link(payload)
            result["link"] = link
            actions = http_flow.details(session, link)
            result["actions"] = len(actions)
            result["companies"] = len({a["rut"] for a in actions})
            result["cves"] = sum(1 for a in actions if a["cve"])
            result["sample"] = actions[:3]
            (output / "actions.json").write_text(json.dumps(actions, ensure_ascii=False, indent=1), encoding="utf-8")
            result["status"] = "ok"
    except Exception as exc:  # noqa: BLE001 - probe must always dump what happened
        result["status"] = "error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["error_status"] = getattr(exc, "status", None)
    finally:
        result["elapsed_seconds"] = round(time.monotonic() - started, 2)
        session.close()
        (output / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
        pilot.say(json.dumps(result, ensure_ascii=False, indent=1)[:4000])
    return 0 if result.get("status") == "ok" else 1


def main(argv=None):
    load_dotenv(".env")
    load_dotenv(".env.res")
    os.environ.setdefault("PROXY_REPUTATION_MODE", "blocklist_only")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--tipo", choices=("3", "4", "6"), default="4", help="4 fecha actuacion, 6 fecha registro")
    parser.add_argument("--output", type=Path, default=Path("data/res-date-search-probe"))
    parser.add_argument("--proxy-provider", choices=("webshare", "brightdata", "direct"), default="webshare")
    parser.add_argument("--proxy-limit", type=int, default=1)
    parser.add_argument("--proxy-index", type=int, default=0)
    parser.add_argument("--proxy-file", type=Path, default=Path("data/residential-proxies.txt"))
    parser.add_argument("--engine", choices=("patchright", "camoufox"), default="patchright")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--captcha-timeout", type=float, default=90.0)
    parser.add_argument("--audio-seconds", type=float, default=60.0)
    parser.add_argument("--audio-model", default="base")
    parser.add_argument("--audio-device", default="cpu")
    parser.add_argument("--audio-compute", default="int8")
    parser.add_argument("--audio-rounds", type=int, default=3)
    parser.add_argument("--native-wait", type=float, default=6.0)
    parser.add_argument("--operator-wait", type=float, default=0.0)
    parser.add_argument("--solves-per-proxy", type=int, default=0)
    parser.add_argument("--mint-attempts", type=int, default=1)
    parser.add_argument("--delay", type=float, default=2.0)
    parser.add_argument("--lazy-proxies", action="store_true")
    parser.add_argument("--expected-proxy-ip")
    args = parser.parse_args(argv)
    args.vlm_model = ""
    args.vlm_url = "http://127.0.0.1:11434"
    args.vlm_rounds = 0
    return asyncio.run(probe(args))


if __name__ == "__main__":
    raise SystemExit(main())
