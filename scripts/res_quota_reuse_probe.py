"""Quota-scope probe: reuse one reCAPTCHA token, exhaust the daily cap, then test a fresh session.

Answers two questions at once:
  * how many documents one token can verify (token reuse),
  * whether the daily document cap follows the IP or the cookie session.
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
from src.sites.dequienes.source import RES_URL
from src.sites.res import http_flow
from src.sites.res.engines import launch_context


def load_cves(paths, limit):
    out, seen = [], set()
    for path in paths:
        for row in json.loads(Path(path).read_text(encoding="utf-8")):
            cve = (row.get("cve") or "").strip()
            if cve and cve not in seen:
                seen.add(cve)
                out.append(cve)
    return out[:limit] if limit else out


def make_args():
    return argparse.Namespace(
        audio_model="base",
        audio_device="cpu",
        audio_compute="int8",
        audio_rounds=3,
        timeout=60,
        captcha_timeout=90,
        audio_seconds=90,
        native_wait=6,
        operator_wait=0,
        solves_per_proxy=0,
        mint_attempts=1,
        delay=0,
        vlm_model="",
        vlm_url="",
        vlm_rounds=0,
    )


async def mint(context, endpoint, audio, args):
    page = await context.new_page()
    cascade = pilot.Cascade(page, args, None, audio, Counter(), [], endpoint.label)
    token = await cascade.mint(RES_URL)
    await page.close()
    return token


async def probe(args):
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    cves = load_cves(args.actions, args.limit)
    endpoints_args = argparse.Namespace(
        proxy_provider=args.proxy_provider,
        proxy_limit=args.proxy_index + 1,
        proxy_index=args.proxy_index,
        lazy_proxies=False,
        proxy_file=None,
    )
    endpoints = await pilot.load_proxy_endpoints(endpoints_args)
    endpoint = endpoints[args.proxy_index]
    audio, _ = pilot.load_audio(make_args())
    proxy_args = make_args()
    result = {"proxy": endpoint.label, "documents": [], "limit_at": None, "fresh_session": None}
    started = time.monotonic()
    session = http_flow.new_session(timeout=args.timeout, proxy=endpoint.url)
    counts = Counter()

    async def run_batch(context, session, cves, tag):
        token = await mint(context, endpoint, audio, proxy_args)
        print(f"[{tag}] token minted, {len(token)} chars", flush=True)
        refreshed = 0
        for cve in cves:
            for attempt in range(2):
                try:
                    _, codigo, link = http_flow.verify(session, cve, token)
                    data = http_flow.download(session, link)
                    counts["downloaded"] += 1
                    result["documents"].append({"cve": cve, "tag": tag, "status": "downloaded", "bytes": len(data)})
                    print(f"[{tag}] {cve} ok {len(data)} docs={counts['downloaded']}", flush=True)
                    break
                except Exception as exc:  # noqa: BLE001
                    status = getattr(exc, "status", "error")
                    message = str(exc)
                    if (
                        status == "captcha_required"
                        and "captcha" in message.lower()
                        and attempt == 0
                        and refreshed < args.max_tokens
                    ):
                        refreshed += 1
                        token = await mint(context, endpoint, audio, proxy_args)
                        print(f"[{tag}] token refreshed ({refreshed}), retrying {cve}", flush=True)
                        continue
                    result["documents"].append({"cve": cve, "tag": tag, "status": status, "error": message[:200]})
                    print(f"[{tag}] {cve} {status}: {message[:120]}", flush=True)
                    if status in {"daily_limit", "blocked", "proxy_failed"}:
                        return status
                    break
        return None

    try:
        result["http_ip"] = pilot.http_exit_ip(session)
        http_flow.prepare(session)
        async with launch_context(output / "profile", proxy=endpoint.browser, engine=args.engine) as context:
            reason = await run_batch(context, session, cves, "initial")
            result["limit_at"] = counts["downloaded"]
            result["limit_reason"] = reason
            if reason == "daily_limit":
                # Fresh cookie jar and a fresh browser context, same IP.
                session.close()
                session = http_flow.new_session(timeout=args.timeout, proxy=endpoint.url)
                http_flow.prepare(session)
                fresh = cves[counts["downloaded"] : counts["downloaded"] + 3]
                stage = {"ip": pilot.http_exit_ip(session)}
                status = await run_batch(context, session, fresh, "fresh-session")
                stage["status"] = status
                stage["downloaded"] = sum(
                    1 for d in result["documents"] if d["tag"] == "fresh-session" and d["status"] == "downloaded"
                )
                result["fresh_session"] = stage
    except Exception as exc:  # noqa: BLE001
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        session.close()
        result["downloaded"] = counts["downloaded"]
        result["elapsed_seconds"] = round(time.monotonic() - started, 2)
        result["docs_per_minute"] = round(counts["downloaded"] * 60 / max(result["elapsed_seconds"], 1), 2)
        (output / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
        print(
            json.dumps({k: v for k, v in result.items() if k != "documents"}, ensure_ascii=False, indent=1)[:3000],
            flush=True,
        )
    return 0


def main(argv=None):
    load_dotenv(".env")
    load_dotenv(".env.res")
    os.environ.setdefault("PROXY_REPUTATION_MODE", "blocklist_only")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--actions", type=Path, nargs="+", required=True)
    parser.add_argument("--limit", type=int, default=105)
    parser.add_argument("--max-tokens", type=int, default=20)
    parser.add_argument("--proxy-provider", choices=("webshare", "brightdata", "direct"), default="webshare")
    parser.add_argument("--proxy-index", type=int, default=0)
    parser.add_argument("--engine", choices=("patchright", "camoufox"), default="patchright")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--output", type=Path, default=Path("data/res-quota-reuse-probe"))
    args = parser.parse_args(argv)
    return asyncio.run(probe(args))


if __name__ == "__main__":
    raise SystemExit(main())
