"""Bounded live check: automatic audio token, burst verification, saved PDF audit."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
from dotenv import load_dotenv

from scripts import res_free_pilot as pilot
from src.sites.res import http_flow
from src.sites.res.audio import load_local_audio_solver
from src.sites.res.burst_verifier import burst_verify, mint_recaptcha_token
from src.sites.res.engines import launch_context
from src.sites.res.ready_download import download_ready
from src.sites.res.tracking import TrackingStore
from src.utils.atomic import atomic_write_json
from src.utils.error_details import safe_message


async def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    report = {"started_at": datetime.now(UTC).isoformat(), "operator": False, "target": args.limit, "tokens": []}
    store = TrackingStore(args.output / "tracking.sqlite3", "live-burst-check")
    started = time.monotonic()
    try:
        rows = json.loads(args.actions.read_text(encoding="utf-8"))
        actions = list({row["cve"]: row for row in rows if row.get("cve")}.values())[: args.limit]
        if len(actions) < args.limit:
            raise ValueError("Not enough unique CVEs in the action list")
        store.register_run({"mode": "live_burst_check", "operator": False}, [])
        store.day_result("live-input", "action", actions, complete=True)
        options = argparse.Namespace(
            proxy_provider=args.proxy_provider, proxy_limit=args.proxy_index + 1, lazy_proxies=False, proxy_file=None
        )
        endpoint = (await pilot.load_proxy_endpoints(options))[args.proxy_index]
        report["proxy"] = endpoint.label
        print("Loading local Whisper base", flush=True)
        solver = await asyncio.to_thread(load_local_audio_solver)
        with http_flow.new_session(timeout=30, proxy=endpoint.url) as session:
            await asyncio.to_thread(http_flow.prepare, session)
            async with launch_context(
                args.output / "profile", proxy=endpoint.browser, engine="patchright", headed=False
            ) as context:
                page = await context.new_page()

                async def minter():
                    token = await mint_recaptcha_token(page, solver, timeout_seconds=130)
                    report["tokens"].append({"sha256": hashlib.sha256(token.encode()).hexdigest(), "length": len(token)})
                    print(f"Automatic token {len(report['tokens'])} obtained", flush=True)
                    return token

                report["verification"] = await burst_verify(
                    store, session, minter, limit=args.limit, max_tokens_per_session=2
                )
        report["audio_events"] = solver.events
        print(json.dumps(report["verification"], ensure_ascii=False), flush=True)
        transport = httpx.AsyncHTTPTransport(proxy=endpoint.url, trust_env=False, retries=1)
        async with httpx.AsyncClient(transport=transport, timeout=30, trust_env=False) as client:
            report["downloads"] = await download_ready(store, client, limit=args.limit, workers=4, interval=0.1)
        files = []
        for row in store.db.execute("SELECT * FROM document_files ORDER BY cve"):
            path = store.base / row["local_path"]
            data = path.read_bytes()
            parsed = await asyncio.to_thread(
                subprocess.run, ["pdfinfo", str(path)], capture_output=True, text=True, timeout=20
            )
            valid = (
                data.startswith(b"%PDF-") and hashlib.sha256(data).hexdigest() == row["sha256"] and parsed.returncode == 0
            )
            files.append({"cve": row["cve"], "path": str(path), "bytes": len(data), "sha256": row["sha256"], "valid": valid})
        report["files"] = files
        report["valid_pdfs"] = sum(item["valid"] for item in files)
        report["passed"] = report["valid_pdfs"] >= args.limit and len(report["tokens"]) < args.limit
    except Exception as exc:
        report["error"] = safe_message(f"{type(exc).__name__}: {exc}")
        report["passed"] = False
    finally:
        store.close()
        report["elapsed_seconds"] = round(time.monotonic() - started, 2)
        atomic_write_json(args.output / "report.json", report)
        print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0 if report["passed"] else 2


def main():
    load_dotenv(".env")
    load_dotenv(".env.res")
    os.environ.setdefault("PROXY_REPUTATION_MODE", "blocklist_only")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--actions", type=Path, default=Path("data/res-date-search-probe-max-20261118/actions.json"))
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--proxy-provider", choices=("webshare", "direct"), default="webshare")
    parser.add_argument("--proxy-index", type=int, default=0)
    parser.add_argument(
        "--output", type=Path, default=Path("data") / datetime.now(UTC).strftime("res-burst-live-%Y%m%dT%H%M%SZ")
    )
    args = parser.parse_args()
    if args.limit < 1 or args.proxy_index < 0:
        parser.error("A positive limit and nonnegative proxy index are required")
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
