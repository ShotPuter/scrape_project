"""Bounded local test: one to four Camoufox browsers on Webshare proxies."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit

from dotenv import load_dotenv

from scripts.pep_camoufox_parallel import WebshareProxyPool, run_parallel
from scripts.pep_camoufox_pilot import CA_BUNDLE, DEFAULT_SAMPLE, ROOT
from src.proxy.sources.webshare import load_webshare_candidates


async def execute(args):
    load_dotenv(ROOT / ".env")
    candidates = await load_webshare_candidates(os.getenv("WEBSHARE_API_TOKEN", ""), mode="direct")
    proxies, hosts = [], set()
    for candidate in candidates:
        parsed = urlsplit(candidate.url)
        if parsed.hostname in hosts:
            continue
        hosts.add(parsed.hostname)
        proxies.append(
            {
                "server": f"http://{parsed.hostname}:{parsed.port}",
                "username": unquote(parsed.username or ""),
                "password": unquote(parsed.password or ""),
            }
        )
    if len(proxies) < args.workers:
        raise ValueError(f"Webshare must supply {args.workers} valid proxies with distinct addresses")
    sample = json.loads(args.sample.read_text())[: args.links]
    if len(sample) < args.workers:
        raise ValueError("The sample must contain at least one link per browser")
    output = ROOT / "data" / ("pep-webshare-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ"))
    print(
        json.dumps(
            {
                "inventory_count": len(candidates),
                "browsers": args.workers,
                "tabs": args.tabs,
                "links": len(sample),
                "output": str(output),
            }
        ),
        flush=True,
    )

    async def notify(report, *, final):
        # This local test writes reports and logs without sending Telegram messages.
        if final:
            print(
                json.dumps(
                    {
                        key: report[key]
                        for key in ("status", "seconds", "job_counts", "pdf_count", "proxy_egress_ips", "stop_reason")
                    }
                ),
                flush=True,
            )

    return await run_parallel(
        sample,
        output,
        workers=args.workers,
        tabs=args.tabs,
        tab_restarts=args.tab_restarts,
        browser_max_seconds=args.browser_max_seconds,
        browser_max_links=args.browser_max_links,
        request_seconds=60,
        ca_bundle=CA_BUNDLE,
        proxy_ca=None,
        max_seconds=args.max_seconds,
        proxy=WebshareProxyPool(proxies, args.workers),
        notify=notify,
        log_interval=30,
        sqlite_state=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=Path, default=DEFAULT_SAMPLE)
    parser.add_argument("--links", type=int, default=2)
    parser.add_argument("--workers", type=int, default=2, choices=range(1, 5))
    parser.add_argument("--tabs", type=int, default=1, choices=range(1, 4))
    parser.add_argument("--tab-restarts", type=int, default=0, choices=range(0, 4))
    parser.add_argument("--max-seconds", type=float, default=480)
    parser.add_argument("--browser-max-seconds", type=float, default=1200)
    parser.add_argument("--browser-max-links", type=int, default=40)
    args = parser.parse_args()
    if args.links < args.workers or not math.isfinite(args.max_seconds) or args.max_seconds <= 0:
        parser.error("Use at least one link per browser and a finite positive runtime limit")
    report = asyncio.run(execute(args))
    return 0 if report["status"] == "finished" and all(job["status"] == "completed" for job in report["jobs"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
