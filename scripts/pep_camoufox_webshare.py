"""Small local test: two independent Camoufox browsers on Webshare proxies."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit

from dotenv import load_dotenv

from scripts.pep_camoufox_parallel import FixedProxyPool, run_parallel
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
        if len(proxies) == 2:
            break
    if len(proxies) != 2:
        raise ValueError("Webshare must supply at least two valid proxies with distinct addresses")
    sample = json.loads(args.sample.read_text())[: args.links]
    if len(sample) < 2:
        raise ValueError("The sample must contain at least two links")
    output = ROOT / "data" / ("pep-webshare-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ"))
    print(
        json.dumps({"inventory_count": len(candidates), "browsers": 2, "links": len(sample), "output": str(output)}),
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

    await run_parallel(
        sample,
        output,
        workers=2,
        request_seconds=60,
        ca_bundle=CA_BUNDLE,
        proxy_ca=None,
        max_seconds=args.max_seconds,
        proxy=FixedProxyPool(proxies),
        notify=notify,
        log_interval=30,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=Path, default=DEFAULT_SAMPLE)
    parser.add_argument("--links", type=int, default=2)
    parser.add_argument("--max-seconds", type=float, default=480)
    args = parser.parse_args()
    if args.links < 2 or args.max_seconds <= 0:
        parser.error("Use at least two links and a positive runtime limit")
    asyncio.run(execute(args))


if __name__ == "__main__":
    main()
