#!/usr/bin/env python3
"""
Standalone CLI utility for discovering, validating, and maintaining free proxies
specifically tested against the SEPREC Bolivian Gazette API.

Usage:
    python proxy_tool.py update          # Fetch fresh proxies and verify against SEPREC
    python proxy_tool.py check           # Verify existing proxies in data/working_proxies.txt
    python proxy_tool.py status          # View status of verified proxies
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from src.proxy.checker import ProxyChecker
from src.proxy.collector import ProxyCollector
from src.proxy.rotator import ProxyRotator

WORKING_PROXIES_FILE = Path("data/working_proxies.txt")
FALLBACK_CSV_FILE = Path("free-proxy-list.csv")


async def update_proxies(output_file: Path, limit_sources: int) -> int:
    """Fetch new free proxies and verify them against SEPREC."""
    print("[*] Collecting free proxy candidates from public sources and local files...")

    candidates = await asyncio.to_thread(ProxyCollector.from_file, FALLBACK_CSV_FILE)
    candidates.extend(await asyncio.to_thread(ProxyCollector.from_file, output_file))

    public_candidates = await ProxyCollector.from_public_sources(limit_per_source=limit_sources)
    candidates.extend(public_candidates)

    candidates = list(dict.fromkeys(candidates))
    print(f"[*] Found {len(candidates)} total candidates. Verifying against SEPREC API...")

    checker = ProxyChecker(timeout_seconds=6.0, concurrency=25)
    working = await checker.find_working(candidates)

    print(f"[+] Verified {len(working)} working proxies for SEPREC API:")
    for verified in working[:15]:
        print(f"    - {verified.url:<35} | {verified.latency_ms:>5.1f} ms")

    if len(working) > 15:
        print(f"    ... and {len(working) - 15} more.")

    rotator = ProxyRotator([v.url for v in working])
    await asyncio.to_thread(rotator.save_to_file, output_file)
    print(f"[✓] Saved {len(working)} verified proxies to: {output_file}")
    return 0 if working else 1


async def check_existing_proxies(file_path: Path) -> int:
    """Check currently saved proxies and remove unresponsive ones."""
    if not await asyncio.to_thread(file_path.exists):
        print(f"[-] No existing proxy file found at: {file_path}")
        print("    Run 'python proxy_tool.py update' to fetch new proxies.")
        return 1

    candidates = await asyncio.to_thread(ProxyCollector.from_file, file_path)
    print(f"[*] Checking {len(candidates)} proxies from {file_path}...")

    checker = ProxyChecker(timeout_seconds=6.0, concurrency=20)
    working = await checker.find_working(candidates)

    print(f"[+] {len(working)}/{len(candidates)} proxies are still alive.")
    rotator = ProxyRotator([v.url for v in working])
    await asyncio.to_thread(rotator.save_to_file, file_path)
    print(f"[✓] Updated {file_path}")
    return 0 if working else 1


def show_status(file_path: Path) -> int:
    """Show summary of working proxies."""
    if not file_path.exists():
        print(f"[-] Proxy file does not exist: {file_path}")
        return 1

    rotator = ProxyRotator()
    count = rotator.load_from_file(file_path)
    print(f"[✓] {file_path} contains {count} ready-to-use proxies.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="SEPREC Free Proxy Management Tool")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    update_cmd = subparsers.add_parser("update", help="Fetch & verify fresh free proxies")
    update_cmd.add_argument("--file", type=Path, default=WORKING_PROXIES_FILE)
    update_cmd.add_argument("--limit", type=int, default=150)

    check_cmd = subparsers.add_parser("check", help="Verify existing working proxies")
    check_cmd.add_argument("--file", type=Path, default=WORKING_PROXIES_FILE)

    status_cmd = subparsers.add_parser("status", help="Show working proxy count")
    status_cmd.add_argument("--file", type=Path, default=WORKING_PROXIES_FILE)

    args = parser.parse_args()

    if args.command == "update":
        return asyncio.run(update_proxies(args.file, args.limit))
    if args.command == "check":
        return asyncio.run(check_existing_proxies(args.file))
    if args.command == "status":
        return show_status(args.file)
    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
