"""Decoupled token-multiplexed document verifier for Chilean RES publications.

Mints a single reCAPTCHA token via headless browser/audio Whisper, then verifies
a burst of 50-70 CVEs over plain HTTP POST before token expiry.
Exits cleanly when the per-IP quota (100 docs/day) is reached.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import httpx

from src.proxy.reputation import screen_proxy
from src.sites.dequienes.source import RES_URL
from src.sites.res import http_flow
from src.sites.res.audio import load_local_audio_solver
from src.sites.res.engines import launch_context
from src.sites.res.source import SearchIssue
from src.sites.res.tracking import TrackingStore

DEFAULT_DB = Path("data/res-bulk/tracking.sqlite3")
DEFAULT_RUN = "res-bulk-discover"


async def token_asset_route(route):
    """Abort unnecessary heavy static assets to make widget load instantly."""
    from urllib.parse import urlsplit

    request = route.request
    if urlsplit(request.url).hostname == "www.registrodeempresasysociedades.cl" and request.resource_type in {
        "stylesheet",
        "image",
        "font",
        "media",
    }:
        await route.abort()
    else:
        await route.continue_()


async def mint_recaptcha_token(page, solver, *, timeout_seconds: int = 60, form_url=RES_URL) -> str:
    """Navigate to the certificate verification page, solve reCAPTCHA, and extract token."""
    await page.route("**/*", token_asset_route)
    await page.goto(form_url, wait_until="commit", timeout=timeout_seconds * 1000)
    anchor = page.frame_locator('iframe[title="reCAPTCHA"]').locator("#recaptcha-anchor")
    await anchor.wait_for(state="visible", timeout=timeout_seconds * 1000)
    await solver(page, timeout_ms=timeout_seconds * 1000, captcha_seconds=timeout_seconds)

    # Extract token from the page
    for _ in range(20):
        token = ""
        try:
            token = await page.locator("#g-recaptcha-response").first.input_value()
        except Exception:
            token = ""
        if not token:
            try:
                token = await page.evaluate(
                    "() => { try { return window.grecaptcha.getResponse(); } catch (e) { return ''; } }"
                )
            except Exception:
                token = ""
        token = (token or "").strip()
        if len(token) >= 40:
            return token
        await asyncio.sleep(0.5)
    raise SearchIssue("captcha_required", "reCAPTCHA token was not exposed after solving")


def fetch_pending_cves(store: TrackingStore, limit: int | None = None) -> list[str]:
    """Retrieve CVEs needing verification from the database."""
    query = """
        SELECT cve FROM documents
        WHERE (status = 'pending' OR status IS NULL)
          AND (download_url IS NULL OR download_url = '')
        ORDER BY updated_at ASC
    """
    if limit is not None:
        query += f" LIMIT {int(limit)}"
    rows = store.db.execute(query).fetchall()
    return [row["cve"] for row in rows]


async def burst_verify(
    store: TrackingStore,
    session: httpx.Client,
    token_minter: Any,
    *,
    limit: int | None = 100,
    max_tokens_per_session: int | None = 4,
) -> dict[str, Any]:
    """Verify up to limit documents using token multiplexing.

    Args:
        store: SQLite tracking store.
        session: Pre-warmed HTTP client session for POST requests.
        token_minter: Async callable returning a freshly minted reCAPTCHA token.
        limit: Max documents to verify (e.g. 100 per IP daily quota).
        max_tokens_per_session: Max tokens to mint before stopping.

    Returns:
        Summary dict of verification performance and outcomes.
    """
    pending = fetch_pending_cves(store, limit=limit)
    if not pending:
        return {
            "status": "completed",
            "verified": 0,
            "failed": 0,
            "tokens_minted": 0,
            "elapsed_seconds": 0.0,
            "docs_per_minute": 0.0,
            "stop_reason": "no_pending_cves",
        }

    counts: Counter = Counter()
    started = time.monotonic()
    current_token: str | None = None
    stop_reason: str | None = None

    for cve in pending:
        if limit is not None and counts["verified"] >= limit:
            stop_reason = "limit_reached"
            break

        for attempt in range(2):
            if current_token is None:
                if max_tokens_per_session is not None and counts["tokens"] >= max_tokens_per_session:
                    stop_reason = "max_tokens_reached"
                    break
                counts["tokens"] += 1
                token_started = time.monotonic()
                try:
                    current_token = await token_minter()
                except SearchIssue as exc:
                    stop_reason = exc.status
                    # Leave the CVE pending so a later automatic run can resume.
                    break
                counts["token_mint_seconds"] += time.monotonic() - token_started

            try:
                # PageMethod POST verify
                cve_out, codigo, link = await asyncio.to_thread(http_flow.verify, session, cve, current_token)
                store.document_status(cve_out, "verified", record={"codigo": codigo, "link": link})
                counts["verified"] += 1
                break
            except SearchIssue as exc:
                if exc.status in {"daily_limit", "proxy_failed", "blocked"}:
                    stop_reason = exc.status
                    store.document_status(cve, "pending", error=str(exc))
                    break
                if exc.status == "captcha_required" and attempt == 0:
                    # Token expired: invalidate token and retry once
                    current_token = None
                    counts["token_expirations"] += 1
                    continue
                if exc.status == "captcha_required":
                    current_token = None
                    stop_reason = exc.status
                    store.document_status(cve, "pending", error=str(exc))
                    break
                if exc.status == "retry_wait":
                    stop_reason = exc.status
                    store.document_status(cve, "pending", error=str(exc))
                    break
                store.document_status(cve, "verify_error", error=str(exc))
                counts["failed"] += 1
                break
            except Exception as exc:
                store.document_status(cve, "verify_error", error=f"{type(exc).__name__}: {exc}")
                counts["failed"] += 1
                break

        if stop_reason is not None:
            break

    elapsed = max(0.001, time.monotonic() - started)
    return {
        "status": stop_reason or "ok",
        "verified": counts["verified"],
        "failed": counts["failed"],
        "tokens_minted": counts["tokens"],
        "token_expirations": counts["token_expirations"],
        "elapsed_seconds": round(elapsed, 2),
        "docs_per_minute": round(counts["verified"] * 60.0 / elapsed, 2),
        "stop_reason": stop_reason,
    }


async def run(args: argparse.Namespace) -> int:
    """Main CLI entrypoint for burst verifier."""
    proxy = await screen_proxy(args.proxy)
    store = TrackingStore(args.db, args.run_id)
    session = http_flow.new_session(timeout=args.timeout, proxy=proxy)
    try:
        print(f"Loading Whisper model '{args.audio_model}'...", file=sys.stderr)
        try:
            solver = await asyncio.to_thread(load_local_audio_solver, args.audio_model)
        except SearchIssue as exc:
            print(str(exc), file=sys.stderr)
            return 2
        await asyncio.to_thread(http_flow.prepare, session)

        from scripts.res_free_pilot import endpoint_from_url

        browser_proxy = endpoint_from_url(proxy).browser if proxy else None
        async with launch_context(args.profile, proxy=browser_proxy, engine=args.engine, headed=False) as context:
            page = await context.new_page()

            async def minter() -> str:
                return await mint_recaptcha_token(page, solver, timeout_seconds=args.captcha_timeout)

            summary = await burst_verify(store, session, minter, limit=args.limit, max_tokens_per_session=args.max_tokens)
    finally:
        session.close()
        store.close()

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0 if summary["status"] in {"ok", "completed"} else 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--run-id", default=DEFAULT_RUN)
    parser.add_argument("--limit", type=int, default=100, help="Max CVEs to verify in this burst (default: 100)")
    parser.add_argument("--max-tokens", type=int, default=4, help="Max reCAPTCHA tokens to mint (default: 4)")
    parser.add_argument("--proxy", help="Optional clean/residential proxy URL")
    parser.add_argument("--audio-model", default="base", help="Whisper model name (default: base)")
    parser.add_argument("--engine", choices=("patchright", "camoufox"), default="patchright")
    parser.add_argument("--profile", type=Path, default=Path("data/res-bulk/token-profile"))
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout in seconds")
    parser.add_argument("--captcha-timeout", type=int, default=130, help="Seconds for automatic CAPTCHA solver")

    args = parser.parse_args(argv)
    if min(args.limit, args.max_tokens, args.timeout, args.captcha_timeout) <= 0:
        parser.error("Limits and timeouts must be positive")
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
