"""One proxy/browser session consumes the durable company queue until route failure."""

from __future__ import annotations

import asyncio
from collections import deque

from scripts import res_free_pilot as pilot
from src.proxy.reputation import check_ip, check_proxy
from src.sites.res import cooldown
from src.sites.res.fleet_quota import FleetQuota
from src.sites.res.free_queue import FreeQueue
from src.utils.atomic import atomic_write_json
from src.utils.error_details import safe_message

TERMINAL = {"downloaded", "not_found", "no_cve_found"}
NETWORK_ERRORS = (
    "ConnectTimeout",
    "ReadTimeout",
    "WriteTimeout",
    "ConnectError",
    "ReadError",
    "WriteError",
    "RemoteProtocolError",
    "ConnectionError",
    "TimeoutError",
)


def network_failure(result: dict) -> bool:
    errors = [result, *result.get("documents", [])]
    forbidden = {"daily_limit", "captcha_required", "blocked", "proxy_ip_mismatch", "proxy_reputation_denied"}
    if any(row.get("status") in forbidden for row in errors):
        return False
    text = " ".join(str(row.get("error", "")) for row in errors)
    return not any(value in text.lower() for value in ("certificate", "ssl", "reputation")) and any(
        name in text for name in NETWORK_ERRORS
    )


async def check_route(http, expected_ip, proxy_url):
    """Never carry a live browser onto a changed or newly disallowed exit."""
    await asyncio.to_thread(check_proxy, proxy_url)
    observed = await asyncio.to_thread(pilot.http_exit_ip, http)
    if observed != expected_ip:
        raise pilot.SearchIssue("proxy_ip_mismatch", f"Proxy exit changed: expected {expected_ip}, got {observed}")
    await asyncio.to_thread(check_ip, observed)


def checkpoint(args, report, counters, outcomes):
    report["companies"] = list(outcomes.values())
    report["progress"] = {
        "completed_companies": len(outcomes),
        "tokens": counters["tokens"],
        "downloaded": counters["downloaded"],
        "reused": counters["reused"],
    }
    atomic_write_json(args.output / f"report-{report['run_id']}.json", report)


async def consume_session(cascade, http, initial, queue, args, state, counters, report, stats):
    outcomes = {}
    company = initial
    while company:
        if cooldown.must_yield(args, {}):
            report["stop_reason"] = {"code": "shared_cooldown", "error": "Shared collection pause requested"}
            break
        args.tracking_store.restore_worker(args.output, [company])
        state.clear()
        state.update(pilot.load_state(args.state_path))
        attempt_start = len(report.get("attempts", []))
        result = None
        for retry in range(args.stream_network_retries + 1):
            report.pop("stop_reason", None)
            checkpoint(args, report, counters, outcomes)
            try:
                await check_route(http, stats["http_ip"], getattr(args, "stream_proxy_url", None))
                await pilot.drain(
                    cascade,
                    http,
                    deque([(company, 0)]),
                    deque(),
                    state,
                    args,
                    counters,
                    report,
                    stats,
                    outcomes,
                    {"stop": False, "failures": 0},
                )
                attempts = report.get("attempts", [])[attempt_start:]
                result = attempts[-1]["result"] if attempts else outcomes.get(company["company_id"])
                if result is None:
                    result = {**company, "status": "failed", "error": "Company produced no result", "documents": []}
            except asyncio.CancelledError:
                raise
            except Exception as error:
                result = pilot.failure_result(company, error, cascade)
                if not isinstance(error, pilot.SearchIssue):
                    result["error"] = f"{type(error).__name__}: {safe_message(error)}"
            if not network_failure(result) or retry == args.stream_network_retries:
                break
            counters["same_session_network_retries"] += 1
            report["same_session_network_retries"] = counters["same_session_network_retries"]
            report["last_network_error"] = result.get("error")
            pilot.say(
                f"  transient network failure: {safe_message(result.get('error'))}; "
                f"retry {retry + 1}/{args.stream_network_retries}, keeping browser"
            )
            checkpoint(args, report, counters, outcomes)
            # Never replay a token-bearing POST: process_company mints a new token
            # or resumes a saved codigo/PDF on each fresh attempt.
            await asyncio.sleep(args.stream_retry_delay)
        result = result or {**company, "status": "failed", "error": "No result", "documents": []}
        outcomes[company["company_id"]] = result
        successful = result["status"] in TERMINAL
        attempted = len(report.get("attempts", [])) > attempt_start
        queue.finish_job(
            company["company_id"],
            args.stream_owner,
            complete=successful,
            attempted=attempted and result["status"] not in {"daily_limit", "fleet_route_limit", "fleet_provider_limit"},
            limit=1 if result["status"] == "ambiguous_match" else args.stream_company_attempts,
            retry=30,
            error=result.get("error"),
        )
        stats["companies"] = len(outcomes)
        checkpoint(args, report, counters, outcomes)
        if not successful and result["status"] != "ambiguous_match":
            reason = (
                result.get("error")
                or "; ".join(str(row.get("error") or row.get("status")) for row in result.get("documents", []))
                or result["status"]
            )
            code = (
                "daily_document_limit"
                if result["status"] == "daily_limit"
                else ("network_retries_exhausted" if network_failure(result) else result["status"])
            )
            report["stop_reason"] = {"code": code, "error": safe_message(reason), "company_id": company["company_id"]}
            stats.update(status=result["status"], error=safe_message(reason))
            break
        report.pop("stop_reason", None)
        stats["status"] = "ready"
        pilot.save_state(args.state_path, state)
        company = queue.claim_company(args.stream_owner)
        while company is None:
            pending = queue.database.execute(
                "SELECT COUNT(*) FROM free_jobs WHERE run_id=? AND state='pending'", (queue.run_id,)
            ).fetchone()[0]
            if not pending or cooldown.must_yield(args, {}):
                report["stop_reason"] = {"code": "queue_drained", "error": "No eligible companies remain"}
                stats["status"] = "done"
                return
            await asyncio.sleep(1)
            company = queue.claim_company(args.stream_owner)
    checkpoint(args, report, counters, outcomes)


async def scan_continuously(proxies, args, state, counters, report, audio, vlm_model, token_records):
    if len(proxies) != 1 or not args.stream_owner or not getattr(args, "tracking_store", None):
        raise ValueError("Continuous worker requires one proxy, an owner lease and tracking DB")
    daily = getattr(args, "stream_task_mode", "companies") == "days"
    if daily:
        from src.sites.res.daily_queue import DailyQueue

        queue = DailyQueue(args.stream_db, args.tracking_run_id, initialize=False)
    else:
        queue = FreeQueue(args.stream_db, args.tracking_run_id, initialize=False)
    if getattr(args, "fleet_route_docs", 0):
        args.fleet_quota = FleetQuota(
            queue.database, args.stream_owner, args.fleet_route_docs, args.fleet_provider_budget, args.fleet_reset_hour
        )
    stats = pilot.new_stats(0, proxies[0])
    report["proxies"].append(stats)
    args.solves_per_proxy = 0
    report["continuous_session"] = True
    pilot.say(
        "daily session: date discovery and CVEs, verification token reuse"
        if daily
        else "continuous session: one company at a time; no company or token budget"
    )
    initial = None if daily else pilot.load_companies(args.db, 1)[0]
    try:
        endpoint = await pilot.resolve_proxy(proxies[0], args)
        stats.update(pilot.new_stats(0, endpoint))
        args.stream_proxy_url = endpoint.url
        async with pilot.endpoint_session(0, endpoint, args, counters, token_records, audio, vlm_model, stats) as (
            cascade,
            http,
        ):
            checkpoint(args, report, counters, {})
            if daily:
                from src.sites.res.daily_worker import consume_days

                await consume_days(cascade, http, queue, args, state, counters, report, stats)
            else:
                await consume_session(cascade, http, initial, queue, args, state, counters, report, stats)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        reason = safe_message(error) or type(error).__name__
        stats.update(status=getattr(error, "status", "proxy_failed"), error=reason)
        report["stop_reason"] = {"code": stats["status"], "error": reason}
    finally:
        queue.close()
        atomic_write_json(args.output / f"report-{report['run_id']}.json", report)
