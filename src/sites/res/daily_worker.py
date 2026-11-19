"""One browser drains date-discovery and CVE jobs from the current RES day."""

from __future__ import annotations

import asyncio
import json
import time

import httpx

from scripts import res_free_pilot as pilot
from src.sites.res import cooldown
from src.sites.res.bulk import iso_day
from src.sites.res.bulk_pipeline import discover_dates
from src.sites.res.continuous_worker import check_route, checkpoint
from src.sites.res.source import SearchIssue
from src.utils.error_details import safe_message


class ReusingCascade:
    """Reuse accepted verification tokens, replacing a rejected token once."""

    def __init__(self, cascade):
        self.cascade = cascade
        self.token = None

    def __getattr__(self, name):
        return getattr(self.cascade, name)

    async def mint(self, url, **kwargs):
        if url == pilot.RES_URL and self.token:
            return self.token
        token = await self.cascade.mint(url, **kwargs)
        if url == pilot.RES_URL:
            self.token = token
        return token


async def discover_job(job, queue, store, http, cascade):
    if not store.day_done(job["day"], job["kind"]):
        from types import SimpleNamespace

        day = iso_day(job["day"])
        await discover_dates(store, http, cascade.mint, SimpleNamespace(start=day, end=day, kind=job["kind"]))
    row = store.db.execute(
        "SELECT actions_json FROM date_searches WHERE run_id=? AND day=? AND kind=? AND complete=1",
        (store.run_id, job["day"], job["kind"]),
    ).fetchone()
    if not row:
        raise SearchIssue("retry_wait", "Date discovery is incomplete; day remains queued")
    queue.expand_day(job["day"], job["kind"], json.loads(row[0]), store)


async def document_job(job, store, http, cascade, state, args, counters):
    cached = store.existing_file(job["cve"])
    if cached:
        counters["reused"] += 1
        return
    subject = {"company_id": job["subject_company_id"], "company_name": job["company_name"], "rut": job["rut"]}
    state.clear()
    state.update(pilot.load_state(args.state_path))
    record = store.db.execute(
        "SELECT codigo,download_url,verified_at,status FROM documents WHERE cve=?", (job["cve"],)
    ).fetchone()
    if record and record["status"] in {"expired_saved_link", "expired_codigo"}:
        state["cves"].pop(job["cve"], None)
    elif record and record["codigo"] and record["download_url"] and job["cve"] not in state["cves"]:
        state["cves"][job["cve"]] = {
            "codigo": record["codigo"],
            "link": record["download_url"],
            "verified_at": record["verified_at"],
            "company_id": subject["company_id"],
        }
    old_token = cascade.token
    try:
        await pilot.ensure_document(cascade, http, job["cve"], subject["company_id"], state, args, args.output, counters)
    except SearchIssue as error:
        if error.status != "captcha_required" or not old_token:
            raise
        cascade.token = None
        await pilot.ensure_document(cascade, http, job["cve"], subject["company_id"], state, args, args.output, counters)


async def consume_days(cascade, http, queue, args, state, counters, report, stats):
    outcomes = {}
    cascade = ReusingCascade(cascade)
    while True:
        if cooldown.must_yield(args, {}):
            report["stop_reason"] = {"code": "shared_cooldown", "error": "Shared collection pause requested"}
            break
        job = queue.claim_first_job(args.stream_owner)
        if job is None:
            if queue.archive_current():
                report["stop_reason"] = {"code": "queue_drained", "error": "Completed days drained"}
                break
            await asyncio.sleep(0.5)
            continue
        checkpoint(args, report, counters, outcomes)
        started = time.monotonic()
        try:
            await check_route(http, stats["http_ip"], getattr(args, "stream_proxy_url", None))
            if job["task"] == "discover":
                await discover_job(job, queue, args.tracking_store, http, cascade)
                status = "not_found"  # Completed discovery job, independently of document jobs.
            else:
                await document_job(job, args.tracking_store, http, cascade, state, args, counters)
                status = "downloaded"
            result = {**job, "status": status, "documents": []}
        except asyncio.CancelledError:
            raise
        except Exception as error:
            result = {
                **job,
                "status": getattr(error, "status", "proxy_failed" if isinstance(error, httpx.TransportError) else "failed"),
                "error": safe_message(error),
                "documents": [],
            }
        complete = result["status"] in {"downloaded", "not_found"}
        limited = result["status"] in {"daily_limit", "fleet_route_limit", "fleet_provider_limit"}
        queue.finish_job(
            job["company_id"],
            args.stream_owner,
            complete=complete,
            attempted=not limited,
            limit=0,
            retry=args.stream_retry_delay,
            error=result.get("error"),
        )
        report.setdefault("attempts", []).append(
            {"company_id": job["company_id"], "result": result, "seconds": round(time.monotonic() - started, 3)}
        )
        outcomes[job["company_id"]] = result
        stats["companies"] = len(outcomes)
        checkpoint(args, report, counters, outcomes)
        if not complete:
            code = "daily_document_limit" if result["status"] == "daily_limit" else result["status"]
            report["stop_reason"] = {"code": code, "error": result.get("error"), "company_id": job["company_id"]}
            stats.update(status=result["status"], error=result.get("error"))
            return
        stats["status"] = "ready"
    checkpoint(args, report, counters, outcomes)
