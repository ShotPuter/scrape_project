"""Bounded Camoufox comparison on the existing ten-link Firefox sample."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import ipaddress
import json
import time
from collections import Counter
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from camoufox.async_api import AsyncCamoufox
from playwright.async_api import Error as BrowserError, TimeoutError as BrowserTimeout

from scripts.pep_camoufox_logging import EventLog
from scripts.pep_camoufox_pilot import CA_BUNDLE, DEFAULT_SAMPLE, ROOT, install_intermediate, install_proxy_ca
from scripts.pep_camoufox_retry import reuse_document
from src.sites.pep.documents import MAX_PDF_BYTES, inspect_saved, save_payload
from src.sites.pep.source import DOWNLOAD_PATH, LEGACY_PATH, RESULT_PATH, parse_results, safe_url
from src.utils.atomic import atomic_write_json


@asynccontextmanager
async def borrowed_context(context):
    """The browser owner closes the context after all its tabs finish."""
    yield context


@asynccontextmanager
async def browser_context(options: dict, launch_lock: asyncio.Lock | None = None):
    """Serialize native fingerprint initialization, then keep browsers running concurrently."""
    async with AsyncExitStack() as stack:
        if launch_lock is None:
            context = await stack.enter_async_context(AsyncCamoufox(**options))
        else:
            async with launch_lock:
                context = await stack.enter_async_context(AsyncCamoufox(**options))
        yield context


async def download_once(page, element, endpoint: str, timeout_ms: int, *, expected_url: str | None = None):
    """Observe the native download or an HTTP failure, whichever arrives first."""
    loop = asyncio.get_running_loop()
    completed = loop.create_future()
    rejected = loop.create_future()
    popup_pages = []

    async def on_popup_response(response):
        if urlsplit(response.url).path != endpoint or response.status < 300:
            return
        try:
            if expected_url:
                related = response.url == expected_url
            else:
                owner = response.frame.page
                related = owner is not page and await owner.opener() is page
            if related and not rejected.done():
                rejected.set_result(response.status)
        except BrowserError:
            pass

    def on_popup(popup):
        popup_pages.append(popup)
        popup.on("download", on_download)

    def on_download(download):
        if not completed.done():
            completed.set_result(download)

    def on_response(response):
        if urlsplit(response.url).path == endpoint and response.status >= 300 and not rejected.done():
            rejected.set_result(response.status)

    page.on("download", on_download)
    page.on("response", on_response)
    if endpoint == LEGACY_PATH:
        page.on("popup", on_popup)
        page.context.on("response", on_popup_response)
    try:
        async with asyncio.timeout(timeout_ms / 1000):
            await element.click(timeout=timeout_ms, no_wait_after=True)
            done, _ = await asyncio.wait((completed, rejected), return_when=asyncio.FIRST_COMPLETED)
            if completed in done:
                return completed.result(), None
            return None, rejected.result()
    finally:
        page.remove_listener("download", on_download)
        page.remove_listener("response", on_response)
        if endpoint == LEGACY_PATH:
            page.remove_listener("popup", on_popup)
            page.context.remove_listener("response", on_popup_response)
            for popup in popup_pages:
                popup.remove_listener("download", on_download)
                if not completed.done():
                    with suppress(Exception):
                        await popup.close()
        completed.cancel()
        rejected.cancel()


async def run(
    sample: list[dict],
    output: Path,
    request_seconds: float,
    *,
    ca_bundle: Path = CA_BUNDLE,
    max_seconds: float = 900,
    on_progress=None,
    proxy: dict | None = None,
    proxy_ca: Path | None = None,
    launch_lock: asyncio.Lock | None = None,
    on_state=None,
    context_override=None,
    next_job=None,
    stop_event: asyncio.Event | None = None,
    egress_ip: str | None = None,
    reusable=None,
) -> dict:
    started = time.monotonic()
    stop_event = stop_event or asyncio.Event()
    page = None
    closing_page = False
    consecutive_failures = 0
    await asyncio.to_thread(output.mkdir, parents=True, mode=0o700)
    report = {
        "engine": "camoufox",
        "status": "running",
        "delay_seconds": 3,
        "sample_sha256": hashlib.sha256(json.dumps(sample, sort_keys=True).encode()).hexdigest(),
        "single_browser": True,
        "tls_verification": True,
        "proxy": bool(proxy),
        "proxy_egress_ip": egress_ip,
        "jobs": [{**row, "status": "pending", "documents": []} for row in sample],
    }
    atomic_write_json(output / "sample.json", sample)
    log = EventLog(output, secrets=tuple(proxy.values()) if proxy else ())

    def redact(value) -> str:
        message = str(value)
        for key in ("password", "username"):
            if proxy and proxy.get(key):
                message = message.replace(proxy[key], "[redacted]")
        if proxy and proxy.get("username"):
            message = message.replace(proxy["username"].split("-session-", 1)[0], "[redacted]")
        return message

    def checkpoint():
        report["seconds"] = round(time.monotonic() - started, 2)
        report["job_counts"] = dict(Counter(job["status"] for job in report["jobs"]))
        report["pdf_count"] = len(
            {
                item["local_path"]
                for job in report["jobs"]
                for doc in job["documents"]
                if doc["status"] == "downloaded"
                for item in (doc.get("files") or [doc])
                if item.get("local_path")
            }
        )
        report["archive_count"] = len(list((output / "archives").glob("*.zip")))
        atomic_write_json(output / "report.json", report)
        reason = report.get("stop_reason", "") or ""
        if reason == "recaptcha_notice" or reason.startswith("download_http_") or reason == "time_budget":
            stop_event.set()
        if on_state:
            on_state(report)

    def activity(stage, *, job=None, document=None):
        report["activity"] = {
            "stage": stage,
            "job": job,
            "document": document,
            "started_seconds": round(time.monotonic() - started, 2),
        }
        checkpoint()
        log.emit("activity", **report["activity"])

    def target_failure(error):
        message = str(error).lower()
        return type(error).__name__ == "TargetClosedError" or any(
            part in message for part in ("target crashed", "target page, context or browser has been closed")
        )

    def stop_target(error):
        if not report.get("stop_reason"):
            report["stop_reason"] = "page_crashed" if "target crashed" in str(error).lower() else "page_closed"
        log.emit("target_unavailable", reason=report["stop_reason"], error=redact(error)[:800])
        checkpoint()

    def page_event(kind):
        event = {"kind": kind, "seconds": round(time.monotonic() - started, 2), "expected": closing_page}
        report.setdefault("page_events", []).append(event)
        if not closing_page:
            report["stop_reason"] = "page_crashed" if kind == "crash" else report.get("stop_reason") or "page_closed"
        log.emit("page_lifecycle", **event, activity=report.get("activity"))
        checkpoint()

    checkpoint()
    log.emit("worker_started", output=str(output), links=len(sample), proxy=bool(proxy))
    try:
        if context_override is None:
            activity("certificates")
            await asyncio.to_thread(install_intermediate, output / "profile", ca_bundle)
            if proxy:
                if proxy_ca is None and urlsplit(proxy["server"]).hostname == "brd.superproxy.io":
                    raise ValueError("Bright Data proxy CA is required")
                if proxy_ca is not None:
                    await asyncio.to_thread(install_proxy_ca, output / "profile", proxy_ca)
        activity("browser_start")
        async with (
            asyncio.timeout(max_seconds),
            (
                borrowed_context(context_override)
                if context_override is not None
                else browser_context(
                    {
                        "headless": "virtual",
                        "os": "linux",
                        "locale": "es-CO",
                        "persistent_context": True,
                        "user_data_dir": str(output / "profile"),
                        "accept_downloads": True,
                        "ignore_https_errors": False,
                        "proxy": proxy,
                    },
                    launch_lock,
                )
            ) as context,
        ):
            page = await context.new_page()
            if proxy and context_override is None:
                activity("proxy_preflight")
                # Check the route in this browser, without sending proxy credentials to a separate client.
                response = await page.goto(
                    "https://api.ipify.org", wait_until="domcontentloaded", timeout=int(request_seconds * 1000)
                )
                if not response or response.status != 200:
                    raise ValueError("Proxy browser preflight failed")
                address = (await page.locator("body").inner_text()).strip()
                report["proxy_egress_ip"] = str(ipaddress.ip_address(address))
                checkpoint()
                log.emit("proxy_ready", egress_ip=report["proxy_egress_ip"])
            active = {}

            def on_request(request):
                if active and urlsplit(request.url).path in {DOWNLOAD_PATH, LEGACY_PATH}:
                    active["request_sent"] = True

            def on_response(response):
                if active and urlsplit(response.url).path in {DOWNLOAD_PATH, LEGACY_PATH}:
                    active["observed_http_status"] = response.status

            def on_error(error):
                if active:
                    lines = redact(error).splitlines()
                    message = lines[0] if lines else type(error).__name__
                    active.setdefault("javascript_errors", []).append(message[:300])

            page.on("request", on_request)
            page.on("response", on_response)
            page.on("pageerror", on_error)
            page.on("crash", lambda *_: page_event("crash"))
            page.on("close", lambda *_: page_event("close"))
            static_jobs = iter(report["jobs"])
            while not stop_event.is_set() and not report.get("stop_reason"):
                if getattr(page, "is_closed", lambda: False)():
                    stop_target(RuntimeError("Target page, context or browser has been closed"))
                    break
                job = next_job() if next_job is not None else next(static_jobs, None)
                if job is None:
                    break
                if next_job is not None:
                    job = {**job, "status": "pending", "documents": []}
                    report["jobs"].append(job)
                job["status"] = "running"
                link_started = time.monotonic()
                job["started_at"] = datetime.now(UTC).isoformat()
                job["started_seconds"] = round(link_started - started, 2)
                checkpoint()
                seen_pages, seen_docs = set(), set()
                next_url = job["url"]
                try:
                    while next_url and not stop_event.is_set() and not report.get("stop_reason"):
                        if next_url in seen_pages or len(seen_pages) >= 25:
                            raise ValueError("Pagination cycle or page limit")
                        seen_pages.add(next_url)
                        activity("navigation", job=job["n"])
                        await asyncio.sleep(3)
                        for navigation_attempt in range(2):
                            try:
                                landing = await page.goto(
                                    next_url, wait_until="domcontentloaded", timeout=int(request_seconds * 1000)
                                )
                                break
                            except BrowserError as exc:
                                if navigation_attempt or "NS_ERROR_UNKNOWN_HOST" not in str(exc):
                                    raise
                                job["dns_retry"] = True
                                await asyncio.sleep(3)
                        if not landing or landing.status != 200:
                            raise ValueError(f"Landing HTTP {landing.status if landing else 'missing'}")
                        activity("discovery", job=job["n"])
                        await page.locator("table.table tbody").wait_for(
                            state="attached", timeout=int(request_seconds * 1000)
                        )
                        documents, next_url = parse_results(await page.content(), page.url)
                        documents = [doc for doc in documents if str(doc["id"]) not in seen_docs]
                        for doc in documents:
                            doc.update(status="pending", attempts=0)
                        job["documents"].extend(documents)
                        checkpoint()
                        for doc in documents:
                            if stop_event.is_set() or report.get("stop_reason"):
                                break
                            doc_id = str(doc["id"])
                            if doc_id in seen_docs:
                                continue
                            seen_docs.add(doc_id)
                            doc.update(status="running", attempts=0)
                            document_started = time.monotonic()
                            active = doc
                            checkpoint()
                            try:
                                activity("document_download", job=job["n"], document=doc_id)
                                existing = await asyncio.to_thread(inspect_saved, output, doc_id)
                                source = (reusable or {}).get((job["url"], doc_id))
                                if existing is None and source is not None:
                                    existing = await asyncio.to_thread(reuse_document, source, output, doc_id, True)
                                if existing:
                                    doc.update(existing, reused=True)
                                    continue
                                await asyncio.sleep(3)
                                modern = doc.get("requires_captcha", False)
                                endpoint = DOWNLOAD_PATH if modern else LEGACY_PATH
                                if modern:
                                    element = page.locator(f"form#descargarDeclaracionForm{doc_id} button")
                                else:
                                    # Resolve only the same-host HTTPS URL validated by the parser.
                                    target = safe_url(doc["download_url"])
                                    anchors = page.locator("a[href]")
                                    index = await anchors.evaluate_all(
                                        """(links, target) => links.findIndex(link => {
                                            try {
                                                const url = new URL(link.getAttribute('href'), document.baseURI);
                                                if (!['http:', 'https:'].includes(url.protocol)) return false;
                                                url.protocol = 'https:'; url.port = ''; url.hash = '';
                                                return url.href === target;
                                            } catch (_) { return false; }
                                        })""",
                                        target,
                                    )
                                    if index < 0:
                                        raise ValueError("Legacy download anchor not found")
                                    element = anchors.nth(index)
                                    await element.evaluate("(anchor, url) => { anchor.href = url; }", target)
                                doc["attempts"] = 1
                                checkpoint()
                                if modern:
                                    download, status = await download_once(
                                        page, element, endpoint, int(request_seconds * 1000)
                                    )
                                else:
                                    download, status = await download_once(
                                        page, element, endpoint, int(request_seconds * 1000), expected_url=target
                                    )
                                if download is None:
                                    doc.update(status=f"http_{status}", http_status=status)
                                    if modern or status in {403, 429}:
                                        # A redirect alone does not establish the CAPTCHA rejection reason.
                                        try:
                                            await page.wait_for_function(
                                                "() => document.body && document.body.innerText"
                                                ".includes('Recaptcha did not pass validation')",
                                                timeout=7000,
                                            )
                                            doc["recaptcha_notice"] = True
                                        except Exception:
                                            doc["recaptcha_notice"] = False
                                        report["stop_reason"] = (
                                            "recaptcha_notice" if doc["recaptcha_notice"] else f"download_http_{status}"
                                        )
                                        break
                                    continue
                                raw = output / "downloads" / f"{doc_id}.bin"
                                await asyncio.to_thread(raw.parent.mkdir, parents=True, exist_ok=True)
                                await download.save_as(str(raw))
                                activity("document_validation", job=job["n"], document=doc_id)
                                if raw.stat().st_size > MAX_PDF_BYTES:
                                    raise ValueError("Download exceeds size limit")
                                metadata = await asyncio.to_thread(save_payload, raw.read_bytes(), output, doc_id)
                                if not await asyncio.to_thread(inspect_saved, output, doc_id):
                                    raise ValueError("Saved PDF validation failed")
                                doc.update(metadata, http_status=200)
                            except (TimeoutError, BrowserTimeout) as exc:
                                doc.update(
                                    status="timeout",
                                    recaptcha_notice=False,
                                    error={"type": type(exc).__name__, "message": redact(exc)[:800]},
                                )
                                try:
                                    doc["recaptcha_notice"] = "Recaptcha did not pass validation" in await page.content()
                                    await page.screenshot(path=str(output / f"timeout-{doc_id}.png"))
                                except Exception as diagnostic_error:
                                    doc["diagnostic_error"] = {
                                        "type": type(diagnostic_error).__name__,
                                        "message": redact(diagnostic_error)[:800],
                                    }
                                    if target_failure(diagnostic_error):
                                        stop_target(diagnostic_error)
                                if doc["recaptcha_notice"]:
                                    report["stop_reason"] = "recaptcha_notice"
                                next_url = None
                                break
                            except Exception as exc:
                                doc.update(status="failed", error={"type": type(exc).__name__, "message": redact(exc)[:800]})
                                if target_failure(exc):
                                    stop_target(exc)
                                next_url = None
                                break
                            finally:
                                if doc["status"] == "running":
                                    doc["status"] = "interrupted"
                                doc["seconds"] = round(time.monotonic() - document_started, 2)
                                checkpoint()
                                log.summary(
                                    "document_finished",
                                    report,
                                    job=job["n"],
                                    document=doc_id,
                                    document_status=doc["status"],
                                    document_seconds=doc["seconds"],
                                    http_status=doc.get("http_status", doc.get("observed_http_status")),
                                    error=doc.get("error"),
                                )
                                active = {}
                    job["status"] = (
                        "completed"
                        if not next_url
                        and not report.get("stop_reason")
                        and all(doc["status"] == "downloaded" for doc in job["documents"])
                        else "partial"
                    )
                except Exception as exc:
                    job.update(status="failed", error={"type": type(exc).__name__, "message": redact(exc)[:800]})
                    if target_failure(exc):
                        stop_target(exc)
                    try:
                        job["page_title"] = redact(await page.title())
                        body = await page.locator("body").inner_text(timeout=5000)
                        job["page_text_excerpt"] = redact(body)[:1500]
                        await page.screenshot(path=str(output / f"job-{job['n']}-error.png"))
                        if "Recaptcha did not pass validation" in body:
                            report["stop_reason"] = "recaptcha_notice"
                    except Exception:
                        pass
                consecutive_failures = consecutive_failures + 1 if job["status"] == "failed" else 0
                if consecutive_failures >= 3 and not report.get("stop_reason"):
                    report["stop_reason"] = "consecutive_navigation_errors"
                job["seconds"] = round(time.monotonic() - link_started, 2)
                activity("between_links", job=job["n"])
                log.summary(
                    "link_finished",
                    report,
                    job=job["n"],
                    link_status=job["status"],
                    link_seconds=job["seconds"],
                    error=job.get("error"),
                )
                if on_progress:
                    await on_progress(report, final=False)
            activity("browser_shutdown")
            closing_page = True
        if stop_event.is_set() and not report.get("stop_reason"):
            report["stop_reason"] = "browser_peer_stopped"
        report["status"] = "stopped" if report.get("stop_reason") else "finished"
    except TimeoutError:
        report.update(status="stopped", stop_reason="time_budget")
    except BaseException as exc:
        report.update(
            status="interrupted" if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)) else "error",
            error={"type": type(exc).__name__, "message": redact(exc)[:800]},
        )
        if target_failure(exc):
            stop_target(exc)
        if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)):
            raise
    finally:
        closing_page = True
        if page is not None:
            with suppress(Exception):
                await page.close()
        for job in report["jobs"]:
            if job["status"] == "running":
                job["status"] = "interrupted"
                job["seconds"] = round(time.monotonic() - started - job.get("started_seconds", 0), 2)
            for doc in job["documents"]:
                if doc["status"] == "running":
                    doc["status"] = "interrupted"
        activity(report["status"])
        log.summary("worker_finished", report, error=report.get("error"))
        log.close()
        if on_progress:
            await on_progress(report, final=True)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=Path, default=DEFAULT_SAMPLE)
    parser.add_argument("--timeout", type=float, default=45)
    args = parser.parse_args()
    if not 5 <= args.timeout <= 120:
        parser.error("timeout must be between 5 and 120 seconds")
    sample = json.loads(args.sample.read_text())
    if len(sample) != 10:
        parser.error("Expected the fixed ten-link sample")
    for row in sample:
        row["url"] = safe_url(row["url"])
        if urlsplit(row["url"]).path != RESULT_PATH:
            parser.error("Unexpected sample URL")
    if len({row["url"] for row in sample}) != 10:
        parser.error("Sample must contain ten unique links")
    output = ROOT / "data" / f"pep-camoufox-10-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    report = asyncio.run(run(sample, output, args.timeout))
    print(
        json.dumps(
            {
                "output": str(output),
                "status": report["status"],
                "jobs": report["job_counts"],
                "pdf_count": report["pdf_count"],
                "stop_reason": report.get("stop_reason"),
            }
        ),
        flush=True,
    )
    return 0 if report["job_counts"].get("completed") == 10 else 1


if __name__ == "__main__":
    raise SystemExit(main())
