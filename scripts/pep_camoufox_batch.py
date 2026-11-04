"""Bounded Camoufox comparison on the existing ten-link Firefox sample."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from camoufox.async_api import AsyncCamoufox
from playwright.async_api import Error as BrowserError, TimeoutError as BrowserTimeout

from scripts.pep_camoufox_pilot import CA_BUNDLE, DEFAULT_SAMPLE, ROOT, install_intermediate
from src.sites.pep.documents import MAX_PDF_BYTES, inspect_saved, save_payload
from src.sites.pep.source import DOWNLOAD_PATH, LEGACY_PATH, RESULT_PATH, parse_results, safe_url
from src.utils.atomic import atomic_write_json


async def download_once(page, element, endpoint: str, timeout_ms: int):
    """Observe the native download or an HTTP failure, whichever arrives first."""
    loop = asyncio.get_running_loop()
    completed = loop.create_future()
    rejected = loop.create_future()

    def on_download(download):
        if not completed.done():
            completed.set_result(download)

    def on_response(response):
        if urlsplit(response.url).path == endpoint and response.status >= 300 and not rejected.done():
            rejected.set_result(response.status)

    page.on("download", on_download)
    page.on("response", on_response)
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
) -> dict:
    started = time.monotonic()
    await asyncio.to_thread(output.mkdir, parents=True, mode=0o700)
    report = {
        "engine": "camoufox",
        "status": "running",
        "delay_seconds": 3,
        "sample_sha256": hashlib.sha256(json.dumps(sample, sort_keys=True).encode()).hexdigest(),
        "single_browser": True,
        "tls_verification": True,
        "proxy": False,
        "jobs": [{**row, "status": "pending", "documents": []} for row in sample],
    }
    atomic_write_json(output / "sample.json", sample)

    def checkpoint():
        report["seconds"] = round(time.monotonic() - started, 2)
        report["job_counts"] = dict(Counter(job["status"] for job in report["jobs"]))
        report["pdf_count"] = len(list((output / "pdf").glob("*.pdf")))
        report["archive_count"] = len(list((output / "archives").glob("*.zip")))
        atomic_write_json(output / "report.json", report)

    checkpoint()
    print(f"OUTPUT {output}", flush=True)
    try:
        await asyncio.to_thread(install_intermediate, output / "profile", ca_bundle)
        async with (
            asyncio.timeout(max_seconds),
            AsyncCamoufox(
                headless="virtual",
                os="linux",
                locale="es-CO",
                persistent_context=True,
                user_data_dir=str(output / "profile"),
                accept_downloads=True,
                ignore_https_errors=False,
            ) as context,
        ):
            page = context.pages[0] if context.pages else await context.new_page()
            active = {}

            def on_request(request):
                if active and urlsplit(request.url).path in {DOWNLOAD_PATH, LEGACY_PATH}:
                    active["request_sent"] = True

            def on_response(response):
                if active and urlsplit(response.url).path in {DOWNLOAD_PATH, LEGACY_PATH}:
                    active["observed_http_status"] = response.status

            def on_error(error):
                if active:
                    active.setdefault("javascript_errors", []).append(str(error).splitlines()[0][:300])

            page.on("request", on_request)
            page.on("response", on_response)
            page.on("pageerror", on_error)
            for job in report["jobs"]:
                if report.get("stop_reason"):
                    break
                job["status"] = "running"
                checkpoint()
                seen_pages, seen_docs = set(), set()
                next_url = job["url"]
                try:
                    while next_url and not report.get("stop_reason"):
                        if next_url in seen_pages or len(seen_pages) >= 25:
                            raise ValueError("Pagination cycle or page limit")
                        seen_pages.add(next_url)
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
                            doc_id = str(doc["id"])
                            if doc_id in seen_docs:
                                continue
                            seen_docs.add(doc_id)
                            doc.update(status="running", attempts=0)
                            active = doc
                            checkpoint()
                            try:
                                existing = await asyncio.to_thread(inspect_saved, output, doc_id)
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
                                    matches = []
                                    for anchor in await anchors.all():
                                        href = await anchor.get_attribute("href")
                                        try:
                                            if href and safe_url(href, page.url) == target:
                                                matches.append(anchor)
                                        except ValueError:
                                            continue
                                    if not matches:
                                        raise ValueError("Legacy download anchor not found")
                                    element = matches[0]
                                doc["attempts"] = 1
                                checkpoint()
                                download, status = await download_once(page, element, endpoint, int(request_seconds * 1000))
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
                                if raw.stat().st_size > MAX_PDF_BYTES:
                                    raise ValueError("Download exceeds size limit")
                                metadata = await asyncio.to_thread(save_payload, raw.read_bytes(), output, doc_id)
                                if not await asyncio.to_thread(inspect_saved, output, doc_id):
                                    raise ValueError("Saved PDF validation failed")
                                doc.update(metadata, http_status=200)
                            except (TimeoutError, BrowserTimeout):
                                doc.update(
                                    status="timeout",
                                    recaptcha_notice=("Recaptcha did not pass validation" in await page.content()),
                                )
                                await page.screenshot(path=str(output / f"timeout-{doc_id}.png"))
                                if doc["recaptcha_notice"]:
                                    report["stop_reason"] = "recaptcha_notice"
                                next_url = None
                                break
                            except Exception as exc:
                                doc.update(status="failed", error={"type": type(exc).__name__, "message": str(exc)[:800]})
                                report["stop_reason"] = "document_error"
                                break
                            finally:
                                checkpoint()
                                print(json.dumps({"job": job["n"], "document": doc_id, "status": doc["status"]}), flush=True)
                                active = {}
                    job["status"] = (
                        "completed"
                        if not report.get("stop_reason") and all(doc["status"] == "downloaded" for doc in job["documents"])
                        else "partial"
                    )
                except Exception as exc:
                    job.update(status="failed", error={"type": type(exc).__name__, "message": str(exc)[:800]})
                    try:
                        job["page_title"] = await page.title()
                        body = await page.locator("body").inner_text(timeout=5000)
                        job["page_text_excerpt"] = body[:1500]
                        await page.screenshot(path=str(output / f"job-{job['n']}-error.png"))
                        if "Recaptcha did not pass validation" in body:
                            report["stop_reason"] = "recaptcha_notice"
                    except Exception:
                        pass
                checkpoint()
                print(json.dumps({"job": job["n"], "status": job["status"], "pdf_count": report["pdf_count"]}), flush=True)
                if on_progress:
                    await on_progress(report, final=False)
        report["status"] = "stopped" if report.get("stop_reason") else "finished"
    except TimeoutError:
        report.update(status="stopped", stop_reason="time_budget")
    except BaseException as exc:
        report.update(
            status="interrupted" if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)) else "error",
            error={"type": type(exc).__name__, "message": str(exc)[:800]},
        )
        if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)):
            raise
    finally:
        for job in report["jobs"]:
            if job["status"] == "running":
                job["status"] = "interrupted"
            for doc in job["documents"]:
                if doc["status"] == "running":
                    doc["status"] = "interrupted"
        checkpoint()
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
