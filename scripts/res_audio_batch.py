"""Bounded, resumable company-search/PDF trial through a proxy and local Whisper."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from types import SimpleNamespace

from scripts.res_audio_pilot import ChromeDownloads, attempt, brightdata_proxy, browser_ip, webshare_proxy
from src.sites.pep.proxy_tunnel import ProxyTunnel
from src.sites.res.audio import LocalAudioSolver
from src.sites.res.browser import lookup
from src.sites.res.response_capture import ResponseCapture
from src.sites.res.source import FORM_URL
from src.utils.atomic import atomic_write_json
from src.utils.error_details import safe_message


def select_companies(database, limit):
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        control = db.execute("SELECT * FROM companies WHERE company_name='VERTIKAL GROUP SpA'").fetchone()
        if control is None:
            raise ValueError("Control company is missing")
        others = db.execute(
            """SELECT c.* FROM companies c WHERE c.rut IS NOT NULL
            AND c.company_id != ? AND NOT EXISTS (
              SELECT 1 FROM res_lookup_results r WHERE r.company_id=c.company_id AND r.status='not_found')
            ORDER BY c.company_name COLLATE NOCASE, c.company_id LIMIT ?""",
            (control["company_id"], limit - 1),
        ).fetchall()
        companies = [dict(row) for row in [control, *others]]
        if len(companies) != limit:
            raise ValueError("Not enough eligible companies")
        return companies


async def diagnose(page, output):
    try:
        await page.screenshot(path=str(output / "search.png"), timeout=5000)
        (output / "search.html").write_text(await page.content(), encoding="utf-8")
    except Exception:
        pass


async def process(company, index, root, model, checkpoint, blocked, args):
    from botonomus import Botonomus, BrowserConfig

    output = root / f"company-{index + 1:02d}"
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    row = {
        "company": company,
        "status": "running",
        "phase": "proxy_preflight",
        "started_at": datetime.now(UTC).isoformat(),
        "search_audio_events": [],
        "provider": args.proxy_provider,
    }
    started = monotonic()
    await checkpoint(index, row)
    try:
        async with asyncio.timeout(360):
            browser_args = ("--disable-quic",)
            if args.proxy_provider == "brightdata":
                proxy, expected_ip, ca_flag = await asyncio.to_thread(brightdata_proxy, output)
                browser_args += (ca_flag,)
            else:
                proxy = await asyncio.to_thread(webshare_proxy, args.proxy_host)
                expected_ip = args.proxy_host
            row["expected_ip"] = expected_ip
            row["phase"] = "browser_startup"
            await checkpoint(index, row)
            tunnel = ProxyTunnel(proxy, timeout=60)
            try:
                local_proxy = await tunnel.start()
                profile = output / "profiles" / "pilot"
                atomic_write_json(profile / "Default" / "Preferences", {"plugins": {"always_open_pdf_externally": True}})
                config = BrowserConfig(
                    profile_root=profile.parent,
                    browser="chrome",
                    persona="off",
                    driver="patchright",
                    virtual_display=True,
                    proxy=local_proxy,
                    extra_args=browser_args,
                )
                async with Botonomus(config=config, max_instances=1) as manager, manager.open(profile="pilot") as browser:
                    context = browser.context
                    row["exit_ip_before"] = await browser_ip(context)
                    if row["exit_ip_before"] != expected_ip:
                        raise ValueError("Browser proxy egress mismatch")
                    directory = output / "browser-downloads"
                    directory.mkdir(exist_ok=True, mode=0o700)
                    downloads = ChromeDownloads(directory)
                    cdp = await context.browser.new_browser_cdp_session()
                    cdp.on("Browser.downloadWillBegin", downloads.started)
                    cdp.on("Browser.downloadProgress", downloads.progress)
                    await cdp.send(
                        "Browser.setDownloadBehavior",
                        {"behavior": "allowAndName", "downloadPath": str(directory.resolve()), "eventsEnabled": True},
                    )
                    page = await context.new_page()

                    class ReadySearch:
                        def __getattr__(self, name):
                            return getattr(page, name)

                        async def goto(self, url, **kwargs):
                            # Search fields start hidden. Its handlers are bound at DOM ready;
                            # selecting options before then never reveals the query input.
                            kwargs["wait_until"] = "domcontentloaded"
                            response = await page.goto(url, **kwargs)
                            if url == FORM_URL:
                                await page.locator("#ddlTipoRegistro").wait_for(state="visible", timeout=45000)
                            return response

                    try:
                        row["phase"] = "company_search"
                        await checkpoint(index, row)
                        async with ResponseCapture(context, page) as capture:
                            row["search"] = await lookup(
                                ReadySearch(),
                                company,
                                timeout_ms=45000,
                                captcha_seconds=120,
                                captcha_solver=LocalAudioSolver(model, events=row["search_audio_events"]),
                                response_reader=capture.read,
                            )
                        row["cves_found"] = row["search"].get("res_cves", [])
                        row["status"] = row["search"]["status"]
                    except Exception as exc:
                        row.update(
                            status=getattr(exc, "status", "failed"), error_type=type(exc).__name__, error=safe_message(exc)
                        )
                        await diagnose(page, output)
                    finally:
                        await page.close()
                    if row.get("cves_found"):
                        row["phase"] = "certificate_download"
                        row["status"] = "running"
                        await checkpoint(index, row)
                        download = {"expected_ip": expected_ip}
                        row["download"] = download
                        args = SimpleNamespace(cve=row["cves_found"][0])
                        await attempt(context, args, output, download, model, downloads)
                        row["status"] = download["status"]
                    try:
                        row["exit_ip_after"] = await browser_ip(context)
                        row["ip_unchanged"] = row["exit_ip_before"] == row["exit_ip_after"]
                        if not row["ip_unchanged"]:
                            row["status"] = "proxy_changed"
                    except Exception as exc:
                        row["ip_after_error"] = type(exc).__name__
                        row["status"] = "proxy_unverified"
            finally:
                await tunnel.close()
    except asyncio.CancelledError:
        row["status"] = "interrupted"
        raise
    except Exception as exc:
        # Setup exceptions may include provider credentials; record their type only.
        row.update(status=getattr(exc, "status", "failed"), error_type=type(exc).__name__)
        if getattr(exc, "code", None):
            row["error_code"] = exc.code
            if exc.code == "ip_blacklisted":
                blocked.set()
    finally:
        row["finished_at"] = datetime.now(UTC).isoformat()
        row["seconds"] = round(monotonic() - started, 2)
        atomic_write_json(output / "result.json", row)
        await checkpoint(index, row)
        print(f"{index + 1}: {company['company_name']}: {row['status']}", flush=True)


async def run(args):
    from dotenv import load_dotenv
    from faster_whisper import WhisperModel

    load_dotenv(".env.res")
    load_dotenv(".env")
    if args.limit < 1 or args.workers not in (1, 2):
        raise ValueError("Use a positive limit and one or two browsers")
    if args.proxy_provider == "webshare" and not args.proxy_host:
        raise ValueError("Webshare requires an explicit --proxy-host")
    root = args.output
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    manifest = root / "selection.json"
    previous_report = root / "report.json"
    if previous_report.exists():
        previous = json.loads(previous_report.read_text())
        if previous.get("provider") != args.proxy_provider:
            raise ValueError("Output belongs to a different proxy provider; select another output directory")
    if manifest.exists():
        companies = json.loads(manifest.read_text())
        if len(companies) != args.limit:
            raise ValueError("Existing sample size differs; select another output directory")
    else:
        companies = select_companies(args.database, args.limit)
        atomic_write_json(manifest, companies)
    report = {
        "provider": args.proxy_provider,
        "proxy_host": args.proxy_host if args.proxy_provider == "webshare" else None,
        "engine": "botonomus",
        "driver": "patchright",
        "model": "base",
        "network_type": "unconfirmed",
        "max_parallel_browsers": args.workers,
        "max_documents_per_company": 1,
        "sample_size": len(companies),
        "results": [None] * len(companies),
    }
    lock = asyncio.Lock()

    async def checkpoint(index, row):
        async with lock:
            report["results"][index] = row
            counts = {}
            for item in report["results"]:
                if item:
                    counts[item["status"]] = counts.get(item["status"], 0) + 1
            report["counts"] = counts
            atomic_write_json(root / "report.json", report)

    model = await asyncio.to_thread(
        WhisperModel,
        "base",
        device="cpu",
        compute_type="int8",
        cpu_threads=4,
        download_root="data/res-audio-models",
        local_files_only=True,
    )
    semaphore = asyncio.Semaphore(args.workers)
    blocked = asyncio.Event()

    async def worker(index, company):
        saved = root / f"company-{index + 1:02d}" / "result.json"
        previous = None
        if saved.exists():
            previous = json.loads(saved.read_text())
            retry_interrupted = previous.get("status") in {"running", "interrupted"}
            retry_error = args.retry_errors and previous.get("status") in {"failed", "retry_wait", "proxy_failed"}
            if (
                not retry_interrupted
                and not retry_error
                and (not args.retry_preflight or previous.get("phase") != "proxy_preflight")
            ):
                await checkpoint(index, previous)
                return
            archive = saved.parent / ("prior-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"))
            archive.mkdir(mode=0o700)
            for name in ("result.json", "search.png", "search.html", "verify.png", "verify.html"):
                if (saved.parent / name).exists():
                    shutil.copyfile(saved.parent / name, archive / name)
        async with semaphore:
            if blocked.is_set():
                row = {
                    "company": company,
                    "status": "not_attempted",
                    "phase": "proxy_preflight",
                    "error_code": "ip_blacklisted",
                }
                atomic_write_json(saved, row)
                await checkpoint(index, row)
                return
            await process(company, index, root, model, checkpoint, blocked, args)
            if previous:
                current = json.loads(saved.read_text())
                history = previous.pop("prior_attempts", [])
                current["prior_attempts"] = [*history, previous]
                atomic_write_json(saved, current)
                await checkpoint(index, current)
            await asyncio.sleep(5)

    print(f"Batch: {root}; companies={len(companies)}; browsers={args.workers}", flush=True)
    await asyncio.gather(*(worker(i, company) for i, company in enumerate(companies)))
    report["overall_status"] = "blocked_proxy_auth" if blocked.is_set() else "finished"
    report["pdfs_downloaded"] = sum(row["status"] == "downloaded" for row in report["results"])
    report["searches_completed"] = sum("search" in row for row in report["results"])
    atomic_write_json(root / "report.json", report)
    with (root / "results.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["company_name", "rut", "status", "phase", "cves_found", "pdf_path", "seconds"])
        for row in report["results"]:
            certificate = row.get("download", {}).get("certificate", {})
            writer.writerow(
                [
                    row["company"]["company_name"],
                    row["company"].get("rut"),
                    row["status"],
                    row["phase"],
                    ";".join(row.get("cves_found", [])),
                    certificate.get("local_path", ""),
                    row.get("seconds"),
                ]
            )
    print(json.dumps(report["counts"]), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path("data/dequienes-companies-2025/companies.sqlite3"))
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--proxy-provider", choices=("brightdata", "webshare"), default="brightdata")
    parser.add_argument("--proxy-host", help="Explicit Webshare endpoint; unused for Bright Data")
    parser.add_argument("--retry-errors", action="store_true", help="Retry technical failures once; retain prior attempts")
    parser.add_argument("--output", type=Path, default=Path("data/res-audio-batch-10"))
    parser.add_argument(
        "--retry-preflight", action="store_true", help="Retry only setup failures; preserve previous results"
    )
    asyncio.run(run(parser.parse_args()))
