"""Stages 1–3: configuration, persistent queues and public discovery."""

from __future__ import annotations

import asyncio
import csv
import fcntl
import json
import uuid
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from src.sites.dequienes.extraction import extract_candidates
from src.sites.dequienes.queue import LostLease, QueueStore
from src.sites.dequienes.settings import PipelineSettings, read_settings
from src.utils.atomic import atomic_text_writer, atomic_write_json


def settings_from_args(args) -> PipelineSettings:
    settings = read_settings(getattr(args, "config", None))
    changes = {}
    for arg, key in [
        ("year", "year"),
        ("month", "months"),
        ("all_months", "all_months"),
        ("output", "output"),
        ("profile", "profile"),
    ]:
        value = getattr(args, arg, None)
        if value is not None:
            changes[key] = tuple(value) if key == "months" else value
    stage = {}
    for arg, key in [
        ("workers", "tabs_per_browser"),
        ("limit", "max_items"),
        ("delay", "delay_seconds"),
        ("timeout", "timeout_seconds"),
        ("rate_per_minute", "rate_per_minute"),
        ("max_in_flight", "max_in_flight"),
        ("max_attempts", "max_attempts"),
        ("transport", "transport"),
        ("session_refreshes", "session_refreshes"),
    ]:
        value = getattr(args, arg, None)
        if value is not None:
            stage[key] = value
    changes["public"] = replace(settings.public, **stage)
    for name, limit_arg in [("account", "max_companies"), ("res", "max_documents")]:
        value = getattr(args, limit_arg, None)
        if value is not None:
            changes[name] = replace(getattr(settings, name), max_items=value)
    return replace(settings, **changes)


def export_pipeline(store: QueueStore, output: Path):
    # Serialize exports across processes and read all files from one WAL snapshot.
    with (output / ".export.lock").open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        store.db.execute("BEGIN")
        try:
            _export_snapshot(store, output)
            store.db.commit()
        except BaseException:
            store.db.rollback()
            raise


def _export_snapshot(store: QueueStore, output: Path):
    from src.sites.dequienes.companies import export

    export(store.db, output)
    tables = {
        "company_queue.csv": """SELECT c.company_id,c.company_name,c.rut,c.identity_kind,j.status,j.attempts
            FROM companies c JOIN jobs j ON j.job_key=c.company_id AND j.queue='company_lookup' ORDER BY c.company_id""",
        "cve_queue.csv": """SELECT d.cve,d.status AS verification_status,j.status AS queue_status,j.attempts
            FROM documents d JOIN jobs j ON j.job_key=d.cve AND j.queue='certificate_download' ORDER BY d.cve""",
        "document_files.csv": "SELECT * FROM document_files ORDER BY cve",
        "code_candidates.csv": "SELECT source_url,code,kind,evidence FROM code_candidates ORDER BY source_url,code",
        "res_lookup_queue.csv": """SELECT c.company_id,c.company_name,c.rut,j.status,j.attempts,j.error
            FROM companies c JOIN jobs j ON j.job_key=c.company_id AND j.queue='res_company_lookup' ORDER BY j.id""",
        "res_actions.csv": """SELECT s.cve,c.company_name,c.rut AS requested_rut,
            s.action_date,s.action_type,s.attention_number,s.source_url
            FROM res_document_sources s JOIN companies c ON c.company_id=s.company_id ORDER BY s.cve,c.company_id""",
    }
    for filename, query in tables.items():
        cursor = store.db.execute(query)
        with atomic_text_writer(output / filename, newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow([column[0] for column in cursor.description])
            writer.writerows(cursor)
    atomic_write_json(output / "queues.json", store.status())


class PublicWorker:
    def __init__(self, store, settings, run_id, months):
        self.store, self.settings, self.run_id, self.months = store, settings, run_id, months
        self.failures = 0
        self.completed = 0
        self.failure_streak = 0
        self.stop = asyncio.Event()
        self.owner = uuid.uuid4().hex
        self.exports = asyncio.Lock()
        self.http = None

    def browser_args(self, *, attempts=1):
        stage = self.settings.public
        return SimpleNamespace(
            timeout=stage.timeout_seconds,
            delay=stage.delay_seconds,
            output=self.settings.output,
            fetch_attempts=attempts,
            request_gate=self.request_gate,
        )

    async def request_gate(self):
        delay = self.store.request_delay("dequienes-public", self.settings.public.rate_per_minute)
        if delay:
            await asyncio.sleep(delay)

    async def heartbeat(self, job):
        while True:
            await asyncio.sleep(max(0.01, self.settings.public.lease_seconds / 3))
            self.store.heartbeat(job, self.settings.public.lease_seconds)

    async def process(self, page, job):
        from src.sites.dequienes import companies
        from src.sites.dequienes.companies import parse_company

        stage = self.settings.public
        task = asyncio.create_task(self.heartbeat(job))
        try:
            if self.http is not None:
                html = await self.http.get_html(job["key"])
                item = parse_company(html, job["key"])
            else:
                item = await companies.fetch(page, job["key"], self.browser_args())
                html = await page.content()
                if parse_company(html, job["key"]) != item:
                    raise ValueError("Publication changed during extraction")
            candidates = extract_candidates(html, job["key"])
            self.store.finish_publication(job, item, candidates)
            self.completed += 1
            self.failure_streak = 0
            print(f"Collected {self.completed}: {item['company_name']}; code evidence: {len(candidates)}", flush=True)
        except asyncio.CancelledError:
            with suppress(LostLease):
                self.store.finish(job, "retry_wait", error="worker cancelled")
            raise
        except LostLease:
            self.failures += 1
        except Exception as exc:
            self.failures += 1
            self.failure_streak += 1
            try:
                message = str(exc)
                status = "blocked" if "HTTP 403" in message else "retry_wait"
                self.store.finish(
                    job, status, error=message, retry_seconds=max(stage.retry_seconds, getattr(exc, "retry_after", 0))
                )
                with self.store.transaction():
                    self.store.db.execute("UPDATE publications SET error=? WHERE source_url=?", (message, job["key"]))
            except LostLease:
                pass
            print(f"Public discovery failed: {exc}", flush=True)
            if self.failure_streak >= 3 or "HTTP 403" in str(exc) or "HTTP 429" in str(exc):
                self.stop.set()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def worker(self, context):
        page = None if self.http is not None else await context.new_page()
        stage = self.settings.public
        try:
            while not self.stop.is_set():
                job = self.store.claim(
                    self.run_id,
                    self.owner,
                    lease_seconds=stage.lease_seconds,
                    max_in_flight=stage.max_in_flight,
                    months=self.months,
                )
                if job is None:
                    break
                await self.process(page, job)
                if self.completed and self.completed % 25 == 0:
                    async with self.exports:
                        export_pipeline(self.store, self.settings.output)
                await asyncio.sleep(stage.delay_seconds)
        finally:
            if page is not None:
                await page.close()


def read_saved_html(path: str) -> str:
    source = Path(path)
    if source.stat().st_size > 10 * 1024 * 1024:
        raise ValueError("Saved publication exceeds 10 MiB")
    return source.read_text(encoding="utf-8")


async def run_public(args) -> int:
    from src.sites.dequienes import companies

    settings = settings_from_args(args)
    settings.output.mkdir(parents=True, exist_ok=True)
    store = QueueStore(settings.output / "companies.sqlite3")
    try:
        action = getattr(args, "action", "collect")
        if action in {"status", "export"}:
            if action == "export":
                export_pipeline(store, settings.output)
            print(json.dumps(store.status(), ensure_ascii=False, indent=2), flush=True)
            return 0
        print(json.dumps(settings.display(), ensure_ascii=False, indent=2), flush=True)
        if getattr(args, "rescan_legacy", False):
            print(f"Legacy pages queued for explicit rescan: {store.rescan_legacy()}", flush=True)
        if getattr(args, "retry_errors", False):
            print(f"Public error jobs explicitly reset: {store.retry_errors()}", flush=True)
        imports = getattr(args, "import_publication", None)
        run_id = store.start_run("public_discovery", settings.public.max_items, run_id=getattr(args, "run_id", None))
        print(f"Public run: {run_id}", flush=True)
        if settings.public.max_items == 0:
            return 0
        if imports:
            from src.sites.dequienes.companies import parse_company

            for url, path in imports:
                item = parse_company(read_saved_html(path), url)
                store.add_publications(item["publication_date"][:7], [url], settings.public.max_attempts)
            worker = PublicWorker(store, settings, run_id, None)
            paths = dict(imports)
            for _ in imports:
                job = store.claim(run_id, worker.owner, lease_seconds=settings.public.lease_seconds, keys=list(paths))
                if job is None:
                    break
                html = read_saved_html(paths[job["key"]])
                store.finish_publication(
                    job, companies.parse_company(html, job["key"]), extract_candidates(html, job["key"])
                )
            return 0
        from camoufox.async_api import AsyncCamoufox

        selection = SimpleNamespace(year=settings.year, month=settings.months, all_months=settings.all_months)
        worker = PublicWorker(store, settings, run_id, None)
        browser_args = worker.browser_args(attempts=settings.public.max_attempts)
        # A separate profile lock permits independent processes to share the queue database.
        with companies.exclusive_profile(settings.profile):
            async with AsyncCamoufox(
                headless="virtual",
                os="linux",
                locale="es-CL",
                persistent_context=True,
                user_data_dir=str(settings.profile.resolve()),
            ) as context:
                if settings.public.transport == "hybrid":
                    from src.sites.dequienes.hybrid import HybridClient

                    client = HybridClient(context, settings.public, worker.request_gate)
                    try:
                        async with client:
                            worker.http = client

                            async def http_sitemap(url, *, index):
                                return companies.parse_sitemap(await client.get_html(url), index=index)

                            await discover_and_collect(context, worker, selection, http_sitemap)
                    finally:
                        atomic_write_json(settings.output / "hybrid_report.json", {"run_id": run_id, **client.metrics})
                else:
                    page = await context.new_page()

                    async def browser_sitemap(url, *, index):
                        return await companies.fetch(page, url, browser_args, sitemap_index=index)

                    try:
                        await discover_and_collect(context, worker, selection, browser_sitemap)
                    finally:
                        await page.close()
        return int(worker.failures > 0)
    finally:
        export_pipeline(store, settings.output)
        store.close()


async def discover_and_collect(context, worker, selection, fetch_sitemap):
    from src.sites.dequienes import companies

    settings, store = worker.settings, worker.store
    months = await fetch_sitemap(companies.INDEX, index=True)
    available = {companies.MONTH_PATH.fullmatch(companies.urlsplit(url).path)[1].replace("/", "-"): url for url in months}
    selected = companies.select_months(available, selection)
    worker.months = selected
    print(f"Archive: {len(available)} months; selected: {', '.join(selected)}", flush=True)
    for month in selected:
        urls = await fetch_sitemap(available[month], index=False)
        if any(
            not companies.urlsplit(url).path.startswith("/diario-oficial/" + month.replace("-", "/") + "/") for url in urls
        ):
            raise ValueError("Publication is outside its sitemap month")
        store.add_publications(month, urls, settings.public.max_attempts)
        print(f"{month}: {len(urls)} publication links", flush=True)
    async with asyncio.TaskGroup() as group:
        for _ in range(settings.public.tabs_per_browser):
            group.create_task(worker.worker(context))
