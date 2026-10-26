"""Scraper orchestration: collaborators, per-publication persistence, and reporting."""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import datetime
from types import TracebackType
from typing import Any, NamedTuple

from config import settings
from src.client.config import ClientConfig
from src.client.network import work_context
from src.client.seprec_client import SeprecClient
from src.models.publication import (
    STATUS_DOWNLOADED,
    STATUS_GENERATED_PDF,
    PublicationDetail,
    PublicationRecord,
    PublicationSummary,
)
from src.services.category_resolver import CategoryResolver
from src.services.run_reporter import RunReporter
from src.services.scrape_pipeline import ScrapePipeline, ScrapeRequest
from src.storage.file_storage import FileStorage, SavedDocument
from src.storage.publication_csv import PublicationCsv
from src.utils.logger import setup_logger

logger = setup_logger("scraper_service")

RUN_IDENTIFIER_LENGTH = 8
MINIMUM_IO_WORKERS = 8
IO_WORKERS_PER_CPU_CORE = 2
DEFAULT_FALLBACK_CPU_COUNT = 4
MAXIMUM_COMPLETED_RECORDS_HISTORY = 10_000
MONITOR_PROBE_INTERVAL_SECONDS = 0.5
METRICS_REPORTING_WINDOW_SECONDS = 60.0
MILLISECONDS_PER_SECOND = 1_000.0


class CompletedDownload(NamedTuple):
    timestamp: float
    size_bytes: int
    is_original: bool


def build_record(
    detail: PublicationDetail,
    summary: PublicationSummary,
    category_code: str | None,
    category_name: str | None,
    document: SavedDocument,
) -> PublicationRecord:
    company = detail.company or summary.company
    return PublicationRecord(
        id=detail.id,
        publication_code=detail.publication_code or summary.publication_code,
        former_publication_code=detail.former_publication_code,
        title=detail.title or summary.title,
        summary=detail.summary or summary.summary,
        registration_number=company.registration_number if company else None,
        former_registration_number=(company.former_registration_number if company else None),
        company_name=company.company_name if company else None,
        publication_date=detail.publication_date or summary.publication_date,
        formatted_publication_date=(detail.formatted_publication_date or summary.formatted_publication_date),
        category_code=category_code,
        category_name=category_name,
        has_original_file=document.is_original,
        file_name=document.filename,
        file_path=str(document.path),
        file_size_bytes=document.size_bytes,
        status=STATUS_DOWNLOADED if document.is_original else STATUS_GENERATED_PDF,
    )


class SeprecScraper:
    def __init__(
        self,
        client: SeprecClient | None = None,
        store: PublicationCsv | None = None,
        file_storage: FileStorage | None = None,
        max_concurrency: int | None = None,
        client_config: ClientConfig | None = None,
    ):
        self.max_concurrency = max_concurrency or (
            client_config.connection_limit if client_config else settings.max_concurrent_requests
        )
        self.client = client or SeprecClient(config=client_config)
        self.store = store or PublicationCsv()
        self.file_storage = file_storage or FileStorage()
        self.semaphore = asyncio.Semaphore(self.max_concurrency)
        self._completed_downloads: deque[CompletedDownload] = deque(maxlen=MAXIMUM_COMPLETED_RECORDS_HISTORY)
        self._monitor_task: asyncio.Task[None] | None = None
        self._active_run_id: str | None = None
        self._categories = CategoryResolver(self.client)
        self._reporter = RunReporter(self.store, self.client)

        cpu_count = os.cpu_count() or DEFAULT_FALLBACK_CPU_COUNT
        allocated_workers = max(MINIMUM_IO_WORKERS, cpu_count * IO_WORKERS_PER_CPU_CORE)
        self._executor = ThreadPoolExecutor(max_workers=allocated_workers, thread_name_prefix="scraper-io")

    async def __aenter__(self) -> SeprecScraper:
        try:
            await self.store.load()
            await self.client.open()
        except BaseException:
            try:
                # open() may have created sessions before failing; release them.
                with suppress(Exception):
                    await self.client.close()
            finally:
                self._executor.shutdown(wait=False)
            raise

        self._monitor_task = asyncio.create_task(self._monitor_performance())
        logger.debug(
            "Configured network runtime settings: workers=%d, proxy_mode=%s",
            self.max_concurrency,
            getattr(self.client, "proxy_mode", "direct"),
        )
        return self

    async def __aexit__(
        self, _exc_type: type[BaseException] | None, _exc_val: BaseException | None, _exc_tb: TracebackType | None
    ) -> None:
        if self._monitor_task:
            self._monitor_task.cancel()
            await asyncio.gather(self._monitor_task, return_exceptions=True)
        try:
            await self.client.close()
        finally:
            self._executor.shutdown(wait=False)

    async def _monitor_performance(self) -> None:
        last_logged_timestamp = time.monotonic()

        while True:
            await asyncio.sleep(MONITOR_PROBE_INTERVAL_SECONDS)

            current_timestamp = time.monotonic()
            if current_timestamp - last_logged_timestamp < settings.metrics_interval_seconds:
                continue

            window_start_timestamp = current_timestamp - METRICS_REPORTING_WINDOW_SECONDS
            recent_downloads = [
                download for download in self._completed_downloads if download.timestamp >= window_start_timestamp
            ]
            original_download_count = sum(download.is_original for download in recent_downloads)
            generated_download_count = len(recent_downloads) - original_download_count
            total_pdf_bytes = sum(download.size_bytes for download in recent_downloads)

            logger.info(
                "Throughput (last 60s): %d files (%d orig, %d gen) | %d KiB | Run: %s",
                len(recent_downloads),
                original_download_count,
                generated_download_count,
                round(total_pdf_bytes / 1024),
                self._active_run_id,
            )
            last_logged_timestamp = current_timestamp

    def _start_run(self) -> tuple[str, datetime]:
        run_identifier = uuid.uuid4().hex[:RUN_IDENTIFIER_LENGTH]
        self._active_run_id = run_identifier
        self.client.run_id = run_identifier
        return run_identifier, datetime.utcnow()

    def _capture_metrics_baseline(self) -> tuple[int, int]:
        self.client.metrics_before_run = dict(self.client.metrics.counts) if hasattr(self.client, "metrics") else {}
        return self.client.requests_made, self.client.retries_made

    async def _process_single_publication(
        self,
        summary: PublicationSummary,
        category_code: str | None = None,
        category_name: str | None = None,
        sub_category_name: str | None = None,
    ) -> PublicationRecord | None:
        category_label = sub_category_name or category_name
        is_debug_timing_enabled = logger.isEnabledFor(logging.DEBUG)
        slot_acquisition_started = time.monotonic() if is_debug_timing_enabled else 0.0

        async with self.semaphore:
            timing_metadata: dict[str, Any] | None = None
            if is_debug_timing_enabled:
                timing_metadata = {
                    **(work_context.get() or {}),
                    "run_id": self._active_run_id,
                    "publication_id": summary.id,
                    "worker_slot_wait_ms": round((time.monotonic() - slot_acquisition_started) * MILLISECONDS_PER_SECOND, 3),
                }

            try:
                detail = await self.client.get_publication_detail(summary.id)
                saved_document = await self._save_document(detail, timing_metadata)
                record = build_record(detail, summary, category_code, category_label, saved_document)

                persistence_start_time = time.monotonic() if timing_metadata else 0.0
                await self.store.save(record)

                if timing_metadata is not None:
                    timing_metadata["csv_save_ms"] = round(
                        (time.monotonic() - persistence_start_time) * MILLISECONDS_PER_SECOND, 3
                    )
                    timing_metadata["pdf_bytes"] = record.file_size_bytes
                    timing_metadata["original"] = record.has_original_file
                    logger.debug("Publication document persisted successfully", extra=timing_metadata)

                self._completed_downloads.append(
                    CompletedDownload(
                        timestamp=time.monotonic(),
                        size_bytes=record.file_size_bytes or 0,
                        is_original=record.has_original_file,
                    )
                )
                return record

            except Exception as processing_error:  # noqa: BLE001 - one publication must not abort the batch
                logger.error(
                    "Failed to process publication document",
                    extra={"publication_id": summary.id, "error": str(processing_error)},
                    exc_info=True,
                )
                return None

    async def _save_document(self, detail: PublicationDetail, timing_metadata: dict[str, Any] | None) -> SavedDocument:
        current_loop = asyncio.get_running_loop()
        submitted_at = time.monotonic() if timing_metadata else 0.0

        def save() -> SavedDocument:
            if timing_metadata is None:
                return self.file_storage.save_publication_document(detail)
            started_at = time.monotonic()
            document = self.file_storage.save_publication_document(detail)
            timing_metadata["executor_wait_ms"] = round((started_at - submitted_at) * MILLISECONDS_PER_SECOND, 3)
            timing_metadata["file_save_ms"] = round((time.monotonic() - started_at) * MILLISECONDS_PER_SECOND, 3)
            return document

        return await current_loop.run_in_executor(self._executor, save)

    async def _filter_new_summaries(self, summaries: list[PublicationSummary]) -> tuple[list[PublicationSummary], int]:
        publication_identifiers = [summary.id for summary in summaries]
        unprocessed_identifiers = set(await self.store.filter_unprocessed_ids(publication_identifiers))
        candidates = [summary for summary in summaries if summary.id in unprocessed_identifiers]
        return candidates, len(summaries) - len(candidates)

    @staticmethod
    async def _polite_delay() -> None:
        if settings.page_delay_seconds and not settings.http_requests_per_second:
            await asyncio.sleep(settings.page_delay_seconds)

    async def run(
        self,
        category: str = "",
        sub_category: str = "",
        date_filter: str = "",
        start_date: str = "",
        end_date: str = "",
        max_pages: int | None = None,
        max_items: int | None = None,
        incremental: bool = True,
        request: ScrapeRequest | None = None,
    ) -> dict[str, Any]:
        """Execute one publication category scraping run.

        Args:
            category: High-level publication category identifier.
            sub_category: Specific publication sub-category code.
            date_filter: Date interval preset.
            start_date: Custom date range start (YYYY-MM-DD).
            end_date: Custom date range end (YYYY-MM-DD).
            max_pages: Ceiling for paginated pages to fetch.
            max_items: Total items to process across successes and failures.
            incremental: Skip already stored publications when True.
            request: Optional pre-configured ScrapeRequest object.

        Returns:
            Dictionary containing run performance and summary metrics.
        """
        active_request = request or ScrapeRequest(
            category=category,
            sub_category=sub_category,
            date_filter=date_filter,
            start_date=start_date,
            end_date=end_date,
            max_pages=max_pages,
            max_items=max_items,
            incremental=incremental,
        )
        run_identifier, started_at = self._start_run()
        category_name, sub_category_name = await self._categories.resolve(
            active_request.category, active_request.sub_category
        )

        logger.info(
            "Starting scrape run [%s] | Category: %s (%s) | Date: %s | Incremental: %s",
            run_identifier,
            category_name or active_request.category or "ALL",
            sub_category_name or active_request.sub_category or "-",
            active_request.date_filter or "any",
            active_request.incremental,
        )
        http_requests_before, http_retries_before = self._capture_metrics_baseline()

        pipeline = ScrapePipeline(
            client=self.client,
            process_publication=self._process_single_publication,
            filter_new_summaries=self._filter_new_summaries,
            polite_delay=self._polite_delay,
            max_concurrency=self.max_concurrency,
            request=active_request,
            category_name=category_name,
            sub_category_name=sub_category_name,
        )
        counters = await pipeline.run()

        return await self._reporter.finalize(
            run_id=run_identifier,
            started_at=started_at,
            finished_at=datetime.utcnow(),
            counters=counters,
            request=active_request,
            category_name=category_name,
            http_requests_before=http_requests_before,
            http_retries_before=http_retries_before,
        )
