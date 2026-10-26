"""Pagination producer-consumer pipeline with bounded queue and worker pool."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from config import settings
from src.client.network import work_context
from src.models.publication import PublicationRecord, PublicationSearchFilter, PublicationSummary
from src.utils.logger import setup_logger

logger = setup_logger("scrape_pipeline")

MAX_CONSECUTIVE_SKIPPED_PAGES = 3
PROGRESS_NOTIFICATION_INTERVAL = 10
MILLISECONDS_PER_SECOND = 1_000.0

ProcessPublication = Callable[..., Awaitable[PublicationRecord | None]]
FilterSummaries = Callable[[list[PublicationSummary]], Awaitable[tuple[list[PublicationSummary], int]]]
PoliteDelay = Callable[[], Awaitable[None]]


class PublicationSearchClient(Protocol):
    async def search_publications(self, filters: PublicationSearchFilter) -> tuple[int, list[PublicationSummary]]: ...


@dataclass
class RunCounters:
    """Tracks running execution metrics across workers and pagination passes."""

    pages_processed: int = 0
    original_files: int = 0
    generated_files: int = 0
    file_bytes: int = 0
    items_scraped: int = 0
    items_skipped: int = 0
    items_failed: int = 0
    consecutive_skipped_pages: int = 0
    scheduled_count: int = 0
    fatal_error: str | None = None


@dataclass(frozen=True)
class ScrapeRequest:
    """
    Parameter Object pattern: encapsulates parameters for a single scraping job.
    Avoids parameter sprawl in pipeline orchestrators.
    """

    category: str = ""
    sub_category: str = ""
    date_filter: str = ""
    start_date: str = ""
    end_date: str = ""
    max_pages: int | None = None
    max_items: int | None = None
    incremental: bool = True


class ScrapePipeline:
    """
    Coordinates asynchronous page production and concurrent worker processing
    via bounded queues.
    """

    def __init__(
        self,
        *,
        client: PublicationSearchClient,
        process_publication: ProcessPublication,
        filter_new_summaries: FilterSummaries,
        polite_delay: PoliteDelay,
        max_concurrency: int,
        request: ScrapeRequest,
        category_name: str | None,
        sub_category_name: str | None,
    ) -> None:
        self.client = client
        self.process_publication = process_publication
        self.filter_new_summaries = filter_new_summaries
        self.polite_delay = polite_delay
        self.max_concurrency = max_concurrency
        self.request = request
        self.category_name = category_name
        self.sub_category_name = sub_category_name
        self.category_code = request.sub_category or request.category
        self._queued_timestamps: dict[str, float] = {}
        self._pipeline_start_time: float = 0.0

    async def run(self) -> RunCounters:
        """
        Execute producer-consumer tasks using structured TaskGroup concurrency.
        Returns final execution counters upon queue completion.
        """
        self._pipeline_start_time = time.monotonic()
        counters = RunCounters()
        queue: asyncio.Queue[PublicationSummary | None] = asyncio.Queue(maxsize=settings.queue_high_watermark)
        scheduled_identifiers: set[str] = set()

        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(self._produce_pages(queue, scheduled_identifiers, counters))
            for _ in range(self.max_concurrency):
                task_group.create_task(self._consume_queue(queue, counters))

        return counters

    async def _produce_pages(
        self, queue: asyncio.Queue[PublicationSummary | None], scheduled_identifiers: set[str], counters: RunCounters
    ) -> None:
        """Fetch pages iteratively and enqueue unprocessed publication summaries."""
        page_number = 1
        try:
            while not self.request.max_pages or page_number <= self.request.max_pages:
                if self.request.max_items is not None and counters.scheduled_count >= self.request.max_items:
                    logger.info("Reached maximum scheduled item budget", extra={"max_items": self.request.max_items})
                    break
                search_filter = PublicationSearchFilter(
                    page=page_number,
                    page_size=settings.page_size,
                    category=self.request.category,
                    subcategory=self.request.sub_category,
                    date_filter=self.request.date_filter,
                    start_date=self.request.start_date,
                    end_date=self.request.end_date,
                )
                total_count, summaries = await self.client.search_publications(search_filter)
                if not summaries:
                    break
                counters.pages_processed += 1
                logger.info(
                    "Retrieved search result page",
                    extra={
                        "page": page_number,
                        "total_pages": math.ceil(total_count / settings.page_size) if total_count else page_number,
                        "items_on_page": len(summaries),
                        "queue_backlog": queue.qsize(),
                    },
                )

                candidates = summaries
                if self.request.incremental:
                    candidates, skipped_count = await self.filter_new_summaries(summaries)
                    counters.items_skipped += skipped_count
                    counters.consecutive_skipped_pages = (
                        counters.consecutive_skipped_pages + 1 if not candidates else 0
                    )
                    if counters.consecutive_skipped_pages >= MAX_CONSECUTIVE_SKIPPED_PAGES and not self.request.date_filter:
                        logger.info(
                            "Consecutive skip threshold reached; incremental run caught up",
                            extra={"skipped_pages": counters.consecutive_skipped_pages},
                        )
                        break

                for summary in candidates:
                    if summary.id in scheduled_identifiers:
                        counters.items_skipped += 1
                        continue
                    if self.request.max_items is not None and counters.scheduled_count >= self.request.max_items:
                        break
                    scheduled_identifiers.add(summary.id)
                    counters.scheduled_count += 1
                    self._queued_timestamps[summary.id] = time.monotonic()
                    await queue.put(summary)
                page_number += 1
                await self.polite_delay()
        except Exception as error:  # noqa: BLE001 - producer boundary records fatal pagination errors
            counters.fatal_error = str(error)
            logger.error(
                "Pagination aborted due to unexpected error", extra={"page": page_number, "error": str(error)}, exc_info=True
            )
        finally:
            for _ in range(self.max_concurrency):
                await queue.put(None)

    async def _consume_queue(self, queue: asyncio.Queue[PublicationSummary | None], counters: RunCounters) -> None:
        """Worker loop consuming items until sentinel value (None) is encountered."""
        while True:
            summary = await queue.get()
            queued_at = (
                self._queued_timestamps.pop(summary.id, time.monotonic()) if summary is not None else time.monotonic()
            )
            queue_wait_duration = round((time.monotonic() - queued_at) * MILLISECONDS_PER_SECOND, 3)
            reset_token = work_context.set({"worker_queue_ms": queue_wait_duration})
            try:
                if summary is None:
                    return
                record = await self.process_publication(
                    summary,
                    category_code=self.category_code,
                    category_name=self.category_name,
                    sub_category_name=self.sub_category_name,
                )
                if record is None:
                    counters.items_failed += 1
                else:
                    counters.items_scraped += 1
                    counters.original_files += int(record.has_original_file)
                    counters.generated_files += int(not record.has_original_file)
                    counters.file_bytes += record.file_size_bytes or 0
                completed_count = counters.items_scraped + counters.items_failed
                if completed_count % PROGRESS_NOTIFICATION_INTERVAL == 0:
                    elapsed_minutes = max(0.01, (time.monotonic() - self._pipeline_start_time) / 60.0)
                    rate_per_minute = round(counters.items_scraped / elapsed_minutes, 1)
                    logger.info(
                        "Progress: %d saved (%d orig, %d gen) | %d failed | Queue: %d | Rate: %.1f docs/min",
                        counters.items_scraped,
                        counters.original_files,
                        counters.generated_files,
                        counters.items_failed,
                        queue.qsize(),
                        rate_per_minute,
                    )
            finally:
                work_context.reset(reset_token)
                queue.task_done()
