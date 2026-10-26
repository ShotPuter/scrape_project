"""Assemble execution summary and metrics for publication scraping runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.client.seprec_client import SeprecClient
from src.models.publication import STATUS_FAILED
from src.services.scrape_pipeline import RunCounters, ScrapeRequest
from src.storage.publication_csv import PublicationCsv
from src.utils.logger import setup_logger

logger = setup_logger("run_reporter")

STATUS_COMPLETED = "COMPLETED"
STATUS_COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"
SECONDS_PER_MINUTE = 60.0
MINIMUM_ELAPSED_SECONDS = 0.001


def determine_run_status(counters: RunCounters) -> str:
    """Determine final execution status based on error occurrences and scraped items."""
    if counters.fatal_error and counters.items_scraped == 0:
        return STATUS_FAILED
    if counters.fatal_error or counters.items_failed > 0:
        return STATUS_COMPLETED_WITH_ERRORS
    return STATUS_COMPLETED


@dataclass(frozen=True)
class ReporterContext:
    """Parameter object containing contextual metadata for generating run reports."""

    run_id: str
    started_at: datetime
    finished_at: datetime
    counters: RunCounters
    request: ScrapeRequest
    category_name: str | None
    http_requests_before: int
    http_retries_before: int


class RunReporter:
    """Aggregates execution metrics, formats report payloads, and records summaries."""

    def __init__(self, store: PublicationCsv, client: SeprecClient) -> None:
        self.store = store
        self.client = client

    async def finalize(
        self, *, run_id: str,
        started_at: datetime, finished_at: datetime, counters: RunCounters,
        request: ScrapeRequest, category_name: str | None, http_requests_before: int,
        http_retries_before: int,
    ) -> dict[str, Any]:
        """Finalize scraper execution by computing metrics and logging summary."""
        context = ReporterContext(
            run_id=run_id, started_at=started_at, finished_at=finished_at,
            counters=counters, request=request, category_name=category_name,
            http_requests_before=http_requests_before, http_retries_before=http_retries_before,
        )
        storage_stats = await self.store.get_stats()
        report = self._build_report(context, storage_stats)
        self._log_summary(report, storage_stats)
        return report

    def _build_report(self, context: ReporterContext, storage_stats: dict[str, Any]) -> dict[str, Any]:
        """Construct a detailed dictionary of execution metrics."""
        duration = (context.finished_at - context.started_at).total_seconds()
        elapsed_seconds = max(MINIMUM_ELAPSED_SECONDS, duration)
        counters = context.counters

        files_per_minute = round(SECONDS_PER_MINUTE * counters.items_scraped / elapsed_seconds, 2)
        original_files_per_minute = round(SECONDS_PER_MINUTE * counters.original_files / elapsed_seconds, 2)

        return {
            "run_id": context.run_id,
            "status": determine_run_status(counters),
            "category": context.request.category or "ALL",
            "category_name": context.category_name,
            "started_at": context.started_at.isoformat(),
            "finished_at": context.finished_at.isoformat(),
            "duration_seconds": round(duration, 2),
            "pages_processed": counters.pages_processed,
            "items_scraped": counters.items_scraped,
            "original_files": counters.original_files,
            "generated_files": counters.generated_files,
            "file_bytes": counters.file_bytes,
            "files_per_minute": files_per_minute,
            "original_files_per_minute": original_files_per_minute,
            "items_skipped": counters.items_skipped,
            "items_failed": counters.items_failed,
            "http_requests": self.client.requests_made - context.http_requests_before,
            "http_retries": self.client.retries_made - context.http_retries_before,
            "http_metrics": self._calculate_http_metrics(),
            "csv_path": str(self.store.path),
            "storage_stats": storage_stats,
            "error": counters.fatal_error,
        }

    def _calculate_http_metrics(self) -> dict[str, int]:
        """Calculate differential HTTP status counts for the active run."""
        if not hasattr(self.client, "metrics"):
            return {}
        baseline_counts = getattr(self.client, "metrics_before_run", {})
        return {
            metric_key: metric_count - baseline_counts.get(metric_key, 0)
            for metric_key, metric_count in self.client.metrics.counts.items()
        }

    @staticmethod
    def _log_summary(report: dict[str, Any], storage_stats: dict[str, Any]) -> None:
        """Log concise execution summary with speed and error totals."""
        logger.info(
            "Run [%s] completed in %.1fs: %d saved (%d orig, %d gen) | "
            "%d skipped | %d errors | Speed: %.1f docs/min | Total index: %d",
            report["run_id"], report["duration_seconds"], report["items_scraped"],
            report["original_files"], report["generated_files"], report["items_skipped"],
            report["items_failed"], report["files_per_minute"], storage_stats.get("total_items", 0),
        )
