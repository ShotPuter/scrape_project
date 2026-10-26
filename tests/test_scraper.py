"""Integration-style tests for the scraper orchestration with a fake HTTP client."""

import pytest

from src.models.publication import PublicationDetail, PublicationSummary
from src.services.scraper import SeprecScraper
from src.storage.file_storage import FileStorage
from src.storage.integrity import read_records
from src.storage.publication_csv import PublicationCsv


class FakeSeprecClient:
    """Minimal in-memory stand-in for SeprecClient."""

    def __init__(self, details):
        self.details = details
        self.requests_made = 0
        self.retries_made = 0
        self.detail_requests = []
        self.closed = False

    async def open(self):
        return None

    async def close(self):
        self.closed = True

    async def get_catalogs(self):
        return []

    async def search_publications(self, filters):
        if filters.page > 1:
            return len(self.details), []
        summaries = []
        for publication_id, detail in self.details.items():
            if isinstance(detail, Exception):
                summaries.append(PublicationSummary(id=publication_id, title="Broken publication"))
                continue
            summaries.append(
                PublicationSummary(
                    id=detail.id,
                    publication_code=detail.publication_code,
                    title=detail.title,
                    summary=detail.summary,
                    fechaPublicacion=detail.publication_date,
                    fechaPublicacionFormato=detail.formatted_publication_date,
                    company=detail.company,
                )
            )
        return len(summaries), summaries

    async def get_publication_detail(self, publication_id):
        self.detail_requests.append(publication_id)
        value = self.details[publication_id]
        if isinstance(value, Exception):
            raise value
        return value


@pytest.mark.asyncio
async def test_scrape_saves_original_and_generated_pdfs(tmp_path, detail_pdf_payload, detail_text_payload):
    details = {
        "228340": PublicationDetail.model_validate(detail_pdf_payload),
        "228345": PublicationDetail.model_validate(detail_text_payload),
    }
    client = FakeSeprecClient(details)

    async with SeprecScraper(
        client=client,
        store=PublicationCsv(tmp_path / "publications.csv"),
        file_storage=FileStorage(tmp_path / "pdf"),
        max_concurrency=2,
    ) as scraper:
        report = await scraper.run(category="1", max_pages=1)

        assert report["status"] == "COMPLETED"
        assert report["items_scraped"] == 2
        assert report["items_failed"] == 0
        assert report["category_name"] == "INSTRUMENTOS"

        saved = read_records(scraper.store.path)
        downloaded = saved["228340"]
        generated = saved["228345"]
        assert downloaded is not None and downloaded.status == "DOWNLOADED"
        assert generated is not None and generated.status == "GENERATED_PDF"
        assert downloaded.file_path is not None
        assert generated.file_path is not None
        assert (tmp_path / "pdf" / "2026" / "10").is_dir()


@pytest.mark.asyncio
async def test_incremental_run_skips_already_stored_publications(tmp_path, detail_pdf_payload, detail_text_payload):
    details = {
        "228340": PublicationDetail.model_validate(detail_pdf_payload),
        "228345": PublicationDetail.model_validate(detail_text_payload),
    }
    client = FakeSeprecClient(details)

    async with SeprecScraper(
        client=client, store=PublicationCsv(tmp_path / "publications.csv"), file_storage=FileStorage(tmp_path / "pdf")
    ) as scraper:
        first = await scraper.run(category="1", max_pages=1)
        second = await scraper.run(category="1")

    assert first["items_scraped"] == 2
    assert second["items_scraped"] == 0
    assert second["items_skipped"] == 2
    assert len(client.detail_requests) == 2  # no repeated detail requests


@pytest.mark.asyncio
async def test_max_items_is_respected(tmp_path, detail_pdf_payload, detail_text_payload):
    details = {
        "228340": PublicationDetail.model_validate(detail_pdf_payload),
        "228345": PublicationDetail.model_validate(detail_text_payload),
    }
    client = FakeSeprecClient(details)

    async with SeprecScraper(
        client=client, store=PublicationCsv(tmp_path / "publications.csv"), file_storage=FileStorage(tmp_path / "pdf")
    ) as scraper:
        report = await scraper.run(category="1", max_items=1)

    assert report["items_scraped"] == 1
    assert len(client.detail_requests) == 1


@pytest.mark.asyncio
async def test_failures_are_retried_on_next_run(tmp_path, detail_pdf_payload, detail_text_payload):
    details = {"228340": PublicationDetail.model_validate(detail_pdf_payload), "228345": RuntimeError("upstream exploded")}
    client = FakeSeprecClient(details)

    async with SeprecScraper(
        client=client, store=PublicationCsv(tmp_path / "publications.csv"), file_storage=FileStorage(tmp_path / "pdf")
    ) as scraper:
        first = await scraper.run(category="1", max_pages=1)
        failed = await scraper.store.filter_unprocessed_ids(["228345"])
        second = await scraper.run(category="1")
        retried = await scraper.store.filter_unprocessed_ids(["228345"])

    assert first["items_failed"] == 1
    assert first["status"] == "COMPLETED_WITH_ERRORS"
    assert failed == ["228345"]
    assert second["items_failed"] == 1
    assert retried == ["228345"]
    assert client.detail_requests.count("228345") == 2


@pytest.mark.asyncio
async def test_context_manager_closes_resources(tmp_path, detail_text_payload):
    client = FakeSeprecClient({"228345": PublicationDetail.model_validate(detail_text_payload)})
    scraper = SeprecScraper(
        client=client, store=PublicationCsv(tmp_path / "publications.csv"), file_storage=FileStorage(tmp_path / "pdf")
    )

    async with scraper:
        assert read_records(scraper.store.path) == {}

    assert client.closed is True


@pytest.mark.asyncio
async def test_scraper_passes_year_and_subcategory_to_api(tmp_path):
    class CapturingClient(FakeSeprecClient):
        async def search_publications(self, filters):
            payload = filters.to_api_payload()
            assert payload["fechaInicial"] == "2025-01-01"
            assert payload["fechaFinal"] == "2025-12-31"
            assert payload["tipoPublicacion"] == "1001"
            assert payload["fecha"] == "5"
            return 0, []

    async with SeprecScraper(
        client=CapturingClient({}),
        store=PublicationCsv(tmp_path / "publications.csv"),
        file_storage=FileStorage(tmp_path / "pdf"),
    ) as scraper:
        report = await scraper.run(
            category="1", sub_category="1001", date_filter="5", start_date="2025-01-01", end_date="2025-12-31"
        )
    assert report["status"] == "COMPLETED"


@pytest.mark.asyncio
async def test_next_page_processed_while_first_document_is_blocked(tmp_path, monkeypatch):
    import asyncio

    from config import settings
    from src.models.publication import PublicationRecord

    monkeypatch.setattr(settings, "page_delay_seconds", 0)
    first_started = asyncio.Event()
    second_finished = asyncio.Event()

    class PagingClient(FakeSeprecClient):
        async def search_publications(self, filters):
            if filters.page == 1:
                return 2, [PublicationSummary(id="slow")]
            if filters.page == 2:
                await first_started.wait()
                return 2, [PublicationSummary(id="fast")]
            return 2, []

    async with SeprecScraper(
        client=PagingClient({}),
        store=PublicationCsv(tmp_path / "publications.csv"),
        file_storage=FileStorage(tmp_path / "pdf"),
        max_concurrency=2,
    ) as scraper:

        async def process(summary, **kwargs):
            if summary.id == "slow":
                first_started.set()
                await asyncio.wait_for(second_finished.wait(), 2)
            else:
                second_finished.set()
            return PublicationRecord(id=summary.id)

        monkeypatch.setattr(scraper, "_process_single_publication", process)
        report = await asyncio.wait_for(scraper.run(), 3)
    assert second_finished.is_set()
    assert report["items_scraped"] == 2


@pytest.mark.asyncio
async def test_queue_budget_counts_failures_and_deduplicates(tmp_path, monkeypatch):
    from config import settings

    monkeypatch.setattr(settings, "page_delay_seconds", 0)

    class PagingClient(FakeSeprecClient):
        async def search_publications(self, filters):
            return 100, [PublicationSummary(id=str(i)) for i in [1, 1, 2, 3, 4]]

    attempted = []
    async with SeprecScraper(
        client=PagingClient({}),
        store=PublicationCsv(tmp_path / "publications.csv"),
        file_storage=FileStorage(tmp_path / "pdf"),
        max_concurrency=3,
    ) as scraper:

        async def process(summary, **kwargs):
            attempted.append(summary.id)

        monkeypatch.setattr(scraper, "_process_single_publication", process)
        report = await scraper.run(max_items=2)
    assert attempted == ["1", "2"]
    assert report["items_failed"] == 2
    assert report["items_skipped"] == 1
