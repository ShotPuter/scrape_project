"""Stress timer, reporting and cancellation without network traffic."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from src import cli
from src.models.publication import PublicationSummary
from src.services import scrape_pipeline


@pytest.mark.asyncio
async def test_timer_writes_final_report_and_cancels_download(tmp_path, monkeypatch):
    args = cli.build_parser().parse_args(["--stress-minutes", "0.001", "--stress-report", str(tmp_path / "report.json")])
    stopped = asyncio.Event()

    class Client:
        class metrics:
            @staticmethod
            def snapshot():
                return {"attempts": 3}

    class Scraper:
        def __init__(self, **kwargs):
            self.client = Client()
            self.failed_documents = 2

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    async def execute(scraper, args):
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setattr(cli, "SeprecScraper", Scraper)
    monkeypatch.setattr(cli, "_execute_scrape", execute)
    (report,) = await asyncio.wait_for(cli._run_discard(args, None), 2)
    assert stopped.is_set()
    assert report["status"] == "DURATION_REACHED"
    assert report["items_failed"] == 2
    assert json.loads(args.stress_report.read_text()) == report
    assert list(tmp_path.iterdir()) == [args.stress_report]


@pytest.mark.parametrize("minutes", ["0", "-1", "nan", "inf"])
def test_invalid_duration_is_rejected(minutes):
    with pytest.raises(SystemExit):
        cli.main(["--stress-minutes", minutes])


@pytest.mark.asyncio
async def test_cancellation_with_full_queue_does_not_wait_for_sentinels(monkeypatch):
    monkeypatch.setattr(scrape_pipeline.settings, "queue_high_watermark", 1)
    worker_started = asyncio.Event()

    async def search(filters):
        return 3, [PublicationSummary(id=str(i)) for i in range(3)]

    async def process(summary, **kwargs):
        worker_started.set()
        await asyncio.Event().wait()

    async def filter_new(summaries):
        return summaries, 0

    async def delay():
        pass

    pipeline = scrape_pipeline.ScrapePipeline(
        client=SimpleNamespace(search_publications=search),
        process_publication=process,
        filter_new_summaries=filter_new,
        polite_delay=delay,
        max_concurrency=1,
        request=scrape_pipeline.ScrapeRequest(),
        category_name=None,
        sub_category_name=None,
    )
    running = asyncio.create_task(pipeline.run())
    await asyncio.wait_for(worker_started.wait(), 2)
    running.cancel()
    (result,) = await asyncio.wait_for(asyncio.gather(running, return_exceptions=True), 2)
    assert isinstance(result, asyncio.CancelledError)
