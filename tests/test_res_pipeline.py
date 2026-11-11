import argparse
import json
import sys
from types import SimpleNamespace

import pytest

from src.sites.dequienes.pipeline import PublicWorker, company_keys, run_public, selection_months
from src.sites.dequienes.queue import QueueStore
from src.sites.dequienes.settings import PipelineSettings, StageSettings
from src.sites.res import pipeline


def options(tmp_path, **overrides):
    return argparse.Namespace(
        **(
            {
                "output": tmp_path / "fresh",
                "profile": tmp_path / "profiles" / "search",
                "year": 2025,
                "month": ["1"],
                "limit": 3,
                "run_id": "fresh-test",
                "collect_workers": 4,
                "collect_rate_per_minute": 60,
                "rate_per_minute": 6,
                "timeout": 60,
                "captcha_wait": 30,
                "max_attempts": 3,
                "retry_errors": False,
                "headed": False,
                "browsers": 5,
                "tabs_per_browser": 2,
                "workers": None,
                "checkpoint_interval": 15,
                "proxy_provider": "direct",
                "no_telegram": True,
                "telegram_interval_minutes": None,
            }
            | overrides
        )
    )


def insert_companies(store, count):
    with store.transaction():
        for i in range(count):
            key = f"company-{i}"
            store.db.execute("INSERT OR IGNORE INTO companies VALUES (?,?,?,?,?)", (key, key, key, None, "name_candidate"))
            url = f"https://dequienes.cl/diario-oficial/2025/01/02/{key}"
            store.db.execute("INSERT OR IGNORE INTO publications(source_url,month) VALUES (?,?)", (url, "2025-01"))
            store.db.execute("INSERT OR IGNORE INTO company_sources VALUES (?,?)", (key, url))


@pytest.mark.parametrize("search_code", [0, 1])
async def test_empty_directory_collects_then_searches_then_downloads_all_and_resumes(tmp_path, monkeypatch, search_code):
    args = options(tmp_path)
    calls = []

    async def collect(public):
        calls.append("collect")
        assert public.limit is None and public.company_target == 3
        assert public.transport == "hybrid"
        assert public.proxy_provider == "direct"
        store = QueueStore(public.output / "companies.sqlite3")
        assert store.status()["companies"] == 0
        insert_companies(store, 4)  # In-flight public requests can finish above the target.
        store.start_run("public_discovery", run_id=public.run_id)
        public.on_collection_progress(store, public.run_id, [], True)
        store.close()
        return 0

    async def search(search_args):
        calls.append("search")
        assert len(search_args.company_id) == search_args.limit == 3
        assert search_args.browsers == 5 and search_args.tabs_per_browser == 2
        return search_code

    async def download(pdf_args):
        calls.append("download")
        assert pdf_args.limit is None
        assert pdf_args.from_search_run == args.run_id
        assert pdf_args.run_id == args.run_id + "-pdf"
        assert pdf_args.profile == args.profile
        return 0

    monkeypatch.setattr(pipeline, "run_public", collect)
    monkeypatch.setattr(pipeline.search, "run", search)
    monkeypatch.setattr(pipeline.download, "run", download)
    assert not args.output.exists()
    assert await pipeline.run(args) == search_code
    assert calls == ["collect", "search", "download"]
    assert await pipeline.run(args) == search_code
    assert calls == ["collect", "search", "download", "search", "download"]
    state = json.loads((args.output / "pipelines" / "fresh-test.json").read_text())
    assert state["company_ids"] == ["company-0", "company-1", "company-2"]
    assert state["state"] == ("stopped" if search_code else "completed")


async def test_insufficient_collection_does_not_run_res_and_preserves_progress(tmp_path, monkeypatch):
    async def collect(public):
        store = QueueStore(public.output / "companies.sqlite3")
        insert_companies(store, 1)
        store.close()
        return 1

    async def forbidden(*args):
        pytest.fail("RES must wait for the company target")

    monkeypatch.setattr(pipeline, "run_public", collect)
    monkeypatch.setattr(pipeline.search, "run", forbidden)
    args = options(tmp_path)
    assert await pipeline.run(args) == 1
    state = json.loads((args.output / "pipelines" / "fresh-test.json").read_text())
    assert state["state"] == "stopped" and state["phase"] == "collect"
    assert state["company_ids"] == []


async def test_selection_mismatch_does_not_overwrite_saved_pipeline(tmp_path):
    args = options(tmp_path)
    path = args.output / "pipelines" / "fresh-test.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"selection": {"year": 2024}, "company_ids": ["saved"]}))
    before = path.read_bytes()
    with pytest.raises(ValueError, match="different"):
        await pipeline.run(args)
    assert path.read_bytes() == before


async def test_unique_company_target_counts_duplicates_and_does_not_repeat_collection(tmp_path, monkeypatch):
    store = QueueStore(tmp_path / "companies.sqlite3")
    urls = [f"https://dequienes.cl/diario-oficial/2025/01/02/company-{i}" for i in range(4)]
    names = ["ALFA SpA", "ALFA SpA", "BETA SpA", "GAMMA SpA"]
    store.add_publications("2025-01", urls)
    run_id = store.start_run("public_discovery")

    async def get_html(url):
        return f"<h1>{names[urls.index(url)]}</h1><h2>Publicado el 02/01/2025</h2>"

    settings = PipelineSettings(output=tmp_path, public=StageSettings(delay_seconds=0))
    worker = PublicWorker(store, settings, run_id, ["2025-01"], company_target=2)
    worker.http = SimpleNamespace(get_html=get_html)
    await worker.worker(None)
    assert store.status()["companies"] == 2
    assert store.status()["queues"]["public_discovery"] == {"completed": 3, "pending": 1}
    store.close()
    monkeypatch.setitem(sys.modules, "camoufox.async_api", None)
    assert await run_public(argparse.Namespace(output=tmp_path, company_target=2)) == 0


async def test_fresh_pipeline_actual_queues_pdf_and_second_run_skip_finished_work(tmp_path, monkeypatch):
    from src.sites.res.pdf import persist_pdf

    calls = []

    async def collect(public):
        calls.append("collect")
        store = QueueStore(public.output / "companies.sqlite3")
        insert_companies(store, 3)
        store.close()
        return 0

    class Browser:
        pages = []

        async def __aenter__(self):
            calls.append("browser")
            return self

        async def __aexit__(self, *args):
            pass

        async def new_page(self):
            return object()

    async def lookup(page, company, **kwargs):
        events = []
        if company["company_id"] == "company-0":
            events = [{"cve": "CODE000001", "date": "2025-01-02", "event_type": "CONSTITUCIÓN", "attention_number": "1"}]
        return {
            **company,
            "status": "completed" if events else "not_found",
            "events": events,
            "source_url": "https://www.registrodeempresasysociedades.cl/BuscarActuaciones2.aspx",
        }

    async def download(page, cve, output, **kwargs):
        calls.append(cve)
        return persist_pdf(output, cve, b"%PDF-1.4\n%%EOF\n", "https://www.registrodeempresasysociedades.cl/pdf")

    monkeypatch.setattr(pipeline, "run_public", collect)
    monkeypatch.setitem(sys.modules, "camoufox.async_api", SimpleNamespace(AsyncCamoufox=lambda **k: Browser()))
    monkeypatch.setattr(pipeline.search, "lookup", lookup)
    monkeypatch.setattr(pipeline.download, "download_certificate", download)
    monkeypatch.setattr(QueueStore, "request_delay", lambda *args: 0)
    args = options(tmp_path)
    assert await pipeline.run(args) == 0
    before = list(calls)
    assert await pipeline.run(args) == 0
    assert calls == before
    store = QueueStore(args.output / "companies.sqlite3")
    try:
        assert store.status()["queues"]["res_company_lookup"] == {"completed": 1, "not_found": 2}
        assert store.status()["queues"]["certificate_download"] == {"completed": 1}
        (relative,) = store.db.execute("SELECT local_path FROM document_files").fetchone()
        assert (args.output / relative).is_file()
    finally:
        store.close()


def test_company_target_is_scoped_to_selected_month(tmp_path):
    store = QueueStore(tmp_path / "companies.sqlite3")
    try:
        insert_companies(store, 3)
        assert company_keys(store, selection_months(2025, ["1"])) == ["company-0", "company-1", "company-2"]
        assert company_keys(store, selection_months(2025, ["2"])) == []
        assert company_keys(store, selection_months(2024, [])) == []
    finally:
        store.close()


async def test_bootstrap_exception_in_task_group_is_blocked_with_reason_not_interrupted(tmp_path, monkeypatch):
    from src.sites.dequienes.hybrid import BootstrapFailure

    async def fail(public):
        public.on_collection_phase({"phase": "automatic_browser_check", "last_http_status": 403})
        raise BootstrapFailure("DeQuienes sitemap HTTP 403", state="blocked")

    monkeypatch.setattr(pipeline, "run_public", fail)
    args = options(tmp_path)
    with pytest.raises(ExceptionGroup):
        await pipeline.run(args)
    saved = json.loads((args.output / "public_collection" / "fresh-test-public.json").read_text())
    assert saved["state"] == "blocked"
    assert saved["phase"] == "automatic_browser_check"
    assert saved["last_http_status"] == 403
    assert "DeQuienes sitemap HTTP 403" in saved["stop_reason"]
    state = json.loads((args.output / "pipelines" / "fresh-test.json").read_text())
    assert state["state"] == "blocked"
    assert "HTTP 403" in state["error"]["summary"]


@pytest.mark.parametrize("choice, expected", [("inherit", "webshare"), ("direct", "direct")])
async def test_collection_route_is_explicit_and_does_not_change_res_or_pdf(tmp_path, monkeypatch, choice, expected):
    monkeypatch.setattr("dotenv.load_dotenv", lambda: None)
    args = options(tmp_path, proxy_provider="webshare", collect_proxy_provider=choice)
    calls = []

    async def public(public_args):
        calls.append("public")
        assert public_args.proxy_provider == expected
        assert public_args.profile.name == "search-public" + ("-direct" if choice == "direct" else "")
        store = QueueStore(public_args.output / "companies.sqlite3")
        insert_companies(store, 3)
        store.close()
        return 0

    async def search(search_args):
        calls.append("res")
        assert search_args.proxy_provider == "webshare"
        assert search_args.profile == args.profile
        return 0

    async def pdf(pdf_args):
        calls.append("pdf")
        assert pdf_args.proxy_provider == "webshare"
        assert pdf_args.profile == args.profile
        return 0

    monkeypatch.setattr(pipeline, "run_public", public)
    monkeypatch.setattr(pipeline.search, "run", search)
    monkeypatch.setattr(pipeline.download, "run", pdf)
    assert await pipeline.run(args) == 0
    assert calls == ["public", "res", "pdf"]
    state = json.loads((args.output / "pipelines" / "fresh-test.json").read_text())
    assert state["proxy_providers"] == {"collect": expected, "res": "webshare"}
