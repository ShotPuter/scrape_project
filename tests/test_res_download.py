import argparse
import asyncio
import json
import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from src.sites.dequienes.queue import LostLease, QueueStore
from src.sites.dequienes.source import RES_URL
from src.sites.res import download as runner
from src.sites.res.certificates import VERIFY_PATH, certificate_link, download_certificate
from src.sites.res.pdf import cached_pdf, certificate_directory, persist_pdf
from src.sites.res.source import SearchIssue

CVE = "ACuKDHhqsvAd"
LINK = f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={CVE}&codigo=public"
PDF = b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF\n"


def seed(store, cves=(CVE,), max_attempts=3):
    with store.transaction():
        for cve in cves:
            store.db.execute("INSERT INTO documents(cve) VALUES (?)", (cve,))
            store._enqueue("certificate_download", cve, max_attempts=max_attempts)


def test_file_cache_recovers_only_exact_cve_and_intact_pdf(tmp_path):
    result = persist_pdf(tmp_path, CVE, PDF, LINK)
    assert cached_pdf(tmp_path, CVE)["reused"]
    assert cached_pdf(tmp_path, CVE.lower()) is None
    assert certificate_directory(CVE) != certificate_directory(CVE.lower())
    (tmp_path / result["local_path"]).write_bytes(PDF.replace(b"obj", b"xxx"))
    assert cached_pdf(tmp_path, CVE) is None  # Still has PDF markers, but SHA differs.
    assert not list(tmp_path.rglob("*.tmp"))


@pytest.mark.parametrize("data", [b"<html>CAPTCHA</html>", b"%PDF-1.4\ntruncated"])
def test_non_pdf_and_incomplete_downloads_are_not_saved(tmp_path, data):
    with pytest.raises(ValueError):
        persist_pdf(tmp_path, CVE, data, LINK)
    assert not list(tmp_path.rglob("*.pdf"))


def test_certificate_commit_is_atomic_and_fenced(tmp_path):
    store = QueueStore(tmp_path / "companies.sqlite3")
    try:
        seed(store)
        run_id = store.start_run("certificate_download", 1)
        old = store.claim(run_id, "expired", now=1, lease_seconds=1)
        job = store.claim(run_id, "new", now=3)
        result = persist_pdf(tmp_path, CVE, PDF, LINK)
        with pytest.raises(LostLease):
            store.finish_certificate(old, result, now=3)
        with pytest.raises(ValueError):
            store.finish_certificate(job, {**result, "download_url": "https://other.example/pdf"}, now=3)
        assert store.db.execute("SELECT * FROM document_files").fetchall() == []
        assert store.db.execute("SELECT status FROM documents").fetchone() == ("candidate",)
        store.finish_certificate(job, result, now=3)
        assert store.db.execute("SELECT cve,sha256 FROM document_files").fetchone() == (CVE, result["sha256"])
        assert store.db.execute("SELECT status FROM documents").fetchone() == ("downloaded",)
        assert store.status()["queues"]["certificate_download"] == {"completed": 1}
        assert store.claim(run_id, "again", now=4) is None
    finally:
        store.close()


def test_failed_download_does_not_mark_document_downloaded_and_retry_is_scoped(tmp_path):
    store = QueueStore(tmp_path / "companies.sqlite3")
    try:
        seed(store, (CVE, CVE.lower()), max_attempts=1)
        first_run = store.start_run("certificate_download", 1)
        job = store.claim(first_run, "worker", keys=[CVE])
        result = {"cve": CVE, "status": "retry_wait", "error": "incomplete PDF"}
        store.finish_certificate(job, result)
        assert result["status"] == "failed"
        job = store.claim(store.start_run("certificate_download", 1), "worker", keys=[CVE.lower()])
        store.finish_certificate(job, {"cve": CVE.lower(), "status": "captcha_required"})
        assert store.retry_stage_jobs("certificate_download", run_id=first_run, keys=[CVE]) == 1
        assert store.status()["queues"]["certificate_download"] == {"pending": 1, "captcha_required": 1}
        assert store.db.execute("SELECT DISTINCT status FROM documents").fetchall() == [("candidate",)]
    finally:
        store.close()


def test_verifier_errors_and_url_validation():
    assert certificate_link({"d": json.dumps({"Link": LINK})}) == LINK
    for payload in [{}, {"d": "bad-json"}, {"d": {"Link": "https://other.example/pdf"}}, {"d": {"Link": ""}}]:
        with pytest.raises(SearchIssue):
            certificate_link(payload)
    with pytest.raises(SearchIssue) as exc:
        certificate_link({"d": {"ErrorMessage": "Captcha inválido"}})
    assert exc.value.status == "captcha_required"


class Page:
    def __init__(self, tmp_path, *, payload=None, downloaded_url=LINK):
        self.url = RES_URL
        self.listeners = []
        self.payload = payload or {"d": json.dumps({"Link": LINK})}
        self.path = tmp_path / "browser-download"
        self.path.write_bytes(PDF)
        self.downloaded_url = downloaded_url
        self.deleted = False

    async def goto(self, *args, **kwargs):
        return SimpleNamespace(status=200)

    def locator(self, selector):
        page = self

        class Element:
            async def fill(self, value):
                assert value == CVE

            async def get_attribute(self, name):
                return "true"

            async def click(self, **kwargs):
                if selector == "#boxer_content_lnkValidarCVE":
                    assert len(page.listeners) == 2
                    for future, value in page.listeners:
                        future.set_result(value)

        return Element()

    def frame_locator(self, selector):
        return self

    @asynccontextmanager
    async def expect_response(self, predicate, **kwargs):
        async def payload():
            return self.payload

        response = SimpleNamespace(
            url="https://www.registrodeempresasysociedades.cl" + VERIFY_PATH, status=200, json=payload
        )
        assert predicate(response)
        future = asyncio.get_running_loop().create_future()
        self.listeners.append((future, response))
        yield SimpleNamespace(value=future)
        await future

    @asynccontextmanager
    async def expect_download(self, **kwargs):
        async def path():
            return self.path

        async def failure():
            return None

        async def delete():
            self.deleted = True

        download = SimpleNamespace(url=self.downloaded_url, path=path, failure=failure, delete=delete)
        future = asyncio.get_running_loop().create_future()
        self.listeners.append((future, download))
        yield SimpleNamespace(value=future)
        await future


async def test_browser_download_correlates_verification_and_cleans_temp_file(tmp_path):
    page = Page(tmp_path)
    result = await download_certificate(page, CVE, tmp_path)
    assert result["status"] == "downloaded"
    assert page.deleted
    page = Page(tmp_path, downloaded_url="https://other.example/file.pdf")
    with pytest.raises(ValueError):
        await download_certificate(page, CVE, tmp_path)
    assert page.deleted


async def test_opt_in_audio_solver_still_requires_verified_pdf(tmp_path):
    called = []

    async def solver(page, **kwargs):
        called.append(page)

    page = Page(tmp_path, payload={"d": {"ErrorMessage": "Captcha inválido"}})
    with pytest.raises(SearchIssue) as error:
        await download_certificate(page, CVE, tmp_path, captcha_solver=solver)
    assert error.value.status == "captcha_required"
    assert called == [page]
    assert not list(tmp_path.rglob("*.pdf"))


def arguments(tmp_path, **kwargs):
    return argparse.Namespace(
        output=tmp_path,
        profile=tmp_path / "profile",
        action="download",
        cve=None,
        limit=1,
        run_id="pdf-test",
        retry_errors=False,
        max_attempts=3,
        workers=2,
        rate_per_minute=6,
        timeout=60,
        captcha_wait=30,
        headed=False,
        **kwargs,
    )


async def test_crash_recovery_and_repeat_run_need_no_browser(tmp_path, monkeypatch):
    store = QueueStore(tmp_path / "companies.sqlite3")
    seed(store)
    store.close()
    # File and manifest exist, but the process died before committing DB completion.
    result = persist_pdf(tmp_path, CVE, PDF, LINK)

    async def forbidden(*args, **kwargs):
        pytest.fail("Recovery must use the validated local PDF")

    monkeypatch.setattr(runner, "download_certificate", forbidden)
    assert await runner.run(arguments(tmp_path)) == 0
    assert await runner.run(arguments(tmp_path)) == 0
    store = QueueStore(tmp_path / "companies.sqlite3")
    try:
        assert store.status()["queues"]["certificate_download"] == {"completed": 1}
        assert store.db.execute("SELECT attempts FROM jobs").fetchone() == (1,)
        assert store.db.execute("SELECT local_path FROM document_files").fetchone() == (result["local_path"],)
    finally:
        store.close()


async def test_runner_uses_one_browser_and_preserves_captcha_jobs(tmp_path, monkeypatch):
    store = QueueStore(tmp_path / "companies.sqlite3")
    seed(store, (CVE, CVE.lower()))
    store.close()
    launches = []

    class Context:
        def __init__(self):
            self.closed = False

            async def close():
                self.closed = True

            self.pages = [SimpleNamespace(close=close)]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def new_page(self):
            assert not self.closed, "Closing the last persistent-context tab would close Firefox"

            async def close():
                pass

            return SimpleNamespace(close=close)

    def browser(**kwargs):
        launches.append(kwargs)
        return Context()

    async def download(page, cve, output, **kwargs):
        if cve != CVE:
            raise SearchIssue("captcha_required", "Operator needed")
        await asyncio.sleep(0)
        return persist_pdf(output, cve, PDF, LINK)

    monkeypatch.setitem(sys.modules, "camoufox.async_api", SimpleNamespace(AsyncCamoufox=browser))
    monkeypatch.setattr(runner, "download_certificate", download)
    monkeypatch.setattr(QueueStore, "request_delay", lambda *args: 0)
    args = arguments(tmp_path)
    args.limit = 2
    assert await runner.run(args) == 1
    assert len(launches) == 1
    assert launches[0]["firefox_user_prefs"] == {"pdfjs.disabled": True}
    store = QueueStore(tmp_path / "companies.sqlite3")
    try:
        assert store.status()["queues"]["certificate_download"] == {"completed": 1, "captcha_required": 1}
        assert store.db.execute("SELECT count(*) FROM document_files").fetchone() == (1,)
    finally:
        store.close()


def test_all_document_limit_and_invalid_limits():
    assert runner.document_limit("all") is None
    assert runner.document_limit("100") == 100
    assert runner.document_limit("0") == 0
    for value in ["-1", "invalid"]:
        with pytest.raises(argparse.ArgumentTypeError):
            runner.document_limit(value)


def test_search_run_download_selection_includes_public_and_account_cves(tmp_path):
    store = QueueStore(tmp_path / "companies.sqlite3")
    try:
        seed(store, ("PUBLIC0001", "ACCOUNT0001", "OTHER0001"))
        with store.transaction():
            for key in ["selected", "other"]:
                store.db.execute("INSERT INTO companies VALUES (?,?,?,?,?)", (key, key, key, None, "name_candidate"))
            url = "https://dequienes.cl/diario-oficial/2025/01/02/company"
            store.db.execute("INSERT INTO publications(source_url,month) VALUES (?,?)", (url, "2025-01"))
            store.db.execute("INSERT INTO document_sources VALUES (?,?,?,?)", ("PUBLIC0001", url, "selected", "RES link"))
            store.db.execute("INSERT INTO document_sources VALUES (?,?,?,?)", ("OTHER0001", url, "other", "RES link"))
            store.db.execute("INSERT INTO account_document_sources VALUES (?,?,?)", ("ACCOUNT0001", "selected", url))
        store.seed_res_jobs(["selected"])
        run = store.start_run("res_company_lookup", 1)
        store.claim(run, "worker")
        found = runner.selected_cves(store, argparse.Namespace(cve=None, from_search_run=run))
        assert found == ["ACCOUNT0001", "PUBLIC0001"]
    finally:
        store.close()


async def test_download_all_is_restricted_to_the_source_search_run(tmp_path):
    store = QueueStore(tmp_path / "companies.sqlite3")
    seed(store, (CVE, "OTHER0001"))
    with store.transaction():
        store.db.execute(
            "INSERT INTO companies VALUES ('company', 'VERTIKAL GROUP SpA', 'vertikal group spa',NULL,'name_candidate')"
        )
    store.seed_res_jobs()
    source_id = store.start_run("res_company_lookup", 1)
    job = store.claim(source_id, "search")
    store.finish_res_lookup(
        job,
        {
            "company_id": "company",
            "status": "completed",
            "source_url": "https://www.registrodeempresasysociedades.cl/BuscarActuaciones2.aspx",
            "events": [{"cve": CVE, "date": "2016-04-29", "event_type": "CONSTITUCIÓN", "attention_number": "697836"}],
        },
    )
    store.close()
    persist_pdf(tmp_path, CVE, PDF, LINK)
    args = arguments(tmp_path)
    args.limit, args.from_search_run = None, source_id
    assert await runner.run(args) == 0
    store = QueueStore(tmp_path / "companies.sqlite3")
    try:
        assert store.status()["queues"]["certificate_download"] == {"completed": 1, "pending": 1}
        assert runner.selected_cves(store, argparse.Namespace(cve=None, from_search_run=source_id)) == [CVE]
        assert runner.selected_cves(store, argparse.Namespace(cve=["OTHER0001"], from_search_run=source_id)) == []
        with pytest.raises(ValueError):
            runner.selected_cves(store, argparse.Namespace(cve=None, from_search_run="missing"))
    finally:
        store.close()
