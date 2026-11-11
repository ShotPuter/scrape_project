import argparse
import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from src.sites.dequienes.queue import QueueStore
from src.sites.res.browser import lookup
from src.sites.res.source import (
    DETAILS_PATH,
    FORM_URL,
    HOST,
    SEARCH_PATH,
    SearchIssue,
    match_actions,
    parse_actions,
    search_link,
)


def row(cve="ACiWqbFTpsmy", name="Aero inspecciones Spa", rut="77755694-0"):
    return {
        "run": rut,
        "rso_razonsocial": name,
        "rso_fecha": "07-05-2023",
        "tac_glosa": "CONSTITUCIÓN",
        "rso_numeroatencion": 6301603,
        "frec_glosa": cve,
    }


def envelope(body):
    return {"d": json.dumps(body)}


def test_parse_official_action_fields_and_case():
    (event,) = parse_actions(envelope([row()]))
    assert event["cve"] == "ACiWqbFTpsmy"
    assert event["date"] == "2023-05-07"
    assert event["rut"] == "77755694-0"
    assert event["attention_number"] == "6301603"


@pytest.mark.parametrize("body", [{}, []])
def test_only_explicit_empty_backend_response_is_not_found(body):
    assert parse_actions(envelope(body)) == []


@pytest.mark.parametrize(
    "payload", [{}, envelope(None), envelope({"unexpected": "field"}), {"d": "invalid-json"}, envelope([{}])]
)
def test_unknown_response_cannot_be_treated_as_empty(payload):
    with pytest.raises(SearchIssue):
        parse_actions(payload)


def test_matching_by_rut_supports_name_changes_and_name_ambiguity_is_not_guessed():
    actions = parse_actions(envelope([row()]))
    assert match_actions(actions, {"rut": "77755694-0", "company_name": "Changed name"})[0] == "completed"
    assert match_actions(actions, {"rut": "78054070-2", "company_name": "Aero inspecciones Spa"})[0] == "ambiguous_match"
    actions += parse_actions(envelope([row(name="Aero inspecciones Spa", rut="78054070-2")]))
    assert match_actions(actions, {"rut": None, "company_name": "AERO INSPECCIONES SPA"}) == ("ambiguous_match", [])


def test_captcha_error_and_result_url_validation():
    with pytest.raises(SearchIssue) as exc:
        search_link(envelope({"ErrorMessage": "Captcha inválido"}))
    assert exc.value.status == "captcha_required"
    with pytest.raises(SearchIssue):
        search_link(envelope({"Link": "https://other.example/file"}))
    assert search_link(envelope({"Link": "Busqueda/ActuacionDetalle.aspx?codigo=public"})).startswith("https://" + HOST)


def seed(store, rut="77755694-0", name="AERO INSPECCIONES SpA"):
    with store.transaction():
        store.db.execute(
            "INSERT OR IGNORE INTO companies VALUES (?,?,?,?,?)", ("rut:" + rut, name, name.casefold(), rut, "rut")
        )
    return "rut:" + rut


def test_res_queue_independent_and_result_commit_is_atomic(tmp_path):
    store = QueueStore(tmp_path / "companies.sqlite3")
    try:
        key = seed(store)
        with store.transaction():
            store._enqueue("company_lookup", key, status="no_cve_found")
        store.seed_res_jobs()
        store.seed_res_jobs()
        job = store.claim(store.start_run("res_company_lookup", 1), "res")
        result = {
            "company_id": key,
            "status": "completed",
            "source_url": FORM_URL,
            "events": parse_actions(envelope([row()])),
        }
        bad = {**result, "events": result["events"] + [dict(result["events"][0], cve="bad/path")]}
        with pytest.raises(ValueError):
            store.finish_res_lookup(job, bad)
        assert store.status()["documents"] == 0
        store.finish_res_lookup(job, result)
        assert store.status()["queues"]["company_lookup"] == {"no_cve_found": 1}
        assert store.status()["queues"]["res_company_lookup"] == {"completed": 1}
        assert store.status()["queues"]["certificate_download"] == {"pending": 1}
        assert store.db.execute("SELECT cve FROM res_document_sources").fetchall() == [("ACiWqbFTpsmy",)]
    finally:
        store.close()


class Reply:
    status = 200

    def __init__(self, path, body):
        self.url = "https://" + HOST + path
        self.body = body

    async def json(self):
        return envelope(self.body)


class Element:
    def __init__(self, page, selector):
        self.page, self.selector = page, selector

    async def select_option(self, value):
        pass

    async def fill(self, value):
        self.page.query = value

    async def click(self, **kwargs):
        if self.selector == "#btnBuscar":
            assert len(self.page.futures) == 2
            for future, reply in self.page.futures:
                future.set_result(reply)

    async def get_attribute(self, name):
        return "true"


class Page:
    def __init__(self):
        self.url = FORM_URL
        self.futures = []

    async def goto(self, *args, **kwargs):
        self.futures = []
        return SimpleNamespace(status=200)

    def locator(self, selector):
        return Element(self, selector)

    def frame_locator(self, selector):
        return self

    async def close(self):
        pass

    @asynccontextmanager
    async def expect_response(self, predicate, **kwargs):
        actions = [row()] if self.query == "77755694-0" else []
        replies = [
            Reply(SEARCH_PATH, {"Result": "OK", "Link": "Busqueda/ActuacionDetalle.aspx?codigo=public"}),
            Reply(DETAILS_PATH, actions),
        ]
        reply = next(r for r in replies if predicate(r))
        future = asyncio.get_running_loop().create_future()
        self.futures.append((future, reply))
        try:
            yield SimpleNamespace(value=future)
            await future
        finally:
            if not future.done():
                future.cancel()


async def test_browser_waits_for_backend_rows_not_empty_dom():
    result = await lookup(Page(), {"company_id": "rut:77755694-0", "company_name": "AERO", "rut": "77755694-0"})
    assert result["status"] == "completed"
    assert result["res_cves"] == ["ACiWqbFTpsmy"]


async def test_captcha_required_never_becomes_not_found():
    class ChallengePage(Page):
        def locator(self, selector):
            element = Element(self, selector)

            async def unchecked(name):
                return "false"

            element.get_attribute = unchecked
            return element

    with pytest.raises(SearchIssue) as exc:
        await lookup(ChallengePage(), {"company_name": "AERO", "rut": "77755694-0"}, captcha_seconds=0)
    assert exc.value.status == "captcha_required"


async def test_runner_resumes_existing_queue_and_skips_completed(tmp_path, monkeypatch):
    import sys

    from src.sites.res import __main__ as runner

    store = QueueStore(tmp_path / "companies.sqlite3")
    seed(store)
    seed(store, "78054070-2", "NANCAGUA")
    store.close()

    class Context:
        pages = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def new_page(self):
            return Page()

    monkeypatch.setitem(sys.modules, "camoufox.async_api", SimpleNamespace(AsyncCamoufox=lambda **kwargs: Context()))
    monkeypatch.setattr(QueueStore, "request_delay", lambda *args: 0)
    args = argparse.Namespace(
        output=tmp_path,
        profile=tmp_path / "profile",
        action="collect",
        company_id=None,
        limit=1,
        run_id=None,
        retry_errors=False,
        max_attempts=3,
        workers=1,
        rate_per_minute=6,
        timeout=60,
        captcha_wait=30,
        headed=False,
    )
    assert await runner.run(args) == 0
    assert await runner.run(args) == 0
    store = QueueStore(tmp_path / "companies.sqlite3")
    try:
        assert store.status()["queues"]["res_company_lookup"] == {"completed": 1, "not_found": 1}
        assert store.status()["documents"] == 1
        assert store.db.execute("SELECT sum(attempts) FROM jobs WHERE queue='res_company_lookup'").fetchone()[0] == 2
    finally:
        store.close()


def test_real_res_date_uses_creation_field_instead_of_dotnet_timestamp():
    actual = row()
    actual["rso_fechacreacion"] = "07-05-2023 00:00:00"
    actual["rso_fecha"] = "/Date(1683432000000)/"
    (event,) = parse_actions(envelope([actual]))
    assert event["date"] == "2023-05-07"
    assert event["cve"] == "ACiWqbFTpsmy"


def test_exhausted_res_retry_and_reset_do_not_change_dequienes_jobs(tmp_path):
    store = QueueStore(tmp_path / "companies.sqlite3")
    try:
        key = seed(store)
        with store.transaction():
            store._enqueue("company_lookup", key, status="blocked")
        store.seed_res_jobs(max_attempts=1)
        job = store.claim(store.start_run("res_company_lookup"), "test")
        result = {"company_id": key, "status": "retry_wait", "source_url": FORM_URL, "events": [], "error": "timeout"}
        store.finish_res_lookup(job, result)
        assert result["status"] == "failed"
        assert store.retry_res_jobs() == 1
        assert store.status()["queues"]["company_lookup"] == {"blocked": 1}
        assert store.status()["queues"]["res_company_lookup"] == {"pending": 1}
    finally:
        store.close()


def test_report_csv_preserves_matched_rut_for_name_search(tmp_path):
    import csv

    from src.sites.res.__main__ import save_report

    store = QueueStore(tmp_path / "companies.sqlite3")
    try:
        report = {
            "run_id": "report-test",
            "results": [
                {
                    "company_id": "test",
                    "company_name": "AERO",
                    "rut": None,
                    "status": "completed",
                    "events": parse_actions(envelope([row()])),
                }
            ],
        }
        save_report(store, tmp_path, report)
        with (tmp_path / "res_search_report.csv").open() as handle:
            (result,) = list(csv.DictReader(handle))
        assert result["matched_rut"] == "77755694-0"
        assert result["status"] == "completed"
        assert result["cves"] == "ACiWqbFTpsmy"
    finally:
        store.close()
