import pytest

from src.sites.dequienes.account_pilot import choose_card, search_cards
from src.sites.dequienes.companies import parse_company
from src.sites.dequienes.queue import QueueStore

URL = "https://dequienes.cl/diario-oficial/2025/01/02/company-spa-1234567"


def test_card_parser_and_rut_matching_allow_recorded_name_change():
    html = """<div class="bg-card"><div><span>Empresa</span><h3>OLD NAME LIMITADA</h3></div>
    <p>RUT: 77.492.450-7</p><h3>Historial Legal</h3></div>"""
    cards = search_cards(html)
    assert len(cards) == 1
    assert cards[0]["rut"] == "77492450-7"
    assert choose_card(cards, {"company_name": "NEW NAME SPA", "rut": "77492450-7"}) == cards[0]
    assert choose_card(cards, {"company_name": "OLD NAME LIMITADA", "rut": "78054070-2"}) is None


def test_name_matching_does_not_guess_and_rejects_duplicates():
    card = {"index": 0, "company_name": "ÁGUILA SpA", "rut": None}
    assert choose_card([card], {"company_name": "águila spa", "rut": None}) == card
    assert choose_card([card], {"company_name": "águila", "rut": None}) is None
    assert choose_card([card, card], {"company_name": "águila spa", "rut": None}) is None


def prepare(store):
    store.add_publications("2025-01", [URL])
    job = store.claim(store.start_run("public_discovery"), "public")
    item = parse_company("<h1>Company SpA</h1><h2>Publicado el 02/01/2025</h2>", URL)
    store.finish_publication(job, item, [])
    return store.claim(store.start_run("company_lookup", 1), "account")


def test_account_result_routes_only_res_and_preserves_sources(tmp_path):
    store = QueueStore(tmp_path / "q.db")
    try:
        job = prepare(store)
        events = [{"source": "Diario Oficial", "cve": "1234567"}, {"source": "RES", "cve": "ACiWqbFTpsmy"}]
        result = {
            "company_id": job["key"],
            "status": "completed",
            "source_url": "https://dequienes.cl/?term=Company",
            "events": events,
        }
        store.finish_company_lookup(job, result)
        assert store.status()["queues"]["company_lookup"] == {"completed": 1}
        assert store.status()["queues"]["certificate_download"] == {"pending": 1}
        assert store.db.execute("SELECT cve FROM documents").fetchall() == [("ACiWqbFTpsmy",)]
        assert store.db.execute("SELECT count(*) FROM account_document_sources").fetchone()[0] == 1
        assert store.db.execute("SELECT status FROM company_lookup_results").fetchone()[0] == "completed"
    finally:
        store.close()


def test_account_negative_result_is_not_fake_completion(tmp_path):
    store = QueueStore(tmp_path / "q.db")
    try:
        job = prepare(store)
        result = {
            "company_id": job["key"],
            "status": "no_cve_found",
            "source_url": "https://dequienes.cl/?term=Company",
            "events": [{"source": "Diario Oficial", "cve": "1234567"}],
        }
        store.finish_company_lookup(job, result)
        assert store.status()["queues"]["company_lookup"] == {"no_cve_found": 1}
        assert store.status()["documents"] == 0
    finally:
        store.close()


def test_account_invalid_code_rolls_back_all_result_writes(tmp_path):
    store = QueueStore(tmp_path / "q.db")
    try:
        job = prepare(store)
        result = {
            "company_id": job["key"],
            "status": "completed",
            "source_url": "https://dequienes.cl/?term=Company",
            "events": [{"source": "RES", "cve": "ACiWqbFTpsmy"}, {"source": "RES", "cve": "../../bad"}],
        }
        with pytest.raises(ValueError):
            store.finish_company_lookup(job, result)
        assert store.status()["documents"] == 0
        assert store.db.execute("SELECT count(*) FROM company_lookup_results").fetchone()[0] == 0
        assert store.status()["queues"]["company_lookup"] == {"running": 1}
    finally:
        store.close()


def test_name_matching_tolerates_vowel_accents_but_preserves_enye():
    card = {"index": 0, "company_name": "AGRICOLA ORO ROJO SPA", "rut": None}
    assert choose_card([card], {"company_name": "AGRÍCOLA ORO ROJO SpA", "rut": None}) == card
    other = {"index": 0, "company_name": "PEÑA SPA", "rut": None}
    assert choose_card([other], {"company_name": "PENA SPA", "rut": None}) is None


def test_profile_copies_preserve_session_files_and_exclude_locks(tmp_path):
    from src.sites.dequienes.account_pilot import prepare_profiles

    source = tmp_path / "account"
    source.mkdir()
    (source / "cookies.sqlite").write_bytes(b"test-session")
    (source / "lock").write_text("old-lock")
    (source / "cache2").mkdir()
    (source / "cache2" / "entry").write_text("cache")
    profiles = prepare_profiles(source, "pilot", 2)
    assert len(set(profiles)) == 2
    for profile in profiles:
        assert (profile / "cookies.sqlite").read_bytes() == b"test-session"
        assert not (profile / "lock").exists()
        assert not (profile / "cache2").exists()
        assert profile.stat().st_mode & 0o777 == 0o700
    assert (source / "cookies.sqlite").read_bytes() == b"test-session"


async def test_two_browsers_two_tabs_share_one_ten_company_limit(tmp_path, monkeypatch):
    import argparse
    import asyncio
    import sys
    from types import SimpleNamespace

    from src.sites.dequienes import account_pilot

    store = QueueStore(tmp_path / "companies.sqlite3")
    for i in range(12):
        url = URL + str(i)
        store.add_publications("2025-01", [url])
        job = store.claim(store.start_run("public_discovery"), "seed")
        html = f"<h1>Company {i} SpA</h1><h2>Publicado el 02/01/2025</h2>"
        store.finish_publication(job, parse_company(html, url), [])
    store.close()
    contexts = []

    class Page:
        closed = False

        async def close(self):
            self.closed = True

    class Context:
        def __init__(self):
            self.pages = []

        async def __aenter__(self):
            contexts.append(self)
            return self

        async def __aexit__(self, *args):
            pass

        async def new_page(self):
            page = Page()
            self.pages.append(page)
            return page

    monkeypatch.setitem(sys.modules, "camoufox.async_api", SimpleNamespace(AsyncCamoufox=lambda **kwargs: Context()))
    monkeypatch.setattr(account_pilot, "prepare_profiles", lambda *args: [tmp_path / "one", tmp_path / "two"])
    monkeypatch.setattr(QueueStore, "request_delay", lambda *args: 0)
    active = peak = 0
    visited = []

    async def lookup(page, company, timeout_ms):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        visited.append(company["company_id"])
        await asyncio.sleep(0.01)
        active -= 1
        return {**company, "status": "no_cve_found", "events": [], "source_url": "https://dequienes.cl/"}

    monkeypatch.setattr(account_pilot, "lookup", lookup)
    args = argparse.Namespace(
        output=tmp_path,
        profile=tmp_path / "original",
        limit=10,
        run_id=None,
        browsers=2,
        tabs_per_browser=2,
        rate_per_minute=60,
        timeout=45,
    )
    assert await account_pilot.run(args) == 0
    assert peak == 4
    assert len(visited) == len(set(visited)) == 10
    assert len(contexts) == 2
    assert all(len(c.pages) == 2 and all(p.closed for p in c.pages) for c in contexts)


def test_account_quota_message_is_blocked_not_empty_search():
    from src.sites.dequienes.account_pilot import access_status

    assert access_status("Has agotado las búsquedas de este mes.") == "blocked"
    assert access_status("Podrás seguir buscando si inicias sesión.") == "needs_login"
    assert access_status("Empresa AGRICOLA Legal Historial Legal") is None
