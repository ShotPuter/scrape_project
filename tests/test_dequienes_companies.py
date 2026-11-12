import argparse
from pathlib import Path

import pytest

from src.sites.dequienes.companies import export, month_value, open_database, parse_company, parse_sitemap, source_url

ARTICLE = "https://dequienes.cl/diario-oficial/2026/11/02/example-spa-12345"
MONTH = "https://dequienes.cl/diario-oficial/2026/11/sitemap.xml"
NS = "http://www.sitemaps.org/schemas/sitemap/0.9"


def article(name, day="02/11/2026"):
    return f"<h1>{name}</h1><h2>Publicado el <span>{day}</span></h2>"


def test_company_from_heading_preserves_accents_and_strips_only_trailing_rut():
    doc = parse_company(article("  COMPAÑÍA   DEL SUR LIMITADA\n77.492.450-7*"), ARTICLE)
    assert doc["company_name"] == "COMPAÑÍA DEL SUR LIMITADA"
    assert doc["rut"] == "77.492.450-7"
    assert doc["publication_date"] == "2026-11-02"
    assert doc["publication_title"].endswith("77.492.450-7*")


def test_company_without_rut_is_not_invented_from_slug():
    doc = parse_company(article("Inversiones Águila SpA"), ARTICLE)
    assert doc["company_name"] == "Inversiones Águila SpA"
    assert doc["rut"] == ""


@pytest.mark.parametrize("html", ["<h1>Un momento…</h1>", "<h1>Company</h1><h2>Publicado el 03/11/2026</h2>", article("")])
def test_challenge_or_mismatched_date_cannot_be_company(html):
    with pytest.raises(ValueError):
        parse_company(html, ARTICLE)


def test_sitemap_validates_type_and_deduplicates():
    raw = f'<urlset xmlns="{NS}"><url><loc>{ARTICLE}</loc></url><url><loc>{ARTICLE}</loc></url></urlset>'
    assert parse_sitemap(raw, index=False) == [ARTICLE]
    with pytest.raises(ValueError):
        parse_sitemap(raw, index=True)
    assert parse_sitemap(f'<sitemapindex xmlns="{NS}"><sitemap><loc>{MONTH}</loc></sitemap></sitemapindex>', index=True) == [
        MONTH
    ]


@pytest.mark.parametrize(
    "url",
    [
        "https://other.example/diario-oficial/2026/11/sitemap.xml",
        "http://dequienes.cl/diario-oficial/2026/11/sitemap.xml",
        "https://dequienes.cl/?term=Aero",
        "https://user@dequienes.cl/diario-oficial/2026/11/sitemap.xml",
    ],
)
def test_archive_cannot_escape_source(url):
    with pytest.raises(ValueError):
        source_url(url)


def test_month_requires_exact_valid_format():
    assert month_value("2026-11") == "2026-11"
    for value in ["2026-13", "2026-1", "abc"]:
        with pytest.raises(argparse.ArgumentTypeError):
            month_value(value)


def test_exports_deduplicate_names_but_preserve_publication_provenance(tmp_path: Path):
    db = open_database(tmp_path / "state.sqlite3")
    try:
        for index, name in enumerate(["Águila SpA", "ÁGUILA SPA", "Other SpA"]):
            url = ARTICLE + str(index)
            item = parse_company(article(name), url)
            with db:
                db.execute(
                    """INSERT INTO publications(source_url,month,status,company_name,name_key,rut,
                    publication_title,publication_date) VALUES (:source_url,'2026-11','collected',:company_name,
                    :name_key,:rut,:publication_title,:publication_date)""",
                    item,
                )
        export(db, tmp_path)
        import csv
        import json

        with (tmp_path / "companies.csv").open() as handle:
            companies = list(csv.DictReader(handle))
        assert len(companies) == 2
        assert sorted(int(c["publication_count"]) for c in companies) == [1, 2]
        assert json.loads((tmp_path / "summary.json").read_text())["statuses"] == {"collected": 3}
        assert db.execute("SELECT count(*) FROM publications WHERE status != 'collected'").fetchone()[0] == 0
    finally:
        db.close()


async def test_run_resumes_without_revisiting_collected_publications(tmp_path, monkeypatch):
    import sys
    from types import SimpleNamespace

    from src.sites.dequienes import companies

    class Page:
        html = ""

        async def content(self):
            return self.html

        async def close(self):
            pass

    class Context:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def new_page(self):
            return Page()

    monkeypatch.setitem(sys.modules, "camoufox.async_api", SimpleNamespace(AsyncCamoufox=lambda **kwargs: Context()))
    visited = []

    async def fake_fetch(page, url, args, *, sitemap_index=None):
        if sitemap_index is True:
            return [MONTH]
        if sitemap_index is False:
            return [ARTICLE, ARTICLE + "2"]
        visited.append(url)
        page.html = article("Name " + str(len(visited)))
        return parse_company(page.html, url)

    monkeypatch.setattr(companies, "fetch", fake_fetch)
    args = SimpleNamespace(
        output=tmp_path,
        profile=tmp_path / "profile",
        month=["2026-11"],
        year=None,
        all_months=False,
        limit=1,
        delay=0,
        timeout=10,
        workers=4,
        transport="browser",
        browser_engine="camoufox",
    )
    assert await companies.run(args) == 0
    assert visited == [ARTICLE]
    assert await companies.run(args) == 0
    assert visited == [ARTICLE, ARTICLE + "2"]


@pytest.mark.parametrize("year, expected", [(2025, ["2025-12", "2025-01"]), (2026, ["2026-11"])])
def test_year_selection_uses_only_available_months(year, expected):
    from src.sites.dequienes.companies import select_months

    available = dict.fromkeys(["2024-12", "2025-01", "2025-12", "2026-11"], "unused")
    args = argparse.Namespace(year=year, month=None, all_months=False)
    assert select_months(available, args) == expected


def test_missing_year_is_an_error():
    from src.sites.dequienes.companies import select_months

    with pytest.raises(ValueError, match="not present"):
        select_months({"2025-01": "unused"}, argparse.Namespace(year=2010, month=None, all_months=False))


async def test_parallel_workers_overlap_and_process_each_url_once(tmp_path, monkeypatch):
    import asyncio
    from types import SimpleNamespace

    from src.sites.dequienes import companies

    active = 0
    peak = 0
    closed = []
    visited = []

    class Page:
        async def close(self):
            closed.append(self)

    class Context:
        async def new_page(self):
            return Page()

    async def fake_fetch(page, url, args):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        visited.append(url)
        await asyncio.sleep(0.01)
        active -= 1
        return parse_company(article("Company"), url)

    monkeypatch.setattr(companies, "fetch", fake_fetch)
    db = open_database(tmp_path / "state.sqlite3")
    urls = [ARTICLE + str(i) for i in range(12)]
    with db:
        db.executemany("INSERT INTO publications(source_url,month) VALUES (?,?)", [(url, "2026-11") for url in urls])
    try:
        args = SimpleNamespace(workers=4, delay=0, output=tmp_path)
        assert await companies.collect_jobs(Context(), db, [(url,) for url in urls], args) == 0
        assert peak == 4
        assert len(visited) == len(set(visited)) == 12
        assert len(closed) == 4
        assert db.execute("SELECT count(*) FROM publications WHERE status='collected'").fetchone()[0] == 12
    finally:
        db.close()


async def test_repeated_failures_leave_unstarted_jobs_pending(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from src.sites.dequienes import companies

    class Page:
        async def close(self):
            pass

    class Context:
        async def new_page(self):
            return Page()

    async def blocked(*args):
        raise ValueError("HTTP 403")

    monkeypatch.setattr(companies, "fetch", blocked)
    db = open_database(tmp_path / "state.sqlite3")
    jobs = [(ARTICLE + str(i),) for i in range(10)]
    with db:
        db.executemany("INSERT INTO publications(source_url,month) VALUES (?,?)", [(url, "2026-11") for (url,) in jobs])
    try:
        assert await companies.collect_jobs(Context(), db, jobs, SimpleNamespace(workers=1, delay=0, output=tmp_path)) == 3
        assert dict(db.execute("SELECT status,count(*) FROM publications GROUP BY status")) == {"failed": 3, "pending": 7}
    finally:
        db.close()


def test_year_and_numeric_month_select_one_month():
    from src.sites.dequienes.companies import select_months

    args = argparse.Namespace(year=2025, month=["1"], all_months=False)
    assert select_months(dict.fromkeys(["2025-01", "2025-02", "2024-01"], ""), args) == ["2025-01"]


def test_numeric_month_needs_year():
    from src.sites.dequienes.companies import select_months

    with pytest.raises(ValueError, match="requires --year"):
        select_months({"2025-01": ""}, argparse.Namespace(year=None, month=["1"], all_months=False))


def test_duplicate_run_cannot_use_same_profile_or_database(tmp_path):
    from src.sites.dequienes.companies import exclusive_run

    args = argparse.Namespace(output=tmp_path / "output", profile=tmp_path / "profile")
    with exclusive_run(args), pytest.raises(ValueError, match="Another collector"), exclusive_run(args):
        pytest.fail("Duplicate run was allowed")
    with exclusive_run(args):
        pass
