import json
from pathlib import Path

import pytest
from openpyxl import Workbook

from src.sites.pep.__main__ import export_json, setup_database
from src.sites.pep.documents import valid_pdf
from src.sites.pep.source import parse_results, safe_url
from src.sites.pep.workbook import read_rows


def test_workbook_duplicates_and_json(tmp_path: Path) -> None:
    path = tmp_path / "input.xlsx"
    workbook = Workbook()
    sheet = workbook.create_sheet("data")
    sheet.append(["NAME", "ENLACE_CONSULTA_LEY_2013"])
    link = "http://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?find=x"
    sheet.append(["first", link])
    sheet.append(["second", link])
    workbook.save(path)
    assert len(list(read_rows(path))) == 2
    db = setup_database(tmp_path / "out" / "state.sqlite3", path)
    assert db.execute("SELECT count(*) FROM jobs").fetchone()[0] == 1
    export_json(db, tmp_path / "out" / "records.json")
    records = json.loads((tmp_path / "out" / "records.json").read_text())
    assert [r["source_row"]["NAME"] for r in records] == ["first", "second"]
    assert all(r["processing_status"] == "pending" for r in records)
    db.close()


def test_parse_results_and_pagination() -> None:
    html = '''<table class="table"><thead><tr><th>Declarante</th><th>Descargar</th>
    <th>Fecha de publicación</th></tr></thead><tbody><tr><td>person</td><td>
    <form action="/fdci/consultaCiudadana/generarReporteConsultaCiudadana">
    <input name="id" value="123"></form></td><td>2025-01-01</td></tr></tbody></table>
    <div class="pagination"><a class="nextLink" href="/fdci/consultaCiudadana/index?offset=10">next</a></div>'''
    docs, next_url = parse_results(html, "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index")
    assert docs[0]["id"] == "123"
    assert docs[0]["publication_date"] == "2025-01-01"
    assert next_url and next_url.endswith("offset=10")


def test_reject_unexpected_pages_and_urls() -> None:
    with pytest.raises(ValueError):
        parse_results("<html>403</html>", "https://www.funcionpublica.gov.co")
    with pytest.raises(ValueError):
        safe_url("https://attacker.example/a")
    assert valid_pdf(b"%PDF-1.4\nhello\n%%EOF")
    assert not valid_pdf(b"<html>error</html>")
