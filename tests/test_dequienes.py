import json

import pytest

from src.sites.dequienes.__main__ import pdf_metadata
from src.sites.dequienes.source import parse_legal, res_url, validate_cve, verification_result


def test_legal_tango_fields_and_case():
    html = """<table><thead><tr><th>Fecha ↕</th><th>Tipo</th><th>Fuente</th><th>CVE</th>
    <th>Descripción</th><th>Link</th></tr></thead><tbody><tr><td>07/05/2023</td>
    <td>CONSTITUCIÓN</td><td>RES</td><td>ACiWqbFTpsmy</td><td>Aero inspecciones Spa</td><td></td>
    </tr></tbody></table>"""
    (doc,) = parse_legal(html, "AERO INSPECCIONES SPA", "77.755.694-0")
    assert doc == {
        "company_name": "AERO INSPECCIONES SPA",
        "rut": "77.755.694-0",
        "date": "2023-05-07",
        "event_type": "CONSTITUCIÓN",
        "source": "RES",
        "cve": "ACiWqbFTpsmy",
        "description": "Aero inspecciones Spa",
        "status": "pending",
    }


@pytest.mark.parametrize(
    "html", ["<html>Podrás seguir buscando si inicias sesión.</html>", "<title>Un momento…</title>", "<table></table>"]
)
def test_missing_table_is_not_empty_success(html):
    with pytest.raises(ValueError, match="table missing"):
        parse_legal(html, "", "")


@pytest.mark.parametrize("cve", ["../../doc", "123", "a b c d", "Ñbcdef", "a" * 65])
def test_cve_rejects_bad_input(cve):
    with pytest.raises(ValueError):
        validate_cve(cve)


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.example/file.pdf",
        "//evil.example/file",
        "http://www.registrodeempresasysociedades.cl/file",
        "https://user@www.registrodeempresasysociedades.cl/file",
        "https://www.registrodeempresasysociedades.cl:444/file",
    ],
)
def test_rejects_foreign_document_links(url):
    with pytest.raises(ValueError):
        res_url(url)


def test_official_verification_response():
    link = verification_result({"d": json.dumps({"ErrorMessage": None, "Link": "/documento.pdf?id=123"})})
    assert link == "https://www.registrodeempresasysociedades.cl/documento.pdf?id=123"
    with pytest.raises(ValueError, match="Captcha"):
        verification_result({"d": json.dumps({"ErrorMessage": "Captcha inválido"})})


def test_html_cannot_be_saved_as_pdf():
    with pytest.raises(ValueError, match="complete PDF"):
        pdf_metadata(b"<html>captcha</html>")
    assert pdf_metadata(b"%PDF-1.7\ncontent\n%%EOF")["size_bytes"] == 22
