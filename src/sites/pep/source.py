"""Parse the Ley 2013 results without treating unexpected pages as empty results."""

from __future__ import annotations

import hashlib
from urllib.parse import parse_qs, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

HOST = "www.funcionpublica.gov.co"
RESULT_PATH = "/fdci/consultaCiudadana/index"
DOWNLOAD_PATH = "/fdci/consultaCiudadana/generarReporteConsultaCiudadana"
LEGACY_PATH = "/ley-transparencia-web/declaracionServlet"
HEADER_ALIASES = {
    "Declarant": "Declarante", "Download": "Descargar", "Publication date": "Fecha de publicación",
    "Publication type": "Tipo de publicación", "Declaration number": "Declaración No.",
    "Declaration status": "Estado declaración",
}


def safe_url(url: str, base: str = "https://www.funcionpublica.gov.co") -> str:
    parsed = urlsplit(urljoin(base, url))
    if parsed.scheme not in {"http", "https"} or parsed.hostname != HOST or parsed.username or parsed.password:
        raise ValueError("Unexpected source URL")
    if parsed.port not in (None, 80, 443):
        raise ValueError("Unexpected source port")
    # The workbook contains HTTP links; never send identity data over plaintext.
    return urlunsplit(("https", HOST, parsed.path, parsed.query, ""))


def parse_results(html: str, current_url: str) -> tuple[list[dict], str | None]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.table")
    if table is None or not table.select_one("tbody"):
        raise ValueError("Results table missing; page may be blocked or changed")
    headers = [cell.get_text(" ", strip=True) for cell in table.select("thead th")]
    headers = [HEADER_ALIASES.get(header, header) for header in headers]
    if not {"Declarante", "Descargar", "Fecha de publicación"}.issubset(headers):
        raise ValueError("Unexpected results table headers")
    rows = table.select("tbody tr")
    if not rows:
        raise ValueError("Results table contains no rows or explicit empty-result message")
    documents = []
    for row in rows:
        if "No se encuentran declaraciones" in row.get_text(" ", strip=True):
            continue
        cells = row.select("td")
        if len(cells) != len(headers):
            raise ValueError("Unexpected result row columns")
        metadata = {key: cell.get_text(" ", strip=True) for key, cell in zip(headers, cells, strict=True)}
        form = row.select_one('form[action$="/generarReporteConsultaCiudadana"]')
        if form is None:
            assets = []
            for anchor in cells[headers.index("Descargar")].select('a[href]'):
                label = anchor.get_text(" ", strip=True).lower()
                if "bienes" not in label and "asset" not in label:
                    continue
                target = safe_url(str(anchor["href"]), current_url)
                parts = urlsplit(target)
                if parts.path != LEGACY_PATH or parse_qs(parts.query).get("action") != ["download"]:
                    raise ValueError("Unknown legacy asset download URL")
                assets.append(target)
            if not assets:
                raise ValueError("Unexpected result row without asset download")
            for target in assets:
                documents.append({"id": "legacy-" + hashlib.sha256(target.encode()).hexdigest()[:24],
                                  "download_url": target, "requires_captcha": False,
                                  "publication_date": metadata["Fecha de publicación"],
                                  "publication_type": metadata.get("Tipo de publicación", ""),
                                  "declaration_number": metadata.get("Declaración No.", ""),
                                  "declaration_status": metadata.get("Estado declaración", "")})
            continue
        action = safe_url(str(form.get("action", "")), current_url)
        if urlsplit(action).path != DOWNLOAD_PATH:
            raise ValueError("Unexpected download action")
        field = form.select_one('input[name="id"]')
        doc_id = str(field.get("value", "")) if field else ""
        if not doc_id.isdecimal():
            raise ValueError("Invalid document identifier")
        documents.append({"id": doc_id, "publication_date": metadata["Fecha de publicación"],
                          "requires_captcha": form.select_one('input[name="captchatoken"]') is not None,
                          "publication_type": metadata.get("Tipo de publicación", ""),
                          "declaration_number": metadata.get("Declaración No.", ""),
                          "declaration_status": metadata.get("Estado declaración", "")})
    next_link = soup.select_one(".pagination a.nextLink")
    next_url = safe_url(str(next_link.get("href")), current_url) if next_link else None
    if next_url and urlsplit(next_url).path != RESULT_PATH:
        raise ValueError("Unexpected pagination URL")
    return documents, next_url
