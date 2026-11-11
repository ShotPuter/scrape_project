"""Parse the legal events table documented in the supplied Tango workflow."""

from __future__ import annotations

import json
import re
from datetime import datetime
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup

RES_URL = "https://www.registrodeempresasysociedades.cl/VerificarCertificados.aspx"


def validate_cve(value: str) -> str:
    value = value.strip()
    if not re.fullmatch(r"[A-Za-z0-9]{6,64}", value):
        raise ValueError("CVE must contain 6–64 ASCII letters and digits")
    return value  # Case is significant; never uppercase or lowercase a CVE.


def res_url(value: str) -> str:
    url = urljoin(RES_URL, value)
    parts = urlsplit(url)
    if (
        parts.scheme != "https"
        or parts.hostname != "www.registrodeempresasysociedades.cl"
        or parts.username
        or parts.password
        or parts.port not in (None, 443)
    ):
        raise ValueError("Unexpected RES document URL")
    return url


def verification_result(payload: dict) -> str:
    result = payload.get("d")
    if isinstance(result, str):
        result = json.loads(result)
    if not isinstance(result, dict):
        raise ValueError("Unexpected RES verification response")
    if result.get("ErrorMessage"):
        raise ValueError(str(result["ErrorMessage"]))
    if not isinstance(result.get("Link"), str) or not result["Link"]:
        raise ValueError("RES did not return a document link")
    return res_url(result["Link"])


def parse_legal(html: str, company_name: str, rut: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    documents = []
    found = False
    for table in soup.select("table"):
        headers = [re.sub(r"[^a-záéíóúñ]", "", cell.get_text(" ", strip=True).lower()) for cell in table.select("thead th")]
        if not {"fecha", "tipo", "fuente", "cve"}.issubset(headers):
            continue
        found = True
        for row in table.select("tbody tr"):
            cells = row.select("td")
            if len(cells) != len(headers):
                if "no " in row.get_text(" ", strip=True).lower() and len(cells) == 1:
                    continue
                raise ValueError("Unexpected legal event table row")
            values = dict(zip(headers, (cell.get_text(" ", strip=True) for cell in cells), strict=True))
            date = datetime.strptime(values["fecha"], "%d/%m/%Y").date().isoformat()
            cve = values["cve"]
            source = values["fuente"]
            documents.append(
                {
                    "company_name": company_name,
                    "rut": rut,
                    "date": date,
                    "event_type": values["tipo"],
                    "source": source,
                    "cve": validate_cve(cve) if source.upper() == "RES" else cve,
                    "description": values.get("descripción", ""),
                    "status": "pending" if source.upper() == "RES" else "unsupported_source",
                }
            )
    if not found:
        raise ValueError("Legal events table missing; login, challenge or changed layout")
    return documents
