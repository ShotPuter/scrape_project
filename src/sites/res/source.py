"""Strict parsers for the responses used by RES's own search page."""

from __future__ import annotations

import json
import unicodedata
from datetime import datetime
from urllib.parse import urljoin, urlsplit

from src.sites.dequienes.queue import normalized_rut
from src.sites.dequienes.source import validate_cve

HOST = "www.registrodeempresasysociedades.cl"
FORM_URL = f"https://{HOST}/BuscarActuaciones2.aspx"
SEARCH_PATH = "/BuscarActuaciones2.aspx/Busqueda"
DETAILS_PATH = "/Busqueda/ActuacionDetalle.aspx/DatosActuaciones"


class SearchIssue(ValueError):
    def __init__(self, status: str, reason: str):
        super().__init__(reason)
        self.status = status


def safe_result_url(value):
    url = urljoin(FORM_URL, value)
    parts = urlsplit(url)
    if (
        parts.scheme != "https"
        or parts.hostname != HOST
        or parts.port not in (None, 443)
        or parts.username
        or parts.password
        or parts.fragment
        or parts.path != "/Busqueda/ActuacionDetalle.aspx"
    ):
        raise SearchIssue("retry_wait", "Unexpected result URL")
    return url


def unpack(payload):
    if not isinstance(payload, dict) or "d" not in payload:
        raise SearchIssue("retry_wait", "Unexpected RES response envelope")
    body = payload["d"]
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except ValueError as exc:
            raise SearchIssue("retry_wait", "Invalid JSON in RES response") from exc
    if isinstance(body, dict) and body.get("ErrorMessage"):
        message = str(body["ErrorMessage"])
        status = "captcha_required" if "captcha" in message.lower() else "retry_wait"
        raise SearchIssue(status, message[:300])
    return body


def search_link(payload):
    body = unpack(payload)
    if not isinstance(body, dict) or not isinstance(body.get("Link"), str):
        raise SearchIssue("retry_wait", "Search did not return a result link")
    return safe_result_url(body["Link"])


def parse_actions(payload):
    body = unpack(payload)
    if body in ({}, []):
        return []  # Explicit empty backend data, never an early empty DOM table.
    if not isinstance(body, list):
        raise SearchIssue("retry_wait", "Unexpected action list")
    actions = []
    for row in body:
        try:
            rut = normalized_rut(str(row["run"]))
            name = " ".join(str(row["rso_razonsocial"]).split())
            if not rut or not name:
                raise ValueError("Missing company identity")
            date = (
                datetime.strptime(str(row.get("rso_fechacreacion") or row["rso_fecha"]).split()[0], "%d-%m-%Y")
                .date()
                .isoformat()
            )
            cve = str(row["frec_glosa"] or "").strip()
            if cve:
                validate_cve(cve)
            actions.append(
                {
                    "rut": rut,
                    "company_name": name,
                    "date": date,
                    "event_type": str(row["tac_glosa"]),
                    "attention_number": str(row["rso_numeroatencion"]),
                    "cve": cve,
                }
            )
        except (KeyError, TypeError, ValueError, IndexError) as exc:
            raise SearchIssue("retry_wait", "Malformed action row; result was not saved as empty") from exc
    return actions


def name_key(value):
    return " ".join(unicodedata.normalize("NFKC", value).casefold().translate(str.maketrans("áéíóúü", "aeiouu")).split())


def match_actions(actions, company):
    if not actions:
        return "not_found", []
    if company.get("rut"):
        matched = [a for a in actions if a["rut"] == company["rut"]]
    else:
        identities = {a["rut"] for a in actions if name_key(a["company_name"]) == name_key(company["company_name"])}
        matched = [a for a in actions if a["rut"] in identities] if len(identities) == 1 else []
    if not matched:
        return "ambiguous_match", []
    return ("completed" if any(a["cve"] for a in matched) else "no_cve_found"), matched
