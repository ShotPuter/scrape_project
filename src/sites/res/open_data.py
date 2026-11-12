"""Bounded company selection from the official RES CKAN datastore."""

from __future__ import annotations

import json
import re

import httpx

from src.sites.dequienes.queue import normalized_rut

API = "https://datos.gob.cl/api/3/action/"
DATASET = "registro-de-empresas-y-sociedades"
MONTHS = (
    "Enero",
    "Febrero",
    "Marzo",
    "Abril",
    "Mayo",
    "Junio",
    "Julio",
    "Agosto",
    "Septiembre",
    "Octubre",
    "Noviembre",
    "Diciembre",
)


def request(session, action, params):
    response = session.get(API + action, params=params)
    response.raise_for_status()
    payload = response.json()
    if payload.get("success") is not True or not isinstance(payload.get("result"), dict):
        raise ValueError(f"CKAN {action} failed")
    return payload["result"]


def select_companies(year, month, limit, *, session=None):
    if not 1 <= month <= 12 or not 1 <= limit <= 10000:
        raise ValueError("month must be 1..12 and limit must be 1..10000")
    if session is None:
        with httpx.Client(timeout=45, trust_env=False, transport=httpx.HTTPTransport(retries=1)) as client:
            return select_companies(year, month, limit, session=client)
    package = request(session, "package_show", {"id": DATASET})
    resources = [
        r
        for r in package.get("resources", [])
        if r.get("datastore_active") and re.search(rf"\b{year}\b", str(r.get("name", "")))
    ]
    if len(resources) != 1:
        raise ValueError(f"Expected one active official company resource for {year}; found {len(resources)}")
    resource = resources[0]
    companies, seen, offset = [], set(), 0
    # Deterministic datastore order; invalid/duplicate identities do not consume the limit.
    while len(companies) < limit:
        page = request(
            session,
            "datastore_search",
            {
                "resource_id": resource["id"],
                "filters": json.dumps({"Mes": MONTHS[month - 1]}),
                "limit": min(500, limit - len(companies)),
                "offset": offset,
                "sort": "_id asc",
            },
        )
        records = page.get("records")
        if not isinstance(records, list):
            raise ValueError("CKAN returned an invalid company page")
        if not records:
            break
        offset += len(records)
        for row in records:
            if str(row.get("Anio")) != str(year) or row.get("Mes") != MONTHS[month - 1]:
                raise ValueError("CKAN returned a company outside the selected year/month")
            rut = normalized_rut(str(row.get("RUT", "")))
            name = " ".join(str(row.get("Razon Social") or "").split())
            if rut and name and rut not in seen:
                seen.add(rut)
                companies.append({"company_id": "rut:" + rut, "company_name": name, "rut": rut})
        if offset >= page.get("total", offset + 1):
            break
    if len(companies) != limit:
        raise ValueError(f"Requested {limit} unique companies; official selection contains only {len(companies)}")
    return {
        "source": "official_res_ckan",
        "resource_id": resource["id"],
        "source_url": f"https://datos.gob.cl/dataset/{DATASET}/resource/{resource['id']}",
        "year": year,
        "month": month,
        "companies": companies,
    }
