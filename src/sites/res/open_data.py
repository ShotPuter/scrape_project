"""Bounded company selection from the official RES CKAN datastore."""

from __future__ import annotations

import csv
import itertools
import json
import re
from urllib.parse import urlsplit

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


def select_archive(from_year, limit=0, *, session=None):
    """Select every available official year up to from_year, newest month first.

    Zero limit means all unique companies. Missing newer years are not invented;
    the result records the actual newest and oldest periods found in the data.
    """
    if from_year < 2013 or limit < 0:
        raise ValueError("Invalid archive year or limit")
    if session is None:
        with httpx.Client(timeout=45, trust_env=False, transport=httpx.HTTPTransport(retries=1)) as client:
            return select_archive(from_year, limit, session=client)
    package = request(session, "package_show", {"id": DATASET})
    resources = {}
    for resource in package.get("resources", []):
        years = re.findall(r"\b(20\d{2})\b", str(resource.get("name", "")))
        if len(set(years)) == 1 and int(years[0]) <= from_year and str(resource.get("format", "")).lower() == "csv":
            year = int(years[0])
            if year in resources:
                raise ValueError(f"Multiple active company resources for {year}")
            resources[year] = resource
    if not resources:
        raise ValueError("No active official company resources available in the requested archive")
    companies, seen, periods = [], set(), []
    for year in sorted(resources, reverse=True):
        resource = resources[year]
        buckets = {month: [] for month in range(1, 13)}
        offset = 0
        csv_rows = None
        if resource.get("url"):
            csv_rows = iter_archive_csv(session, resource["url"])
        while True:
            page = (
                {"records": list(itertools.islice(csv_rows, 500))}
                if csv_rows is not None
                else request(
                    session,
                    "datastore_search",
                    {"resource_id": resource["id"], "limit": 500, "offset": offset, "sort": "_id desc"},
                )
            )
            records = page.get("records")
            if not isinstance(records, list):
                raise ValueError("Invalid official archive page")
            if not records:
                break
            for row in records:
                if str(row.get("Anio")) != str(year) or row.get("Mes") not in MONTHS:
                    raise ValueError("Unexpected year/month in official archive")
                rut = normalized_rut(str(row.get("RUT", "")))
                name = " ".join(str(row.get("Razon Social") or "").split())
                if rut and name:
                    month = MONTHS.index(row["Mes"]) + 1
                    buckets[month].append(
                        {
                            "company_id": "rut:" + rut,
                            "company_name": name,
                            "rut": rut,
                            "source_period": f"{year}-{month:02d}",
                        }
                    )
            offset += len(records)
            if offset >= page.get("total", offset + 1):
                break
        for month in range(12, 0, -1):
            if buckets[month]:
                periods.append(f"{year}-{month:02d}")
            for company in buckets[month]:
                if company["rut"] not in seen:
                    seen.add(company["rut"])
                    companies.append(company)
                    if limit and len(companies) >= limit:
                        return {
                            "companies": companies,
                            "periods": periods,
                            "newest_period": periods[0],
                            "source": "official_res_ckan_archive",
                        }
    if not companies:
        raise ValueError("Official archive contains no valid companies")
    return {
        "companies": companies,
        "periods": periods,
        "newest_period": periods[0],
        "oldest_period": periods[-1],
        "source": "official_res_ckan_archive",
    }


def iter_archive_csv(session, url):
    """Stream official CSV fallback for resources without Datastore access."""
    if urlsplit(url).scheme != "https" or urlsplit(url).hostname != "datos.gob.cl":
        raise ValueError("Unexpected archive CSV host")
    with session.stream("GET", url) as response:
        response.raise_for_status()
        lines = response.iter_lines()
        header = next(lines, "").lstrip("\ufeff")
        delimiter = ";" if ";" in header else ","
        reader = csv.DictReader(itertools.chain([header], lines), delimiter=delimiter)
        if not {"RUT", "Razon Social", "Anio", "Mes"}.issubset(reader.fieldnames or []):
            raise ValueError("Official archive CSV has an unexpected schema")
        yield from reader
