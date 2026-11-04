"""Read the supplied PEP snapshot without losing duplicate source rows."""

from __future__ import annotations

from collections.abc import Generator
from datetime import date, datetime, time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from openpyxl import load_workbook

from src.sites.pep.source import RESULT_PATH, safe_url

LINK_COLUMN = "ENLACE_CONSULTA_LEY_2013"


def read_rows(path: Path) -> Generator[tuple[int, dict[str, Any]], None, None]:
    workbook = load_workbook(path, read_only=True, data_only=False)
    try:
        if len(workbook.worksheets) < 2:
            raise ValueError("Workbook has no second sheet")
        sheet = workbook.worksheets[1]
        rows = sheet.iter_rows()
        headers = [str(cell.value).strip() if cell.value is not None else "" for cell in next(rows)]
        if not all(headers) or len(set(headers)) != len(headers) or LINK_COLUMN not in headers:
            raise ValueError("Workbook headers are empty, duplicated or missing the consultation link")
        for number, cells in enumerate(rows, start=2):
            if all(cell.value is None for cell in cells):
                continue
            row: dict[str, Any] = {}
            for key, cell in zip(headers, cells, strict=True):
                value = cell.value
                if cell.data_type == "f":
                    raise ValueError(f"Formula in row {number}; refusing to discard source data")
                if isinstance(value, (datetime, date, time)):
                    value = value.isoformat()
                row[key] = value
            yield number, row
    finally:
        workbook.close()


def first_consultation(workbook: Path) -> str:
    rows = read_rows(workbook)
    try:
        for _, row in rows:
            if row.get(LINK_COLUMN):
                url = safe_url(str(row[LINK_COLUMN]))
                if urlsplit(url).path != RESULT_PATH:
                    raise ValueError("Unexpected consultation path")
                return url
    finally:
        rows.close()
    raise ValueError("Workbook contains no consultation links")
