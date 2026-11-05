"""Export Excel source records and declaration PDF paths, including existing runs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.sites.pep.source import safe_url
from src.sites.pep.workbook import LINK_COLUMN, read_rows
from src.utils.atomic import atomic_write_json


def build_export(report: dict) -> list[dict]:
    records = []
    for job in report["jobs"]:
        declarations = []
        for doc in job["documents"]:
            pdfs = []
            if doc["status"] == "downloaded":
                files = doc.get("files") or ([doc] if doc.get("local_path") else [])
                pdfs = [
                    {"path": item["local_path"], "sha256": item.get("sha256"), "size_bytes": item.get("size_bytes")}
                    for item in files
                ]
            declarations.append(
                {
                    "id": doc.get("id"),
                    "download_url": doc.get("download_url"),
                    "publication_date": doc.get("publication_date"),
                    "publication_type": doc.get("publication_type"),
                    "declaration_number": doc.get("declaration_number"),
                    "declaration_status": doc.get("declaration_status"),
                    "download_status": doc["status"],
                    "pdfs": pdfs,
                    "error": doc.get("error"),
                }
            )
        records.append(
            {
                "excel_records": job.get("source_records", []),
                "consultation_url": job["url"],
                "status": job["status"],
                "declarations": declarations,
                "error": job.get("error"),
            }
        )
    return records


def write_export(output: Path, report: dict) -> None:
    atomic_write_json(output / "politicians.json", build_export(report))
    atomic_write_json(
        output / "unavailable-documents.json",
        [
            {"consultation_url": job["url"], **doc}
            for job in report["jobs"]
            for doc in job["documents"]
            if doc["status"] == "unavailable_http_404"
        ],
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("workbook", type=Path)
    args = parser.parse_args()
    report = json.loads(args.report.read_text())
    selected = {job["url"]: job for job in report["jobs"]}
    for job in selected.values():
        job["source_records"] = []
    for number, row in read_rows(args.workbook):
        link = row.get(LINK_COLUMN)
        if link and (job := selected.get(safe_url(str(link)))) is not None:
            job["source_records"].append({"excel_row": number, "data": row})
    if any(not job["source_records"] for job in selected.values()):
        raise ValueError("Some consultation links have no matching source records in this workbook")
    write_export(args.report.parent, report)
    print(args.report.parent / "politicians.json")


if __name__ == "__main__":
    main()
