"""Bounded PDF validation and atomic, recoverable certificate storage."""

from __future__ import annotations

import hashlib
import json
import re
import time
from pathlib import Path

from src.sites.dequienes.source import RES_URL, res_url, validate_cve
from src.utils.atomic import atomic_write_bytes, atomic_write_json

MAX_PDF_BYTES = 50 * 1024 * 1024


def pdf_metadata(data: bytes) -> dict:
    if not (0 < len(data) <= MAX_PDF_BYTES and data.startswith(b"%PDF-") and b"%%EOF" in data[-2048:]):
        raise ValueError("Response is not a complete PDF or exceeds 50 MiB")
    return {"sha256": hashlib.sha256(data).hexdigest(), "size_bytes": len(data)}


def read_pdf(path: Path) -> bytes:
    with path.open("rb") as handle:
        data = handle.read(MAX_PDF_BYTES + 1)
    pdf_metadata(data)
    return data


def certificate_directory(cve: str) -> Path:
    # CVEs are case-sensitive; hashed paths also work on case-insensitive disks.
    return Path("pdf") / hashlib.sha256(validate_cve(cve).encode()).hexdigest()


def persist_pdf(output: Path, cve: str, data: bytes, download_url: str) -> dict:
    metadata = pdf_metadata(data)
    relative = certificate_directory(cve) / f"{metadata['sha256']}.pdf"
    result = {
        "cve": cve,
        "status": "downloaded",
        **metadata,
        "local_path": relative.as_posix(),
        "download_url": res_url(download_url),
        "source_url": RES_URL,
        "downloaded_at": time.time(),
    }
    # Content-addressed files are immutable. A crash before DB commit can be recovered from the manifest.
    atomic_write_bytes(output / relative, data)
    atomic_write_json(output / certificate_directory(cve) / "manifest.json", result)
    return result


def cached_pdf(output: Path, cve: str) -> dict | None:
    try:
        manifest = output / certificate_directory(cve) / "manifest.json"
        if manifest.stat().st_size > 16384:
            return None
        result = json.loads(manifest.read_text(encoding="utf-8"))
        if result["cve"] != cve or result["status"] != "downloaded":
            return None
        if not re.fullmatch(r"[a-f0-9]{64}", result["sha256"]):
            return None
        expected = certificate_directory(cve) / f"{result['sha256']}.pdf"
        if result["local_path"] != expected.as_posix():
            return None
        metadata = pdf_metadata(read_pdf(output / expected))
        if any(result[key] != value for key, value in metadata.items()):
            return None
        res_url(result["download_url"])
        return {**result, "reused": True}
    except (OSError, ValueError, KeyError, TypeError):
        return None
