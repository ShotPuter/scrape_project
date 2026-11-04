"""Local PDF persistence and bounded HTTP downloading, without CAPTCHA bypass."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from io import BytesIO
from pathlib import Path
from zipfile import BadZipFile, ZipFile

from curl_cffi.requests.exceptions import CertificateVerifyError, RequestException

from src.sites.pep.source import DOWNLOAD_PATH, LEGACY_PATH, safe_url
from src.utils.atomic import atomic_write_bytes, atomic_write_json

MAX_PDF_BYTES = 50 * 1024 * 1024


def valid_id(doc_id: str) -> bool:
    return re.fullmatch(r"(?:[0-9]+|legacy-[a-f0-9]{24})", doc_id) is not None


def valid_pdf(data: bytes) -> bool:
    return 0 < len(data) <= MAX_PDF_BYTES and data.startswith(b"%PDF-") and b"%%EOF" in data[-2048:]


def inspect_file(path: Path) -> dict | None:
    try:
        if path.stat().st_size > MAX_PDF_BYTES:
            return None
        data = path.read_bytes()
    except FileNotFoundError:
        return None
    if not valid_pdf(data):
        return None
    return {"sha256": hashlib.sha256(data).hexdigest(), "size_bytes": len(data)}


def inspect_saved(output: Path, doc_id: str) -> dict | None:
    manifest = output / "manifests" / f"{doc_id}.json"
    if manifest.exists():
        try:
            metadata = json.loads(manifest.read_text())
            files = metadata["files"]
            if not files:
                return None
            for item in files:
                path = (output / item["local_path"]).resolve()
                if not path.is_relative_to((output / "pdf").resolve()):
                    return None
                actual = inspect_file(path)
                if not actual or actual["sha256"] != item["sha256"]:
                    return None
            return metadata
        except (ValueError, KeyError, TypeError):
            return None
    relative = f"pdf/{doc_id}.pdf"
    metadata = inspect_file(output / relative)
    return {**metadata, "local_path": relative, "status": "downloaded"} if metadata else None


def save_payload(data: bytes, output: Path, doc_id: str) -> dict:
    """The portal returns a ZIP containing a declaration PDF and an XLSX form.

    Never extract member paths; only validated PDFs receive generated filenames.
    Preserve the original archive locally for provenance and future inspection.
    """
    if not valid_id(doc_id) or len(data) > MAX_PDF_BYTES:
        raise ValueError("Invalid identifier or oversized response")
    pdfs: list[bytes] = []
    archive_path = None
    if valid_pdf(data):
        pdfs.append(data)
    elif data.startswith(b"PK"):
        try:
            with ZipFile(BytesIO(data)) as archive:
                members = archive.infolist()
                if len(members) > 100 or sum(m.file_size for m in members) > 100 * 1024 * 1024:
                    raise ValueError("Archive exceeds extraction limits")
                for member in members:
                    if member.is_dir() or not member.filename.lower().endswith(".pdf"):
                        continue
                    if member.file_size > MAX_PDF_BYTES or member.flag_bits & 1:
                        raise ValueError("Oversized or encrypted PDF member")
                    payload = archive.read(member)
                    if not valid_pdf(payload):
                        raise ValueError("Invalid PDF inside ZIP")
                    pdfs.append(payload)
        except (BadZipFile, RuntimeError, NotImplementedError) as exc:
            raise ValueError("Invalid or unsupported ZIP") from exc
        if not pdfs:
            raise ValueError("Archive contains no PDFs")
        archive_path = f"archives/{doc_id}.zip"
    else:
        raise ValueError("Response is neither PDF nor a supported ZIP")
    files = []
    for index, payload in enumerate(pdfs):
        relative = f"pdf/{doc_id}{'-' + str(index + 1) if index else ''}.pdf"
        atomic_write_bytes(output / relative, payload, preserve_mode=False)
        files.append({"local_path": relative, "sha256": hashlib.sha256(payload).hexdigest(),
                      "size_bytes": len(payload), "s3_uri": None})
    if archive_path:
        atomic_write_bytes(output / archive_path, data, preserve_mode=False)
    result = {**files[0], "files": files, "archive_path": archive_path, "status": "downloaded"}
    atomic_write_json(output / "manifests" / f"{doc_id}.json", result)
    return result


class DocumentDownloader:
    def __init__(self, client, output: Path) -> None:
        self.client = client
        self.output = output
        # Fixed lock stripes bound memory while serializing duplicate documents.
        self.locks = [asyncio.Lock() for _ in range(64)]

    async def download(self, document: dict) -> dict:
        doc = dict(document)
        doc_id = str(doc["id"])
        if not valid_id(doc_id):
            raise ValueError("Invalid document identifier")
        async with self.locks[int.from_bytes(hashlib.sha256(doc_id.encode()).digest()[:2]) % len(self.locks)]:
            doc.update(local_path=None, s3_uri=None)
            try:
                metadata = await asyncio.to_thread(inspect_saved, self.output, doc_id)
                if metadata:
                    doc.update(metadata, status="downloaded")
                    return doc
                if doc.get("requires_captcha", True):
                    doc["status"] = "captcha_required"
                    return doc
                await self.client.pace()
                target_url = safe_url(doc.get("download_url") or DOWNLOAD_PATH)
                if doc.get("download_url"):
                    from urllib.parse import urlsplit

                    if urlsplit(target_url).path != LEGACY_PATH:
                        raise ValueError("Unexpected direct download endpoint")
                async with self.client.session.stream(
                    "GET" if doc.get("download_url") else "POST", target_url,
                    data=None if doc.get("download_url") else {"id": doc_id},
                    headers={"Referer": safe_url(doc["source_page"])}, allow_redirects=False,
                ) as response:
                    if response.status_code != 200:
                        doc["status"] = f"download_failed_http_{response.status_code}"
                        return doc
                    payload = bytearray()
                    async for chunk in response.aiter_content():
                        if len(payload) + len(chunk) > MAX_PDF_BYTES:
                            doc["status"] = "download_failed_size_limit"
                            return doc
                        payload.extend(chunk)
                data = bytes(payload)
                doc.update(await asyncio.to_thread(save_payload, data, self.output, doc_id))
            except CertificateVerifyError:
                doc["status"] = "download_failed_tls"
            except RequestException:
                doc["status"] = "download_failed_network"
            except OSError:
                doc["status"] = "download_failed_storage"
            except ValueError:
                doc["status"] = "download_failed_content"
            return doc
