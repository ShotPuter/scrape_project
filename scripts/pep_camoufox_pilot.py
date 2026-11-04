"""Try one native PEP download in Camoufox, with a separate profile and output."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import shutil
import ssl
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from camoufox.async_api import AsyncCamoufox

from src.sites.pep.browser_download import DownloadFailure, click_download
from src.sites.pep.documents import inspect_saved, save_payload
from src.sites.pep.source import DOWNLOAD_PATH, RESULT_PATH, parse_results, safe_url
from src.utils.atomic import atomic_write_json

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SAMPLE = ROOT / "data/pep-firefox-10-20261103T132909Z/sample.json"
CA_BUNDLE = ROOT / "data/pep/ca-bundle.pem"
INTERMEDIATE_SHA256 = "72A34AC2B424AED3F6B0B04755B88CC027DCCC806FDDB22B4CD7C47773973EC0"


def install_intermediate(profile: Path, ca_bundle: Path = CA_BUNDLE) -> None:
    """Import the already verified intermediate into this Firefox profile without trusting it as a root."""
    if not ca_bundle.is_file():
        raise FileNotFoundError(ca_bundle)
    certutil = shutil.which("certutil")
    openssl = shutil.which("openssl")
    if not certutil or not openssl:
        raise RuntimeError("certutil and openssl are required to validate the PEP intermediate")
    candidates = re.findall(rb"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", ca_bundle.read_bytes(), re.S)
    certificate = next(
        (
            cert
            for cert in candidates
            if hashlib.sha256(ssl.PEM_cert_to_DER_cert(cert.decode("ascii"))).hexdigest().upper() == INTERMEDIATE_SHA256
        ),
        None,
    )
    if certificate is None:
        raise ValueError("Validated Sectigo intermediate is missing from the CA bundle")
    profile.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.NamedTemporaryFile(prefix="pep-camoufox-cert-", suffix=".pem") as certfile:
        certfile.write(certificate + b"\n")
        certfile.flush()
        subprocess.run(
            [openssl, "verify", "-CAfile", "/etc/ssl/certs/ca-certificates.crt", certfile.name],
            check=True,
            capture_output=True,
            timeout=10,
        )
        database = f"sql:{profile}"
        if not (profile / "cert9.db").exists():
            subprocess.run([certutil, "-N", "-d", database, "--empty-password"], check=True, capture_output=True, timeout=10)
        subprocess.run(
            [certutil, "-A", "-d", database, "-n", "PEP Sectigo intermediate", "-t", ",,", "-i", certfile.name],
            check=True,
            capture_output=True,
            timeout=10,
        )


def selected_url(sample: Path, index: int) -> str:
    rows = json.loads(sample.read_text(encoding="utf-8"))
    if not isinstance(rows, list) or not 1 <= index <= len(rows):
        raise ValueError("Sample index is out of range")
    url = safe_url(rows[index - 1]["url"])
    if urlsplit(url).path != RESULT_PATH:
        raise ValueError("Sample contains an unexpected URL")
    return url


async def run(url: str, output: Path, timeout_seconds: float) -> dict:
    report: dict = {"engine": "camoufox", "url": url, "status": "starting", "documents": []}
    profile = output / "profile"
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False, mode=0o700)
    try:
        await asyncio.to_thread(install_intermediate, profile)
        async with AsyncCamoufox(
            headless="virtual",
            os="linux",
            locale="es-CO",
            persistent_context=True,
            user_data_dir=str(profile),
            accept_downloads=True,
            ignore_https_errors=False,
        ) as context:
            page = context.pages[0] if context.pages else await context.new_page()

            def on_response(response) -> None:
                if urlsplit(response.url).path == DOWNLOAD_PATH:
                    report["post_http_status"] = response.status

            page.on("response", on_response)
            landing = await page.goto(url, wait_until="domcontentloaded", timeout=int(timeout_seconds * 1000))
            report["landing_http_status"] = landing.status if landing else None
            if report["landing_http_status"] != 200:
                report["status"] = "landing_failed"
                return report
            await page.locator("table.table tbody").wait_for(timeout=int(timeout_seconds * 1000))
            documents, _ = parse_results(await page.content(), page.url)
            report["documents"] = [str(doc["id"]) for doc in documents]
            document = next((doc for doc in documents if doc.get("requires_captcha") and str(doc["id"]).isdecimal()), None)
            if document is None:
                report["status"] = "no_modern_download_form"
                return report
            doc_id = str(document["id"])
            report["selected_document_id"] = doc_id
            form = page.locator(f"form#descargarDeclaracionForm{doc_id}")
            try:
                download = await click_download(page, form, int(timeout_seconds * 1000))
            except DownloadFailure as exc:
                report["status"] = str(exc)
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=5000)
                    report["recaptcha_notice"] = "Recaptcha did not pass validation" in await page.content()
                except Exception:
                    pass
                return report
            raw = output / "browser-download.bin"
            await download.save_as(str(raw))
            payload = raw.read_bytes()
            report["download_bytes"] = len(payload)
            report["saved"] = save_payload(payload, output, doc_id)
            report["validated"] = inspect_saved(output, doc_id) is not None
            report["status"] = "downloaded" if report["validated"] else "validation_failed"
    except Exception as exc:
        report["status"] = "error"
        report["error"] = {"type": type(exc).__name__, "message": str(exc)[:1000]}
    finally:
        atomic_write_json(output / "report.json", report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=Path, default=DEFAULT_SAMPLE)
    parser.add_argument("--index", type=int, default=1, help="1-based position in the Firefox sample")
    parser.add_argument("--timeout", type=float, default=45)
    args = parser.parse_args()
    if not 5 <= args.timeout <= 120:
        parser.error("--timeout must be between 5 and 120 seconds")
    url = selected_url(args.sample, args.index)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output = ROOT / "data" / f"pep-camoufox-pilot-{timestamp}"
    report = asyncio.run(run(url, output, args.timeout))
    print(
        json.dumps(
            {
                "output": str(output),
                "status": report["status"],
                "landing_http_status": report.get("landing_http_status"),
                "post_http_status": report.get("post_http_status"),
                "selected_document_id": report.get("selected_document_id"),
                "validated": report.get("validated"),
            },
            ensure_ascii=False,
        )
    )
    return 0 if report["status"] == "downloaded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
