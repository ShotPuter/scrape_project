"""Repair this source's missing intermediate without disabling verification.

The intermediate is public certificate data fetched from its published AIA.
Its signature chain must validate against existing trusted roots before it
can be added to a local bundle. Downloaded certificates never become roots.
"""

from __future__ import annotations

import asyncio
import re
import shutil
import ssl
import subprocess
import tempfile
from pathlib import Path

import certifi
from curl_cffi.requests import AsyncSession

from src.utils.atomic import atomic_write_bytes

INTERMEDIATE_URL = "http://crt.sectigo.com/SectigoRSAOrganizationValidationSecureServerCA.crt"
SECTIGO_INTERMEDIATE_SHA256 = (
    "72:A3:4A:C2:B4:24:AE:D3:F6:B0:B0:47:55:B8:8C:C0:27:DC:CC:80:6F:DD:B2:2B:4C:D7:C4:77:73:97:3E:C0"
)


def install_chrome_intermediate(ca_bundle: Path) -> None:
    """Add the validated Sectigo intermediate to NSS without granting trust."""
    certutil = shutil.which("certutil")
    openssl = shutil.which("openssl")
    if not certutil or not openssl:
        raise ValueError("certutil and OpenSSL are required for Chrome's NSS certificate store")
    certs = re.findall(rb"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", ca_bundle.read_bytes(), re.S)
    intermediate = None
    with tempfile.TemporaryDirectory(prefix="pep-nss-") as temp:
        candidate = Path(temp) / "candidate.pem"
        for cert in certs:
            candidate.write_bytes(cert + b"\n")
            result = subprocess.run(
                [openssl, "x509", "-in", str(candidate), "-noout", "-fingerprint", "-sha256"],
                capture_output=True, text=True, check=False, timeout=10,
            )
            actual = result.stdout.strip().split("=", 1)[-1].replace(":", "").upper()
            expected = SECTIGO_INTERMEDIATE_SHA256.replace(":", "")
            if result.returncode == 0 and actual == expected:
                intermediate = cert + b"\n"
                break
    if intermediate is None:
        raise ValueError("Validated Sectigo intermediate is absent from the CA bundle")

    database = Path.home() / ".pki" / "nssdb"
    database.mkdir(parents=True, exist_ok=True, mode=0o700)
    cert9 = database / "cert9.db"
    if not cert9.exists():
        subprocess.run([certutil, "-N", "-d", f"sql:{database}", "--empty-password"],
                       capture_output=True, check=True, timeout=10)
    listing = subprocess.run([certutil, "-L", "-d", f"sql:{database}"], capture_output=True,
                             text=True, check=True, timeout=10).stdout
    nickname = "PEP Sectigo OV intermediate (untrusted)"
    if nickname not in listing:
        with tempfile.NamedTemporaryFile(prefix="pep-sectigo-", suffix=".pem") as certfile:
            certfile.write(intermediate)
            certfile.flush()
            subprocess.run([certutil, "-A", "-d", f"sql:{database}", "-n", nickname,
                            "-t", ",,", "-i", certfile.name], capture_output=True, check=True, timeout=10)


async def prepare_ca_bundle(destination: Path) -> Path:
    openssl = shutil.which("openssl")
    if not openssl:
        raise ValueError("OpenSSL is required to validate the missing intermediate certificate")
    default = ssl.get_default_verify_paths().cafile
    roots = Path(default) if default and await asyncio.to_thread(Path(default).is_file) else Path(certifi.where())
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    data = bytearray()
    async with (
        AsyncSession(timeout=20, trust_env=False) as session,
        session.stream("GET", INTERMEDIATE_URL, allow_redirects=False) as response,
    ):
        if response.status_code != 200:
            raise ValueError(f"Certificate authority returned HTTP {response.status_code}")
        async for chunk in response.aiter_content():
            if len(data) + len(chunk) > 256 * 1024:
                raise ValueError("Intermediate certificate response is too large")
            data.extend(chunk)
    # This only converts encoding. Trust is established by openssl verify below.
    pem = ssl.DER_cert_to_PEM_cert(bytes(data)).encode("ascii")
    with tempfile.TemporaryDirectory(prefix="pep-ca-", dir=destination.parent) as temp:
        intermediate = Path(temp) / "intermediate.pem"
        await asyncio.to_thread(intermediate.write_bytes, pem)
        process = await asyncio.create_subprocess_exec(
            openssl, "verify", "-CAfile", str(roots), str(intermediate),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        try:
            await asyncio.wait_for(process.communicate(), timeout=10)
        except TimeoutError:
            process.kill()
            await process.wait()
            raise ValueError("Intermediate certificate validation timed out") from None
        if process.returncode != 0:
            raise ValueError("Intermediate certificate is not signed by existing trusted roots; refusing it")
    root_data = await asyncio.to_thread(roots.read_bytes)
    await asyncio.to_thread(atomic_write_bytes, destination, root_data + b"\n" + pem)
    return await asyncio.to_thread(destination.resolve)
