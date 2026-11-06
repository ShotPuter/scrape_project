import hashlib
import shutil
import ssl
import subprocess
from pathlib import Path

import pytest

from scripts.pep_camoufox_pilot import (
    INTERMEDIATE_SHA256,
    PINNED_INTERMEDIATES,
    YR1_SHA256,
    YR_CROSS_SHA256,
    install_intermediate,
)

ROOT = Path(__file__).resolve().parents[1]
CERTIFICATES = {
    INTERMEDIATE_SHA256: ROOT / "docker/pep-sectigo-intermediate.pem",
    YR1_SHA256: ROOT / "docker/pep-letsencrypt-yr1.pem",
    YR_CROSS_SHA256: ROOT / "docker/pep-letsencrypt-root-yr-cross.pem",
}


@pytest.mark.parametrize("digest,path", CERTIFICATES.items())
def test_official_chain_fingerprints_are_pinned(digest, path):
    assert hashlib.sha256(ssl.PEM_cert_to_DER_cert(path.read_text())).hexdigest().upper() == digest


@pytest.mark.skipif(not shutil.which("certutil"), reason="NSS certutil required")
def test_complete_chain_imports_as_untrusted_intermediates_and_is_repeatable(tmp_path):
    bundle = tmp_path / "bundle.pem"
    bundle.write_bytes(b"\n".join(path.read_bytes() for path in CERTIFICATES.values()))
    profile = tmp_path / "profile"
    install_intermediate(profile, bundle)
    install_intermediate(profile, bundle)
    listing = subprocess.run(
        ["certutil", "-L", "-d", f"sql:{profile}"], check=True, capture_output=True, text=True, timeout=10
    ).stdout
    for name in PINNED_INTERMEDIATES.values():
        line = next(line for line in listing.splitlines() if name in line)
        assert line.split()[-1] == ",,"  # Chain material, not a new trusted root.


@pytest.mark.parametrize("missing", [YR1_SHA256, YR_CROSS_SHA256])
def test_partial_new_chain_is_rejected_before_import(tmp_path, missing):
    bundle = tmp_path / "bundle.pem"
    bundle.write_bytes(b"\n".join(path.read_bytes() for digest, path in CERTIFICATES.items() if digest != missing))
    with pytest.raises(ValueError, match="Both YR1"):
        install_intermediate(tmp_path / "profile", bundle)
    assert not (tmp_path / "profile/cert9.db").exists()


@pytest.mark.asyncio
async def test_unknown_issuer_stops_browser_with_explicit_tls_reason(monkeypatch, tmp_path):
    import asyncio

    from scripts import pep_camoufox_batch as batch

    original_sleep = asyncio.sleep

    async def fast_sleep(seconds):
        await original_sleep(0)

    class CertificateErrorPage:
        def on(self, *args):
            pass

        async def goto(self, *args, **kwargs):
            raise batch.BrowserError("Page.goto: SEC_ERROR_UNKNOWN_ISSUER")

        async def title(self):
            return "Security warning"

        def locator(self, *args):
            return self

        async def inner_text(self, **kwargs):
            return "Certificate warning"

        async def close(self):
            pass

    class Context:
        async def new_page(self):
            return CertificateErrorPage()

    monkeypatch.setattr(batch.asyncio, "sleep", fast_sleep)
    jobs = iter([{"n": n, "url": f"https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?n={n}"} for n in (1, 2)])
    stop = asyncio.Event()
    result = await batch.run(
        [], tmp_path / "run", 5, context_override=Context(), next_job=lambda: next(jobs, None), stop_event=stop
    )
    assert result["stop_reason"] == "tls_certificate_error:SEC_ERROR_UNKNOWN_ISSUER"
    assert len(result["jobs"]) == 1 and stop.is_set()
    assert next(jobs)["n"] == 2
