import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from curl_cffi.requests.exceptions import CertificateVerifyError

from src.sites.pep.discovery import DiscoveryClient, TLSPreflightError
from src.sites.pep.tls import prepare_ca_bundle


@pytest.mark.asyncio
async def test_preflight_repairs_missing_ca_once(tmp_path, monkeypatch) -> None:
    client = DiscoveryClient(workers=1, delay=0, timeout=5)
    original = client.session
    bundle = tmp_path / "ca-bundle.pem"
    repair = AsyncMock(return_value=bundle)
    monkeypatch.setattr("src.sites.pep.tls.prepare_ca_bundle", repair)
    client.session = SimpleNamespace(get=AsyncMock(side_effect=[CertificateVerifyError("chain"), object()]))
    try:
        await client.preflight(tmp_path)
        repair.assert_awaited_once_with(bundle)
        assert client.session.verify == str(bundle)
        assert client.session.get.await_count == 2
    finally:
        await original.close()


@pytest.mark.asyncio
async def test_explicit_bundle_is_not_silently_replaced(tmp_path, monkeypatch) -> None:
    client = DiscoveryClient(workers=1, delay=0, timeout=5, ca_bundle=tmp_path / "custom.pem")
    original = client.session
    repair = AsyncMock()
    monkeypatch.setattr("src.sites.pep.tls.prepare_ca_bundle", repair)
    client.session = SimpleNamespace(get=AsyncMock(side_effect=CertificateVerifyError("chain")))
    try:
        with pytest.raises(TLSPreflightError, match="CA bundle"):
            await client.preflight(tmp_path)
        repair.assert_not_awaited()
    finally:
        await original.close()


@pytest.mark.asyncio
async def test_untrusted_intermediate_is_never_installed(tmp_path, monkeypatch) -> None:
    class Response:
        status_code = 200

        async def aiter_content(self):
            yield b"untrusted certificate"

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        @asynccontextmanager
        async def stream(self, *args, **kwargs):
            yield Response()

    monkeypatch.setattr("src.sites.pep.tls.AsyncSession", lambda **kwargs: Session())
    monkeypatch.setattr("src.sites.pep.tls.shutil.which", lambda name: "/usr/bin/openssl")
    process = SimpleNamespace(returncode=1, communicate=AsyncMock(return_value=(b"", b"untrusted")))
    launch = AsyncMock(return_value=process)
    monkeypatch.setattr("src.sites.pep.tls.asyncio.create_subprocess_exec", launch)
    destination = tmp_path / "ca-bundle.pem"
    with pytest.raises(ValueError, match="not signed"):
        await prepare_ca_bundle(destination)
    assert not destination.exists()
    args = launch.call_args.args
    assert args[1] == "verify"
    assert await asyncio.to_thread(Path(args[3]).is_file)  # Existing roots, not the downloaded certificate.
