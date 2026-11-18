"""Unit tests for the token-multiplexed burst verifier."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.sites.res.burst_verifier import burst_verify, fetch_pending_cves
from src.sites.res.source import SearchIssue
from src.sites.res.tracking import TrackingStore

CVES = [f"ACtest{i:04d}" for i in range(1, 11)]
RUN_ID = "burst-test"


@pytest.fixture
def tracking_store(tmp_path):
    db_path = tmp_path / "tracking.sqlite3"
    store = TrackingStore(db_path, RUN_ID)
    store.register_run({"mode": "test"}, [])
    with store.db:
        for cve in CVES:
            store.db.execute("INSERT INTO documents(cve, status, updated_at) VALUES (?, 'pending', 1.0)", (cve,))
    yield store
    store.close()


def test_fetch_pending_cves(tracking_store):
    cves = fetch_pending_cves(tracking_store, limit=5)
    assert len(cves) == 5
    assert cves == CVES[:5]


@pytest.mark.asyncio
async def test_burst_verify_uses_single_token_for_multiple_documents(tracking_store, monkeypatch):
    tokens_minted = []

    async def fake_minter():
        token = f"token_{len(tokens_minted) + 1}"
        tokens_minted.append(token)
        return token

    def fake_verify(session, cve, token):
        assert token == "token_1"
        return cve, "AA-BB-CC-DD-EE-FF-00-11-22-33-44-55-66-77-88-99", f"https://example.com/pdf?cve={cve}"

    monkeypatch.setattr("src.sites.res.http_flow.verify", fake_verify)

    dummy_session = MagicMock()
    result = await burst_verify(tracking_store, dummy_session, fake_minter, limit=5)

    assert result["status"] == "ok"
    assert result["verified"] == 5
    assert result["tokens_minted"] == 1
    assert len(tokens_minted) == 1

    # Verify all 5 documents are marked 'verified' in SQLite
    verified_rows = tracking_store.db.execute(
        "SELECT cve, status, download_url FROM documents WHERE status='verified'"
    ).fetchall()
    assert len(verified_rows) == 5


@pytest.mark.asyncio
async def test_burst_verify_refreshes_token_on_captcha_expiration(tracking_store, monkeypatch):
    tokens_minted = []

    async def fake_minter():
        token = f"token_{len(tokens_minted) + 1}"
        tokens_minted.append(token)
        return token

    calls = 0

    def fake_verify(session, cve, token):
        nonlocal calls
        calls += 1
        # Expire token on 3rd call
        if calls == 3 and token == "token_1":
            raise SearchIssue("captcha_required", "El captcha ingresado no es válido")
        return cve, "AA-BB-CC-DD-EE-FF-00-11-22-33-44-55-66-77-88-99", f"https://example.com/pdf?cve={cve}"

    monkeypatch.setattr("src.sites.res.http_flow.verify", fake_verify)

    dummy_session = MagicMock()
    result = await burst_verify(tracking_store, dummy_session, fake_minter, limit=4)

    assert result["status"] == "ok"
    assert result["verified"] == 4
    assert result["tokens_minted"] == 2
    assert result["token_expirations"] == 1
    assert len(tokens_minted) == 2


@pytest.mark.asyncio
async def test_burst_verify_stops_cleanly_on_daily_limit(tracking_store, monkeypatch):
    async def fake_minter():
        return "token_1"

    def fake_verify(session, cve, token):
        raise SearchIssue("daily_limit", "No es posible generar mas de 100 documentos diarios")

    monkeypatch.setattr("src.sites.res.http_flow.verify", fake_verify)

    dummy_session = MagicMock()
    result = await burst_verify(tracking_store, dummy_session, fake_minter, limit=5)

    assert result["status"] == "daily_limit"
    assert result["stop_reason"] == "daily_limit"
    assert result["verified"] == 0


async def test_automatic_token_failure_leaves_queue_pending(tracking_store):
    minter = AsyncMock(side_effect=SearchIssue("captcha_required", "Audio unavailable"))
    result = await burst_verify(tracking_store, MagicMock(), minter)
    assert result["status"] == "captcha_required"
    assert result["tokens_minted"] == 1
    assert fetch_pending_cves(tracking_store) == CVES


async def test_one_automatic_token_produces_50_downloaded_pdfs(tmp_path, monkeypatch):
    import httpx

    from src.sites.res.ready_download import download_ready

    cves = [f"ACtest{i:04d}" for i in range(50)]
    store = TrackingStore(tmp_path / "tracking.sqlite3", RUN_ID)
    store.register_run({}, [{"company_id": "company", "company_name": "Example"}])
    store.search_result("company", {"cves": cves})
    codigo = "-".join(["AB"] * 16)
    minter = AsyncMock(return_value="automatic-whisper-token")
    verified = []

    def verify(session, cve, token):
        assert token == "automatic-whisper-token"
        verified.append(cve)
        return (
            cve,
            codigo,
            (f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={cve}&codigo={codigo}"),
        )

    def respond(request):
        assert request.method == "GET"
        return httpx.Response(200, headers={"content-type": "application/pdf"}, content=b"%PDF-1.4\nfixture\n%%EOF")

    monkeypatch.setattr("src.sites.res.http_flow.verify", verify)
    try:
        result = await burst_verify(store, MagicMock(), minter, limit=50)
        assert result["verified"] == 50 and result["tokens_minted"] == 1
        assert len(verified) == 50
        minter.assert_awaited_once()
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            downloaded = await download_ready(store, client, limit=50, workers=10, interval=0)
            resumed = await download_ready(store, client, limit=50, workers=10, interval=0)
        assert downloaded["downloaded"] == 50 and downloaded["failed"] == 0
        assert store.db.execute("SELECT count(*) FROM document_files").fetchone()[0] == 50
        assert resumed["requested"] == 0
    finally:
        store.close()


async def test_token_budget_stops_with_remaining_work_pending(tracking_store, monkeypatch):
    monkeypatch.setattr(
        "src.sites.res.http_flow.verify", MagicMock(side_effect=SearchIssue("captcha_required", "Token expired"))
    )
    minter = AsyncMock(return_value="token")
    result = await burst_verify(tracking_store, MagicMock(), minter, max_tokens_per_session=1)
    assert result["status"] == "max_tokens_reached"
    assert fetch_pending_cves(tracking_store) == CVES
    minter.assert_awaited_once()


async def test_token_minter_forwards_audio_budget():
    from src.sites.res.burst_verifier import mint_recaptcha_token

    page = MagicMock()
    page.route = AsyncMock()
    page.goto = AsyncMock()
    page.frame_locator.return_value.locator.return_value.wait_for = AsyncMock()
    page.locator.return_value.first.input_value = AsyncMock(return_value="t" * 80)
    solver = AsyncMock()
    assert await mint_recaptcha_token(page, solver, timeout_seconds=130) == "t" * 80
    solver.assert_awaited_once_with(page, timeout_ms=130000, captcha_seconds=130)
