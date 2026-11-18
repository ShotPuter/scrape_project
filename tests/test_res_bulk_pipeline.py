"""Persistent date discovery and Docker command routing."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from src.sites.res import bulk_pipeline, server
from src.sites.res.tracking import TrackingStore


@pytest.mark.parametrize(("kind", "search_type"), [("action", "4"), ("registration", "6")])
async def test_date_discovery_resumes_without_repeating_token(tmp_path, monkeypatch, kind, search_type):
    store = TrackingStore(tmp_path / "tracking.sqlite3", "bulk-test")
    store.register_run({}, [])
    minter = AsyncMock(return_value="automatic-token")
    post = Mock(return_value={"d": {"Link": "Busqueda/ActuacionDetalle.aspx?codigo=fixture"}})
    actions = [{"rut": "77755694-0", "company_name": "Example", "cve": "ACtest0001"}]
    monkeypatch.setattr(bulk_pipeline.http_flow, "_post", post)
    monkeypatch.setattr(bulk_pipeline.http_flow, "details", Mock(return_value=actions))
    args = SimpleNamespace(start=date(2025, 7, 29), end=date(2025, 7, 29), kind=kind)
    try:
        await bulk_pipeline.discover_dates(store, None, minter, args)
        await bulk_pipeline.discover_dates(store, None, minter, args)
        minter.assert_awaited_once_with(bulk_pipeline.FORM_URL)
        assert post.call_args.args[2] == {
            "tipoRegistro": "1",
            "tipoBusqueda": search_type,
            "texto": "2025-07-29",
            "Response": "automatic-token",
        }
        assert store.day_done("2025-07-29", kind)
        assert store.db.execute("SELECT count(*) FROM documents").fetchone()[0] == 1
    finally:
        store.close()


def test_bulk_container_entrypoint(monkeypatch):
    import sys

    forwarded = []
    monkeypatch.setattr(bulk_pipeline, "main", lambda args: forwarded.extend(args) or 2)
    monkeypatch.setattr(sys, "argv", ["server", "bulk-run", "--start", "2025-07-29", "--limit", "10"])
    with pytest.raises(SystemExit) as stopped:
        server.main()
    assert stopped.value.code == 2
    assert forwarded == ["--start", "2025-07-29", "--limit", "10"]


async def test_daily_limit_still_downloads_issued_links_without_new_proxy(tmp_path, monkeypatch):
    import json
    from contextlib import asynccontextmanager
    from unittest.mock import MagicMock

    @asynccontextmanager
    async def browser(*args, **kwargs):
        assert kwargs["headed"] is False
        yield SimpleNamespace(new_page=AsyncMock())

    endpoints = AsyncMock(return_value=[bulk_pipeline.pilot.DIRECT])
    monkeypatch.setattr(bulk_pipeline.pilot, "load_proxy_endpoints", endpoints)
    monkeypatch.setattr(bulk_pipeline, "load_local_audio_solver", Mock(return_value=SimpleNamespace(events=[])))
    monkeypatch.setattr(bulk_pipeline.http_flow, "new_session", MagicMock())
    monkeypatch.setattr(bulk_pipeline.http_flow, "prepare", Mock())
    monkeypatch.setattr(bulk_pipeline, "launch_context", browser)
    monkeypatch.setattr(bulk_pipeline, "discover_dates", AsyncMock())
    monkeypatch.setattr(
        bulk_pipeline, "burst_verify", AsyncMock(return_value={"status": "daily_limit", "verified": 5, "failed": 0})
    )
    downloader = AsyncMock(return_value={"downloaded": 5, "failed": 0, "stop_reason": None})
    monkeypatch.setattr(bulk_pipeline, "download_ready", downloader)
    args = SimpleNamespace(
        db=tmp_path / "tracking.sqlite3",
        run_id="quota-test",
        start=date(2025, 7, 29),
        end=date(2025, 7, 29),
        proxy_provider="direct",
        proxy_index=0,
        proxy_file=None,
        audio_model="base",
        timeout=1,
        engine="patchright",
        limit=10,
        max_tokens=2,
        direct_download=False,
        workers=4,
        interval=0.1,
    )
    assert await bulk_pipeline.run(args) == 2
    endpoints.assert_awaited_once()
    downloader.assert_awaited_once()
    report = json.loads((tmp_path / "bulk-report.json").read_text())
    assert report["status"] == "daily_limit" and report["downloads"]["downloaded"] == 5
