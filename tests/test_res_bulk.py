"""Automatic date discovery must never silently switch to an operator."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from src.sites.res import bulk
from src.sites.res.source import SearchIssue


async def test_model_failure_does_not_launch_manual_browser(tmp_path, monkeypatch):
    monkeypatch.setattr(bulk, "screen_proxy", AsyncMock(return_value=None))
    monkeypatch.setattr(bulk, "load_local_audio_solver", MagicMock(side_effect=SearchIssue("captcha_required", "No model")))
    playwright = MagicMock(side_effect=AssertionError("Must not launch a manual browser"))
    monkeypatch.setattr("playwright.async_api.async_playwright", playwright)
    args = SimpleNamespace(
        db=tmp_path / "tracking.sqlite3",
        run_id="test",
        start=date(2025, 1, 1),
        end=date(2025, 1, 1),
        kind="action",
        proxy=None,
        operator=False,
        audio_model="base",
    )
    assert await bulk.discover(args) == 2
    playwright.assert_not_called()


async def test_automatic_day_search_uses_solver_and_configured_budget(monkeypatch):
    page = MagicMock()
    page.locator.return_value.select_option = AsyncMock()
    page.locator.return_value.fill = AsyncMock()
    solver = AsyncMock()
    monkeypatch.setattr(bulk, "open_search_form", AsyncMock())
    monkeypatch.setattr(bulk, "submit_search", AsyncMock(return_value=[]))
    assert await bulk.discover_day(page, "2025-01-01", "action", captcha_solver=solver, captcha_wait_ms=130000) == []
    solver.assert_awaited_once_with(page, timeout_ms=130000, captcha_seconds=130)
    page.frame_locator.assert_not_called()
