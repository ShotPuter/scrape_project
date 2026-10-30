"""Private proxy inputs use the normal verifier and redact public reports."""

import json

import pytest

from src.proxy import collector
from src.proxy.checker import CheckStats, Stage1Result, Stage2Result
from src.proxy.config import CollectorConfig
from src.proxy.repository import ProxyDatabase
from src.proxy.sources.adapters import ADAPTERS
from src.proxy.sources.paid import load_paid_candidates
from src.proxy.sources.registry import SOURCE_REGISTRY
from src.services.seprec_proxy_probe import seprec_target_probe


def test_every_public_provider_has_an_adapter():
    assert set(ADAPTERS) == set(SOURCE_REGISTRY)


def test_invalid_private_line_does_not_disclose_credentials(tmp_path):
    path = tmp_path / "paid.txt"
    path.write_text("http://secret:private-pass@host:invalid\n")
    with pytest.raises(ValueError) as exc:
        load_paid_candidates(path)
    assert "secret" not in str(exc.value)
    assert "private-pass" not in str(exc.value)


@pytest.mark.asyncio
async def test_paid_proxy_is_verified_and_report_redacts_credentials(tmp_path, monkeypatch):
    path = tmp_path / "paid.txt"
    url = "http://private-user:private-password@gateway.example:8000"
    path.write_text(f"# private export\n{url}\n{url}\n")
    calls = []

    async def stage1(address, **kwargs):
        calls.append(address)
        return Stage1Result(True, 1)

    async def stage2(address, **kwargs):
        calls.append(address)
        return Stage2Result(True, CheckStats(successful_requests=1, total_attempts=1))

    monkeypatch.setattr(collector, "verify_stage1", stage1)
    monkeypatch.setattr(collector, "verify_stage2", stage2)
    db = tmp_path / "proxies.db"
    report = await collector.collect(
        config=CollectorConfig(proxy_paid_file=path),
        target=seprec_target_probe("https://target.example/api"),
        provider="paid",
        csv_path=tmp_path / "unused.csv",
        db_path=db,
        requests_per_second=0,
        publish_after=1,
    )
    assert calls == [url, url]
    assert "private-password" not in json.dumps(report)
    assert "private-user" not in json.dumps(report)
    async with ProxyDatabase(db) as reader:
        assert (await reader.list_active())[0].url == url
