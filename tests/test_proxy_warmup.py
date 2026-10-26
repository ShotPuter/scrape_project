"""Standalone warmup selection and safe CSV replacement."""

import csv
from pathlib import Path

import pytest

import warmup_proxies


@pytest.mark.asyncio
async def test_scan_checks_all_unique_addresses_even_marked_dead(monkeypatch):
    calls = []

    async def check(url, request_timeout):
        calls.append(url)
        return "working", 0.1

    monkeypatch.setattr(warmup_proxies, "check_proxy", check)
    rows = [{"proxy": "one:80", "alive": "false"}, {"proxy": "two:80"}, {"proxy": "one:80"}, {"proxy": "bad"}]
    results = await warmup_proxies.scan(rows, 2, 1)
    assert sorted(calls) == ["http://one:80", "http://two:80"]
    assert sorted(item["status"] for item in results) == ["duplicate", "invalid_address", "working", "working"]


def test_save_keeps_fastest_and_preserves_original(tmp_path):
    path = tmp_path / "proxies.csv"
    original = b"proxy,alive\none:80,false\ntwo:80,true\nthree:80,true\n"
    path.write_bytes(original)
    rows = list(csv.DictReader(original.decode().splitlines()))
    results = [
        {"row": 0, "status": "working", "seconds": 1},
        {"row": 1, "status": "working", "seconds": 2},
        {"row": 2, "status": "http_403", "seconds": 0.1},
    ]
    summary = warmup_proxies.save_results(path, original, ["proxy", "alive"], rows, results, 1)
    assert Path(summary["backup"]).read_bytes() == original
    assert list(csv.DictReader(path.read_text().splitlines())) == [{"proxy": "one:80", "alive": "true"}]
    assert summary["selected"] == 1


def test_no_working_proxies_leaves_header_and_backup(tmp_path):
    path = tmp_path / "proxies.csv"
    original = b"proxy\none:80\n"
    path.write_bytes(original)
    summary = warmup_proxies.save_results(
        path, original, ["proxy"], [{"proxy": "one:80"}], [{"row": 0, "status": "timeout", "seconds": 10}], 50
    )
    assert summary["selected"] == 0
    assert Path(summary["backup"]).read_bytes() == original
    assert list(csv.DictReader(path.read_text().splitlines())) == []


def test_source_edits_are_not_overwritten(tmp_path):
    path = tmp_path / "proxies.csv"
    path.write_text("new contents")
    with pytest.raises(RuntimeError, match="changed during scan"):
        warmup_proxies.save_results(path, b"old contents", ["proxy"], [], [], 50)
    assert path.read_text() == "new contents"


@pytest.mark.asyncio
async def test_check_accepts_created_catalog_and_requires_search(monkeypatch):
    class Response:
        def __init__(self, status, data):
            self.status_code, self.data = status, data

        def json(self):
            return self.data

    class Session:
        responses = []

        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def post(self, *args, **kwargs):
            return self.responses.pop(0)

    monkeypatch.setattr(warmup_proxies, "AsyncSession", Session)
    Session.responses = [
        Response(201, {"finalizado": True, "datos": []}),
        Response(200, {"finalizado": True, "datos": {"total": 0, "filas": []}}),
    ]
    status, _ = await warmup_proxies.check_proxy("http://example.test:80", 1)
    assert status == "working"
    Session.responses = [Response(201, {"finalizado": True, "datos": []}), Response(200, {"html": "challenge"})]
    status, _ = await warmup_proxies.check_proxy("http://example.test:80", 1)
    assert status == "invalid_search"
