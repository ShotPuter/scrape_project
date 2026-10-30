"""Offline tests for the upstream proxy-list synchroniser."""

from datetime import UTC, datetime, timedelta

import pytest

from src.proxy.sources import iplocate as proxy_source


class FakeResponse:
    def __init__(self, body: bytes, headers: dict[str, str] | None = None):
        self.body = body
        self.headers = headers or {}

    def read(self) -> bytes:
        return self.body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None


def test_parse_proxies_filters_and_deduplicates():
    text = "socks5://1.2.3.4:1080\nhttp://5.6.7.8:8080\nhttp://5.6.7.8:8080\nftp://bad:1\n# comment\n"
    assert proxy_source.parse_proxies(text) == [("socks5", "1.2.3.4:1080"), ("http", "5.6.7.8:8080")]


def test_sync_downloads_and_then_respects_interval(tmp_path, monkeypatch):
    csv_path = tmp_path / "proxies.csv"
    state_path = tmp_path / "state.json"
    calls = []

    def fake_urlopen(request, timeout):
        calls.append((request, timeout))
        return FakeResponse(b"socks5://1.2.3.4:1080\nhttp://5.6.7.8:8080\n", {"ETag": '"v1"'})

    monkeypatch.setattr(proxy_source, "urlopen", fake_urlopen)
    first = proxy_source.sync_proxy_source(
        "https://example.test/all.txt", csv_path, state_path, refresh_minutes=30, force=True
    )
    assert first["updated"] is True
    assert first["count"] == 2
    assert len(calls) == 1
    assert csv_path.read_text().splitlines()[1:] == ["true,socks5,1.2.3.4:1080", "true,http,5.6.7.8:8080"]

    second = proxy_source.sync_proxy_source("https://example.test/all.txt", csv_path, state_path, refresh_minutes=30)
    assert second["updated"] is False
    assert second["reason"] == "refresh_interval"
    assert len(calls) == 1


def test_sync_rejects_empty_upstream(tmp_path, monkeypatch):
    monkeypatch.setattr(proxy_source, "urlopen", lambda request, timeout: FakeResponse(b"not a proxy\n"))
    with pytest.raises(proxy_source.ProxySourceError, match="no usable"):
        proxy_source.sync_proxy_source("https://example.test/all.txt", tmp_path / "p.csv", tmp_path / "s.json", force=True)


def test_sync_updates_after_interval(tmp_path, monkeypatch):
    csv_path = tmp_path / "proxies.csv"
    state_path = tmp_path / "state.json"
    last_checked = (datetime.now(UTC) - timedelta(minutes=31)).isoformat()
    state_path.write_text(f'{{"last_checked": "{last_checked}", "count": 1}}')
    monkeypatch.setattr(proxy_source, "urlopen", lambda request, timeout: FakeResponse(b"http://9.9.9.9:80\n"))
    report = proxy_source.sync_proxy_source("https://example.test/all.txt", csv_path, state_path, refresh_minutes=30)
    assert report["updated"] is True
    assert report["count"] == 1
