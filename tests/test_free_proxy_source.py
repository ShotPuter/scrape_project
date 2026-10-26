"""Tests for the optional free-proxy candidate source."""

import pytest

from src.client import free_proxy_source


class FakeFreeProxyException(Exception):
    pass


class FakeFreeProxy:
    def __init__(self, country_id=None, request_timeout=10):
        self.country_id = country_id
        self.request_timeout = request_timeout

    def get_proxy_list(self, repeat):
        return ["1.2.3.4:8080", "1.2.3.4:8080", "not-a-proxy", "5.6.7.8:3128", "socks5://9.9.9.9:1080"]


class FailingFreeProxy:
    def __init__(self, country_id=None, request_timeout=10):
        pass

    def get_proxy_list(self, repeat):
        raise FakeFreeProxyException("source unavailable")


def test_normalise_proxy_address():
    assert free_proxy_source.normalise_proxy_address("1.2.3.4:8080") == "http://1.2.3.4:8080"
    assert free_proxy_source.normalise_proxy_address("socks5://9.9.9.9:1080") == "socks5://9.9.9.9:1080"
    assert free_proxy_source.normalise_proxy_address("bad") is None
    assert free_proxy_source.normalise_proxy_address("") is None


@pytest.mark.asyncio
async def test_load_free_proxy_candidates_deduplicates(monkeypatch):
    monkeypatch.setattr(free_proxy_source, "_import_free_proxy", lambda: (FakeFreeProxy, FakeFreeProxyException))

    urls = await free_proxy_source.load_free_proxy_candidates(request_timeout=1)

    assert urls == ["http://1.2.3.4:8080", "http://5.6.7.8:3128", "socks5://9.9.9.9:1080"]


@pytest.mark.asyncio
async def test_load_free_proxy_candidates_tolerates_source_failures(monkeypatch):
    monkeypatch.setattr(free_proxy_source, "_import_free_proxy", lambda: (FailingFreeProxy, FakeFreeProxyException))

    assert await free_proxy_source.load_free_proxy_candidates(request_timeout=1) == []
