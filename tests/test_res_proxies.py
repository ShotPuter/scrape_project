import argparse
import asyncio
import json
import sys
from types import SimpleNamespace

import pytest

from src.sites.res.proxies import ProxyRoutes, ProxySetupError, proxy_provider, webshare_options
from src.sites.res.runtime import BrowserLayout, BrowserPool, BrowserStartError


def candidate(number, *, port=8000):
    return SimpleNamespace(url=f"http://user%40example:secret%3Avalue@192.0.2.{number}:{port}")


def inventory(monkeypatch, items):
    from src.proxy.sources import webshare

    calls = []

    async def load(token, **kwargs):
        calls.append((token, kwargs))
        return items

    monkeypatch.setenv("WEBSHARE_API_TOKEN", "private-token")
    monkeypatch.setattr(webshare, "load_webshare_candidates", load)
    return calls


async def test_five_random_distinct_routes_persist_without_credentials(tmp_path, monkeypatch):
    calls = inventory(monkeypatch, [candidate(i) for i in range(1, 11)])
    shuffles = []

    class Random:
        def shuffle(self, values):
            shuffles.append(len(values))
            values.reverse()

    monkeypatch.setattr("src.sites.res.proxies.secrets.SystemRandom", Random)
    profiles = [tmp_path / f"profile-{i}" for i in range(5)]
    routes = ProxyRoutes(profiles, "webshare")
    options = await asyncio.gather(*(routes.for_browser(i) for i in range(5)))
    assert len({option["server"] for option in options}) == 5
    assert options[0]["username"] == "user@example"
    assert options[0]["password"] == "secret:value"
    assert len(calls) == 1 and shuffles == [10]
    persisted = "".join((profile / "res-proxy.json").read_text() for profile in profiles)
    for secret in ("private-token", "secret", "user@example"):
        assert secret not in persisted + json.dumps(routes.public)
    resumed = ProxyRoutes(profiles, "webshare")
    assert [await resumed.for_browser(i) for i in range(5)] == options


async def test_reserve_persisted_slots_before_random_new_assignments(tmp_path, monkeypatch):
    inventory(monkeypatch, [candidate(i) for i in range(1, 6)])
    profiles = [tmp_path / f"profile-{i}" for i in range(5)]
    existing = ProxyRoutes([profiles[-1]], "webshare")
    reserved = await existing.for_browser(0)
    expanded = ProxyRoutes(profiles, "webshare")
    options = [await expanded.for_browser(i) for i in range(5)]
    assert options[-1] == reserved
    assert len({option["server"] for option in options}) == 5


async def test_not_enough_distinct_addresses_never_falls_back_to_direct(tmp_path, monkeypatch):
    inventory(monkeypatch, [candidate(1), candidate(1, port=9000)])
    routes = ProxyRoutes([tmp_path / "a", tmp_path / "b"], "webshare")
    with pytest.raises(ProxySetupError, match="distinct"):
        await routes.for_browser(0)
    assert not list(tmp_path.rglob("res-proxy.json"))


async def test_missing_saved_proxy_requires_explicit_operator_action(tmp_path, monkeypatch):
    inventory(monkeypatch, [candidate(1)])
    profile = tmp_path / "profile"
    await ProxyRoutes([profile], "webshare").for_browser(0)
    inventory(monkeypatch, [candidate(2)])
    with pytest.raises(ProxySetupError, match="Cannot reuse"):
        await ProxyRoutes([profile], "webshare").for_browser(0)


async def test_inventory_exception_is_sanitized(tmp_path, monkeypatch):
    from src.proxy.sources import webshare

    async def fail(*args, **kwargs):
        raise RuntimeError("credential-secret must never enter a report")

    monkeypatch.setattr(webshare, "load_webshare_candidates", fail)
    with pytest.raises(ProxySetupError) as caught:
        await ProxyRoutes([tmp_path / "p"], "webshare").for_browser(0)
    assert "credential-secret" not in str(caught.value)


@pytest.mark.parametrize("success", [True, False])
async def test_browser_receives_proxy_and_requires_preflight(tmp_path, monkeypatch, success):
    inventory(monkeypatch, [candidate(1)])
    options = []

    class Page:
        closed = False

        async def goto(self, url, **kwargs):
            assert url == "https://api.ipify.org"
            return SimpleNamespace(status=200 if success else 407)

        def locator(self, selector):
            return self

        async def inner_text(self):
            return "192.0.2.1"

        async def close(self):
            self.closed = True

    class Context:
        pages = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def new_page(self):
            return Page()

    def browser(**kwargs):
        options.append(kwargs)
        return Context()

    monkeypatch.setitem(sys.modules, "camoufox.async_api", SimpleNamespace(AsyncCamoufox=browser))
    async with BrowserPool(tmp_path / "p", BrowserLayout(1, 2), proxy_provider="webshare") as pool:
        if success:
            await pool.page(0, 0)
            assert pool.routes.public[0]["verified"] is True
            assert pool.routes.public[0]["exit_ip"] == "192.0.2.1"
        else:
            with pytest.raises(BrowserStartError, match="proxy connection"):
                await pool.page(0, 0)
        assert options[0]["proxy"]["password"] == "secret:value"
        assert "secret:value" not in json.dumps(pool.routes.public)


def test_auto_provider_and_explicit_direct(monkeypatch):
    monkeypatch.setattr("dotenv.load_dotenv", lambda: None)
    monkeypatch.setenv("WEBSHARE_API_TOKEN", "token")
    assert proxy_provider(argparse.Namespace(proxy_provider="auto")) == "webshare"
    assert proxy_provider(argparse.Namespace(proxy_provider="direct")) == "direct"
    monkeypatch.delenv("WEBSHARE_API_TOKEN")
    assert proxy_provider(argparse.Namespace(proxy_provider="auto")) == "direct"


def test_endpoint_validation_does_not_accept_uncredentialed_or_non_http_proxies():
    for url in ["http://192.0.2.1:8000", "socks5://user:secret@192.0.2.1:8000"]:
        with pytest.raises(ProxySetupError):
            webshare_options(SimpleNamespace(url=url))
