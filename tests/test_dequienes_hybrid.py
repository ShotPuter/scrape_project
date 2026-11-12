import argparse
import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from src.sites.dequienes.hybrid import MAX_RESPONSE_BYTES, HTTPFailure, HybridClient
from src.sites.dequienes.settings import StageSettings

INDEX = "https://dequienes.cl/diario-oficial/sitemap-index.xml"
MONTH = "https://dequienes.cl/diario-oficial/2025/01/sitemap.xml"
URL = "https://dequienes.cl/diario-oficial/2025/01/02/company-spa-1234567"
NS = "http://www.sitemaps.org/schemas/sitemap/0.9"
INDEX_XML = f'<sitemapindex xmlns="{NS}"><sitemap><loc>{MONTH}</loc></sitemap></sitemapindex>'


def article(number=0):
    return f"<h1>Company {number} SpA</h1><h2>Publicado el 02/01/2025</h2>"


class Cookies:
    def __init__(self):
        self.values = {}

    def set(self, name, value, **kwargs):
        self.values[name] = value


class Page:
    def __init__(self):
        self.url = ""
        self.visited = []
        self.closed = False
        self.listeners = []

    def on(self, event, callback):
        self.listeners.append(callback)

    def remove_listener(self, event, callback):
        self.listeners.remove(callback)

    async def goto(self, url, **kwargs):
        await asyncio.sleep(0)
        self.url = url
        self.visited.append(url)
        return SimpleNamespace(status=200, text=self.text)

    async def text(self):
        return INDEX_XML

    async def wait_for_load_state(self, *args, **kwargs):
        pass

    async def evaluate(self, code):
        return "test-user-agent"

    async def close(self):
        self.closed = True


class Context:
    def __init__(self):
        self.pages = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def new_page(self):
        page = Page()
        self.pages.append(page)
        return page

    async def cookies(self, urls):
        return [
            {"name": "private-cookie", "value": "do-not-log", "domain": ".dequienes.cl", "path": "/"},
            {"name": "unrelated-cookie", "value": "unrelated", "domain": ".example.com", "path": "/"},
        ]


class Session:
    def __init__(self, handler):
        self.handler, self.cookies = handler, Cookies()
        self.requests = []
        self.closed = False

    @asynccontextmanager
    async def stream(self, method, url, **kwargs):
        assert kwargs["allow_redirects"] is False
        self.requests.append(url)
        await asyncio.sleep(0)
        status, body, headers = self.handler(url)

        async def chunks():
            yield body.encode() if isinstance(body, str) else body

        yield SimpleNamespace(status_code=status, url=url, headers=headers, aiter_content=chunks)

    async def close(self):
        self.closed = True


async def gate():
    pass


def factory_for(handler):
    sessions = []

    def factory(**kwargs):
        assert kwargs["verify"] is True
        assert kwargs["trust_env"] is False
        session = Session(lambda url: handler(len(sessions), url))
        sessions.append(session)
        return session

    return factory, sessions


async def test_one_browser_page_and_parallel_http_without_cookie_logs(capsys):
    context = Context()
    factory, sessions = factory_for(lambda generation, url: (200, article(), {}))
    async with HybridClient(context, StageSettings(browser_engine="camoufox"), gate, session_factory=factory) as client:
        texts = await asyncio.gather(*(client.get_html(URL + str(i)) for i in range(4)))
        assert len(texts) == 4
        assert len(context.pages) == 1
        assert context.pages[0].visited == [INDEX]
        assert client.metrics["http_successes"] == 4
        assert sessions[0].cookies.values == {"private-cookie": "do-not-log"}
    assert sessions[0].closed and context.pages[0].closed
    assert "do-not-log" not in capsys.readouterr().out


async def test_public_webshare_browser_and_http_use_the_same_proxy(tmp_path, monkeypatch):
    import sys

    from src.sites.dequienes import hybrid
    from src.sites.dequienes.pipeline import run_public
    from src.sites.dequienes.queue import QueueStore
    from src.sites.res.proxies import ProxyRoutes

    proxy = {"server": "http://192.0.2.10:8000", "username": "test@user", "password": "test:password"}
    browsers, requests = [], []

    def browser(**kwargs):
        browsers.append(kwargs)
        return Context()

    async def for_browser(self, index):
        return proxy

    async def verify(self, context, index):
        pass

    urls = [URL + str(i) for i in range(3)]

    def handler(url):
        if url == INDEX:
            return 200, INDEX_XML, {}
        if url == MONTH:
            entries = "".join(f"<url><loc>{u}</loc></url>" for u in urls)
            return 200, f'<urlset xmlns="{NS}">{entries}</urlset>', {}
        return 200, article(urls.index(url)), {}

    def factory(**kwargs):
        requests.append(kwargs)
        return Session(handler)

    original = hybrid.HybridClient
    monkeypatch.setitem(sys.modules, "camoufox.async_api", SimpleNamespace(AsyncCamoufox=browser))
    monkeypatch.setattr(ProxyRoutes, "for_browser", for_browser)
    monkeypatch.setattr(ProxyRoutes, "verify", verify)
    monkeypatch.setattr(QueueStore, "request_delay", lambda *args: 0)
    monkeypatch.setattr(hybrid, "HybridClient", lambda *a, **k: original(*a, **k, session_factory=factory))
    args = argparse.Namespace(
        output=tmp_path,
        profile=tmp_path / "profile",
        proxy_provider="webshare",
        transport="hybrid",
        browser_engine="camoufox",
        company_target=2,
        workers=1,
        delay=0,
        year=2025,
        month=["1"],
    )
    assert await run_public(args) == 0
    assert browsers[0]["proxy"] == proxy
    assert requests[0]["proxy"] == "http://test%40user:test%3Apassword@192.0.2.10:8000"
    store = QueueStore(tmp_path / "companies.sqlite3")
    try:
        assert store.status()["companies"] == 2
        assert store.status()["queues"]["public_discovery"] == {"completed": 2, "pending": 1}
    finally:
        store.close()


async def test_concurrent_403_refreshes_session_once():
    context = Context()
    factory, sessions = factory_for(
        lambda generation, url: (403, "blocked", {}) if generation == 1 else (200, article(), {})
    )
    async with HybridClient(context, StageSettings(browser_engine="camoufox"), gate, session_factory=factory) as client:
        await asyncio.gather(client.get_html(URL), client.get_html(URL + "2"))
        assert len(sessions) == 2
        assert client.metrics["session_refreshes"] == 1
        assert client.metrics["http_successes"] == 2
        assert context.pages[0].visited == [INDEX, INDEX]
    assert all(s.closed for s in sessions)


async def test_429_is_not_a_reason_to_rotate_browser_session():
    factory, sessions = factory_for(lambda generation, url: (429, "", {"Retry-After": "120"}))
    async with HybridClient(Context(), StageSettings(browser_engine="camoufox"), gate, session_factory=factory) as client:
        with pytest.raises(HTTPFailure) as error:
            await client.get_html(URL)
        assert error.value.retry_after == 120
        assert client.metrics["session_refreshes"] == 0
        assert len(sessions) == 1


async def test_403_refresh_budget_is_bounded():
    factory, sessions = factory_for(lambda generation, url: (403, "blocked", {}))
    async with HybridClient(
        Context(), StageSettings(session_refreshes=1, browser_engine="camoufox"), gate, session_factory=factory
    ) as client:
        for _ in range(2):
            with pytest.raises(ValueError, match="403"):
                await client.get_html(URL)
        assert client.metrics["session_refreshes"] == 1
        assert len(sessions) == 2


async def test_stream_limit_and_untrusted_url_rejection():
    factory, sessions = factory_for(lambda generation, url: (200, b"x" * (MAX_RESPONSE_BYTES + 1), {}))
    async with HybridClient(Context(), StageSettings(browser_engine="camoufox"), gate, session_factory=factory) as client:
        with pytest.raises(ValueError, match="10 MiB"):
            await client.get_html(URL)
        count = len(sessions[0].requests)
        with pytest.raises(ValueError, match="Unexpected"):
            await client.get_html("https://other.example/file")
        assert len(sessions[0].requests) == count


async def test_hybrid_pipeline_resumes_queue_without_browser_tabs(tmp_path, monkeypatch):
    import sys

    from src.sites.dequienes import hybrid
    from src.sites.dequienes.pipeline import run_public

    contexts = []

    def browser(**kwargs):
        context = Context()
        contexts.append(context)
        return context

    monkeypatch.setitem(sys.modules, "camoufox.async_api", SimpleNamespace(AsyncCamoufox=browser))
    urls = [URL + str(i) for i in range(3)]

    def response(generation, url):
        if url == INDEX:
            return 200, INDEX_XML, {}
        if url == MONTH:
            entries = "".join(f"<url><loc>{u}</loc></url>" for u in urls)
            return 200, f'<urlset xmlns="{NS}">{entries}</urlset>', {}
        return 200, article(urls.index(url)), {}

    factory, sessions = factory_for(response)
    original = hybrid.HybridClient
    monkeypatch.setattr(
        hybrid,
        "HybridClient",
        lambda context, stage, request_gate, **kwargs: original(context, stage, gate, session_factory=factory, **kwargs),
    )
    args = argparse.Namespace(
        output=tmp_path,
        profile=tmp_path / "profile",
        year=2025,
        month=["1"],
        limit=2,
        delay=0,
        workers=4,
        transport="hybrid",
        browser_engine="camoufox",
    )
    assert await run_public(args) == 0
    first = json.loads((tmp_path / "queues.json").read_text())
    assert first["queues"]["public_discovery"] == {"completed": 2, "pending": 1}
    assert await run_public(args) == 0
    second = json.loads((tmp_path / "queues.json").read_text())
    assert second["queues"]["public_discovery"] == {"completed": 3}
    requested = [url for session in sessions for url in session.requests if url in urls]
    assert len(requested) == len(set(requested)) == 3
    assert all(len(context.pages) == 1 for context in contexts)


async def test_initial_browser_challenge_is_allowed_to_finish():
    class ChallengePage(Page):
        async def goto(self, url, **kwargs):
            self.url = url
            self.visited.append(url)
            return SimpleNamespace(status=403)

        @asynccontextmanager
        async def expect_response(self, predicate, **kwargs):
            response = SimpleNamespace(status=200, url=INDEX, text=self.text)
            assert predicate(response)
            future = asyncio.get_running_loop().create_future()
            future.set_result(response)
            yield SimpleNamespace(value=future)

    class ChallengeContext(Context):
        async def new_page(self):
            page = ChallengePage()
            self.pages.append(page)
            return page

    context = ChallengeContext()
    factory, _ = factory_for(lambda generation, url: (200, article(), {}))
    async with HybridClient(context, StageSettings(browser_engine="camoufox"), gate, session_factory=factory) as client:
        assert client.metrics["browser_navigations"] == 1
        assert context.pages[0].visited == [INDEX]


async def test_bootstrap_retries_user_agent_during_automatic_navigation():
    class NavigatingPage(Page):
        calls = 0

        async def evaluate(self, code):
            self.calls += 1
            if self.calls == 1:
                raise ValueError("Execution context was destroyed, most likely because of a navigation")
            return "test-user-agent"

    class NavigatingContext(Context):
        async def new_page(self):
            page = NavigatingPage()
            self.pages.append(page)
            return page

    context = NavigatingContext()
    factory, _ = factory_for(lambda generation, url: (200, article(), {}))
    async with HybridClient(context, StageSettings(browser_engine="camoufox"), gate, session_factory=factory):
        assert context.pages[0].calls == 2


def timeout_event():
    class Event:
        async def __aenter__(self):
            raise TimeoutError("Automatic check timed out")

        async def __aexit__(self, *args):
            pass

    return Event()


async def test_homepage_fallback_does_not_require_a_search_input():
    class FallbackPage(Page):
        async def goto(self, url, **kwargs):
            self.url = url
            self.visited.append(url)
            return SimpleNamespace(status=403 if len(self.visited) == 1 else 200, text=self.text)

        def expect_response(self, *args, **kwargs):
            return timeout_event()

        def locator(self, *args):
            pytest.fail("Homepage inputs are irrelevant to sitemap session preparation")

    context = Context()
    page = FallbackPage()

    async def new_page():
        return page

    context.new_page = new_page
    factory, _ = factory_for(lambda generation, url: (200, article(), {}))
    async with HybridClient(context, StageSettings(browser_engine="camoufox"), gate, session_factory=factory):
        assert page.visited == [INDEX, "https://dequienes.cl/", INDEX]


async def test_fast_automatic_reload_before_goto_returns_is_not_missed():
    class FastPage(Page):
        async def goto(self, url, **kwargs):
            self.url = url
            self.visited.append(url)
            response = SimpleNamespace(status=200, url=url, text=self.text)
            for callback in list(self.listeners):
                callback(response)
            return SimpleNamespace(status=403)

        def expect_response(self, *args, **kwargs):
            pytest.fail("The completed reload was already observed during goto")

    context = Context()
    page = FastPage()

    async def new_page():
        return page

    context.new_page = new_page
    factory, _ = factory_for(lambda generation, url: (200, article(), {}))
    async with HybridClient(context, StageSettings(browser_engine="camoufox"), gate, session_factory=factory):
        assert page.visited == [INDEX]
        assert page.listeners == []


async def test_blocked_bootstrap_saves_metadata_html_and_screenshot(tmp_path):
    from src.sites.dequienes.hybrid import BootstrapFailure

    class BlockedPage(Page):
        async def goto(self, url, **kwargs):
            self.url = url
            self.visited.append(url)
            return SimpleNamespace(status=403)

        def expect_response(self, *args, **kwargs):
            return timeout_event()

        async def content(self):
            return "<title>Just a moment...</title>"

        async def screenshot(self, *, path, **kwargs):
            write_test_image(path)

    context = Context()
    page = BlockedPage()

    async def new_page():
        return page

    context.new_page = new_page
    client = HybridClient(context, StageSettings(browser_engine="camoufox"), gate, diagnostics_dir=tmp_path)
    with pytest.raises(BootstrapFailure) as error:
        async with client:
            pytest.fail("Blocked bootstrap must not start HTTP collection")
    assert error.value.state == "blocked"
    failure = json.loads((tmp_path / "failure.json").read_text())
    assert failure["failure"]["state"] == "blocked"
    assert failure["last_http_status"] == 403
    assert failure["http_requests"] == 0
    assert "HTTP 403" in failure["failure"]["message"]
    assert (tmp_path / "page.html").is_file() and (tmp_path / "page.png").is_file()
    assert page.closed


def write_test_image(path):
    from pathlib import Path

    Path(path).write_bytes(b"test-image")
