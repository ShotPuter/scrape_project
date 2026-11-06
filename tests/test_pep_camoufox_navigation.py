from types import SimpleNamespace

import pytest
from playwright.async_api import TimeoutError as BrowserTimeout

from scripts.pep_camoufox_batch import PageCreationTimeout, create_page, navigate_results

URL = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?numeroDocumento=1&tipoPersonaId=25"
TABLE = """<table class="table"><thead><tr><th>Declarante</th><th>Descargar</th>
<th>Fecha de publicación</th></tr></thead><tbody><tr><td>Person</td><td>
<form action="/fdci/consultaCiudadana/generarReporteConsultaCiudadana">
<input name="id" value="123"><input name="captchatoken"></form></td><td>2026-01-01</td></tr></tbody></table>"""


class Page:
    url = URL

    async def goto(self, url, *, wait_until, **kwargs):
        assert url == URL and wait_until == "commit"
        return SimpleNamespace(status=200)

    async def wait_for_function(self, expression, **kwargs):
        assert "readyState" in expression and "table.table tbody" in expression
        assert kwargs["polling"] == 250


@pytest.mark.asyncio
async def test_ready_results_do_not_require_domcontentloaded():
    assert (await navigate_results(Page(), URL, 5)).status == 200


@pytest.mark.asyncio
async def test_wrong_identity_is_not_accepted():
    page = Page()
    page.url = URL.replace("numeroDocumento=1", "numeroDocumento=2")
    with pytest.raises(ValueError, match="different identity"):
        await navigate_results(page, URL, 5)


@pytest.mark.asyncio
async def test_incomplete_html_is_not_accepted():
    class Incomplete(Page):
        async def wait_for_function(self, *args, **kwargs):
            raise BrowserTimeout("Result table not ready")

    with pytest.raises(BrowserTimeout):
        await navigate_results(Incomplete(), URL, 5)


class SuspendedWaiter(Page):
    async def wait_for_function(self, *args, **kwargs):
        raise BrowserTimeout("waiter did not run")

    async def evaluate(self, *args):
        return {"url": self.url, "ready": "complete", "html": TABLE}


@pytest.mark.asyncio
async def test_timed_out_waiter_accepts_verified_ready_results():
    diagnostic = {}
    response = await navigate_results(SuspendedWaiter(), URL, 5, diagnostic)
    assert response.status == 200 and diagnostic["navigation_wait_fallback"]


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["identity", "loading", "missing_table"])
async def test_timeout_fallback_rejects_wrong_or_unready_html(invalid):
    class Invalid(SuspendedWaiter):
        async def evaluate(self, *args):
            snapshot = await super().evaluate()
            if invalid == "identity":
                snapshot["url"] = URL.replace("numeroDocumento=1", "numeroDocumento=2")
            elif invalid == "loading":
                snapshot["ready"] = "loading"
            else:
                snapshot["html"] = "<html>Generic help</html>"
            return snapshot

    with pytest.raises(BrowserTimeout):
        await navigate_results(Invalid(), URL, 5)


@pytest.mark.asyncio
async def test_no_committed_response_cannot_reuse_old_results():
    class NoResponse(SuspendedWaiter):
        async def goto(self, *args, **kwargs):
            raise BrowserTimeout("no response")

        async def evaluate(self, *args):
            raise AssertionError("Must not inspect stale HTML before commit")

    diagnostics = {"landing_http_status": 200}
    with pytest.raises(BrowserTimeout):
        await navigate_results(NoResponse(), URL, 5, diagnostics)
    assert diagnostics["landing_http_status"] is None


@pytest.mark.asyncio
async def test_outer_navigation_deadline_can_recover_already_ready_html():
    import asyncio

    class SlowWait(SuspendedWaiter):
        async def wait_for_function(self, *args, **kwargs):
            await asyncio.sleep(1)

    diagnostics = {}
    response = await navigate_results(SlowWait(), URL, 0.01, diagnostics)
    assert response.status == 200 and diagnostics["navigation_wait_fallback"]


@pytest.mark.asyncio
async def test_stalled_page_creation_times_out_and_other_context_can_continue():
    import asyncio

    cancelled = asyncio.Event()

    class Stalled:
        async def new_page(self):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    marker = object()

    class Healthy:
        async def new_page(self):
            return marker

    bad = asyncio.create_task(create_page(Stalled(), 0.02))
    assert await create_page(Healthy(), 0.02) is marker
    with pytest.raises(PageCreationTimeout):
        await bad
    assert cancelled.is_set()
