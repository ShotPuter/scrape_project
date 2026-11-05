from types import SimpleNamespace

import pytest
from playwright.async_api import TimeoutError as BrowserTimeout

from scripts.pep_camoufox_batch import navigate_results

URL = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index?numeroDocumento=1&tipoPersonaId=25"


class Page:
    url = URL

    async def goto(self, url, *, wait_until, **kwargs):
        assert url == URL and wait_until == "commit"
        return SimpleNamespace(status=200)

    async def wait_for_function(self, expression, **kwargs):
        assert "readyState" in expression and "table.table tbody" in expression


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
