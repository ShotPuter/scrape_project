import base64
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.sites.res.response_capture import ResponseCapture
from src.sites.res.source import HOST, SEARCH_PATH, SearchIssue


@pytest.mark.parametrize("encoded", [False, True])
async def test_capture_keeps_original_json_before_redirect_eviction(encoded):
    payload = {"d": json.dumps({"Link": "Busqueda/ActuacionDetalle.aspx?codigo=public"})}
    text = json.dumps(payload)
    calls = []

    async def send(method, params=None):
        calls.append(method)
        if method == "Fetch.getResponseBody":
            return {"body": base64.b64encode(text.encode()).decode() if encoded else text, "base64Encoded": encoded}
        return None

    session = SimpleNamespace(send=send, on=lambda *a: None, detach=AsyncMock())
    context = SimpleNamespace(new_cdp_session=AsyncMock(return_value=session))
    url = f"https://{HOST}{SEARCH_PATH}"
    response = SimpleNamespace(url=url, json=AsyncMock(side_effect=RuntimeError("body evicted by redirect")))
    async with ResponseCapture(context, object()) as capture:
        await capture.capture({"requestId": "real-request", "request": {"url": url}})
        assert await capture.read(response) == payload
    assert calls.index("Fetch.getResponseBody") < calls.index("Fetch.continueResponse")
    response.json.assert_not_awaited()
    session.detach.assert_awaited_once()


async def test_invalid_capture_is_not_an_empty_success_and_response_is_released():
    calls = []

    async def send(method, params=None):
        calls.append(method)
        return {"body": "<html>error</html>", "base64Encoded": False}

    session = SimpleNamespace(send=send, on=lambda *a: None, detach=AsyncMock())
    context = SimpleNamespace(new_cdp_session=AsyncMock(return_value=session))
    url = f"https://{HOST}{SEARCH_PATH}"
    async with ResponseCapture(context, object()) as capture:
        await capture.capture({"requestId": "request", "request": {"url": url}})
        with pytest.raises(SearchIssue, match="Could not capture"):
            await capture.read(SimpleNamespace(url=url))
    assert "Fetch.continueResponse" in calls
