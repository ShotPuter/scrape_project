"""HTTP client tests against a real local aiohttp server."""

from contextlib import asynccontextmanager

import pytest
from aiohttp import web

from src.client.base_client import HttpClient
from src.client.config import ClientConfig, RetryConfig
from src.client.seprec_client import SeprecClient
from src.models.publication import PublicationSearchFilter
from src.network.budget import TimeoutPolicy
from src.utils.helpers import SeprecAPIError


@asynccontextmanager
async def local_api(app: web.Application):
    """Run an aiohttp app on an ephemeral port and yield its API base URL."""
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}/api"
    finally:
        await runner.cleanup()


def make_client(base_url: str, retry_attempts: int = 3) -> SeprecClient:
    retry = RetryConfig(attempts=retry_attempts, backoff_factor=0.01, jitter_seconds=0.0)
    return SeprecClient(config=ClientConfig(base_url=base_url, retry=retry))


@pytest.mark.asyncio
async def test_retries_server_errors_with_backoff():
    calls = {"count": 0}

    async def handler(request):
        calls["count"] += 1
        if calls["count"] == 1:
            return web.Response(status=503, text="temporarily down")
        return web.json_response({"finalizado": True, "datos": {"total": 0, "filas": []}})

    app = web.Application()
    app.router.add_post("/api/publicaciones", handler)

    async with local_api(app) as base_url:
        client = make_client(base_url, retry_attempts=2)
        async with client:
            total, rows = await client.search_publications(PublicationSearchFilter())

    assert (total, rows) == (0, [])
    assert calls["count"] == 2
    assert client.requests_made == 2
    assert client.retries_made == 1


@pytest.mark.asyncio
async def test_honours_retry_after_on_429():
    calls = {"count": 0}

    async def handler(request):
        calls["count"] += 1
        if calls["count"] == 1:
            return web.Response(status=429, headers={"Retry-After": "0"})
        return web.json_response({"finalizado": True, "datos": {"total": 1, "filas": []}})

    app = web.Application()
    app.router.add_post("/api/publicaciones", handler)

    async with local_api(app) as base_url:
        client = make_client(base_url, retry_attempts=2)
        async with client:
            total, _ = await client.search_publications(PublicationSearchFilter())

    assert total == 1
    assert calls["count"] == 2


@pytest.mark.asyncio
async def test_uses_json_retry_after_instead_of_distant_header_date():
    calls = {"count": 0}

    async def handler(request):
        calls["count"] += 1
        if calls["count"] == 1:
            return web.json_response({"error_code": 1015, "retry_after": 0}, status=429, headers={"Retry-After": "999"})
        return web.json_response({"finalizado": True, "datos": {"total": 1, "filas": []}})

    app = web.Application()
    app.router.add_post("/api/publicaciones", handler)

    async with local_api(app) as base_url:
        client = make_client(base_url, retry_attempts=2)
        async with client:
            total, _ = await client.search_publications(PublicationSearchFilter())

    assert total == 1
    assert calls["count"] == 2


@pytest.mark.asyncio
async def test_client_errors_are_not_retried():
    calls = {"count": 0}

    async def handler(request):
        calls["count"] += 1
        return web.json_response({"mensaje": "bad request"}, status=400)

    app = web.Application()
    app.router.add_post("/api/publicaciones", handler)

    async with local_api(app) as base_url:
        client = make_client(base_url, retry_attempts=3)
        async with client:
            with pytest.raises(SeprecAPIError) as exc_info:
                await client.search_publications(PublicationSearchFilter())

    assert exc_info.value.status_code == 400
    assert calls["count"] == 1
    assert client.retries_made == 0


@pytest.mark.asyncio
async def test_business_error_flag_raises():
    async def handler(request):
        return web.json_response({"finalizado": False, "mensaje": "operación no completada"})

    app = web.Application()
    app.router.add_post("/api/publicaciones", handler)

    async with local_api(app) as base_url:
        client = make_client(base_url)
        async with client:
            with pytest.raises(SeprecAPIError, match="operación no completada"):
                await client.search_publications(PublicationSearchFilter())


@pytest.mark.asyncio
async def test_catalogs_and_detail_endpoints(detail_text_payload, detail_pdf_payload):
    async def catalogs(request):
        return web.json_response(
            {
                "finalizado": True,
                "datos": [
                    {
                        "idParametrica": 1001,
                        "nombreParametrica": "Aumento de Capital Social",
                        "codigoParametrica": "1001",
                        "idParametricaCategoria": 1001,
                        "nombreParametricaCategoria": "INSTRUMENTOS",
                        "codigoParametricaCategoria": "1",
                    }
                ],
            }
        )

    async def detail(request):
        publication_id = request.match_info["publication_id"]
        payload = detail_pdf_payload if publication_id == "228340" else detail_text_payload
        return web.json_response({"finalizado": True, "datos": payload})

    app = web.Application()
    app.router.add_post("/api/publicaciones/catalogos/lista", catalogs)
    app.router.add_get("/api/publicaciones/{publication_id}", detail)

    async with local_api(app) as base_url:
        client = make_client(base_url)
        async with client:
            categories = await client.get_catalogs()
            pdf_publication = await client.get_publication_detail("228340")
            text_publication = await client.get_publication_detail("228345")

    assert categories[0].subcategory_code == "1001"
    assert categories[0].category_code == "1"
    assert pdf_publication.document_base64 is not None
    assert text_publication.document_base64 is None
    assert text_publication.company.registration_number == "422064022"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 503])
async def test_exhausted_retries_count_only_actual_repeats(status):
    from src.utils.helpers import SeprecRateLimitError

    async def handler(request):
        return web.Response(status=status, headers={"Retry-After": "0"})

    app = web.Application()
    app.router.add_post("/api/publicaciones", handler)
    async with local_api(app) as base_url, make_client(base_url, retry_attempts=2) as client:
        error_type = SeprecRateLimitError if status == 429 else SeprecAPIError
        with pytest.raises(error_type) as error:
            await client.search_publications(PublicationSearchFilter())
        assert error.value.status_code == status
        assert client.requests_made == 2
        assert client.retries_made == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        "not JSON",
        "[]",
        '{"finalizado":true}',
        '{"datos":null}',
        '{"datos":{}}',
        '{"datos":{"total":0,"filas":null}}',
        '{"datos":{"total":1,"filas":[{}]}}',
    ],
)
async def test_malformed_search_response_is_not_an_empty_page(body):
    async def handler(request):
        return web.Response(text=body)

    app = web.Application()
    app.router.add_post("/api/publicaciones", handler)
    async with local_api(app) as base_url, make_client(base_url) as client:
        with pytest.raises(SeprecAPIError):
            await client.search_publications(PublicationSearchFilter())
        assert client.requests_made == 1
        assert client.retries_made == 0


@pytest.mark.asyncio
async def test_cloudflare_challenge_is_not_retried():
    async def handler(request):
        return web.Response(status=403, headers={"cf-mitigated": "challenge"})

    app = web.Application()
    app.router.add_post("/api/publicaciones", handler)
    async with local_api(app) as base_url, make_client(base_url) as client:
        with pytest.raises(SeprecAPIError, match="Cloudflare challenge"):
            await client.search_publications(PublicationSearchFilter())
        assert client.requests_made == 1


def test_retry_after_supports_http_dates():
    from datetime import UTC, datetime, timedelta
    from email.utils import format_datetime

    retry = RetryConfig(backoff_factor=0.01, jitter_seconds=0)
    deadline = datetime.now(UTC) + timedelta(seconds=30)
    assert 28 <= retry.delay_for(1, format_datetime(deadline, usegmt=True)) <= 30
    assert retry.delay_for(1, "invalid") == 0.01
    assert retry.delay_for(1, "1562") == 60


@pytest.mark.asyncio
async def test_transport_retries_network_errors_and_propagates_cancellation(monkeypatch):
    import asyncio
    from unittest.mock import AsyncMock

    from curl_cffi.requests.exceptions import ConnectionError

    client = HttpClient(ClientConfig(base_url="http://localhost"))
    request = AsyncMock(side_effect=[ConnectionError("disconnected"), {"ok": True}])
    monkeypatch.setattr(client, "_request_once", request)
    assert await client.request("GET", "test") == {"ok": True}
    assert (client.requests_made, client.retries_made) == (2, 1)
    request.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await client.request("GET", "test")
    assert client.retries_made == 1


@pytest.mark.parametrize(
    "build_config", [lambda: RetryConfig(attempts=0), lambda: TimeoutPolicy(total=0), lambda: RetryConfig(backoff_factor=-1)]
)
def test_invalid_client_configuration_is_rejected(build_config):
    with pytest.raises(ValueError):
        build_config()
