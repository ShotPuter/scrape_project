"""Resource-lifecycle regression tests for clients, pools, and the scraper."""

from unittest.mock import AsyncMock

import pytest

from src.client.base_client import BaseHTTPClient
from src.client.proxy_pool import ProxyPool
from src.services.scraper import SeprecScraper
from src.storage.file_storage import FileStorage
from src.storage.publication_csv import PublicationCsv


@pytest.mark.asyncio
async def test_client_close_releases_pool_even_when_direct_session_close_fails():
    client = BaseHTTPClient("https://example.test")
    session = AsyncMock()
    session.close.side_effect = RuntimeError("already closed")
    pool = AsyncMock()
    client._session = session
    client._proxy_pool = pool

    await client.close()

    pool.close.assert_awaited_once()
    assert client._session is None
    assert client._proxy_pool is None

    await client.close()
    pool.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_proxy_pool_close_is_idempotent():
    first, second = AsyncMock(), AsyncMock()
    pool = ProxyPool([], lambda **kwargs: None)
    pool.add_session("http://one:80", first)
    pool.add_session("http://two:80", second)

    await pool.close()
    await pool.close()

    first.close.assert_awaited_once()
    second.close.assert_awaited_once()
    assert pool.sessions == []
    assert pool.retired == []


@pytest.mark.asyncio
async def test_scraper_aenter_closes_partially_opened_client(tmp_path):
    client = AsyncMock()
    client.open.side_effect = RuntimeError("open failed")
    client.close.side_effect = RuntimeError("close failed")
    scraper = SeprecScraper(
        client=client, store=PublicationCsv(tmp_path / "publications.csv"), file_storage=FileStorage(tmp_path / "pdf")
    )

    with pytest.raises(RuntimeError, match="open failed"):
        await scraper.__aenter__()

    client.close.assert_awaited_once()
