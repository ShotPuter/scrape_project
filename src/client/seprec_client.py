"""High-level client for SEPREC API endpoints and response schema validation."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, Self, TypeVar

from pydantic import TypeAdapter, ValidationError

from config import BASE_DIR, Settings, settings
from src.client.base_client import HttpClient
from src.client.config import ClientConfig, client_config_from_settings
from src.client.errors import HTTPClientError
from src.models.publication import CatalogItem, PublicationDetail, PublicationSearchFilter, PublicationSummary
from src.utils.helpers import SeprecAPIError, SeprecRateLimitError
from src.utils.logger import setup_logger

logger = setup_logger("seprec_client")
T = TypeVar("T")

CATALOGS_ADAPTER = TypeAdapter(list[CatalogItem])
SUMMARIES_ADAPTER = TypeAdapter(list[PublicationSummary])
DETAIL_ADAPTER = TypeAdapter(PublicationDetail)
CATALOGS_ENDPOINT = "publicaciones/catalogos/lista"
PUBLICATIONS_SEARCH_ENDPOINT = "publicaciones"
RATE_LIMIT_STATUS_CODE = 429


class SeprecClient:
    """
    High-level API client for the SEPREC Bolivian Commercial Gazette.

    Wraps a generic ``HttpClient`` and encapsulates endpoint paths, request data
    extraction, and Pydantic validation. Configuration comes from the composition
    root as a ``ClientConfig`` (or from ``Settings`` as a convenience default).
    """

    def __init__(
        self, base_url: str | None = None, headers: Mapping[str, str] | None = None,
        timeout_seconds: int | None = None, retry_attempts: int | None = None, retry_backoff_factor: float | None = None,
        retry_jitter_seconds: float | None = None, connection_limit: int | None = None, proxy_warmup: bool | None = None,
        proxy_file: Path | None = None, proxy_db_path: Path | None = None, transport: str | None = None,
        proxy_mode: str | None = None, proxy_mix_share: float | None = None,
        config: ClientConfig | None = None, app_settings: Settings | None = None,
    ) -> None:
        resolved_settings = app_settings or settings
        self._client = HttpClient(
            config=config
            or client_config_from_settings(
                resolved_settings,
                base_url=base_url, headers=headers, timeout_seconds=timeout_seconds,
                retry_attempts=retry_attempts, retry_backoff_factor=retry_backoff_factor,
                retry_jitter_seconds=retry_jitter_seconds, connection_limit=connection_limit,
                proxy_warmup=proxy_warmup, proxy_file=proxy_file, proxy_db_path=proxy_db_path,
                transport=transport, proxy_mode=proxy_mode, proxy_mix_share=proxy_mix_share,
                proxy_check_path=CATALOGS_ENDPOINT, proxy_check_payload={"codigo": ""},
                project_dir=BASE_DIR,
            )
        )
        self.logger = logger
        self.metrics_before_run: dict[str, int] = {}

    @property
    def http(self) -> HttpClient:
        return self._client

    @property
    def requests_made(self) -> int:
        return self._client.requests_made

    @property
    def retries_made(self) -> int:
        return self._client.retries_made

    @property
    def metrics(self):
        return self._client.metrics

    @property
    def budget(self):
        return self._client.budget

    @property
    def proxy_mode(self) -> str:
        return self._client.proxy_mode

    @property
    def run_id(self) -> str | None:
        return self._client.run_id

    @run_id.setter
    def run_id(self, value: str | None) -> None:
        self._client.run_id = value

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def open(self) -> None:
        await self._client.open()

    async def close(self) -> None:
        await self._client.close()

    def _backoff_seconds(self, attempt: int, retry_after: str | None = None) -> float:
        return self._client._backoff_seconds(attempt, retry_after)

    async def get_catalogs(self) -> list[CatalogItem]:
        """Fetch categories and subcategories definitions from SEPREC catalog."""
        response_payload = await self._request_data("POST", CATALOGS_ENDPOINT, {"codigo": ""})
        return self._validate_response(CATALOGS_ADAPTER, response_payload, CATALOGS_ENDPOINT)

    async def search_publications(self, filters: PublicationSearchFilter) -> tuple[int, list[PublicationSummary]]:
        """
        Execute paginated publication search against SEPREC database.

        Args:
            filters: Typed search criteria (page, limit, category, dates).

        Returns:
            Tuple of (total_matching_count, list of PublicationSummary).
        """
        response_payload = await self._request_data("POST", PUBLICATIONS_SEARCH_ENDPOINT, filters.to_api_payload())
        if not isinstance(response_payload, dict):
            raise SeprecAPIError("Invalid search response: expected dictionary payload")

        if "total" not in response_payload or "filas" not in response_payload:
            raise SeprecAPIError("Invalid search response: expected 'total' and 'filas' keys")

        try:
            total_matches = int(response_payload["total"])
        except (ValueError, TypeError, OverflowError) as parse_error:
            raise SeprecAPIError("Invalid search response: total must be a valid integer") from parse_error

        summaries = self._validate_response(SUMMARIES_ADAPTER, response_payload["filas"], PUBLICATIONS_SEARCH_ENDPOINT)
        return total_matches, summaries

    async def get_publication_detail(self, publication_id: str) -> PublicationDetail:
        """Fetch complete publication content, metadata, and attached documents."""
        endpoint_path = f"publicaciones/{publication_id}"
        response_payload = await self._request_data("GET", endpoint_path)
        return self._validate_response(DETAIL_ADAPTER, response_payload, endpoint_path)

    async def _request_data(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        """Execute HTTP request, unwrap 'datos', and verify business completion flag."""
        try:
            response_json = await self._client.request(method, path, payload)
        except HTTPClientError as client_error:
            if client_error.status_code == RATE_LIMIT_STATUS_CODE:
                raise SeprecRateLimitError(str(client_error), status_code=client_error.status_code) from client_error
            raise SeprecAPIError(str(client_error), status_code=client_error.status_code) from client_error

        if "finalizado" in response_json and not response_json["finalizado"]:
            error_message = response_json.get("mensaje", "Unknown SEPREC business error")
            raise SeprecAPIError(f"API operation not completed: {error_message}", details=response_json)

        if "datos" not in response_json or response_json["datos"] is None:
            raise SeprecAPIError(f"Missing 'datos' payload field in response from {path}")

        return response_json["datos"]

    @staticmethod
    def _validate_response(adapter: TypeAdapter[T], raw_data: Any, path: str) -> T:
        """Validate raw API payload through Pydantic TypeAdapter."""
        try:
            return adapter.validate_python(raw_data)
        except ValidationError as validation_error:
            raise SeprecAPIError(f"Invalid schema format in publication response from {path}") from validation_error
