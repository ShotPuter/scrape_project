"""High-level client for SEPREC API endpoints and response schema validation."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, TypeVar

from pydantic import TypeAdapter, ValidationError

from config import BASE_DIR, settings
from src.client.base_client import BaseHTTPClient
from src.client.errors import HTTPClientError
from src.client.network import TimeoutPolicy
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


class SeprecClient(BaseHTTPClient):
    """
    High-level API client for the SEPREC Bolivian Commercial Gazette.
    Encapsulates endpoint paths, request data extraction, and Pydantic validation.
    """

    def __init__(
        self, base_url: str | None = None, headers: Mapping[str, str] | None = None,
        timeout_seconds: int | None = None, retry_attempts: int | None = None, retry_backoff_factor: float | None = None,
        retry_jitter_seconds: float | None = None, connection_limit: int | None = None, proxy_warmup: bool | None = None,
        proxy_file: Path | None = None, proxy_db_path: Path | None = None, transport: str | None = None,
        proxy_mode: str | None = None, proxy_mix_share: float | None = None,
    ) -> None:
        resolved_timeout = settings.request_timeout_seconds if timeout_seconds is None else timeout_seconds
        api_timeout = TimeoutPolicy(
            total=resolved_timeout, connect=settings.connect_timeout_seconds,
            sock_connect=settings.sock_connect_timeout_seconds, sock_read=settings.sock_read_timeout_seconds,
        )
        detail_timeout = TimeoutPolicy(
            total=settings.detail_timeout_seconds if timeout_seconds is None else timeout_seconds,
            connect=settings.connect_timeout_seconds, sock_connect=settings.sock_connect_timeout_seconds,
            sock_read=settings.sock_read_timeout_seconds,
        )

        super().__init__(
            base_url=base_url or settings.api_base_url, headers={**settings.default_headers, **(headers or {})},
            timeout_seconds=resolved_timeout,
            retry_attempts=(settings.retry_attempts if retry_attempts is None else retry_attempts),
            retry_backoff_factor=(settings.retry_backoff_factor if retry_backoff_factor is None else retry_backoff_factor),
            retry_jitter_seconds=(settings.retry_jitter_seconds if retry_jitter_seconds is None else retry_jitter_seconds),
            connection_limit=(settings.max_concurrent_requests if connection_limit is None else connection_limit),
            impersonate=settings.browser_profile,
            proxy_file=proxy_file or (BASE_DIR / settings.proxy_file if settings.proxy_file else None),
            proxy_db_path=proxy_db_path, proxy_sample_size=settings.proxy_sample_size,
            proxy_check_concurrency=settings.proxy_check_concurrency, proxy_check_timeout=settings.proxy_check_timeout,
            proxy_min_ready=settings.proxy_min_ready, proxy_replenish_threshold=settings.proxy_replenish_threshold,
            proxy_failure_cooldown_seconds=settings.proxy_failure_cooldown_seconds,
            proxy_max_failures=settings.proxy_max_failures, proxy_dead_ttl_hours=settings.proxy_dead_ttl_hours,
            proxy_check_path=CATALOGS_ENDPOINT, proxy_check_payload={"codigo": ""},
            cookies_file=(BASE_DIR / settings.cookies_file if settings.cookies_file else None),
            proxy_warmup=(settings.proxy_warmup if proxy_warmup is None else proxy_warmup),
            transport=settings.http_transport if transport is None else transport, api_timeout_policy=api_timeout,
            detail_timeout_policy=detail_timeout, requests_per_second=settings.http_requests_per_second,
            proxy_per_ip_limit=settings.proxy_per_ip_limit, proxy_probe_concurrency=settings.proxy_probe_concurrency,
            proxy_acquire_timeout=settings.proxy_acquire_timeout,
            direct_block_cooldown_seconds=settings.direct_block_cooldown_seconds,
            proxy_mode=settings.proxy_mode if proxy_mode is None else proxy_mode,
            proxy_mix_share=(settings.proxy_mix_share if proxy_mix_share is None else proxy_mix_share),
        )
        self.logger = logger

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
            response_json = await self._request(method, path, payload)
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
