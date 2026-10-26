"""Two-stage proxy verification.

Stage 1 checks a real HTTPS connection through the proxy.  Stage 2 checks the
actual Gaceta de Comercio (SEPREC) endpoints used by the scraper, validates the
JSON payload, retries transient failures, and records per-proxy statistics.
"""

from __future__ import annotations

import asyncio
import json
import ssl
import time
from collections import Counter
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import (
    CertificateVerifyError as CurlCertificateVerifyError,
    ChunkedEncodingError as CurlChunkedEncodingError,
    ConnectionError as CurlConnectionError,
    ConnectTimeout as CurlConnectTimeout,
    ContentDecodingError as CurlContentDecodingError,
    InvalidProxyURL as CurlInvalidProxyURL,
    InvalidSchema as CurlInvalidSchema,
    InvalidURL as CurlInvalidURL,
    ProxyError as CurlProxyError,
    ReadTimeout as CurlReadTimeout,
    SSLError as CurlSSLError,
    Timeout as CurlTimeout,
)

from config import settings
from src.client.aiohttp_session import AIOHTTP_AVAILABLE, AiohttpSession
from src.client.network import NetworkBudget, TimeoutPolicy
from src.client.readiness import is_catalog_payload, is_search_payload
from src.models.publication import PublicationSearchFilter
from src.utils.http import parse_retry_after
from src.utils.logger import setup_logger

logger = setup_logger("proxy_verifier")

CATALOG_PATH = "publicaciones/catalogos/lista"
SEARCH_PATH = "publicaciones"
SEARCH_PAGE_SIZE = 10
SEARCH_CATEGORY = "1"

try:
    from aiohttp import (
        ClientConnectionError,
        ClientHttpProxyError,
        ClientOSError,
        ClientPayloadError,
        ClientProxyConnectionError,
        ClientResponseError,
        ConnectionTimeoutError,
        ServerDisconnectedError,
        ServerTimeoutError,
        SocketTimeoutError,
    )
except ImportError:  # pragma: no cover - aiohttp is a runtime dependency
    ClientConnectionError = OSError  # type: ignore[misc,assignment]
    ClientHttpProxyError = OSError  # type: ignore[misc,assignment]
    ClientOSError = OSError  # type: ignore[misc,assignment]
    ClientPayloadError = OSError  # type: ignore[misc,assignment]
    ClientProxyConnectionError = OSError  # type: ignore[misc,assignment]
    ClientResponseError = OSError  # type: ignore[misc,assignment]
    ConnectionTimeoutError = TimeoutError  # type: ignore[misc,assignment]
    ServerDisconnectedError = OSError  # type: ignore[misc,assignment]
    ServerTimeoutError = TimeoutError  # type: ignore[misc,assignment]
    SocketTimeoutError = TimeoutError  # type: ignore[misc,assignment]

try:
    from aiohttp_socks import (
        ProxyConnectionError as AiohttpProxyConnectionError,
        ProxyError as AiohttpProxyError,
        ProxyTimeoutError as AiohttpProxyTimeoutError,
    )
except ImportError:  # pragma: no cover - optional SOCKS adapter
    AiohttpProxyConnectionError = OSError  # type: ignore[misc,assignment]
    AiohttpProxyError = OSError  # type: ignore[misc,assignment]
    AiohttpProxyTimeoutError = TimeoutError  # type: ignore[misc,assignment]

CONNECT_TIMEOUT_ERRORS = (CurlConnectTimeout, ConnectionTimeoutError)
READ_TIMEOUT_ERRORS = (CurlReadTimeout, SocketTimeoutError, ServerTimeoutError)
PROXY_ERRORS = (
    CurlProxyError,
    ClientHttpProxyError,
    ClientProxyConnectionError,
    AiohttpProxyError,
    AiohttpProxyConnectionError,
    AiohttpProxyTimeoutError,
)
CONNECTION_RESET_ERRORS = (
    ConnectionResetError,
    ServerDisconnectedError,
    ClientOSError,
    ClientConnectionError,
    CurlConnectionError,
)
SSL_ERRORS = (ssl.SSLError, ssl.CertificateError, CurlSSLError, CurlCertificateVerifyError)
INVALID_RESPONSE_ERRORS = (ClientPayloadError, CurlChunkedEncodingError, CurlContentDecodingError)
CONFIGURATION_ERRORS = (CurlInvalidProxyURL, CurlInvalidSchema, CurlInvalidURL, ValueError)
NO_RETRY_ERRORS = {"ssl_error", "proxy_configuration_error", "unsupported_protocol", "http_400", "http_401", "http_404"}


@dataclass(frozen=True, slots=True)
class Stage1Result:
    ok: bool
    latency_ms: float = 0.0
    error_type: str = ""
    detail: str = ""


@dataclass(slots=True)
class CheckStats:
    """Per-proxy statistics for the target API check."""

    successful_requests: int = 0
    failed_requests: int = 0
    total_attempts: int = 0
    average_latency_ms: float = 0.0
    last_success: str | None = None
    last_failure: str | None = None
    consecutive_failures: int = 0
    error_types: Counter[str] = field(default_factory=Counter)
    latencies: list[float] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        return self.successful_requests / self.total_attempts if self.total_attempts else 0.0

    def record_success(self, latency_ms: float) -> None:
        self.successful_requests += 1
        self.last_success = datetime.now(UTC).isoformat()
        self.consecutive_failures = 0
        self.latencies.append(latency_ms)

    def record_failure(self, error_type: str) -> None:
        self.failed_requests += 1
        self.last_failure = datetime.now(UTC).isoformat()
        self.consecutive_failures += 1
        self.error_types[error_type] += 1

    def finish(self) -> None:
        self.average_latency_ms = round(sum(self.latencies) / len(self.latencies), 2) if self.latencies else 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "total_attempts": self.total_attempts,
            "success_rate": round(self.success_rate, 4),
            "average_latency": self.average_latency_ms,
            "last_success": self.last_success,
            "last_failure": self.last_failure,
            "consecutive_failures": self.consecutive_failures,
            "error_types": dict(self.error_types),
        }


@dataclass(slots=True)
class Stage2Result:
    ok: bool
    stats: CheckStats
    error_type: str = ""
    detail: str = ""


@dataclass(frozen=True, slots=True)
class _AttemptOutcome:
    ok: bool
    latency_ms: float
    error_type: str = ""
    detail: str = ""
    retry_after: float | None = None


@dataclass(frozen=True, slots=True)
class _ResponseCheck:
    ok: bool
    error_type: str = ""
    detail: str = ""
    retry_after: float | None = None


def catalog_ready(data: object) -> bool:
    return is_catalog_payload(data)


def search_ready(data: object) -> bool:
    return is_search_payload(data)


def classify_status(status: int) -> str:
    if 500 <= status <= 599:
        return "http_5xx"
    return f"http_{status}"


def classify_exception(exc: BaseException) -> str:
    """Map transport/parser exceptions to stable, reportable error classes."""
    if isinstance(exc, CONNECT_TIMEOUT_ERRORS):
        return "connect_timeout"
    if isinstance(exc, READ_TIMEOUT_ERRORS):
        return "read_timeout"
    if isinstance(exc, CurlTimeout):
        message = str(exc).lower()
        if "connection timed out" in message or "connect timeout" in message:
            return "connect_timeout"
        return "read_timeout"
    if isinstance(exc, TimeoutError):
        return "read_timeout"
    if isinstance(exc, PROXY_ERRORS):
        return "proxy_connection_error"
    if isinstance(exc, SSL_ERRORS):
        return "ssl_error"
    if isinstance(exc, json.JSONDecodeError):
        return "invalid_json"
    if isinstance(exc, CONNECTION_RESET_ERRORS):
        message = str(exc).lower()
        if "proxy" in message or "connect tunnel" in message:
            return "proxy_connection_error"
        return "connection_reset"
    if isinstance(exc, ClientResponseError):
        return classify_status(int(getattr(exc, "status", 0) or 0))
    if isinstance(exc, INVALID_RESPONSE_ERRORS):
        return "unexpected_response"
    if isinstance(exc, CONFIGURATION_ERRORS):
        return "proxy_configuration_error"
    name = type(exc).__name__.lower()
    if "proxy" in name:
        return "proxy_connection_error"
    if "ssl" in name or "certificate" in name:
        return "ssl_error"
    if "timeout" in name:
        return "read_timeout"
    return name or "unknown_error"


def should_retry(error_type: str, attempt: int, attempts: int) -> bool:
    if attempt >= attempts:
        return False
    if error_type in NO_RETRY_ERRORS:
        return False
    if error_type == "http_403":
        # A 403 can be a proxy/UA/rate issue.  One retry distinguishes a
        # transient block from a persistent one without burning every attempt.
        return attempt < min(attempts, 2)
    return True


def retry_delay_for(
    error_type: str, attempt: int, *, base_delay: float, retry_after: float | None, max_delay: float
) -> float:
    if error_type == "http_429" and retry_after is not None:
        return max(0.0, min(max_delay, retry_after))
    delay = base_delay * (2 ** max(0, attempt - 1))
    return max(0.0, min(max_delay, delay))


def _retry_after(headers: Any) -> float | None:
    if not headers or not hasattr(headers, "get"):
        return None
    raw = headers.get("Retry-After")
    if raw is None:
        return None
    return parse_retry_after(str(raw).strip())


def _session_for(proxy_url: str, *, transport: str, user_agent: str, policy: TimeoutPolicy):
    headers = {**settings.default_headers, "User-Agent": user_agent}
    if transport == "curl_cffi":
        return AsyncSession(
            proxy=proxy_url,
            headers=headers,
            impersonate=settings.browser_profile,
            timeout=(policy.connect, policy.total),
            max_clients=1,
            trust_env=False,
        )
    if not AIOHTTP_AVAILABLE:
        raise RuntimeError("aiohttp is not installed")
    return AiohttpSession(proxy=proxy_url, headers=headers, timeout=policy, max_clients=1)


async def _request(
    session: Any,
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    timeout: float,  # noqa: ASYNC109 - hard guard around the transport deadline
):
    guard = timeout + 1.0
    if isinstance(session, AiohttpSession):
        return await asyncio.wait_for(session.request(method, url, json=payload), guard)
    if method == "GET":
        return await asyncio.wait_for(session.get(url), guard)
    return await asyncio.wait_for(session.post(url, json=payload), guard)


async def verify_stage1(
    proxy_url: str,
    *,
    transport: str,
    user_agent: str,
    check_url: str | None = None,
    timeout: float | None = None,  # noqa: ASYNC109 - network deadline
    connect_timeout: float | None = None,
    budget: NetworkBudget | None = None,
) -> Stage1Result:
    """Verify that the proxy can make a real HTTPS request."""
    check_url = check_url or settings.proxy_stage1_check_url
    timeout = settings.proxy_stage1_timeout if timeout is None else timeout
    connect_timeout = settings.proxy_stage1_connect_timeout if connect_timeout is None else connect_timeout
    budget = budget or NetworkBudget(1)
    policy = TimeoutPolicy(total=timeout, connect=connect_timeout, sock_connect=connect_timeout, sock_read=timeout)
    session = None
    started = 0.0
    try:
        session = _session_for(proxy_url, transport=transport, user_agent=user_agent, policy=policy)
        async with budget.slot():
            started = time.monotonic()
            response = await _request(session, "GET", check_url, timeout=timeout)
            latency_ms = round((time.monotonic() - started) * 1000, 2)
        status = int(response.status_code)
        if not 200 <= status < 300:
            return Stage1Result(False, latency_ms, classify_status(status), f"HTTP {status}")
        try:
            data = response.json()
        except (ValueError, UnicodeError):
            return Stage1Result(False, latency_ms, "invalid_json", "stage-1 response is not JSON")
        if not isinstance(data, dict) or not data.get("ip"):
            return Stage1Result(False, latency_ms, "unexpected_response", "stage-1 response has no ip field")
        return Stage1Result(True, latency_ms)
    except Exception as exc:  # noqa: BLE001 - one bad proxy must not stop the scan
        latency_ms = round((time.monotonic() - started) * 1000, 2) if started else 0.0
        error_type = classify_exception(exc)
        return Stage1Result(False, latency_ms, error_type, f"{type(exc).__name__}: {exc}")
    finally:
        if session is not None:
            with suppress(Exception):  # noqa: BLE001
                await session.close()


def _check_response(response: Any, validator) -> _ResponseCheck:
    status = int(response.status_code)
    if not 200 <= status < 300:
        return _ResponseCheck(False, classify_status(status), f"HTTP {status}", _retry_after(response.headers))
    try:
        data = response.json()
    except (ValueError, UnicodeError):
        return _ResponseCheck(False, "invalid_json", "response body is not valid JSON")
    if not validator(data):
        return _ResponseCheck(False, "unexpected_response", "response does not contain the expected API fields")
    return _ResponseCheck(True)


async def _target_attempt(
    session: Any, *, api_base_url: str, catalog_path: str, search_path: str, policy: TimeoutPolicy
) -> _AttemptOutcome:
    started = time.monotonic()
    catalog = await _request(session, "POST", f"{api_base_url}/{catalog_path}", payload={"codigo": ""}, timeout=policy.total)
    check = _check_response(catalog, catalog_ready)
    if not check.ok:
        latency_ms = round((time.monotonic() - started) * 1000, 2)
        return _AttemptOutcome(False, latency_ms, check.error_type, check.detail, check.retry_after)

    filters = PublicationSearchFilter(page_size=SEARCH_PAGE_SIZE, category=SEARCH_CATEGORY)
    search = await _request(
        session, "POST", f"{api_base_url}/{search_path}", payload=filters.to_api_payload(), timeout=policy.total
    )
    check = _check_response(search, search_ready)
    latency_ms = round((time.monotonic() - started) * 1000, 2)
    return _AttemptOutcome(check.ok, latency_ms, check.error_type, check.detail, check.retry_after)


async def verify_stage2(
    proxy_url: str,
    *,
    transport: str,
    user_agent: str,
    attempts: int | None = None,
    timeout: float | None = None,  # noqa: ASYNC109 - network deadline
    connect_timeout: float | None = None,
    retry_delay: float | None = None,
    max_retry_delay: float | None = None,
    api_base_url: str | None = None,
    catalog_path: str = CATALOG_PATH,
    search_path: str = SEARCH_PATH,
    budget: NetworkBudget | None = None,
) -> Stage2Result:
    """Check the real SEPREC catalog and publication-search endpoints."""
    attempts = settings.proxy_stage2_attempts if attempts is None else attempts
    timeout = settings.proxy_stage2_timeout if timeout is None else timeout
    connect_timeout = settings.proxy_stage2_connect_timeout if connect_timeout is None else connect_timeout
    retry_delay = settings.proxy_stage2_retry_delay if retry_delay is None else retry_delay
    max_retry_delay = settings.proxy_stage2_max_retry_delay if max_retry_delay is None else max_retry_delay
    api_base_url = (api_base_url or settings.api_base_url).rstrip("/")
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    policy = TimeoutPolicy(total=timeout, connect=connect_timeout, sock_connect=connect_timeout, sock_read=timeout)
    budget = budget or NetworkBudget(1)
    stats = CheckStats()
    final_error = ""
    final_detail = ""

    for attempt in range(1, attempts + 1):
        stats.total_attempts += 1
        session = None
        started = 0.0
        outcome: _AttemptOutcome
        try:
            session = _session_for(proxy_url, transport=transport, user_agent=user_agent, policy=policy)
            async with budget.slot():
                started = time.monotonic()
                outcome = await _target_attempt(
                    session, api_base_url=api_base_url, catalog_path=catalog_path, search_path=search_path, policy=policy
                )
        except Exception as exc:  # noqa: BLE001 - classify and retry
            latency_ms = round((time.monotonic() - started) * 1000, 2) if started else 0.0
            outcome = _AttemptOutcome(False, latency_ms, classify_exception(exc), f"{type(exc).__name__}: {exc}")
        finally:
            if session is not None:
                with suppress(Exception):  # noqa: BLE001
                    await session.close()

        if outcome.ok:
            stats.record_success(outcome.latency_ms)
            final_error = ""
            final_detail = ""
            break

        stats.record_failure(outcome.error_type)
        final_error = outcome.error_type
        final_detail = outcome.detail
        if not should_retry(outcome.error_type, attempt, attempts):
            break
        delay = retry_delay_for(
            outcome.error_type, attempt, base_delay=retry_delay, retry_after=outcome.retry_after, max_delay=max_retry_delay
        )
        if delay:
            await asyncio.sleep(delay)

    stats.finish()
    return Stage2Result(ok=stats.successful_requests > 0, stats=stats, error_type=final_error, detail=final_detail)


def compatible_transports(protocol: str, requested: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    """Return transports that can speak the candidate's proxy protocol."""
    available = []
    for transport in requested:
        if transport == "aiohttp" and not AIOHTTP_AVAILABLE:
            continue
        if transport == "aiohttp" and protocol.startswith("socks"):
            # Import lazily because the optional SOCKS adapter may be unavailable.
            from src.client.aiohttp_session import ProxyConnector

            if ProxyConnector is None:
                continue
        available.append(transport)
    return tuple(available)
