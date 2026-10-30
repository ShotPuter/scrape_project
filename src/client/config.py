"""Typed, validated HTTP-client configuration and settings-to-config mapping."""

from __future__ import annotations

import random
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.network.budget import TimeoutPolicy
from src.proxy.config import ProxyConfig
from src.proxy.probes import is_success_response
from src.utils.http import parse_retry_after

MAX_RETRY_DELAY_SECONDS = 60.0
TRANSPORT_CHOICES = frozenset({"aiohttp", "curl_cffi", "auto"})


@dataclass(frozen=True, slots=True)
class RetryConfig:
    attempts: int = 3
    backoff_factor: float = 1.5
    jitter_seconds: float = 0.3
    max_delay_seconds: float = MAX_RETRY_DELAY_SECONDS

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("Retry attempts must be >= 1")
        if self.backoff_factor < 0 or self.jitter_seconds < 0:
            raise ValueError("Retry delays must be non-negative")
        if self.max_delay_seconds <= 0:
            raise ValueError("Retry max delay must be positive")

    def delay_for(self, attempt: int, retry_after: str | None = None) -> float:
        if retry_after and (server_delay := parse_retry_after(retry_after)) is not None:
            # Cloudflare can send very distant Retry-After dates. Cap the wait so a
            # single rate-limited response cannot stall the whole scrape for hours.
            return min(self.max_delay_seconds, server_delay)
        exponential = self.backoff_factor * 2 ** (attempt - 1)
        return float(min(self.max_delay_seconds, exponential) + random.uniform(0, self.jitter_seconds))


@dataclass(frozen=True, slots=True)
class TimeoutConfig:
    """Per-endpoint timeout policies; detail requests may have their own total."""

    api: TimeoutPolicy = field(default_factory=TimeoutPolicy)
    detail: TimeoutPolicy | None = None

    def policy_for(self, endpoint_kind: str) -> TimeoutPolicy:
        if endpoint_kind == "detail" and self.detail is not None:
            return self.detail
        return self.api


@dataclass(frozen=True, slots=True)
class ClientConfig:
    base_url: str = ""
    headers: Mapping[str, str] = field(default_factory=dict)
    timeouts: TimeoutConfig = field(default_factory=TimeoutConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    proxy: ProxyConfig = field(default_factory=ProxyConfig)
    connection_limit: int = 9
    impersonate: str = "chrome"
    cookies_file: Path | None = None
    transport: str = "aiohttp"
    requests_per_second: float = 0
    rate_limit_pause_seconds: float = 150

    def __post_init__(self) -> None:
        base_url = self.base_url.strip().rstrip("/")
        if not base_url:
            raise ValueError("base_url must not be empty")
        object.__setattr__(self, "base_url", base_url)
        transport = self.transport.lower()
        if transport not in TRANSPORT_CHOICES:
            raise ValueError("transport must be 'aiohttp', 'curl_cffi' or 'auto'")
        object.__setattr__(self, "transport", transport)
        if self.connection_limit < 1:
            raise ValueError("connection limit must be >= 1")
        if self.requests_per_second < 0:
            raise ValueError("HTTP_REQUESTS_PER_SECOND must be >= 0")
        if self.rate_limit_pause_seconds <= 0:
            raise ValueError("rate_limit_pause_seconds must be positive")


def client_config_from_settings(
    app_settings: Any,
    *,
    proxy_check_path: str = "",
    proxy_check_payload: Mapping[str, Any] | None = None,
    project_dir: Path,
    proxy_check_validator: Callable[[Any], bool] = is_success_response,
) -> ClientConfig:
    """Build a ``ClientConfig`` from application settings and the site readiness probe."""

    stage_timeouts = {
        "connect": app_settings.connect_timeout_seconds,
        "sock_connect": app_settings.sock_connect_timeout_seconds,
        "sock_read": app_settings.sock_read_timeout_seconds,
    }
    cookie_file = project_dir / app_settings.cookies_file if app_settings.cookies_file else None
    proxy_file = project_dir / app_settings.proxy_file if app_settings.proxy_file else None

    return ClientConfig(
        base_url=app_settings.api_base_url,
        headers=dict(app_settings.default_headers),
        timeouts=TimeoutConfig(
            api=TimeoutPolicy(total=app_settings.request_timeout_seconds, **stage_timeouts),
            detail=TimeoutPolicy(total=app_settings.detail_timeout_seconds, **stage_timeouts),
        ),
        retry=RetryConfig(
            attempts=app_settings.retry_attempts,
            backoff_factor=app_settings.retry_backoff_factor,
            jitter_seconds=app_settings.retry_jitter_seconds,
        ),
        proxy=ProxyConfig(
            file=proxy_file,
            mode=app_settings.proxy_mode,
            sample_size=app_settings.proxy_sample_size,
            check_concurrency=app_settings.proxy_check_concurrency,
            check_timeout=app_settings.proxy_check_timeout,
            min_ready=app_settings.proxy_min_ready,
            replenish_threshold=app_settings.proxy_replenish_threshold,
            failure_cooldown_seconds=app_settings.proxy_failure_cooldown_seconds,
            max_failures=app_settings.proxy_max_failures,
            dead_ttl_hours=app_settings.proxy_dead_ttl_hours,
            db_max_age_hours=app_settings.proxy_db_max_age_hours,
            db_refresh_seconds=app_settings.proxy_db_refresh_seconds,
            verified_reuse_seconds=app_settings.proxy_verified_reuse_seconds,
            warmup_timeout_seconds=app_settings.proxy_warmup_timeout_seconds,
            check_path=proxy_check_path,
            check_validator=proxy_check_validator,
            check_payload=dict(proxy_check_payload or {}),
            warmup=app_settings.proxy_warmup,
            per_ip_limit=app_settings.proxy_per_ip_limit,
            probe_concurrency=app_settings.proxy_probe_concurrency,
            acquire_timeout=app_settings.proxy_acquire_timeout,
            direct_block_cooldown_seconds=app_settings.direct_block_cooldown_seconds,
            mix_share=app_settings.proxy_mix_share,
        ),
        connection_limit=app_settings.max_concurrent_requests,
        impersonate=app_settings.browser_profile,
        cookies_file=cookie_file,
        transport=app_settings.http_transport,
        requests_per_second=app_settings.http_requests_per_second,
        rate_limit_pause_seconds=app_settings.rate_limit_pause_seconds,
    )


__all__ = ["ClientConfig", "ProxyConfig", "RetryConfig", "TimeoutConfig", "client_config_from_settings"]
