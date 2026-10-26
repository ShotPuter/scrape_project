"""Typed, validated HTTP-client configuration and settings-to-config mapping."""

from __future__ import annotations

import random
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.client.network import TimeoutPolicy
from src.utils.http import parse_retry_after

MAX_RETRY_DELAY_SECONDS = 60.0
TRANSPORT_CHOICES = frozenset({"aiohttp", "curl_cffi", "auto"})
PROXY_MODES = frozenset({"direct", "fallback", "always", "pool"})


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
class ProxyConfig:
    file: Path | None = None
    db_path: Path | None = None
    mode: str = "pool"
    sample_size: int = 0
    check_concurrency: int = 5
    check_timeout: float = 10
    min_ready: int = 20
    replenish_threshold: int = 10
    failure_cooldown_seconds: float = 120
    max_failures: int = 3
    dead_ttl_hours: float = 1
    db_max_age_hours: float = 24
    warmup_timeout_seconds: float = 90
    check_path: str = ""
    check_payload: Mapping[str, Any] = field(default_factory=dict)
    warmup: bool = True
    per_ip_limit: int = 1
    probe_concurrency: int = 2
    acquire_timeout: float = 150
    direct_block_cooldown_seconds: float = 300
    mix_share: float = 0

    def __post_init__(self) -> None:
        mode = self.mode.lower()
        if mode not in PROXY_MODES:
            raise ValueError("proxy_mode must be 'direct', 'fallback', 'always' or 'pool'")
        object.__setattr__(self, "mode", mode)
        if self.direct_block_cooldown_seconds <= 0:
            raise ValueError("direct_block_cooldown_seconds must be positive")
        if not 0 <= self.mix_share <= 1:
            raise ValueError("proxy_mix_share must be between 0 and 1")
        if self.sample_size < 0 or self.check_concurrency < 1 or self.check_timeout <= 0:
            raise ValueError("Proxy sample size must be >= 0 (0 = all); check limits must be positive")
        if self.min_ready < 1 or self.max_failures < 1:
            raise ValueError("Proxy readiness and failure limits must be positive")
        if self.replenish_threshold < 0 or self.failure_cooldown_seconds < 0:
            raise ValueError("Proxy replenish limits must be non-negative")
        if self.dead_ttl_hours < 0:
            raise ValueError("Proxy dead TTL must be non-negative")
        if self.db_max_age_hours < 0:
            raise ValueError("Proxy database max age must be non-negative")
        if self.warmup_timeout_seconds <= 0:
            raise ValueError("Proxy warmup timeout must be positive")
        if (self.file or self.db_path) and not self.check_path:
            raise ValueError("Proxy mode requires a readiness endpoint")
        if self.per_ip_limit < 1 or self.probe_concurrency < 1 or self.acquire_timeout <= 0:
            raise ValueError("Proxy concurrency limits must be positive")


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


def client_config_from_settings(
    app_settings: Any, *, proxy_check_path: str = "", proxy_check_payload: Mapping[str, Any] | None = None, project_dir: Path
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
            warmup_timeout_seconds=app_settings.proxy_warmup_timeout_seconds,
            check_path=proxy_check_path,
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
    )
