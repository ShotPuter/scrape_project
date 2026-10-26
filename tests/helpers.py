"""Shared factories for typed client configuration in tests."""

from __future__ import annotations

from src.client.base_client import HttpClient
from src.client.config import ClientConfig, ProxyConfig, RetryConfig, TimeoutConfig


def make_client_config(
    base_url: str = "https://example.test",
    *,
    attempts: int = 3,
    backoff_factor: float = 0.0,
    jitter_seconds: float = 0.0,
    proxy: ProxyConfig | None = None,
    timeouts: TimeoutConfig | None = None,
    connection_limit: int = 9,
    **client_kwargs,
) -> ClientConfig:
    return ClientConfig(
        base_url=base_url,
        retry=RetryConfig(attempts=attempts, backoff_factor=backoff_factor, jitter_seconds=jitter_seconds),
        proxy=proxy or ProxyConfig(),
        timeouts=timeouts or TimeoutConfig(),
        connection_limit=connection_limit,
        **client_kwargs,
    )


def make_http_client(base_url: str = "https://example.test", **kwargs) -> HttpClient:
    return HttpClient(make_client_config(base_url, **kwargs))
