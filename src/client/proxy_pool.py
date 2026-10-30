"""Compatibility imports; use src.proxy.pool for new code."""

from src.proxy.pool import (
    DEAD_PROXY_TTL_HOURS,
    DEFAULT_FAILURE_COOLDOWN_SECONDS,
    DEFAULT_MAX_FAILURES,
    HARD_TIMEOUT_GRACE_SECONDS,
    WARMUP_PROGRESS_EVERY,
    WARMUP_PROGRESS_SECONDS,
    ProxyEndpoint,
    ProxyPool,
    ProxySession,
    SessionFactory,
    WarmupSpec,
    dead_proxy_path,
    load_dead_proxies,
    load_proxies,
)

__all__ = [
    "DEAD_PROXY_TTL_HOURS",
    "DEFAULT_FAILURE_COOLDOWN_SECONDS",
    "DEFAULT_MAX_FAILURES",
    "HARD_TIMEOUT_GRACE_SECONDS",
    "ProxyEndpoint",
    "ProxyPool",
    "ProxySession",
    "SessionFactory",
    "WARMUP_PROGRESS_EVERY",
    "WARMUP_PROGRESS_SECONDS",
    "WarmupSpec",
    "dead_proxy_path",
    "load_dead_proxies",
    "load_proxies",
]
