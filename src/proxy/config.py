"""Explicit configuration for proxy consumers and collection jobs."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.proxy.probes import is_success_response

PROXY_MODES = frozenset({"direct", "fallback", "always", "pool"})


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
    db_refresh_seconds: float = 30
    verified_reuse_seconds: float = 300
    warmup_timeout_seconds: float = 90
    check_path: str = ""
    check_validator: Callable[[Any], bool] = is_success_response
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
        if not self.db_refresh_seconds >= 0:
            raise ValueError("Proxy database refresh interval must be non-negative")
        if not self.verified_reuse_seconds >= 0:
            raise ValueError("Verified proxy reuse interval must be non-negative")
        if self.warmup_timeout_seconds <= 0:
            raise ValueError("Proxy warmup timeout must be positive")
        if (self.file or self.db_path) and not self.check_path:
            raise ValueError("Proxy mode requires a readiness endpoint")
        if self.per_ip_limit < 1 or self.probe_concurrency < 1 or self.acquire_timeout <= 0:
            raise ValueError("Proxy concurrency limits must be positive")


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    """A per-job snapshot; supplied by the CLI, never read from the environment here."""

    browser_profile: str = "chrome"
    proxy_source_url: str = "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/all-proxies.txt"
    proxy_source_refresh_minutes: float = 30
    proxy_source_timeout: float = 30
    proxy_sources: str = ""
    webshare_api_token: str = field(default="", repr=False)
    webshare_proxy_mode: str = "direct"
    webshare_free_fallback_threshold: int = 10
    proxy_db_max_age_hours: float = 24
    proxy_paid_file: Path = field(default_factory=lambda: Path("data") / "paid-proxies.txt")
    proxy_source_max_response_bytes: int = 32 * 1024 * 1024
    proxy_source_download_concurrency: int = 3
    proxy_source_max_records: int = 10_000
    proxy_catalog_cache_dir: Path = field(default_factory=lambda: Path("data") / "proxy-sources")
    proxy_source_cache_max_age_hours: float = 24
    proxy_check_concurrency: int = 50
    proxy_max_candidates: int = 2000
    proxy_max_per_source: int = 1000
    proxy_stage1_check_url: str = "https://api.ipify.org?format=json"
    proxy_stage1_timeout: float = 10
    proxy_stage1_connect_timeout: float = 5
    proxy_stage1_min_uptime: float = 80
    proxy_stage1_max_latency_ms: float = 1500
    proxy_stage1_preferred_age_minutes: float = 15
    proxy_stage1_max_age_minutes: float = 1440
    proxy_stage1_require_https: bool = True
    proxy_stage2_concurrency: int = 20
    proxy_stage2_timeout: float = 30
    proxy_stage2_connect_timeout: float = 10
    proxy_stage2_attempts: int = 1
    proxy_user_agent_limit: int = 3
    proxy_min_ready: int = 20
    proxy_stage2_retry_delay: float = 0.5
    proxy_stage2_max_retry_delay: float = 5
    proxy_stage2_requests_per_second: float = 5
    proxy_stage2_min_success_rate: float = 0
    proxy_stage2_min_successes: int = 1
    proxy_report_path: Path = field(default_factory=lambda: Path("data") / "proxy-report.json")
    proxy_db_path: Path = field(default_factory=lambda: Path("data") / "proxies.db")
    proxy_list_path: Path = field(default_factory=lambda: Path("data") / "iplocate-proxies.csv")
    proxy_state_path: Path = field(default_factory=lambda: Path("data") / "iplocate-proxies.state.json")
    default_headers: dict[str, str] = field(default_factory=dict)
