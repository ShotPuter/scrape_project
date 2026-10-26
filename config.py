"""
Configuration management for the Gaceta de Comercio (SEPREC) Scraper.

All values can be overridden through environment variables or a local ``.env``
file. Relative paths are resolved against the project root, so the scraper can be
started from any working directory (cron, systemd, Docker, IDE, ...).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, fields
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
TRUTHY_VALUES = {"1", "true", "yes", "on"}
ENV_NAME_ALIASES = {"api_base_url": "SEPREC_API_BASE_URL"}
LOWERCASE_ENV_FIELDS = {"proxy_mode", "http_transport"}
UPPERCASE_ENV_FIELDS = {"log_level"}


def _coerce_env_value(field_name: str, env_name: str, raw: str, value_type: str) -> object:
    """Coerce a raw environment value to the declared field type."""
    if value_type == "bool":
        return raw.strip().lower() in TRUTHY_VALUES
    if value_type == "int":
        try:
            return int(raw)
        except ValueError as exc:
            raise ValueError(f"{env_name} must be an integer, got {raw!r}") from exc
    if value_type == "float":
        try:
            return float(raw)
        except ValueError as exc:
            raise ValueError(f"{env_name} must be a number, got {raw!r}") from exc
    if value_type == "Path":
        path = Path(raw).expanduser()
        return path if path.is_absolute() else BASE_DIR / path
    value = raw
    if field_name in LOWERCASE_ENV_FIELDS:
        return value.lower()
    if field_name in UPPERCASE_ENV_FIELDS:
        return value.upper()
    return value


@dataclass(slots=True)
class Settings:
    # Settings stay mutable so tests and long-running callers can adjust
    # one field without rebuilding the object.
    # API endpoints
    api_base_url: str = "https://servicios.seprec.gob.bo/api"

    # Networking, concurrency & rate limiting
    max_concurrent_requests: int = 15
    page_size: int = 50
    queue_high_watermark: int = 1000
    page_delay_seconds: float = 0.5
    request_timeout_seconds: int = 30
    detail_timeout_seconds: float = 90
    connect_timeout_seconds: float = 10
    sock_connect_timeout_seconds: float = 10
    sock_read_timeout_seconds: float = 30
    http_requests_per_second: float = 2
    proxy_mode: str = "pool"
    direct_block_cooldown_seconds: float = 300
    proxy_mix_share: float = 0
    proxy_per_ip_limit: int = 1
    proxy_probe_concurrency: int = 2
    proxy_acquire_timeout: float = 150
    metrics_interval_seconds: float = 60
    retry_attempts: int = 3
    retry_backoff_factor: float = 1.5
    retry_jitter_seconds: float = 0.3
    browser_profile: str = "chrome"
    http_transport: str = "aiohttp"
    proxy_file: str = ""
    proxy_source_url: str = "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/all-proxies.txt"
    proxy_source_refresh_minutes: float = 30
    proxy_source_timeout: float = 30
    proxy_sample_size: int = 0
    proxy_check_concurrency: int = 50
    proxy_check_timeout: float = 10

    # Public multi-source catalog and two-stage verification.
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
    proxy_stage2_attempts: int = 3
    proxy_stage2_retry_delay: float = 0.5
    proxy_stage2_max_retry_delay: float = 5
    proxy_stage2_requests_per_second: float = 5
    proxy_stage2_min_success_rate: float = 0
    proxy_stage2_min_successes: int = 1
    proxy_report_path: Path = field(
        default_factory=lambda: BASE_DIR / "data" / "proxy-report.json"
    )
    proxy_min_ready: int = 20
    proxy_replenish_threshold: int = 10
    proxy_failure_cooldown_seconds: float = 120
    proxy_max_failures: int = 3
    proxy_dead_ttl_hours: float = 1
    proxy_warmup: bool = True
    # Verified records older than this are ignored; stale proxies are worse than none.
    proxy_db_max_age_hours: float = 24
    # Startup must stay bounded even when every candidate is slow or the collector hangs.
    proxy_warmup_timeout_seconds: float = 90
    proxy_startup_timeout_seconds: float = 180
    proxy_collector_stop_timeout_seconds: float = 5
    proxy_db_path: Path = field(default_factory=lambda: BASE_DIR / "data" / "proxies.db")
    cookies_file: str = ""

    # File & database storage
    storage_base_dir: Path = field(default_factory=lambda: BASE_DIR / "data")
    pdf_storage_dir: Path = field(default_factory=lambda: BASE_DIR / "data" / "pdf")
    csv_export_path: Path = field(
        default_factory=lambda: BASE_DIR / "data" / "publications.csv"
    )
    proxy_list_path: Path = field(
        default_factory=lambda: BASE_DIR / "data" / "iplocate-proxies.csv"
    )
    proxy_state_path: Path = field(
        default_factory=lambda: BASE_DIR / "data" / "iplocate-proxies.state.json"
    )

    # Logging
    log_level: str = "INFO"
    log_to_file: bool = True
    log_file: Path = field(default_factory=lambda: BASE_DIR / "data" / "scraper.log")
    log_max_bytes: int = 10 * 1024 * 1024
    log_backup_count: int = 5

    # Browser-like headers required by the public API (Cloudflare / Kong in front).
    default_headers: dict[str, str] = field(
        default_factory=lambda: {
            **({"User-Agent": os.environ["USER_AGENT"]} if os.getenv("USER_AGENT") else {}),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Content-Type": "application/json",
            "Origin": "https://gacetadecomercio.gob.bo",
            "Referer": "https://gacetadecomercio.gob.bo/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "cross-site",
        }
    )

    def __post_init__(self) -> None:
        self._validate_core()
        self._validate_proxy()
        self._validate_stage1()
        self._validate_stage2()
        self.api_base_url = self.api_base_url.rstrip("/")

    @classmethod
    def from_env(cls) -> Settings:
        """Build settings from the current environment (and ``.env``).

        ``Settings()`` is deterministic; this method is the composition-root
        entry point for production configuration and test overrides.
        """
        load_dotenv()
        overrides = {}
        for setting in fields(cls):
            if not setting.init:
                continue
            env_name = ENV_NAME_ALIASES.get(setting.name, setting.name.upper())
            raw = os.getenv(env_name)
            if raw is None:
                continue
            value_type = setting.type if isinstance(setting.type, str) else getattr(setting.type, "__name__", "")
            if value_type.startswith("dict"):
                continue
            if raw == "" and value_type == "Path":
                continue
            overrides[setting.name] = _coerce_env_value(setting.name, env_name, raw, value_type)
        return cls(**overrides)

    def _validate_core(self) -> None:
        self._require(self.max_concurrent_requests >= 1, "MAX_CONCURRENT_REQUESTS must be >= 1")
        self._require(self.page_size >= 10, "PAGE_SIZE must be >= 10 (SEPREC API minimum)")
        self._require(self.queue_high_watermark >= 1, "QUEUE_HIGH_WATERMARK must be >= 1")
        timeouts = (
            self.request_timeout_seconds,
            self.detail_timeout_seconds,
            self.connect_timeout_seconds,
            self.sock_connect_timeout_seconds,
            self.sock_read_timeout_seconds,
        )
        self._require(min(timeouts) > 0, "Timeouts must be positive")
        self._require(self.http_requests_per_second >= 0, "HTTP_REQUESTS_PER_SECOND must be >= 0")
        self._require(
            self.proxy_mode in {"direct", "fallback", "always", "pool"},
            "PROXY_MODE must be 'direct', 'fallback', 'always' or 'pool'",
        )
        self._require(self.direct_block_cooldown_seconds > 0, "DIRECT_BLOCK_COOLDOWN_SECONDS must be positive")
        self._require(0 <= self.proxy_mix_share <= 1, "PROXY_MIX_SHARE must be between 0 and 1")
        self._require(self.proxy_per_ip_limit >= 1 and self.proxy_probe_concurrency >= 1, "Proxy concurrency must be >= 1")
        self._require(self.proxy_acquire_timeout > 0, "PROXY_ACQUIRE_TIMEOUT must be positive")
        self._require(self.metrics_interval_seconds > 0, "METRICS_INTERVAL_SECONDS must be positive")
        self._require(self.retry_attempts >= 1, "RETRY_ATTEMPTS must be >= 1")
        self._require(self.page_delay_seconds >= 0, "PAGE_DELAY_SECONDS must be >= 0")
        self._require(
            self.http_transport in {"aiohttp", "curl_cffi", "auto"},
            "HTTP_TRANSPORT must be 'aiohttp', 'curl_cffi' or 'auto'",
        )

    def _validate_proxy(self) -> None:
        self._require(self.proxy_source_refresh_minutes >= 0, "PROXY_SOURCE_REFRESH_MINUTES must be >= 0")
        self._require(self.proxy_source_timeout > 0, "PROXY_SOURCE_TIMEOUT must be > 0")
        self._require(self.proxy_sample_size >= 0, "PROXY_SAMPLE_SIZE must be >= 0 (0 = all)")
        self._require(self.proxy_min_ready >= 1, "PROXY_MIN_READY must be >= 1")
        self._require(self.proxy_replenish_threshold >= 0, "PROXY_REPLENISH_THRESHOLD must be >= 0")
        self._require(self.proxy_failure_cooldown_seconds >= 0, "PROXY_FAILURE_COOLDOWN_SECONDS must be >= 0")
        self._require(self.proxy_max_failures >= 1, "PROXY_MAX_FAILURES must be >= 1")
        self._require(self.proxy_dead_ttl_hours >= 0, "PROXY_DEAD_TTL_HOURS must be >= 0")
        self._require(self.proxy_db_max_age_hours >= 0, "PROXY_DB_MAX_AGE_HOURS must be >= 0")
        self._require(self.proxy_warmup_timeout_seconds > 0, "PROXY_WARMUP_TIMEOUT_SECONDS must be > 0")
        self._require(self.proxy_startup_timeout_seconds > 0, "PROXY_STARTUP_TIMEOUT_SECONDS must be > 0")
        self._require(
            self.proxy_collector_stop_timeout_seconds > 0,
            "PROXY_COLLECTOR_STOP_TIMEOUT_SECONDS must be > 0",
        )
        self._require(self.proxy_max_candidates >= 0, "PROXY_MAX_CANDIDATES must be >= 0 (0 = all)")
        self._require(self.proxy_max_per_source >= 0, "PROXY_MAX_PER_SOURCE must be >= 0 (0 = all)")

    def _validate_stage1(self) -> None:
        self._require(
            self.proxy_stage1_timeout > 0
            and self.proxy_stage1_connect_timeout > 0
            and self.proxy_stage1_connect_timeout <= self.proxy_stage1_timeout,
            "Stage-1 timeouts must be positive and connect timeout must not exceed total",
        )
        self._require(0 <= self.proxy_stage1_min_uptime <= 100, "PROXY_STAGE1_MIN_UPTIME must be between 0 and 100")
        self._require(self.proxy_stage1_max_latency_ms > 0, "PROXY_STAGE1_MAX_LATENCY_MS must be > 0")
        self._require(
            self.proxy_stage1_preferred_age_minutes >= 0 and self.proxy_stage1_max_age_minutes >= 0,
            "Proxy age limits must be >= 0",
        )

    def _validate_stage2(self) -> None:
        self._require(
            self.proxy_stage2_concurrency >= 1
            and self.proxy_stage2_timeout > 0
            and self.proxy_stage2_connect_timeout > 0
            and self.proxy_stage2_connect_timeout <= self.proxy_stage2_timeout,
            "Stage-2 concurrency/timeouts are invalid",
        )
        self._require(self.proxy_stage2_attempts >= 1, "PROXY_STAGE2_ATTEMPTS must be >= 1")
        self._require(
            self.proxy_stage2_retry_delay >= 0
            and self.proxy_stage2_max_retry_delay >= 0
            and self.proxy_stage2_requests_per_second >= 0,
            "Stage-2 delays/rate must be non-negative",
        )
        self._require(0 <= self.proxy_stage2_min_success_rate <= 1, "PROXY_STAGE2_MIN_SUCCESS_RATE must be between 0 and 1")
        self._require(self.proxy_stage2_min_successes >= 0, "PROXY_STAGE2_MIN_SUCCESSES must be >= 0")

    @staticmethod
    def _require(condition: bool, message: str) -> None:
        if not condition:
            raise ValueError(message)

    def ensure_directories(self) -> None:
        for directory in (
            self.storage_base_dir,
            self.pdf_storage_dir,
            self.csv_export_path.parent,
            self.proxy_list_path.parent,
            self.proxy_db_path.parent,
            self.proxy_report_path.parent,
            self.log_file.parent,
        ):
            directory.mkdir(parents=True, exist_ok=True)


settings = Settings.from_env()
