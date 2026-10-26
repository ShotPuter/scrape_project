"""
Configuration management for the Gaceta de Comercio (SEPREC) Scraper.

All values can be overridden through environment variables or a local ``.env``
file. Relative paths are resolved against the project root, so the scraper can be
started from any working directory (cron, systemd, Docker, IDE, ...).
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
TRUTHY_VALUES = {"1", "true", "yes", "on"}


def _env_path(name: str, default: Path) -> Path:
    """Read a path from the environment and resolve it against the project root."""
    if not (raw := os.getenv(name)):
        return default
    path = Path(raw).expanduser()
    return path if path.is_absolute() else BASE_DIR / path


def _env_text(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_lower(name: str, default: str) -> str:
    return _env_text(name, default).lower()


def _env_upper(name: str, default: str) -> str:
    return _env_text(name, default).upper()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    return default if raw is None else raw.strip().lower() in TRUTHY_VALUES


@dataclass(slots=True)
class Settings:
    # Settings stay mutable so tests and long-running callers can adjust
    # one field without rebuilding the object.
    # API endpoints
    api_base_url: str = _env_text("SEPREC_API_BASE_URL", "https://servicios.seprec.gob.bo/api")

    # Networking, concurrency & rate limiting
    max_concurrent_requests: int = _env_int("MAX_CONCURRENT_REQUESTS", 15)
    page_size: int = _env_int("PAGE_SIZE", 50)
    queue_high_watermark: int = _env_int("QUEUE_HIGH_WATERMARK", 1000)
    page_delay_seconds: float = _env_float("PAGE_DELAY_SECONDS", 0.5)
    request_timeout_seconds: int = _env_int("REQUEST_TIMEOUT_SECONDS", 30)
    detail_timeout_seconds: float = _env_float("DETAIL_TIMEOUT_SECONDS", 90)
    connect_timeout_seconds: float = _env_float("CONNECT_TIMEOUT_SECONDS", 10)
    sock_connect_timeout_seconds: float = _env_float("SOCK_CONNECT_TIMEOUT_SECONDS", 10)
    sock_read_timeout_seconds: float = _env_float("SOCK_READ_TIMEOUT_SECONDS", 30)
    http_requests_per_second: float = _env_float("HTTP_REQUESTS_PER_SECOND", 2)
    proxy_mode: str = _env_lower("PROXY_MODE", "pool")
    direct_block_cooldown_seconds: float = _env_float("DIRECT_BLOCK_COOLDOWN_SECONDS", 300)
    proxy_mix_share: float = _env_float("PROXY_MIX_SHARE", 0)
    proxy_per_ip_limit: int = _env_int("PROXY_PER_IP_LIMIT", 1)
    proxy_probe_concurrency: int = _env_int("PROXY_PROBE_CONCURRENCY", 2)
    proxy_acquire_timeout: float = _env_float("PROXY_ACQUIRE_TIMEOUT", 150)
    metrics_interval_seconds: float = _env_float("METRICS_INTERVAL_SECONDS", 60)
    retry_attempts: int = _env_int("RETRY_ATTEMPTS", 3)
    retry_backoff_factor: float = _env_float("RETRY_BACKOFF_FACTOR", 1.5)
    retry_jitter_seconds: float = _env_float("RETRY_JITTER_SECONDS", 0.3)
    browser_profile: str = _env_text("BROWSER_PROFILE", "chrome")
    http_transport: str = _env_lower("HTTP_TRANSPORT", "aiohttp")
    proxy_file: str = _env_text("PROXY_FILE", "")
    proxy_source_url: str = _env_text(
        "PROXY_SOURCE_URL", "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/all-proxies.txt"
    )
    proxy_source_refresh_minutes: float = _env_float("PROXY_SOURCE_REFRESH_MINUTES", 30)
    proxy_source_timeout: float = _env_float("PROXY_SOURCE_TIMEOUT", 30)
    proxy_sample_size: int = _env_int("PROXY_SAMPLE_SIZE", 0)
    proxy_check_concurrency: int = _env_int("PROXY_CHECK_CONCURRENCY", 50)
    proxy_check_timeout: float = _env_float("PROXY_CHECK_TIMEOUT", 10)

    # Public multi-source catalog and two-stage verification.
    proxy_max_candidates: int = _env_int("PROXY_MAX_CANDIDATES", 2000)
    proxy_max_per_source: int = _env_int("PROXY_MAX_PER_SOURCE", 1000)
    proxy_stage1_check_url: str = _env_text("PROXY_STAGE1_CHECK_URL", "https://api.ipify.org?format=json")
    proxy_stage1_timeout: float = _env_float("PROXY_STAGE1_TIMEOUT", 10)
    proxy_stage1_connect_timeout: float = _env_float("PROXY_STAGE1_CONNECT_TIMEOUT", 5)
    proxy_stage1_min_uptime: float = _env_float("PROXY_STAGE1_MIN_UPTIME", 80)
    proxy_stage1_max_latency_ms: float = _env_float("PROXY_STAGE1_MAX_LATENCY_MS", 1500)
    proxy_stage1_preferred_age_minutes: float = _env_float("PROXY_STAGE1_PREFERRED_AGE_MINUTES", 15)
    proxy_stage1_max_age_minutes: float = _env_float("PROXY_STAGE1_MAX_AGE_MINUTES", 1440)
    proxy_stage1_require_https: bool = _env_bool("PROXY_STAGE1_REQUIRE_HTTPS", True)
    proxy_stage2_concurrency: int = _env_int("PROXY_STAGE2_CONCURRENCY", 20)
    proxy_stage2_timeout: float = _env_float("PROXY_STAGE2_TIMEOUT", 30)
    proxy_stage2_connect_timeout: float = _env_float("PROXY_STAGE2_CONNECT_TIMEOUT", 10)
    proxy_stage2_attempts: int = _env_int("PROXY_STAGE2_ATTEMPTS", 3)
    proxy_stage2_retry_delay: float = _env_float("PROXY_STAGE2_RETRY_DELAY", 0.5)
    proxy_stage2_max_retry_delay: float = _env_float("PROXY_STAGE2_MAX_RETRY_DELAY", 5)
    proxy_stage2_requests_per_second: float = _env_float("PROXY_STAGE2_REQUESTS_PER_SECOND", 5)
    proxy_stage2_min_success_rate: float = _env_float("PROXY_STAGE2_MIN_SUCCESS_RATE", 0)
    proxy_stage2_min_successes: int = _env_int("PROXY_STAGE2_MIN_SUCCESSES", 1)
    proxy_report_path: Path = field(
        default_factory=lambda: _env_path("PROXY_REPORT_PATH", BASE_DIR / "data" / "proxy-report.json")
    )
    proxy_min_ready: int = _env_int("PROXY_MIN_READY", 20)
    proxy_replenish_threshold: int = _env_int("PROXY_REPLENISH_THRESHOLD", 10)
    proxy_failure_cooldown_seconds: float = _env_float("PROXY_FAILURE_COOLDOWN_SECONDS", 120)
    proxy_max_failures: int = _env_int("PROXY_MAX_FAILURES", 3)
    proxy_dead_ttl_hours: float = _env_float("PROXY_DEAD_TTL_HOURS", 1)
    proxy_warmup: bool = _env_bool("PROXY_WARMUP", True)
    proxy_db_path: Path = field(default_factory=lambda: _env_path("PROXY_DB_PATH", BASE_DIR / "data" / "proxies.db"))
    cookies_file: str = _env_text("COOKIES_FILE", "")

    # Scheduler
    schedule_interval_hours: float = _env_float("SCHEDULE_INTERVAL_HOURS", 24)

    # File & database storage
    storage_base_dir: Path = field(default_factory=lambda: _env_path("STORAGE_BASE_DIR", BASE_DIR / "data"))
    pdf_storage_dir: Path = field(default_factory=lambda: _env_path("PDF_STORAGE_DIR", BASE_DIR / "data" / "pdf"))
    csv_export_path: Path = field(
        default_factory=lambda: _env_path("CSV_EXPORT_PATH", BASE_DIR / "data" / "publications.csv")
    )
    proxy_list_path: Path = field(
        default_factory=lambda: _env_path("PROXY_LIST_PATH", BASE_DIR / "data" / "iplocate-proxies.csv")
    )
    proxy_state_path: Path = field(
        default_factory=lambda: _env_path("PROXY_STATE_PATH", BASE_DIR / "data" / "iplocate-proxies.state.json")
    )

    # Logging
    log_level: str = _env_upper("LOG_LEVEL", "INFO")
    log_to_file: bool = _env_bool("LOG_TO_FILE", True)
    log_file: Path = field(default_factory=lambda: _env_path("LOG_FILE", BASE_DIR / "data" / "scraper.log"))
    log_max_bytes: int = _env_int("LOG_MAX_BYTES", 10 * 1024 * 1024)
    log_backup_count: int = _env_int("LOG_BACKUP_COUNT", 5)

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
        self._require(self.schedule_interval_hours > 0, "SCHEDULE_INTERVAL_HOURS must be > 0")
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


settings = Settings()
