"""Configuration object and composition-root tests."""

from datetime import UTC, datetime, timedelta

import pytest

from config import BASE_DIR, Settings
from src.client.base_client import HttpClient
from src.client.config import ClientConfig, ProxyConfig, RetryConfig, TimeoutConfig, client_config_from_settings
from src.client.network import TimeoutPolicy
from src.client.seprec_client import SeprecClient
from src.storage.proxy_database import ProxyDatabase, VerifiedProxy


def test_settings_from_env_coerces_types_and_resolves_paths(monkeypatch, tmp_path):
    monkeypatch.setenv("REQUEST_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("PROXY_ACQUIRE_TIMEOUT", "12.5")
    monkeypatch.setenv("PROXY_WARMUP", "off")
    monkeypatch.setenv("PROXY_DB_MAX_AGE_HOURS", "12.5")
    monkeypatch.setenv("PROXY_WARMUP_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("LOG_LEVEL", "debug")
    monkeypatch.setenv("PROXY_DB_PATH", str(tmp_path / "proxies.db"))

    resolved = Settings.from_env()

    assert resolved.request_timeout_seconds == 45
    assert resolved.proxy_acquire_timeout == 12.5
    assert resolved.proxy_warmup is False
    assert resolved.proxy_db_max_age_hours == 12.5
    assert resolved.proxy_warmup_timeout_seconds == 45
    assert resolved.log_level == "DEBUG"
    assert resolved.proxy_db_path == tmp_path / "proxies.db"


def test_relative_env_paths_resolve_against_project_root(monkeypatch):
    monkeypatch.setenv("LOG_FILE", "logs/custom.log")
    assert Settings.from_env().log_file == BASE_DIR / "logs" / "custom.log"


def test_client_config_rejects_invalid_values():
    with pytest.raises(ValueError):
        RetryConfig(attempts=0)
    with pytest.raises(ValueError):
        ProxyConfig(mode="sometimes")
    with pytest.raises(ValueError):
        ProxyConfig(mix_share=1.5)
    with pytest.raises(ValueError):
        ClientConfig(base_url="")
    with pytest.raises(ValueError):
        ClientConfig(base_url="https://example.test", transport="ftp")


def test_settings_mapping_keeps_defaults_and_explicit_overrides(monkeypatch):
    monkeypatch.setenv("RETRY_ATTEMPTS", "5")
    resolved_settings = Settings.from_env()
    config = client_config_from_settings(
        resolved_settings, base_url="https://example.test/api", timeout_seconds=7, proxy_mode="direct", proxy_mix_share=0.25
    )

    assert config.base_url == "https://example.test/api"
    assert config.timeouts == TimeoutConfig(
        api=TimeoutPolicy(
            total=7,
            connect=resolved_settings.connect_timeout_seconds,
            sock_connect=resolved_settings.sock_connect_timeout_seconds,
            sock_read=resolved_settings.sock_read_timeout_seconds,
        ),
        detail=TimeoutPolicy(
            total=7,
            connect=resolved_settings.connect_timeout_seconds,
            sock_connect=resolved_settings.sock_connect_timeout_seconds,
            sock_read=resolved_settings.sock_read_timeout_seconds,
        ),
    )
    assert config.retry.attempts == 5
    assert config.proxy.mode == "direct"
    assert config.proxy.mix_share == 0.25


def test_seprec_client_uses_injected_config_without_global_settings():
    config = ClientConfig(
        base_url="http://localhost/api", timeouts=TimeoutConfig(api=TimeoutPolicy(total=5)), proxy=ProxyConfig(mode="direct")
    )
    client = SeprecClient(config=config)
    assert client.http.config is config
    assert client.proxy_mode == "direct"
    assert client.http.base_url == "http://localhost/api"


@pytest.mark.asyncio
async def test_client_ignores_stale_database_records(tmp_path):
    db_path = tmp_path / "proxies.db"
    stale = (datetime.now(UTC) - timedelta(days=2)).isoformat()
    async with ProxyDatabase(db_path) as database:
        await database.replace_all([VerifiedProxy("http://stale.example:80", "UA", "aiohttp", checked_at=stale)])

    client = HttpClient(
        "https://example.test",
        proxy_db_path=db_path,
        proxy_db_max_age_hours=24,
        proxy_check_path="publicaciones/catalogos/lista",
    )
    try:
        assert await client._create_proxy_pool() is None
    finally:
        await client.close()


def test_proxy_config_rejects_unbounded_warmup():
    with pytest.raises(ValueError):
        ProxyConfig(warmup_timeout_seconds=0)
    with pytest.raises(ValueError):
        ProxyConfig(db_max_age_hours=-1)
