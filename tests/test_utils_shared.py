"""Tests for the shared proxy, atomic-write, readiness, and HTTP helpers."""

import json
import stat
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import pytest

from src.client.readiness import is_catalog_payload, is_ready_response, is_search_payload
from src.utils.atomic import atomic_text_writer, atomic_write_bytes, atomic_write_json, atomic_write_text
from src.utils.http import parse_retry_after
from src.utils.proxy_url import is_socks_proxy, mask_proxy_url, normalize_proxy_url, parse_proxy_url, proxy_host


def test_parse_proxy_url_accepts_supported_addresses():
    address = parse_proxy_url("socks5://user:pass@proxy.example:1080")

    assert address is not None
    assert address.url == "socks5://user:pass@proxy.example:1080"
    assert address.scheme == "socks5"
    assert address.host == "proxy.example"
    assert address.port == 1080
    assert address.username == "user"
    assert address.password == "pass"
    assert address.host_port == "proxy.example:1080"
    assert address.masked == "socks5://proxy.example:1080"
    assert normalize_proxy_url("127.0.0.1:8080") == "http://127.0.0.1:8080"
    assert normalize_proxy_url("HTTP://Proxy.Example:3128") == "http://Proxy.Example:3128"


def test_parse_proxy_url_supports_ipv6_and_rejects_bad_values():
    address = parse_proxy_url("[::1]:8080")

    assert address is not None
    assert address.host_port == "[::1]:8080"
    assert normalize_proxy_url("http://[::1]:8080") == "http://[::1]:8080"
    assert normalize_proxy_url("ftp://127.0.0.1:21") is None
    assert normalize_proxy_url("invalid_without_port") is None
    assert normalize_proxy_url("127.0.0.1:99999") is None
    assert normalize_proxy_url("# commented out proxy") is None
    assert normalize_proxy_url("   ") is None


def test_proxy_display_and_scheme_helpers():
    assert proxy_host("http://user:pass@proxy.example:8080") == "proxy.example"
    assert proxy_host("not-a-url") == "not-a-url"
    assert is_socks_proxy("socks5h://proxy.example:1080") is True
    assert is_socks_proxy("http://proxy.example:8080") is False
    assert is_socks_proxy(None) is False
    assert mask_proxy_url("http://user:pass@proxy.example:8080") == "http://proxy.example:8080"
    assert mask_proxy_url("bad") == "<invalid-proxy>"


def test_atomic_write_helpers_replace_content_and_clean_temporary_files(tmp_path):
    target = tmp_path / "state.json"
    target.write_text("old", encoding="utf-8")
    target.chmod(0o640)

    atomic_write_text(target, "new")
    assert target.read_text(encoding="utf-8") == "new"
    assert stat.S_IMODE(target.stat().st_mode) == 0o640

    atomic_write_json(target, {"b": 2, "a": 1}, sort_keys=True)
    assert json.loads(target.read_text(encoding="utf-8")) == {"a": 1, "b": 2}

    binary = tmp_path / "document.pdf"
    atomic_write_bytes(binary, b"%PDF-1.4\n%%EOF\n")
    assert binary.read_bytes().startswith(b"%PDF")
    assert list(tmp_path.glob("*.tmp")) == []
    assert list(tmp_path.glob(".*.tmp")) == []


def test_atomic_writer_keeps_original_when_body_raises(tmp_path):
    target = tmp_path / "state.json"
    target.write_text("original", encoding="utf-8")

    with pytest.raises(RuntimeError, match="boom"):
        with atomic_text_writer(target) as handle:
            handle.write("partial")
            raise RuntimeError("boom")

    assert target.read_text(encoding="utf-8") == "original"
    assert list(tmp_path.glob(".*.tmp")) == []


def test_parse_retry_after_handles_numbers_dates_and_invalid_values():
    assert parse_retry_after("2.5") == 2.5
    assert parse_retry_after("-3") == 0.0
    assert parse_retry_after("invalid") is None

    future = datetime.now(UTC) + timedelta(seconds=30)
    parsed = parse_retry_after(format_datetime(future, usegmt=True))
    assert parsed is not None
    assert 25 <= parsed <= 30


def test_readiness_payload_and_response_validators():
    catalog = {"finalizado": True, "datos": []}
    assert is_catalog_payload(catalog) is True
    assert is_catalog_payload({"finalizado": True, "datos": {}}) is False
    assert is_search_payload({"finalizado": True, "datos": {"filas": [], "total": 0}}) is True
    assert is_search_payload({"finalizado": True, "datos": {"filas": []}}) is False

    class Response:
        def __init__(self, status, payload):
            self.status_code = status
            self._payload = payload

        def json(self):
            return self._payload

    assert is_ready_response(Response(200, catalog)) is True
    assert is_ready_response(Response(403, catalog)) is False
    assert is_ready_response(Response(200, {"finalizado": False})) is False
