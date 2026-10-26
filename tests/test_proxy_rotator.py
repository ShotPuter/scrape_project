"""Unit tests for the standalone ProxyRotator and ProxyCollector."""

from pathlib import Path

from src.proxy.collector import ProxyCollector
from src.proxy.rotator import ProxyRotator, normalize_proxy_url


def test_normalize_proxy_url():
    assert normalize_proxy_url("127.0.0.1:8080") == "http://127.0.0.1:8080"
    assert normalize_proxy_url("http://192.168.1.1:3128") == "http://192.168.1.1:3128"
    assert normalize_proxy_url("socks5://user:pass@proxy.com:1080") == "socks5://user:pass@proxy.com:1080"
    assert normalize_proxy_url("invalid_without_port") is None
    assert normalize_proxy_url("ftp://127.0.0.1:21") is None
    assert normalize_proxy_url("# commented out proxy") is None


def test_rotator_round_robin():
    rotator = ProxyRotator(["http://p1:8080", "http://p2:8080", "http://p3:8080"])
    assert rotator.total_count == 3
    assert rotator.healthy_count == 3

    assert rotator.get_proxy() == "http://p1:8080"
    assert rotator.get_proxy() == "http://p2:8080"
    assert rotator.get_proxy() == "http://p3:8080"
    assert rotator.get_proxy() == "http://p1:8080"


def test_rotator_fault_detection_and_cooldown(monkeypatch):
    rotator = ProxyRotator(["http://good:8080", "http://bad:8080"], max_failures=2, cooldown_seconds=60.0)

    # First failure does not trigger cooldown yet
    rotator.mark_failure("http://bad:8080")
    assert rotator.healthy_count == 2

    # Second failure triggers cooldown
    rotator.mark_failure("http://bad:8080")
    assert rotator.healthy_count == 1

    # Only good proxy should be returned
    assert rotator.get_proxy() == "http://good:8080"
    assert rotator.get_proxy() == "http://good:8080"

    # Resetting on success
    rotator.mark_success("http://bad:8080")
    assert rotator.healthy_count == 2


def test_rotator_empty_pool():
    rotator = ProxyRotator()
    assert rotator.get_proxy() is None
    assert rotator.total_count == 0


def test_rotator_file_io(tmp_path: Path):
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("10.0.0.1:8080\nhttp://10.0.0.2:8080\ninvalid\n", encoding="utf-8")

    rotator = ProxyRotator()
    loaded = rotator.load_from_file(proxy_file)
    assert loaded == 2
    assert rotator.total_count == 2

    output_file = tmp_path / "exported.txt"
    exported = rotator.save_to_file(output_file)
    assert exported == 2
    assert output_file.read_text(encoding="utf-8") == "http://10.0.0.1:8080\nhttp://10.0.0.2:8080\n"


def test_collector_from_csv(tmp_path: Path):
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("ip,port,protocol\n1.2.3.4,8080,http\n5.6.7.8,3128,https\n", encoding="utf-8")

    candidates = ProxyCollector.from_file(csv_file)
    assert len(candidates) == 2
    assert "http://1.2.3.4:8080" in candidates
    assert "https://5.6.7.8:3128" in candidates
