import json
import time
from pathlib import Path
from unittest.mock import Mock

import pytest

from src.proxy import reputation, threat_feeds as feeds


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("PROXY_THREAT_CACHE", str(tmp_path / "feeds"))
    monkeypatch.setenv("PROXY_THREAT_REFRESH_HOURS", "6")
    monkeypatch.setenv("PROXY_THREAT_MAX_AGE_HOURS", "12")
    feeds._memory.clear()
    monkeypatch.setattr(
        feeds,
        "_download",
        Mock(
            side_effect=lambda source: (
                "||mine.example^\n||13.0.0.1^$third-party"
                if source == "nocoin"
                else "11.0.0.0/24\n11.0.0.128/25\n12.0.0.1\n"
            )
        ),
    )


@pytest.mark.parametrize(
    "value,listed",
    [
        ("11.0.0.0", True),
        ("11.0.0.255", True),
        ("11.0.1.0", False),
        ("12.0.0.1", True),
        ("12.0.0.2", False),
        ("13.0.0.1", True),
    ],
)
def test_exact_and_merged_cidr_boundaries(value, listed):
    if listed:
        with pytest.raises(feeds.ThreatListed):
            feeds.check_ip(value)
    else:
        feeds.check_ip(value)


def test_no_api_or_dns_for_listed_hostname(monkeypatch):
    resolver = Mock(side_effect=AssertionError("DNS must not run"))
    monkeypatch.setattr(reputation.socket, "getaddrinfo", resolver)
    with pytest.raises(reputation.ReputationDenied, match="reason=nocoin"):
        reputation.check_proxy("http://sub.mine.example:3128")
    resolver.assert_not_called()


def test_domain_label_boundary_and_idna():
    feeds.check_host("notmine.example")
    feeds.check_host("mine.example.evil.example")
    with pytest.raises(feeds.ThreatListed):
        feeds.check_host("SUB.MINE.EXAMPLE.")
    assert feeds.normalize_domain("MÜNCHEN.example") == "xn--mnchen-3ya.example"


def test_nocoin_paths_scopes_and_exceptions_are_not_whole_host_bans():
    payload = (
        "! comment\n# comment\n; comment\n[Adblock Plus 2.0]\n"
        "||mine.example^\n@@||safe.example^\n||cdn.example/miner.js\n"
        "||scoped.example^$domain=example.org\n0.0.0.0 pool.example\n"
    )
    parsed = feeds.parse_feed("nocoin", payload)
    assert parsed.domains == frozenset({"mine.example", "pool.example"})


def test_cache_reused_across_process_memory_and_original_payload_preserved():
    feeds.check_ip("15.0.0.1")
    assert feeds._download.call_count == 3
    feeds._memory.clear()
    feeds.check_ip("15.0.0.1")
    assert feeds._download.call_count == 3


def test_expired_cache_and_feed_failure_denies_with_backoff(monkeypatch):
    feeds.snapshot("firehol_level1")
    now = time.time()
    monkeypatch.setattr(feeds.time, "time", lambda: now + 13 * 3600)
    feeds._memory.clear()
    monkeypatch.setattr(feeds, "_download", Mock(side_effect=OSError()))
    for _ in range(2):
        with pytest.raises(feeds.ThreatFeedError, match="unavailable or expired"):
            feeds.snapshot("firehol_level1")
    assert feeds._download.call_count == 1


def test_valid_cache_survives_failed_refresh_only_until_max_age(monkeypatch):
    feeds.snapshot("firehol_level1")
    now = time.time()
    monkeypatch.setattr(feeds.time, "time", lambda: now + 7 * 3600)
    feeds._memory.clear()
    monkeypatch.setattr(feeds, "_download", Mock(side_effect=OSError()))
    assert feeds.snapshot("firehol_level1").networks
    monkeypatch.setattr(feeds.time, "time", lambda: now + 13 * 3600)
    feeds._memory.clear()
    with pytest.raises(feeds.ThreatFeedError):
        feeds.snapshot("firehol_level1")


@pytest.mark.parametrize("payload", ["", "<html>unavailable</html>", "# comments only", "0.0.0.0/0", "11.0.0.0/24\ninvalid"])
def test_invalid_payload_never_installed(monkeypatch, payload):
    monkeypatch.setattr(feeds, "_download", lambda source: payload)
    with pytest.raises(feeds.ThreatFeedError):
        feeds.snapshot("firehol_level1")


def test_all_mandatory_feeds_must_exist_before_connection(monkeypatch):
    monkeypatch.setenv("PROXY_REPUTATION_MODE", "blocklist_only")
    original = feeds._download
    monkeypatch.setattr(
        feeds, "_download", lambda source: (_ for _ in ()).throw(OSError()) if source == "nocoin" else original(source)
    )
    checker = Mock(side_effect=AssertionError("Old bulk/individual guard must not be reached"))
    monkeypatch.setattr("src.proxy.blocklists.check_blocklists", checker)
    with pytest.raises(reputation.ReputationDenied, match="nocoin"):
        reputation.check_proxy("http://15.0.0.1:3128")
    checker.assert_not_called()


def test_resolved_threat_address_not_used_when_a_permitted_peer_exists(monkeypatch):
    monkeypatch.setenv("PROXY_REPUTATION_MODE", "blocklist_only")
    monkeypatch.setenv("ABUSEIPDB_API_KEY", "test")
    monkeypatch.setattr("src.proxy.blocklists.check_blocklists", lambda *args: None)
    monkeypatch.setattr(
        reputation.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(2, 1, 6, "", ("11.0.0.1", 3128)), (2, 1, 6, "", ("15.0.0.1", 3128))],
    )
    assert reputation.check_proxy("http://proxy.example:3128") == "http://15.0.0.1:3128"


def test_binary_index_merges_adjacent_ranges_and_covers_ipv6_literals():
    import ipaddress

    parsed = feeds.parse_feed("nocoin", "||11.0.0.0^\n||2001:4860::1^\n")
    assert parsed.matches_ip(ipaddress.ip_address("2001:4860::1"))
    assert not parsed.matches_ip(ipaddress.ip_address("2001:4860::2"))
    index = feeds.RangeIndex.build([ipaddress.ip_network("11.0.0.0/25"), ipaddress.ip_network("11.0.0.128/25")], 4)
    assert len(index.starts) == 1
    assert not index.contains(ipaddress.ip_address("10.255.255.255"))
    assert index.contains(ipaddress.ip_address("11.0.0.255"))


def test_corrupted_cache_cannot_admit_a_proxy(monkeypatch):
    feeds.snapshot("firehol_level1")
    feeds._memory.clear()
    directory = Path(feeds.os.environ["PROXY_THREAT_CACHE"])
    path = directory / "firehol_level1.json"
    state = json.loads(path.read_text())
    state["payload"] = "15.0.0.0/24"
    path.write_text(json.dumps(state))
    monkeypatch.setattr(feeds, "_download", Mock(side_effect=OSError()))
    with pytest.raises(feeds.ThreatFeedError):
        feeds.snapshot("firehol_level1")
