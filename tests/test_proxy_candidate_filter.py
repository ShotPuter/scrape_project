"""Batch filtering reads each snapshot once and never contacts candidate hosts."""

import ipaddress
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.proxy import candidate_filter as filtering


@pytest.fixture
def prepared(monkeypatch):
    monkeypatch.setenv("ABUSEIPDB_API_KEY", "unit-test-key")
    feed = SimpleNamespace(matches_ip=lambda address: address == ipaddress.IPv4Address("11.0.0.1"))
    snapshots = Mock(return_value=feed)
    checker = Mock(side_effect=lambda address: "abuseipdb" if str(address) == "12.0.0.1" else None)
    bulk = Mock(return_value=checker)
    monkeypatch.setattr(filtering.threat_feeds, "snapshot", snapshots)
    monkeypatch.setattr(filtering.blocklists, "prepare_ipv4_checker", bulk)
    monkeypatch.setattr("src.proxy.reputation._lookup", Mock(side_effect=AssertionError("No individual API calls")))
    return snapshots, bulk, checker


def test_batches_use_each_feed_once_and_share_ip_verdict_between_ports(prepared):
    snapshots, bulk, checker = prepared
    rows = [
        ("http://13.0.0.1:80", "one", "13.0.0.1"),
        ("socks5://13.0.0.1:1080", "two", "13.0.0.1"),
        ("http://11.0.0.1:80", "one", "11.0.0.1"),
        ("http://12.0.0.1:80", "one", "12.0.0.1"),
        ("http://8.211.1.1:80", "one", "8.211.1.1"),
    ]
    accepted, rejected = filtering.filter_candidate_ips(rows)
    assert len(accepted) == 2
    assert rejected == {"firehol_level1": 1, "abuseipdb": 1, "network_policy": 1}
    assert snapshots.call_count == len(filtering.threat_feeds.FEEDS)
    bulk.assert_called_once_with("unit-test-key")
    assert checker.call_count == 2


def test_missing_required_snapshot_aborts_entire_batch(prepared):
    snapshots, bulk, checker = prepared
    snapshots.side_effect = filtering.threat_feeds.ThreatFeedError("feed unavailable")
    with pytest.raises(filtering.threat_feeds.ThreatFeedError, match="unavailable"):
        filtering.filter_candidate_ips([("http://13.0.0.1:80", "one", "13.0.0.1")])
    bulk.assert_not_called()
    checker.assert_not_called()


def test_empty_batch_does_not_download_lists(prepared):
    snapshots, bulk, _ = prepared
    assert filtering.filter_candidate_ips([]) == ([], {})
    snapshots.assert_not_called()
    bulk.assert_not_called()
