import concurrent.futures
import ipaddress
import json
import time
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from src.proxy import blocklists as bl, reputation as rep


def drop(cidr="11.0.0.0/24"):
    return "\n".join(
        json.dumps(r)
        for r in [
            {"cidr": cidr, "sblid": "SBL1"},
            {
                "type": "metadata",
                "timestamp": int(time.time()),
                "records": 1,
                "copyright": "(c) Spamhaus Project",
                "terms": "https://www.spamhaus.org/drop/terms/",
            },
        ]
    )


def abuse(ip="12.0.0.1"):
    return json.dumps(
        {"meta": {"generatedAt": datetime.now(UTC).isoformat()}, "data": [{"ipAddress": ip, "abuseConfidenceScore": 100}]}
    )


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    monkeypatch.setattr("src.proxy.threat_feeds.check_ip", lambda *args: None)
    monkeypatch.setattr("src.proxy.threat_feeds.check_host", lambda *args: None)
    monkeypatch.setenv("PROXY_REPUTATION_MODE", "strict")
    monkeypatch.setenv("PROXY_REPUTATION_CACHE", str(tmp_path / "cache.db"))
    monkeypatch.setenv("ABUSEIPDB_API_KEY", "private-test-key")
    monkeypatch.setenv("PROXY_BLOCKLIST_REFRESH_HOURS", "24")
    monkeypatch.setenv("PROXY_BLOCKLIST_MAX_AGE_HOURS", "48")
    bl._parse.cache_clear()
    monkeypatch.setattr(
        bl,
        "_download",
        Mock(
            side_effect=lambda s, k: (
                abuse() if s == "abuseipdb" else drop("2001:4860:123::/48" if s.endswith("v6") else "11.0.0.0/24")
            )
        ),
    )


@pytest.mark.parametrize(
    "ip,source", [("11.0.0.255", "spamhaus_v4"), ("2001:4860:123::1", "spamhaus_v6"), ("12.0.0.1", "abuseipdb")]
)
def test_feed_matches_skip_individual_api(monkeypatch, ip, source):
    lookup = Mock(side_effect=AssertionError("Individual API must not run"))
    monkeypatch.setattr(rep, "_lookup", lookup)
    with pytest.raises(rep.ReputationDenied, match=source):
        rep.check_ip(ip)
    lookup.assert_not_called()


def test_absence_still_requires_individual_lookup(monkeypatch):
    lookup = Mock(return_value=(0, 0))
    monkeypatch.setattr(rep, "_lookup", lookup)
    rep.check_ip("11.0.1.1")
    rep.check_ip("11.0.1.1")
    assert lookup.call_count == 1
    assert bl._download.call_count == 2


def test_new_block_overrides_previous_cached_allow(monkeypatch):
    monkeypatch.setattr(rep, "_lookup", lambda *a: (0, 0))
    rep.check_ip("11.0.1.1")
    with bl._connect() as db:
        db.execute("UPDATE blocklist_snapshots SET payload=? WHERE source='spamhaus_v4'", (drop("11.0.1.0/24"),))
    with pytest.raises(rep.ReputationDenied, match="spamhaus"):
        rep.check_ip("11.0.1.1")


def test_unavailable_feed_denies_before_individual_or_candidate_connection(monkeypatch):
    monkeypatch.setattr(bl, "_download", Mock(side_effect=OSError("secret-url-or-key")))
    lookup = Mock()
    monkeypatch.setattr(rep, "_lookup", lookup)
    for _ in range(2):
        with pytest.raises(rep.ReputationDenied, match="snapshot unavailable") as e:
            rep.check_proxy("http://user:password@11.0.1.1:8080")
        assert "secret" not in str(e.value) and "password" not in str(e.value)
    assert bl._download.call_count == 1
    lookup.assert_not_called()


def test_failed_refresh_keeps_bounded_old_snapshot_then_denies(monkeypatch):
    bl.check_blocklists("11.0.1.1", "key")
    now = time.time()
    monkeypatch.setattr(bl.time, "time", lambda: now + 25 * 3600)
    monkeypatch.setattr(bl, "_download", Mock(side_effect=OSError()))
    bl.check_blocklists("11.0.1.1", "key")
    assert bl._download.call_count == 2
    monkeypatch.setattr(bl.time, "time", lambda: now + 49 * 3600)
    with pytest.raises(bl.BlocklistUnavailable):
        bl.check_blocklists("11.0.1.1", "key")


def test_key_scope_separate_only_for_abuse():
    bl.check_blocklists("11.0.1.1", "first-key")
    bl.check_blocklists("11.0.1.1", "second-key")
    assert bl._download.call_count == 3
    with bl._connect() as db:
        scopes = [r[0] for r in db.execute("SELECT scope FROM blocklist_snapshots")]
    assert not any("key" in scope for scope in scopes)


def test_parallel_workers_download_once():
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(lambda _: bl.check_blocklists("11.0.1.1", "key"), range(10)))
    assert bl._download.call_count == 2


@pytest.mark.parametrize(
    "payload",
    ["<html>blocked</html>", "", drop().replace('"records": 1', '"records": 2'), drop("0.0.0.0/0"), drop("2001:4860::/32")],
)
def test_invalid_drop_never_installed(monkeypatch, payload):
    monkeypatch.setattr(bl, "_download", lambda *a: payload)
    with pytest.raises(bl.BlocklistUnavailable):
        bl._snapshot("spamhaus_v4", "key")
    with bl._connect() as db:
        assert db.execute("SELECT payload FROM blocklist_snapshots").fetchone()[0] is None


@pytest.mark.parametrize(
    "payload",
    [
        abuse().replace("100", '"100"'),
        abuse().replace("12.0.0.1", "127.0.0.1"),
        json.dumps({"meta": {"generatedAt": "2020-01-01T00:00:00Z"}, "data": []}),
    ],
)
def test_invalid_abuse_response_denied(monkeypatch, payload):
    monkeypatch.setattr(bl, "_download", lambda *a: payload)
    with pytest.raises(bl.BlocklistUnavailable):
        bl._snapshot("abuseipdb", "key")


def test_provider_request_has_no_proxy_no_redirect_and_key_only_for_abuse(monkeypatch):
    requests = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def read(self, size):
            assert size == bl.MAX_BYTES + 1
            return b"{}"

    def opener(*handlers):
        assert handlers[0].proxies == {}
        assert handlers[1].redirect_request(None, None, 302, "", {}, "https://other.invalid") is None

        class Opener:
            def open(self, request, timeout):
                assert timeout == 15
                requests.append(request)
                return Response()

        return Opener()

    monkeypatch.setattr(bl, "build_opener", opener)
    ORIGINAL_DOWNLOAD("spamhaus_v4", "private-key")
    ORIGINAL_DOWNLOAD("abuseipdb", "private-key")
    assert requests[0].get_header("Key") is None
    assert requests[1].get_header("Key") == "private-key"
    assert all("private-key" not in r.full_url for r in requests)


ORIGINAL_DOWNLOAD = bl._download


def test_incident_ranges_skip_all_downloads():
    with pytest.raises(rep.ReputationDenied, match="local network policy"):
        rep.check_ip("8.213.1.1")
    bl._download.assert_not_called()


def test_malformed_refresh_preserves_last_valid_snapshot(monkeypatch):
    original = bl._snapshot("spamhaus_v4", "key")
    now = time.time()
    monkeypatch.setattr(bl.time, "time", lambda: now + 25 * 3600)
    monkeypatch.setattr(bl, "_download", Mock(return_value="<html>not a list</html>"))
    assert bl._snapshot("spamhaus_v4", "key") == original
    assert bl._snapshot("spamhaus_v4", "key") == original
    assert bl._download.call_count == 1
    assert bl.status("key")[0]["refresh_error"] == "JSONDecodeError"


def test_old_snapshot_status_is_visible_even_in_a_new_process(monkeypatch):
    bl.check_blocklists("11.0.1.1", "key")
    now = time.time()
    monkeypatch.setattr(bl.time, "time", lambda: now + 49 * 3600)
    bl._parse.cache_clear()
    assert all(not row["usable"] for row in bl.status("key"))


@pytest.mark.parametrize("ip", ["8.213.1.1", "127.0.0.1", "11.0.0.1", "12.0.0.1"])
def test_blocklist_mode_preserves_network_and_feed_denials(monkeypatch, ip):
    monkeypatch.setenv("PROXY_REPUTATION_MODE", "blocklist_only")
    lookup = Mock(side_effect=AssertionError("No CHECK"))
    monkeypatch.setattr(rep, "_lookup", lookup)
    with pytest.raises(rep.ReputationDenied):
        rep.check_ip(ip)
    lookup.assert_not_called()


def test_blocklist_outage_never_falls_back_to_check(monkeypatch):
    monkeypatch.setenv("PROXY_REPUTATION_MODE", "blocklist_only")
    monkeypatch.setattr(bl, "_download", Mock(side_effect=OSError()))
    lookup = Mock()
    monkeypatch.setattr(rep, "_lookup", lookup)
    with pytest.raises(rep.ReputationDenied, match="snapshot unavailable"):
        rep.check_ip("11.0.1.1")
    lookup.assert_not_called()


def test_prepared_batch_checker_reads_snapshots_once():
    # Use the existing isolated fixture; only snapshot acquisition should touch SQLite.
    checker = bl.prepare_ipv4_checker("private-test-key")
    assert checker(ipaddress.IPv4Address("11.0.0.1")) == "spamhaus_v4"
    assert checker(ipaddress.IPv4Address("12.0.0.1")) == "abuseipdb"
    assert checker(ipaddress.IPv4Address("13.0.0.1")) is None
    assert bl._download.call_count == 2
