import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.proxy import reputation as rep


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    monkeypatch.setattr("src.proxy.threat_feeds.check_ip", lambda *args: None)
    monkeypatch.setattr("src.proxy.threat_feeds.check_host", lambda *args: None)
    monkeypatch.setenv("PROXY_REPUTATION_MODE", "strict")
    monkeypatch.setattr("src.proxy.blocklists.check_blocklists", lambda *args: None)
    monkeypatch.setenv("PROXY_REPUTATION_CACHE", str(tmp_path / "reputation.db"))
    monkeypatch.setenv("ABUSEIPDB_API_KEY", "test-private-key")
    monkeypatch.setenv("PROXY_REPUTATION_SCORE_LIMIT", "25")
    monkeypatch.setenv("PROXY_REPUTATION_MAX_REPORTS", "0")
    monkeypatch.delenv("ALLOW_PUBLIC_PROXY_COLLECTION", raising=False)


@pytest.mark.parametrize(
    "ip",
    [
        "8.211.0.1",
        "8.212.255.254",
        "8.213.1.2",
        "8.220.1.2",
        "47.245.1.2",
        "47.251.1.2",
        "47.252.1.2",
        "110.238.1.2",
        "::ffff:8.213.1.2",
        "127.0.0.1",
        "169.254.169.254",
        "::1",
    ],
)
def test_local_denial_never_queries_api_or_connects(monkeypatch, ip):
    lookup = Mock(side_effect=AssertionError("API must not run"))
    monkeypatch.setattr(rep, "_lookup", lookup)
    with pytest.raises(rep.ReputationDenied):
        rep.check_ip(ip)
    lookup.assert_not_called()


def test_cache_and_stricter_policy_on_cached_data(monkeypatch):
    lookup = Mock(return_value=(0, 0))
    monkeypatch.setattr(rep, "_lookup", lookup)
    rep.check_ip("1.1.1.1")
    rep.check_ip("1.1.1.1")
    assert lookup.call_count == 1
    monkeypatch.setenv("PROXY_REPUTATION_MAX_REPORTS", "2")
    lookup.return_value = (1, 1)
    rep.check_ip("9.9.9.9")
    monkeypatch.setenv("PROXY_REPUTATION_MAX_REPORTS", "0")
    with pytest.raises(rep.ReputationDenied):
        rep.check_ip("9.9.9.9")
    assert lookup.call_count == 2


@pytest.mark.parametrize("result", [(25, 0), (1, 1), (100, 100)])
def test_recent_reports_or_score_block(monkeypatch, result):
    monkeypatch.setattr(rep, "_lookup", lambda *a: result)
    with pytest.raises(rep.ReputationDenied):
        rep.check_ip("1.1.1.1")


def test_no_key_fails_closed_without_leaking_proxy_credentials(monkeypatch):
    monkeypatch.delenv("ABUSEIPDB_API_KEY")
    with pytest.raises(rep.ReputationDenied) as e:
        rep.check_proxy("http://user:password@1.1.1.1:8080")
    assert "password" not in str(e.value)


def test_api_offline_does_not_admit_or_cache(monkeypatch):
    def failed(*a):
        raise rep.ReputationDenied("unavailable")

    monkeypatch.setattr(rep, "_lookup", failed)
    with pytest.raises(rep.ReputationDenied):
        rep.check_ip("1.1.1.1")
    lookup = Mock(return_value=(0, 0))
    monkeypatch.setattr(rep, "_lookup", lookup)
    with pytest.raises(rep.ReputationDenied):
        rep.check_ip("1.1.1.1")
    lookup.assert_not_called()
    now = rep.time.time()
    monkeypatch.setattr(rep.time, "time", lambda: now + 61)
    rep.check_ip("1.1.1.1")
    lookup.assert_called_once()


def test_blocked_dns_peer_never_queries_api_and_safe_peer_is_pinned(monkeypatch):
    monkeypatch.setattr(
        rep.socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("8.213.1.2", 8080)), (2, 1, 6, "", ("9.9.9.9", 8080))]
    )
    lookup = Mock(return_value=(0, 0))
    monkeypatch.setattr(rep, "_lookup", lookup)
    assert rep.check_proxy("http://proxy.example:8080") == "http://9.9.9.9:8080"
    lookup.assert_called_once_with("9.9.9.9", "test-private-key")


def test_bad_reputation_dns_peer_is_not_used_when_clean_peer_exists(monkeypatch):
    monkeypatch.setattr(
        rep.socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("1.1.1.1", 8080)), (2, 1, 6, "", ("9.9.9.9", 8080))]
    )
    monkeypatch.setattr(rep, "_lookup", lambda ip, key: (20, 5) if ip == "1.1.1.1" else (0, 0))
    assert rep.check_proxy("http://proxy.example:8080") == "http://9.9.9.9:8080"


def test_all_bad_dns_peers_still_deny_connection(monkeypatch):
    monkeypatch.setattr(
        rep.socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("8.213.1.2", 8080)), (2, 1, 6, "", ("9.9.9.9", 8080))]
    )
    monkeypatch.setattr(rep, "_lookup", lambda ip, key: (20, 5))
    with pytest.raises(rep.ReputationDenied):
        rep.check_proxy("http://proxy.example:8080")


def test_dns_pinned_and_credentials_preserved(monkeypatch):
    monkeypatch.setattr(rep.socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("1.1.1.1", 8080))])
    monkeypatch.setattr(rep, "_lookup", lambda *a: (0, 0))
    assert rep.check_proxy("http://u:p%40ss@proxy.example:8080") == "http://u:p%40ss@1.1.1.1:8080"


@pytest.mark.parametrize(
    "data",
    [
        {"ipAddress": "9.9.9.9", "isPublic": True, "abuseConfidenceScore": 0, "totalReports": 0},
        {"ipAddress": "1.1.1.1", "isPublic": True, "abuseConfidenceScore": "0", "totalReports": 0},
    ],
)
def test_invalid_api_response_fails_closed(monkeypatch, data):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def read(self, *a):
            return json.dumps({"data": data}).encode()

    opener = SimpleNamespace(open=lambda *a, **k: Response())
    monkeypatch.setattr(rep, "build_opener", lambda *a: opener)
    with pytest.raises(rep.ReputationDenied):
        rep.check_ip("1.1.1.1")


async def test_collector_blocked_ip_never_constructs_transport(monkeypatch):
    from src.proxy import checker

    async def immediate(url):
        return rep.check_proxy(url)

    monkeypatch.setattr(rep, "screen_proxy", immediate)

    factory = Mock(side_effect=AssertionError("Must not connect"))
    monkeypatch.setattr(checker, "AsyncSession", factory)
    result = await checker.verify_stage1("http://8.213.151.128:3128", transport="curl_cffi", user_agent="test")
    assert not result.ok
    factory.assert_not_called()


async def test_public_catalog_disabled_even_if_explicit_sources_requested(monkeypatch):
    from src.proxy import collector

    async def forbidden(*a, **k):
        raise AssertionError("Public source loader must not run")

    monkeypatch.setattr(collector, "load_public_catalog", forbidden)
    monkeypatch.setattr(collector, "load_free_proxy_candidates", forbidden)
    result = await collector._load_candidate_sources(["proxyscrape", "free-proxy", "iplocate"], None, "", SimpleNamespace())
    assert result.candidates == []


def test_res_blocked_proxy_never_constructs_http_transport(monkeypatch):
    from src.sites.res import http_flow

    factory = Mock(side_effect=AssertionError("Must not construct transport"))
    monkeypatch.setattr(http_flow.httpx, "HTTPTransport", factory)
    with pytest.raises(rep.ReputationDenied):
        http_flow.new_session(proxy="http://47.251.1.2:8080")
    factory.assert_not_called()


async def test_browser_blocked_before_launch_or_profile_creation(monkeypatch, tmp_path):
    from src.sites.res.engines import launch_context

    async def immediate(url):
        return rep.check_proxy(url)

    monkeypatch.setattr(rep, "screen_proxy", immediate)
    profile = tmp_path / "must-not-exist"
    with pytest.raises(rep.ReputationDenied):
        async with launch_context(profile, proxy={"server": "http://8.211.1.2:8080"}):
            pytest.fail("Browser must not launch")
    assert not profile.exists()


def test_api_uses_direct_https_header_key_and_no_redirects(monkeypatch):
    captured = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def read(self, limit):
            return json.dumps(
                {"data": {"ipAddress": "1.1.1.1", "isPublic": True, "abuseConfidenceScore": 0, "totalReports": 0}}
            ).encode()

    def open_request(request, **kwargs):
        captured["request"] = request
        assert kwargs["timeout"] == 10
        return Response()

    def opener(*handlers):
        assert handlers[0].proxies == {}
        assert handlers[1].redirect_request(None, None, 302, "", {}, "https://other.invalid") is None
        return SimpleNamespace(open=open_request)

    monkeypatch.setattr(rep, "build_opener", opener)
    rep.check_ip("1.1.1.1")
    request = captured["request"]
    assert request.full_url.startswith("https://api.abuseipdb.com/api/v2/check?")
    assert "test-private-key" not in request.full_url
    assert request.get_header("Key") == "test-private-key"


def test_stale_cached_allow_cannot_bypass_outage(monkeypatch):
    monkeypatch.setattr(rep, "_lookup", lambda *args: (0, 0))
    rep.check_ip("1.1.1.1")
    now = rep.time.time()
    monkeypatch.setattr(rep.time, "time", lambda: now + 21601)

    def unavailable(*args):
        raise rep.ReputationDenied("API unavailable")

    monkeypatch.setattr(rep, "_lookup", unavailable)
    with pytest.raises(rep.ReputationDenied):
        rep.check_ip("1.1.1.1")


@pytest.mark.parametrize("mode", ["blocklist_only", " BLOCKLIST_ONLY "])
def test_blocklist_only_never_calls_check_or_writes_individual_cache(monkeypatch, tmp_path, mode):
    monkeypatch.setenv("PROXY_REPUTATION_MODE", mode)
    lookup = Mock(side_effect=AssertionError("Individual CHECK must never run"))
    monkeypatch.setattr(rep, "_lookup", lookup)
    rep.check_ip("1.1.1.1")
    assert rep.check_proxy("http://1.1.1.1:8080") == "http://1.1.1.1:8080"
    lookup.assert_not_called()
    assert not (tmp_path / "reputation.db").exists()


def test_return_to_strict_requires_individual_verdict(monkeypatch):
    monkeypatch.setenv("PROXY_REPUTATION_MODE", "blocklist_only")
    lookup = Mock(return_value=(0, 0))
    monkeypatch.setattr(rep, "_lookup", lookup)
    rep.check_ip("1.1.1.1")
    lookup.assert_not_called()
    monkeypatch.setenv("PROXY_REPUTATION_MODE", "strict")
    rep.check_ip("1.1.1.1")
    lookup.assert_called_once()


@pytest.mark.parametrize("mode", ["strict", "blocklist_only"])
def test_check_is_last_after_blocklists(monkeypatch, mode):
    monkeypatch.setenv("PROXY_REPUTATION_MODE", mode)
    events = []
    monkeypatch.setattr("src.proxy.blocklists.check_blocklists", lambda *a: events.append("blocklists"))
    monkeypatch.setattr(rep, "_lookup", lambda *a: events.append("individual") or (0, 0))
    rep.check_ip("1.1.1.1")
    assert events == (["blocklists", "individual"] if mode == "strict" else ["blocklists"])


@pytest.mark.parametrize("mode", ["unknown", "off", "", "false", "disabled"])
def test_invalid_mode_denies_connection(monkeypatch, mode):
    monkeypatch.setenv("PROXY_REPUTATION_MODE", mode)
    lookup = Mock()
    monkeypatch.setattr(rep, "_lookup", lookup)
    with pytest.raises(rep.ReputationDenied, match="PROXY_REPUTATION_MODE"):
        rep.check_ip("1.1.1.1")
    with pytest.raises(rep.ReputationDenied, match="PROXY_REPUTATION_MODE"):
        rep.check_proxy("http://1.1.1.1:8080")
    lookup.assert_not_called()
