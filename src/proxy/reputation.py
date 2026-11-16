"""Fail-closed passive proxy screening. Never contacts the candidate endpoint."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import os
import socket
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from urllib.request import ProxyHandler, Request, build_opener

BLOCKED_NETWORKS = tuple(
    ipaddress.ip_network(n)
    for n in (
        "8.211.0.0/16",
        "8.212.0.0/16",
        "8.213.0.0/16",
        "8.220.0.0/16",
        "47.245.0.0/16",
        "47.251.0.0/16",
        "47.252.0.0/16",
        "110.238.0.0/16",
    )
)


class ReputationDenied(ValueError):
    """Safe to log: contains neither proxy credentials nor API credentials."""

    status = "proxy_reputation_denied"


def reputation_mode() -> str:
    mode = os.getenv("PROXY_REPUTATION_MODE", "strict").strip().lower()
    if mode not in {"strict", "blocklist_only"}:
        raise ReputationDenied("Invalid PROXY_REPUTATION_MODE; expected strict or blocklist_only")
    return mode


def public_collection_allowed() -> bool:
    return os.getenv("ALLOW_PUBLIC_PROXY_COLLECTION", "0") == "1"


def validate_ip(value: str):
    try:
        ip = ipaddress.ip_address(value)
    except ValueError:
        raise ReputationDenied("Invalid proxy IP") from None
    if getattr(ip, "ipv4_mapped", None):
        ip = ip.ipv4_mapped
    if not ip.is_global or any(ip in net for net in BLOCKED_NETWORKS if net.version == ip.version):
        raise ReputationDenied(f"Proxy IP {ip} denied by local network policy")
    return str(ip)


def _lookup(ip, key):
    request = Request(
        "https://api.abuseipdb.com/api/v2/check?" + urlencode({"ipAddress": ip, "maxAgeInDays": 30}),
        headers={"Key": key, "Accept": "application/json"},
    )
    # No environment proxy, no candidate proxy, no redirects with a credential header.
    from urllib.request import HTTPRedirectHandler

    class NoRedirect(HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None

    try:
        with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=10) as response:
            body = response.read(65537)
            if len(body) > 65536:
                raise ValueError("Oversized API response")
            data = json.loads(body)["data"]
        if str(ipaddress.ip_address(data["ipAddress"])) != ip or data.get("isPublic") is not True:
            raise ValueError("IP mismatch")
        score, reports = data["abuseConfidenceScore"], data["totalReports"]
        if type(score) is not int or not 0 <= score <= 100 or type(reports) is not int or reports < 0:
            raise ValueError("Invalid reputation data")
        return score, reports
    except Exception:
        raise ReputationDenied("Proxy reputation API unavailable or invalid; connection denied") from None


def check_ip(value: str) -> None:
    ip = validate_ip(value)  # Always before cache, key checks or API requests.
    mode = reputation_mode()
    from src.proxy.threat_feeds import ThreatFeedError, check_ip as check_threat_ip

    try:
        check_threat_ip(ip)
    except ThreatFeedError as exc:
        raise ReputationDenied(str(exc)) from None
    key = os.getenv("ABUSEIPDB_API_KEY", "").strip()
    if not key:
        raise ReputationDenied("ABUSEIPDB_API_KEY missing; proxy connection denied")
    # Additive deny gates run even before an existing cached allow verdict.
    from src.proxy.blocklists import BlocklistDenied, BlocklistUnavailable, check_blocklists

    try:
        check_blocklists(ip, key)
    except (BlocklistDenied, BlocklistUnavailable) as exc:
        raise ReputationDenied(str(exc)) from None
    if mode == "blocklist_only":
        # An explicit list-based policy, not an individual clean-IP verdict.
        # Never use/populate the individual CHECK cache or API backoff here.
        return
    try:
        score_limit = int(os.getenv("PROXY_REPUTATION_SCORE_LIMIT", "25"))
        reports_limit = int(os.getenv("PROXY_REPUTATION_MAX_REPORTS", "0"))
        ttl = float(os.getenv("PROXY_REPUTATION_CACHE_HOURS", "6")) * 3600
        if not 1 <= score_limit <= 100 or reports_limit < 0 or not 60 <= ttl <= 86400:
            raise ValueError()
        path = Path(os.getenv("PROXY_REPUTATION_CACHE", "data/proxy-reputation.sqlite3"))
        path.parent.mkdir(parents=True, exist_ok=True)
        # Separate cache scopes for API credentials, without persisting the credential.
        scope = hashlib.sha256(key.encode()).hexdigest()
        with closing(sqlite3.connect(path, timeout=20)) as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS reputation("
                "ip TEXT,scope TEXT,score INTEGER,reports INTEGER,checked REAL,PRIMARY KEY(ip,scope))"
            )
            db.execute("CREATE TABLE IF NOT EXISTS api_backoff(scope TEXT PRIMARY KEY,until_at REAL)")
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT score,reports,checked FROM reputation WHERE ip=? AND scope=?", (ip, scope)).fetchone()
            now = time.time()
            if row and 0 <= now - row[2] < ttl:
                score, reports = row[:2]
            else:
                backoff = db.execute("SELECT until_at FROM api_backoff WHERE scope=?", (scope,)).fetchone()
                if backoff and backoff[0] > now:
                    raise ReputationDenied("Reputation API backoff active; unchecked proxy denied")
                try:
                    score, reports = _lookup(ip, key)
                except ReputationDenied:
                    db.execute("INSERT OR REPLACE INTO api_backoff VALUES(?,?)", (scope, time.time() + 60))
                    db.commit()
                    raise
                db.execute("INSERT OR REPLACE INTO reputation VALUES(?,?,?,?,?)", (ip, scope, score, reports, time.time()))
            db.commit()
    except ReputationDenied:
        raise
    except Exception:
        raise ReputationDenied("Invalid reputation configuration or cache unavailable; connection denied") from None
    if score >= score_limit or reports > reports_limit:
        raise ReputationDenied(f"Proxy IP {ip} denied: abuse score {score}, recent reports {reports}")


def check_proxy(url: str | None) -> str | None:
    if not url:
        return None
    try:
        part = urlsplit(url)
        host = part.hostname
        port = part.port
        if not host or not port:
            raise ValueError()
        try:
            addresses = [str(ipaddress.ip_address(host))]
            numeric_host = True
        except ValueError:
            from src.proxy.threat_feeds import check_host

            check_host(host)
            numeric_host = False
            addresses = sorted({r[4][0] for r in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)})
        if not addresses:
            raise ValueError()
    except ReputationDenied:
        raise
    except Exception as exc:
        from src.proxy.threat_feeds import ThreatFeedError

        if isinstance(exc, ThreatFeedError):
            raise ReputationDenied(str(exc)) from None
        raise ReputationDenied("Proxy endpoint resolution failed; connection denied") from None
    # Pin only a permitted numeric endpoint; rejected DNS peers are never used.
    if not numeric_host and part.scheme != "http":
        raise ReputationDenied("Hostname proxy requires HTTP CONNECT for screened IP pinning")
    selected = None
    last_reason = "no addresses passed screening"
    for ip in addresses[:8]:
        try:
            candidate = validate_ip(ip)
            check_ip(candidate)
        except ReputationDenied as exc:
            last_reason = str(exc)
        else:
            selected = candidate
            break
    if selected is None:
        raise ReputationDenied("Proxy endpoint denied: " + last_reason)
    netloc = ("[" + selected + "]") if ":" in selected else selected
    if "@" in part.netloc:
        netloc = part.netloc.rsplit("@", 1)[0] + "@" + netloc
    return part._replace(netloc=netloc + ":" + str(port)).geturl()


async def screen_proxy(url: str | None) -> str | None:
    return await asyncio.to_thread(check_proxy, url)
