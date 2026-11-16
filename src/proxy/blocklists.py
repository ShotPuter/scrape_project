"""Shared passive denylist snapshots; absence from a feed is never permission.

Spamhaus Project DROP data retains its original date, copyright and terms in
SQLite. https://www.spamhaus.org/blocklists/do-not-route-or-peer/
AbuseIPDB: https://docs.abuseipdb.com/#blacklist-endpoint
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import sqlite3
import time
from contextlib import closing
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

SOURCES = {
    "spamhaus_v4": "https://www.spamhaus.org/drop/drop_v4.json",
    "spamhaus_v6": "https://www.spamhaus.org/drop/drop_v6.json",
    "abuseipdb": "https://api.abuseipdb.com/api/v2/blacklist?limit=10000",
}
MAX_BYTES = 5 * 1024 * 1024
RETRY_SECONDS = 6 * 3600


class BlocklistDenied(ValueError):
    pass


class BlocklistUnavailable(ValueError):
    pass


def _download(source: str, key: str) -> str:
    class NoRedirect(HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None

    headers = {"Accept": "application/json", "User-Agent": "proxy-reputation-screen/1.0"}
    if source == "abuseipdb":
        headers["Key"] = key
    request = Request(SOURCES[source], headers=headers)
    # Provider connections only; never through a candidate or environment proxy.
    with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=15) as response:
        body = response.read(MAX_BYTES + 1)
    if len(body) > MAX_BYTES:
        raise ValueError("Oversized blocklist")
    return body.decode("utf-8")


@lru_cache(maxsize=6)
def _parse(source: str, payload: str):
    """Validate entire snapshot before installing it; keep original metadata."""
    if source.startswith("spamhaus"):
        rows = [json.loads(line) for line in payload.splitlines() if line.strip()]
        metadata = rows[-1]
        if metadata.get("type") != "metadata" or not metadata.get("copyright") or not metadata.get("terms"):
            raise ValueError("Missing DROP metadata")
        timestamp = metadata["timestamp"]
        if type(timestamp) is not int or timestamp <= 0:
            raise ValueError("Invalid DROP timestamp")
        version = 4 if source.endswith("v4") else 6
        networks = [ipaddress.ip_network(row["cidr"], strict=True) for row in rows[:-1]]
        if not networks or any(n.version != version or n.prefixlen == 0 for n in networks):
            raise ValueError("Invalid DROP networks")
        if type(metadata.get("records")) is not int or metadata["records"] != len(networks):
            raise ValueError("DROP count mismatch")
    else:
        data = json.loads(payload)
        generated = datetime.fromisoformat(data["meta"]["generatedAt"].replace("Z", "+00:00"))
        if generated.tzinfo is None or generated.timestamp() <= 0:
            raise ValueError("Invalid blacklist generation time")
        timestamp = generated.timestamp()
        rows = data["data"]
        if not isinstance(rows, list) or not 1 <= len(rows) <= 10000:
            raise ValueError("Invalid blacklist length")
        networks = []
        for row in rows:
            ip = ipaddress.ip_address(row["ipAddress"])
            score = row["abuseConfidenceScore"]
            if not ip.is_global or type(score) is not int or not 25 <= score <= 100:
                raise ValueError("Invalid blacklist entry")
            networks.append(ipaddress.ip_network(str(ip)))
    # CIDR indexes, not an O(number-of-networks) scan for every candidate.
    indexes = {4: {}, 6: {}}
    for network in networks:
        indexes[network.version].setdefault(network.prefixlen, set()).add(int(network.network_address))
    return indexes, len(networks), timestamp


def _settings():
    refresh = float(os.getenv("PROXY_BLOCKLIST_REFRESH_HOURS", "24")) * 3600
    max_age = float(os.getenv("PROXY_BLOCKLIST_MAX_AGE_HOURS", "48")) * 3600
    if not 6 * 3600 <= refresh <= max_age <= 7 * 86400:
        raise ValueError("Invalid blocklist age settings")
    return refresh, max_age


def _connect():
    path = Path(os.getenv("PROXY_REPUTATION_CACHE", "data/proxy-reputation.sqlite3"))
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=60)
    db.execute(
        "CREATE TABLE IF NOT EXISTS blocklist_snapshots ("
        "source TEXT,scope TEXT,payload TEXT,fetched REAL,attempted REAL,error TEXT,"
        "PRIMARY KEY(source,scope))"
    )
    return db


def _scope(source, key):
    return hashlib.sha256(key.encode()).hexdigest() if source == "abuseipdb" else "public"


def _snapshot(source: str, key: str) -> str:
    try:
        refresh, max_age = _settings()
        scope = _scope(source, key)
        with closing(_connect()) as db:
            # One updater across threads/processes; no per-worker feed downloads.
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT payload,fetched,attempted,error FROM blocklist_snapshots WHERE source=? AND scope=?", (source, scope)
            ).fetchone()
            payload, fetched, attempted, error = row or (None, 0, 0, None)
            now = time.time()
            if not (payload and 0 <= now - fetched < refresh) and now - attempted >= RETRY_SECONDS:
                attempted = now
                try:
                    new_payload = _download(source, key)
                    generated_at = _parse(source, new_payload)[2]
                    if generated_at > now + 3600 or (source == "abuseipdb" and now - generated_at > 48 * 3600):
                        raise ValueError("Invalid blocklist generation time")
                    payload, fetched, error = new_payload, now, None
                except Exception as exc:
                    # Exception class only; never persist URLs, headers or credentials.
                    error = type(exc).__name__
                db.execute(
                    "INSERT OR REPLACE INTO blocklist_snapshots VALUES(?,?,?,?,?,?)",
                    (source, scope, payload, fetched, attempted, error),
                )
            db.commit()
            if not payload or not 0 <= now - fetched < max_age:
                raise BlocklistUnavailable(f"{source} snapshot unavailable or expired; connection denied")
            _parse(source, payload)
            return payload
    except BlocklistUnavailable:
        raise
    except Exception:
        raise BlocklistUnavailable(f"{source} cache or configuration invalid; connection denied") from None


def check_blocklists(value: str, key: str) -> None:
    ip = ipaddress.ip_address(value)
    for source in ("spamhaus_v4" if ip.version == 4 else "spamhaus_v6", "abuseipdb"):
        indexes = _parse(source, _snapshot(source, key))[0]
        bits = ip.max_prefixlen
        number = int(ip)
        for prefix, networks in indexes[ip.version].items():
            if (number >> (bits - prefix)) << (bits - prefix) in networks:
                raise BlocklistDenied(f"Proxy IP {ip} denied by local blocklist {source}")


def status(key: str) -> list[dict]:
    _, max_age = _settings()
    with closing(_connect()) as db:
        result = []
        for source in SOURCES:
            row = db.execute(
                "SELECT payload,fetched,attempted,error FROM blocklist_snapshots WHERE source=? AND scope=?",
                (source, _scope(source, key)),
            ).fetchone()
            if row:
                payload, fetched, attempted, error = row
                result.append(
                    {
                        "source": source,
                        "entries": _parse(source, payload)[1] if payload else 0,
                        "fetched_at": fetched,
                        "last_attempt": attempted,
                        "usable": bool(payload) and 0 <= time.time() - fetched < max_age,
                        "refresh_error": error,
                    }
                )
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("refresh", "status"))
    args = parser.parse_args()
    from dotenv import load_dotenv

    load_dotenv(".env.res")
    load_dotenv(".env")
    key = os.getenv("ABUSEIPDB_API_KEY", "").strip()
    if args.action == "refresh":
        if not key:
            parser.error("ABUSEIPDB_API_KEY missing")
        for source in SOURCES:
            _snapshot(source, key)  # Respects refresh and failure backoff, even on manual runs.
    print(json.dumps(status(key), indent=2))


if __name__ == "__main__":
    main()
