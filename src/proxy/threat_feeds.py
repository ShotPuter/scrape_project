"""Mandatory cached FireHOL/NoCoin screening before any candidate connection.

Only HTTPS feed downloads are performed here. DNS is never used to expand mining
names into shared hosting IPs. Network membership uses merged integer intervals.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
import time
from bisect import bisect_right
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from src.utils.atomic import atomic_write_json

FEEDS: dict[str, str] = {
    "firehol_level1": "https://raw.githubusercontent.com/firehol/blocklist-ipsets/master/firehol_level1.netset",
    "firehol_abusers_30d": "https://raw.githubusercontent.com/firehol/blocklist-ipsets/master/firehol_abusers_30d.netset",
    "nocoin": "https://raw.githubusercontent.com/hoshsadiq/adblock-nocoin-list/master/nocoin.txt",
}
MAX_BYTES = 10 * 1024 * 1024
RETRY_SECONDS = 600
Network = ipaddress.IPv4Network | ipaddress.IPv6Network
Address = ipaddress.IPv4Address | ipaddress.IPv6Address


class ThreatFeedError(ValueError):
    """A mandatory feed is unusable; callers must deny candidate connections."""


class ThreatListed(ThreatFeedError):
    """A candidate is listed, with a safe machine-readable source reason."""

    def __init__(self, reason: str, indicator: str):
        self.reason = reason
        super().__init__(f"Proxy threat policy denied {indicator}: reason={reason}")


@dataclass(frozen=True)
class RangeIndex:
    starts: tuple[int, ...]
    ends: tuple[int, ...]

    @classmethod
    def build(cls, networks: Iterable[Network], version: int) -> RangeIndex:
        merged: list[tuple[int, int]] = []
        for start, end in sorted(
            (int(network.network_address), int(network.broadcast_address))
            for network in networks
            if network.version == version
        ):
            if merged and start <= merged[-1][1] + 1:
                merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
            else:
                merged.append((start, end))
        return cls(tuple(start for start, _ in merged), tuple(end for _, end in merged))

    def contains(self, address: Address) -> bool:
        value = int(address)
        position = bisect_right(self.starts, value) - 1
        return position >= 0 and value <= self.ends[position]


@dataclass(frozen=True)
class FeedData:
    networks: tuple[Network, ...]
    ipv4: RangeIndex
    ipv6: RangeIndex
    domains: frozenset[str]

    def matches_ip(self, address: Address) -> bool:
        return (self.ipv4 if address.version == 4 else self.ipv6).contains(address)


_memory: dict[tuple[str, str, float, float], tuple[float, FeedData]] = {}


def normalize_domain(host: str) -> str:
    domain = host.rstrip(".").encode("idna").decode("ascii").lower()
    if (
        len(domain) > 253
        or "." not in domain
        or not all(re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) for label in domain.split("."))
    ):
        raise ValueError("Invalid domain")
    return domain


def parse_feed(source: str, payload: str) -> FeedData:
    """Parse netsets or conservative host-wide NoCoin rules; retain literal IPs.

    Adblock path filters, regexes, exceptions and scoped domain= rules are not
    promoted into whole-host bans. Subdomain matching is label-boundary based.
    """
    networks: list[Network] = []
    domains: set[str] = set()
    for raw_line in payload.splitlines():
        line = raw_line.strip()
        if not line or line.startswith(("#", ";", "!", "[", "@@")):
            continue
        if source != "nocoin":
            token = re.split(r"\s*[#;!]", line, maxsplit=1)[0].strip()
            network = ipaddress.ip_network(token, strict=True)
            if network.version != 4 or network.prefixlen == 0:
                raise ValueError("Invalid FireHOL IPv4 netset")
            networks.append(network)
            continue
        if line.startswith("||"):
            match = re.fullmatch(r"\|\|([^/*^]+)\^(?:\$([^\s]+))?", line)
            if not match or "domain=" in (match.group(2) or ""):
                continue
            token = match.group(1)
        else:
            columns = line.split()
            token = columns[-1] if len(columns) == 2 and columns[0] in {"0.0.0.0", "127.0.0.1"} else line
        try:
            address = ipaddress.ip_address(token)
        except ValueError:
            try:
                domains.add(normalize_domain(token))
            except (ValueError, UnicodeError):
                continue
        else:
            networks.append(ipaddress.ip_network(str(address)))
    if not networks and not domains:
        raise ValueError("Empty or unsupported threat feed")
    return FeedData(tuple(networks), RangeIndex.build(networks, 4), RangeIndex.build(networks, 6), frozenset(domains))


def _download(source: str) -> str:
    class NoRedirect(HTTPRedirectHandler):
        def redirect_request(self, *args: Any, **kwargs: Any) -> None:
            return None

    request = Request(FEEDS[source], headers={"User-Agent": "proxy-threat-screen/1.0", "Accept": "text/plain"})
    with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=15) as response:
        if response.status != 200:
            raise ValueError("Threat feed HTTP status")
        body = response.read(MAX_BYTES + 1)
    if len(body) > MAX_BYTES:
        raise ValueError("Threat feed size limit")
    return body.decode("utf-8-sig")


@contextmanager
def _lock(path: Path) -> Iterator[None]:
    import fcntl

    with path.open("a", encoding="ascii") as handle:
        deadline = time.monotonic() + 25
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise ThreatFeedError("Threat feed cache lock timeout") from None
                time.sleep(0.05)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def snapshot(source: str) -> FeedData:
    """Refresh once per process group; use prior data only within maximum age."""
    try:
        if source not in FEEDS:
            raise ValueError("Unknown threat source")
        refresh = float(os.getenv("PROXY_THREAT_REFRESH_HOURS", "6")) * 3600
        max_age = float(os.getenv("PROXY_THREAT_MAX_AGE_HOURS", "12")) * 3600
        if not 3600 <= refresh <= max_age <= 48 * 3600:
            raise ValueError("Invalid threat feed timing")
        directory = Path(os.getenv("PROXY_THREAT_CACHE", "data/threat-feeds"))
        memory_key = (str(directory.resolve()), source, refresh, max_age)
        now = time.time()
        cached_memory = _memory.get(memory_key)
        if cached_memory and now < cached_memory[0]:
            return cached_memory[1]
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / (source + ".json")
        with _lock(directory / (source + ".lock")):
            state: dict[str, Any] = {}
            parsed: FeedData | None = None
            try:
                if path.stat().st_size > MAX_BYTES * 2:
                    raise ValueError("Oversized cache")
                state = json.loads(path.read_text(encoding="utf-8"))
                payload = state["payload"]
                if state["sha256"] != hashlib.sha256(payload.encode()).hexdigest():
                    raise ValueError("Cache digest mismatch")
                if not 0 <= now - state["fetched_at"] < max_age:
                    raise ValueError("Expired cache")
                parsed = parse_feed(source, payload)
            except (OSError, ValueError, KeyError, TypeError):
                # Preserve a valid failure backoff, but never reuse expired data.
                parsed = None
            fetched = state.get("fetched_at", 0)
            attempted = state.get("attempted_at", 0)
            if not (parsed and now - fetched < refresh) and now - attempted >= RETRY_SECONDS:
                attempted = now
                try:
                    payload = _download(source)
                    new_parsed = parse_feed(source, payload)
                    state = {
                        "source": source,
                        "url": FEEDS[source],
                        "payload": payload,
                        "sha256": hashlib.sha256(payload.encode()).hexdigest(),
                        "fetched_at": now,
                        "attempted_at": now,
                        "error": None,
                    }
                    parsed, fetched = new_parsed, now
                except Exception as error:
                    state["attempted_at"] = now
                    state["error"] = type(error).__name__
                atomic_write_json(path, state)
            if parsed is None:
                raise ThreatFeedError(f"Mandatory threat feed {source} unavailable or expired; connection denied")
            # Check at least once a minute, including updates from other processes.
            _memory[memory_key] = (min(now + 60, fetched + max_age), parsed)
            return parsed
    except ThreatFeedError:
        raise
    except Exception:
        raise ThreatFeedError(f"Mandatory threat feed {source} cache/configuration invalid; connection denied") from None


def check_ip(value: str) -> None:
    address = ipaddress.ip_address(value)
    for source in FEEDS:
        if snapshot(source).matches_ip(address):
            raise ThreatListed(source, str(address))


def check_host(host: str) -> None:
    """Reject known mining hosts before DNS; do not resolve listed domains."""
    domain = normalize_domain(host)
    # All mandatory sources must be available, even for a hostname-only check.
    for source in FEEDS:
        feed = snapshot(source)
        labels = domain.split(".")
        if any(".".join(labels[position:]) in feed.domains for position in range(len(labels) - 1)):
            raise ThreatListed(source, domain)
