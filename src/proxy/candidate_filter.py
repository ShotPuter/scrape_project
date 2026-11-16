"""Batch IP screening against prepared local lists, without contacting candidates."""

from __future__ import annotations

import ipaddress
import os
from collections import Counter
from collections.abc import Sequence

from src.proxy import blocklists, threat_feeds
from src.proxy.reputation import ReputationDenied, validate_ip


def filter_candidate_ips(candidates: Sequence[tuple[str, str, str]]) -> tuple[list[tuple[str, str]], dict[str, int]]:
    """Load mandatory snapshots once per batch; never use individual CHECK requests.

    Each row contains endpoint, source and a numeric IPv4 address. A broken required
    snapshot aborts the entire batch, preserving the existing fail-closed policy.
    """
    if not candidates:
        return [], {}
    key = os.getenv("ABUSEIPDB_API_KEY", "").strip()
    if not key:
        raise ReputationDenied("ABUSEIPDB_API_KEY missing; candidate batch denied")
    feeds = {source: threat_feeds.snapshot(source) for source in threat_feeds.FEEDS}
    bulk_checker = blocklists.prepare_ipv4_checker(key)
    verdicts: dict[str, str | None] = {}
    accepted: list[tuple[str, str]] = []
    rejected: Counter[str] = Counter()
    for endpoint, source, address_text in candidates:
        if address_text not in verdicts:
            reason = None
            try:
                address = ipaddress.IPv4Address(validate_ip(address_text))
                reason = next((source for source, feed in feeds.items() if feed.matches_ip(address)), None)
                if reason is None:
                    reason = bulk_checker(address)
            except (ReputationDenied, ValueError):
                reason = "network_policy"
            verdicts[address_text] = reason
        reason = verdicts[address_text]
        if reason:
            rejected[reason] += 1
        else:
            accepted.append((endpoint, source))
    return accepted, dict(rejected)
