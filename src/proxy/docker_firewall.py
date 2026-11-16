"""Host-side Docker bridge egress policy. No INPUT/OUTPUT/FORWARD mutations.

Dry-run is the default. Apply needs root, iptables backend, ip6tables and ipset.
Only owned chains and one DOCKER-USER hook are changed. IPv6 egress is closed
until IPv6 threat sets are implemented; internal container forwarding is retained.
"""

from __future__ import annotations

import argparse
import fcntl
import ipaddress
import json
import os
import re
import shlex
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from src.proxy.reputation import BLOCKED_NETWORKS
from src.proxy.threat_feeds import FeedData, snapshot

ANCHOR = "SCRAPER-EGRESS"
STAGES = ("SCRAPER-EGRESS-A", "SCRAPER-EGRESS-B")
THREAT_SET = "scraper_firehol_v4"
TEMP_SET = "scraper_firehol_next"


def interfaces_from_env() -> list[str]:
    interfaces = os.getenv("DOCKER_BRIDGE_INTERFACES", "docker0 br-+").split()
    if not interfaces or any(not re.fullmatch(r"[a-zA-Z0-9_.-]{1,15}\+?", interface) for interface in interfaces):
        raise ValueError("Invalid Docker bridge interface patterns")
    return list(dict.fromkeys(interfaces))


def endpoints_from_env() -> list[tuple[str, int]]:
    """Exact, public IPv4:port grants; DNS names and broad subnets are refused."""
    endpoints = []
    for endpoint in os.getenv("ALLOWED_PROXY_ENDPOINTS", "").split():
        host, port_text = endpoint.rsplit(":", 1)
        address = ipaddress.IPv4Address(host)
        port = int(port_text)
        if not address.is_global or any(address in network for network in BLOCKED_NETWORKS) or not 1 <= port <= 65535:
            raise ValueError("Invalid or statically blocked proxy endpoint")
        endpoints.append((str(address), port))
    return list(dict.fromkeys(endpoints))


def dns_servers_from_env() -> list[str]:
    """Keep exact host-configured IPv4 resolvers usable across FireHOL bogons."""
    configured = os.getenv("TRUSTED_DNS_SERVERS")
    candidates = configured.split() if configured is not None else []
    if configured is None:
        for path in (Path("/etc/resolv.conf"), Path("/run/systemd/resolve/resolv.conf")):
            if path.exists():
                candidates.extend(
                    line.split()[1]
                    for line in path.read_text().splitlines()
                    if line.split()[:1] == ["nameserver"] and len(line.split()) >= 2
                )
    resolvers = []
    for candidate in candidates:
        try:
            address = ipaddress.IPv4Address(candidate)
        except ValueError:
            if configured is not None:
                raise ValueError("Trusted DNS must be exact IPv4 addresses") from None
            continue
        if address.is_loopback or address.is_unspecified or address.is_multicast:
            if configured is not None:
                raise ValueError("Trusted DNS must be routable from a Docker bridge")
            continue
        if any(address in network for network in BLOCKED_NETWORKS):
            raise ValueError("Trusted DNS is in an incident-blocked range")
        resolvers.append(str(address))
    return list(dict.fromkeys(resolvers))


def policy_rules(
    interfaces: list[str],
    endpoints: list[tuple[str, int]],
    *,
    ipv6: bool = False,
    closed: bool = False,
    dns_servers: tuple[str, ...] = (),
) -> list[list[str]]:
    """Compile only bridge-ingress rules, preserving host and inbound traffic."""
    rules: list[list[str]] = []
    for source_interface in interfaces:
        for destination_interface in interfaces:
            rules.append(["-i", source_interface, "-o", destination_interface, "-j", "RETURN"])
        if closed or ipv6:
            rules.append(["-i", source_interface, "-j", "DROP"])
            continue
        # VPC DNS often sits in RFC1918 ranges included in FireHOL's bogons.
        # Grant only exact trusted resolvers on port 53, never all private IPs.
        for resolver in dns_servers:
            for protocol in ("udp", "tcp"):
                rules.append(
                    ["-i", source_interface, "-d", resolver + "/32", "-p", protocol, "--dport", "53", "-j", "RETURN"]
                )
        # Known threats cannot survive through the established-connection return.
        rules.append(["-i", source_interface, "-m", "set", "--match-set", THREAT_SET, "dst", "-j", "DROP"])
        rules.append(["-i", source_interface, "-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "RETURN"])
        rules.append(["-i", source_interface, "-p", "tcp", "-m", "multiport", "--dports", "53,80,443", "-j", "RETURN"])
        rules.append(["-i", source_interface, "-p", "udp", "--dport", "53", "-j", "RETURN"])
        for address, port in endpoints:
            rules.append(["-i", source_interface, "-d", address + "/32", "-p", "tcp", "--dport", str(port), "-j", "RETURN"])
        rules.append(
            [
                "-i",
                source_interface,
                "-m",
                "limit",
                "--limit",
                "3/min",
                "--limit-burst",
                "5",
                "-j",
                "LOG",
                "--log-prefix",
                "SCRAPER-EGRESS DROP ",
                "--log-level",
                "6",
            ]
        )
        rules.append(["-i", source_interface, "-j", "DROP"])
    rules.append(["-j", "RETURN"])
    return rules


class Runner:
    def run(self, arguments: list[str], *, body: str | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(arguments, input=body, capture_output=True, text=True, check=check, timeout=30)


def table(runner: Runner, binary: str, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return runner.run([binary, "-w", "10", *arguments], check=check)


def ensure_chain(runner: Runner, binary: str, chain: str) -> None:
    if table(runner, binary, "-S", chain, check=False).returncode:
        table(runner, binary, "-N", chain)


def install_policy(runner: Runner, binary: str, rules: list[list[str]]) -> None:
    """Build the inactive chain and atomically replace the owned anchor jump."""
    ensure_chain(runner, binary, ANCHOR)
    previous = table(runner, binary, "-S", ANCHOR).stdout
    active = next((stage for stage in STAGES if "-j " + stage in previous), None)
    stage = STAGES[1] if active == STAGES[0] else STAGES[0]
    ensure_chain(runner, binary, stage)
    table(runner, binary, "-F", stage)  # Never flush the active chain or DOCKER-USER.
    for rule in rules:
        table(runner, binary, "-A", stage, *rule)
    anchor_rules = [line for line in previous.splitlines() if line.startswith("-A ")]
    table(runner, binary, "-R" if anchor_rules else "-A", ANCHOR, *(["1"] if anchor_rules else []), "-j", stage)
    # Own only this exact hook; preserve all unrelated DOCKER-USER rules.
    expected = ["-A", "DOCKER-USER", "-j", ANCHOR]
    current = [
        shlex.split(line)
        for line in table(runner, binary, "-S", "DOCKER-USER").stdout.splitlines()
        if line.startswith("-A ")
    ]
    if not current or current[0] != expected:
        table(runner, binary, "-I", "DOCKER-USER", "1", "-j", ANCHOR)
        current.insert(0, expected)
    for position in reversed(range(1, len(current))):
        if current[position] == expected:
            table(runner, binary, "-D", "DOCKER-USER", str(position + 1))


def replace_ipset(runner: Runner, feed: FeedData) -> None:
    """Populate a staging hash:net and swap only after the complete restore."""
    base = ["hash:net", "family", "inet", "maxelem", "1048576"]
    runner.run(["ipset", "create", THREAT_SET, *base, "-exist"])
    runner.run(["ipset", "create", TEMP_SET, *base, "-exist"])
    networks = {str(network) for network in (*feed.networks, *BLOCKED_NETWORKS) if network.version == 4}
    payload = "flush " + TEMP_SET + "\n" + "".join("add " + TEMP_SET + " " + network + "\n" for network in sorted(networks))
    runner.run(["ipset", "restore", "-exist"], body=payload)
    runner.run(["ipset", "swap", TEMP_SET, THREAT_SET])
    runner.run(["ipset", "destroy", TEMP_SET])


@contextmanager
def host_lock() -> Iterator[None]:
    directory = Path(os.getenv("FIREWALL_LOCK_DIR", "/run/lock"))
    with (directory / "scraper-docker-firewall.lock").open("a") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield


def apply(
    runner: Runner, interfaces: list[str], endpoints: list[tuple[str, int]], dns_servers: tuple[str, ...] = ()
) -> None:
    """Close bridge egress before refreshing feeds; any error keeps it closed."""
    table(runner, "iptables", "-S", "DOCKER-USER")  # Require Docker's iptables backend.
    table(runner, "iptables", "-C", "FORWARD", "-j", "DOCKER-USER")
    # Commit a closed policy before network operations, including on first install.
    install_policy(runner, "iptables", policy_rules(interfaces, [], closed=True))
    ipv6_chain = table(runner, "ip6tables", "-S", "DOCKER-USER", check=False)
    if ipv6_chain.returncode == 0:
        table(runner, "ip6tables", "-C", "FORWARD", "-j", "DOCKER-USER")
        install_policy(runner, "ip6tables", policy_rules(interfaces, [], ipv6=True))
    else:
        network_ids = runner.run(["docker", "network", "ls", "--filter", "driver=bridge", "--quiet"]).stdout.split()
        networks = json.loads(runner.run(["docker", "network", "inspect", *network_ids]).stdout) if network_ids else []
        if any(network.get("EnableIPv6") for network in networks):
            raise ValueError("IPv6 Docker bridge exists without an enforceable DOCKER-USER hook")
    feed = snapshot("firehol_level1")
    for address, _ in endpoints:
        if feed.matches_ip(ipaddress.IPv4Address(address)):
            raise ValueError("Allowlisted proxy endpoint is blocked by FireHOL; egress remains closed")
    replace_ipset(runner, feed)
    install_policy(runner, "iptables", policy_rules(interfaces, endpoints, dns_servers=dns_servers))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Apply on the Docker host as root; default prints a plan")
    args = parser.parse_args()
    try:
        interfaces, endpoints = interfaces_from_env(), endpoints_from_env()
        resolvers = tuple(dns_servers_from_env())
        if not args.apply:
            print(
                json.dumps(
                    {
                        "mode": "dry-run",
                        "interfaces": interfaces,
                        "endpoints": endpoints,
                        "trusted_dns_servers": resolvers,
                        "ipv4_rules": policy_rules(interfaces, endpoints, dns_servers=resolvers),
                        "ipv6_rules": policy_rules(interfaces, [], ipv6=True),
                        "feed": "firehol_level1",
                        "host_INPUT_OUTPUT_modified": False,
                    },
                    indent=2,
                )
            )
            return 0
        if os.geteuid() != 0:
            raise ValueError("--apply requires root on the Docker host")
        os.environ.setdefault("PROXY_THREAT_CACHE", "/var/cache/scraper-threat-feeds")
        with host_lock():
            apply(Runner(), interfaces, endpoints, resolvers)
        print("Docker bridge egress policy applied; host INPUT/OUTPUT unchanged")
        return 0
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        # Do not leak command output, keys, or endpoint credentials.
        print(
            f"Docker firewall failed ({type(error).__name__}): review configuration/dependencies; "
            "any installed closed gate remains closed",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
