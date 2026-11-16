import subprocess
from collections import defaultdict
from unittest.mock import Mock

import pytest

from src.proxy import docker_firewall as firewall, threat_feeds


class FakeRunner:
    def __init__(self):
        self.chains = defaultdict(list)
        self.chains["DOCKER-USER"] = [["-j", "RETURN"]]
        self.commands = []
        self.fail_restore = False

    def run(self, arguments, *, body=None, check=True):
        self.commands.append((arguments, body))
        if arguments[:2] == ["ipset", "restore"] and self.fail_restore:
            raise subprocess.CalledProcessError(1, arguments)
        if arguments[0] not in ("iptables", "ip6tables"):
            return subprocess.CompletedProcess(arguments, 0, "", "")
        operation, chain, *tail = arguments[3:]
        if operation == "-S":
            if chain not in self.chains:
                return subprocess.CompletedProcess(arguments, 1, "", "")
            text = "\n".join("-A " + chain + " " + " ".join(rule) for rule in self.chains[chain])
            return subprocess.CompletedProcess(arguments, 0, text, "")
        if operation in {"-N", "-F"}:
            self.chains[chain] = []
        elif operation == "-A":
            self.chains[chain].append(tail)
        elif operation == "-I":
            self.chains[chain].insert(int(tail[0]) - 1, tail[1:])
        elif operation == "-R":
            self.chains[chain][int(tail[0]) - 1] = tail[1:]
        elif operation == "-D":
            self.chains[chain].pop(int(tail[0]) - 1)
        return subprocess.CompletedProcess(arguments, 0, "", "")


def test_internal_returns_precede_bogon_set_and_threats_precede_established():
    rules = firewall.policy_rules(["docker0"], [("1.1.1.1", 3128)])
    assert rules[0] == ["-i", "docker0", "-o", "docker0", "-j", "RETURN"]
    assert "--match-set" in rules[1] and "--ctstate" in rules[2]
    extra = next(rule for rule in rules if "3128" in rule)
    assert extra[extra.index("-d") + 1] == "1.1.1.1/32"
    assert rules[-2] == ["-i", "docker0", "-j", "DROP"]


def test_ipv6_and_stale_feed_closed_rules_allow_only_internal_forwarding():
    for options in ({"ipv6": True}, {"closed": True}):
        rules = firewall.policy_rules(["docker0", "br-+"], [], **options)
        assert not any("--ctstate" in rule or "--dport" in rule for rule in rules)
        assert sum(rule[-1] == "DROP" for rule in rules) == 2


def test_repeat_install_is_idempotent_and_preserves_unrelated_docker_rules():
    runner = FakeRunner()
    rules = firewall.policy_rules(["docker0"], [])
    for _ in range(3):
        firewall.install_policy(runner, "iptables", rules)
    assert runner.chains["DOCKER-USER"] == [["-j", firewall.ANCHOR], ["-j", "RETURN"]]
    target = runner.chains[firewall.ANCHOR][0][1]
    assert runner.chains[target] == rules
    assert all(
        "INPUT" not in command and "OUTPUT" not in command and "FORWARD" not in command for command, body in runner.commands
    )
    assert not any(command[3:] == ["-F", "DOCKER-USER"] for command, body in runner.commands)


def test_failed_restore_never_swaps_or_flushes_active_set():
    runner = FakeRunner()
    runner.fail_restore = True
    with pytest.raises(subprocess.CalledProcessError):
        firewall.replace_ipset(runner, threat_feeds.parse_feed("firehol_level1", "11.0.0.0/24"))
    assert not any(command[:2] == ["ipset", "swap"] for command, body in runner.commands)
    assert not any(body and "flush " + firewall.THREAT_SET + "\n" in body for command, body in runner.commands)


def test_failed_feed_load_leaves_closed_bridge_gate(monkeypatch):
    runner = FakeRunner()
    monkeypatch.setattr(firewall, "snapshot", Mock(side_effect=threat_feeds.ThreatFeedError("unavailable")))
    with pytest.raises(threat_feeds.ThreatFeedError):
        firewall.apply(runner, ["docker0"], [])
    target = runner.chains[firewall.ANCHOR][0][1]
    assert ["-i", "docker0", "-j", "DROP"] in runner.chains[target]
    assert not any(command[:2] == ["ipset", "swap"] for command, body in runner.commands)


@pytest.mark.parametrize(
    "endpoint",
    [
        "8.213.151.128:3128",
        "47.245.165.201:1080",
        "proxy.example:3128",
        "1.1.1.1:0",
        "1.1.1.1:70000",
        "127.0.0.1:1080",
        "1.1.1.0/24:3128",
    ],
)
def test_unsafe_allowlist_configuration_rejected(monkeypatch, endpoint):
    monkeypatch.setenv("ALLOWED_PROXY_ENDPOINTS", endpoint)
    with pytest.raises(ValueError):
        firewall.endpoints_from_env()


def test_firehol_allowlist_conflict_never_opens_gate(monkeypatch):
    runner = FakeRunner()
    monkeypatch.setattr(firewall, "snapshot", lambda source: threat_feeds.parse_feed(source, "11.0.0.0/24"))
    with pytest.raises(ValueError, match="blocked by FireHOL"):
        firewall.apply(runner, ["docker0"], [("11.0.0.1", 3128)])
    active = runner.chains[firewall.ANCHOR][0][1]
    assert ["-i", "docker0", "-j", "DROP"] in runner.chains[active]
    assert not any("--dports" in rule for rule in runner.chains[active])


def test_apply_never_mutates_host_chains(monkeypatch):
    runner = FakeRunner()
    monkeypatch.setattr(firewall, "snapshot", lambda source: threat_feeds.parse_feed(source, "11.0.0.0/24"))
    firewall.apply(runner, ["docker0"], [])
    for command, _body in runner.commands:
        if command[0] in ("iptables", "ip6tables") and command[3] in ("-A", "-I", "-R", "-D", "-N", "-F"):
            assert command[4] not in ("INPUT", "OUTPUT", "FORWARD")


def test_dry_run_needs_no_firewall_tools_or_root(monkeypatch, capsys):
    import json

    monkeypatch.setattr("sys.argv", ["firewall"])
    monkeypatch.setenv("ALLOWED_PROXY_ENDPOINTS", "")
    monkeypatch.setenv("DOCKER_BRIDGE_INTERFACES", "docker0 br-+")
    monkeypatch.setattr(firewall.subprocess, "run", Mock(side_effect=AssertionError("No commands in dry-run")))
    assert firewall.main() == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["mode"] == "dry-run" and plan["host_INPUT_OUTPUT_modified"] is False


def test_private_vpc_dns_is_allowed_only_on_port_53_before_bogon_set():
    rules = firewall.policy_rules(["docker0"], [], dns_servers=("10.0.0.2",))
    dns_rules = [rule for rule in rules if "-d" in rule]
    assert len(dns_rules) == 2
    assert all(rule[rule.index("-d") + 1] == "10.0.0.2/32" and rule[rule.index("--dport") + 1] == "53" for rule in dns_rules)
    threat_position = next(position for position, rule in enumerate(rules) if "--match-set" in rule)
    assert all(rules.index(rule) < threat_position for rule in dns_rules)


@pytest.mark.parametrize("value", ["10.0.0.2", "1.1.1.1 8.8.8.8"])
def test_exact_dns_configuration(monkeypatch, value):
    monkeypatch.setenv("TRUSTED_DNS_SERVERS", value)
    assert firewall.dns_servers_from_env() == value.split()


@pytest.mark.parametrize("value", ["8.213.1.1", "127.0.0.53", "0.0.0.0", "10.0.0.0/16", "resolver.example"])
def test_bad_dns_grants_rejected(monkeypatch, value):
    monkeypatch.setenv("TRUSTED_DNS_SERVERS", value)
    with pytest.raises(ValueError):
        firewall.dns_servers_from_env()
