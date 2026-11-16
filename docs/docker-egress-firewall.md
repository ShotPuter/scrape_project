# Proxy threat feeds and Docker bridge egress

This change adds application screening and a separate, explicitly installed
Linux host firewall. It does not install rules on EC2 automatically, suppress
GuardDuty findings, or guarantee that AWS and our feeds contain the same indicators.
Mining infrastructure on TCP 80/443 and unknown compromised proxies remain possible.

## Application barrier

All proxy endpoints and observed exit IPs now pass these gates in both
`strict` and `blocklist_only` modes:

1. The eight incident ranges and non-public addresses.
2. FireHOL Level 1, FireHOL Abusers 30d and NoCoin literal IP indicators.
3. Existing Spamhaus DROP and AbuseIPDB bulk blacklist.
4. In `strict` only, the individual AbuseIPDB verdict, last before connection.

Proxy hostnames are checked against NoCoin before DNS. DNS answers are screened
by IP and a permitted numeric endpoint is pinned. NoCoin domain matches cover
subdomains, not unrelated names containing the same string. Domain names are not
resolved en masse into potentially shared-hosting IP bans.

Sources are downloaded directly over HTTPS, with certificate validation, no
redirects/environment proxy, timeout and size bounds. FireHOL descriptions do not
mean all listed addresses are miners: Level 1 includes bogons and other threat
lists; Abusers 30d contains spam/comment-abuse indicators too.

The proposed CoinBlockerLists URLs returned HTTP errors during validation, so
they are **not** silently installed as required unavailable sources. The working
NoCoin feed supplies both literal mining IPs and host rules. Its comments and
original source text remain in the on-disk cache. This is a conservative host
extractor, not an implementation of every Adblock filter: paths, exceptions,
regexes and rules scoped by `domain=` are not whole-host bans.

```dotenv
PROXY_THREAT_REFRESH_HOURS=6
PROXY_THREAT_MAX_AGE_HOURS=12
# Optional, on a persistent writable data volume:
PROXY_THREAT_CACHE=data/threat-feeds
```

Each required feed has a locked, atomic JSON cache with download timestamp and
SHA-256. Failed refreshes retain a previous snapshot only until maximum age;
missing/empty/malformed/expired snapshots block candidate use. Failed downloads
back off ten minutes. Parsed merged integer intervals use binary search; runtime
checks do not loop over 178,000 subnets. Process memory is revalidated at least
every minute. No individual API calls are needed for these feeds.

## Host barrier: prerequisites and scope

Host dependencies: Linux, **Python 3.11+**, Docker's **iptables backend**,
`iptables`, `ip6tables`, `ipset`, and systemd for persistence. Do not run this in a
container. The helper imports only Python's standard library and this checkout.
Use a trusted, root-owned code installation; a root systemd service must not
execute Python files writable by scraping containers or other untrusted users.

The supported topology is Docker bridge networking with `docker0` / `br-*`.
Custom bridge interface names must be explicitly listed. Host networking,
rootless Docker, macvlan and the native Docker nftables backend are not covered.
Existing Docker FORWARD hooks must reach DOCKER-USER; the script checks this.
It never changes INPUT/OUTPUT or the host's own SSH/SSM rules, and never flushes
DOCKER-USER or unrelated rules. Host INPUT/OUTPUT processes do not pass this
container barrier.

IPv4 rule order, for packets entering from a configured Docker bridge:

1. Return internal bridge-to-bridge forwarding **before** FireHOL bogons.
2. Permit TCP/UDP 53 to exact trusted host IPv4 DNS resolvers (including VPC DNS).
3. Drop destinations in the `scraper_firehol_v4` ipset (plus the eight incident ranges).
4. Return ESTABLISHED,RELATED traffic.
5. Return TCP 53/80/443 and UDP 53.
6. Return exact configured public IPv4:TCP-port exceptions.
7. Rate-limited log, then DROP everything else.

IPv6 Docker egress is blocked, with internal bridge forwarding retained. If the
host has no IPv6 DOCKER-USER chain, the helper verifies that Docker bridge IPv6 is
disabled; enabling IPv6 later requires a supported hook and reapplying the policy.
Do not enable unreviewed network modes/interfaces while relying on this policy.

AWS VPC DNS resolvers commonly use private addresses included in FireHOL bogons.
Without a narrow exception Docker DNS can fail despite port 53 being allowed.
The plan displays exact trusted resolvers auto-detected from host resolv.conf
files, excluding loopback/multicast/unspecified addresses. Override with
`TRUSTED_DNS_SERVERS="..."` (exact IPv4 addresses), or explicitly set an empty
value to disable this exception. Each grant is only TCP/UDP port 53; no whole
private subnet is opened. Incident-blocked DNS addresses are refused. Review
these exceptions and verify actual Docker DNS resolution before deployment.

A proxy allowlist entry cannot bypass FireHOL or the eight ranges. The incident IPs
`8.213.151.128` and `47.245.165.201` from the proposed example are intentionally
rejected. Bright Data port **44445**, ordinary proxies on **3128/1080**, and any
other nonstandard TCP proxy ports stop working until their **exact screened
numeric gateway IP:port** is configured. Hostnames and subnet grants are refused.
The application must select one of these explicitly permitted gateway IPs; DNS
rotation does not grant automatic network access.

## Review and apply on the server

First inspect the non-mutating plan (no root or feed download required):

```bash
bash scripts/setup_docker_firewall.sh
```

Copy `docker/firewall.conf.example` to a root-owned
`/etc/scraper-docker-firewall.conf`. Keep `ALLOWED_PROXY_ENDPOINTS=""` unless exact
screened endpoints are required. Set `FIREWALL_PYTHON` to your host's Python 3.11+
path. Do not add a whole network to avoid maintaining individual gateways.

Example manual application, after reviewing the plan, on the Docker host:

```bash
sudo env ALLOWED_PROXY_ENDPOINTS="" bash scripts/setup_docker_firewall.sh --apply
```

Kernel set names are reserved under `scraper_firehol_*`, so an administrator's
existing `firehol_level1` set (possibly used by host INPUT/OUTPUT rules) is not
modified. The dataset remains FireHOL Level 1 plus the eight incident ranges.

The interactive wrapper takes configuration from its environment; the systemd
units read `/etc/scraper-docker-firewall.conf`. The helper installs a closed
bridge gate before refreshing FireHOL, fills a staging `hash:net` and uses
`ipset swap` only on success. Rules are compiled into an inactive owned chain,
then the anchor is replaced. Unavailable mandatory data leaves the closed gate
in place. After fixing the problem, reapply to reopen permitted traffic.

Reapplying may briefly pause forwarded connections while feeds update. Existing
unlisted established connections are retained by the final policy: stop old
scraping containers before initial deployment if they must be re-evaluated.

## Persistence

The supplied units assume a root-owned checkout at `/opt/scraper_project`.
Adjust BOTH WorkingDirectory and ExecStart to your actual installation before
copying them. Enable only after manual application and verification succeed:

```bash
sudo cp docker/systemd/scraper-docker-firewall.service /etc/systemd/system/
sudo cp docker/systemd/scraper-threat-feeds.service /etc/systemd/system/
sudo cp docker/systemd/scraper-threat-feeds.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now scraper-docker-firewall.service
sudo systemctl enable --now scraper-threat-feeds.timer
```

The timer checks freshness every 30 minutes; download refresh is six hours.
On failures, inspect `journalctl -u scraper-docker-firewall -u scraper-threat-feeds`.
After fixing an initial failure, restart `scraper-docker-firewall.service` before
starting the scraper.

Start scraping only after the firewall service succeeds. For systemd-managed
Compose startup, add `Requires=scraper-docker-firewall.service` and
`After=scraper-docker-firewall.service` to the **application service**. Docker can
otherwise restart containers before an After=docker firewall unit runs, so these
units alone do not promise a zero-window boot for arbitrary restart-policy
containers. Docker restarts propagate to the base firewall unit via PartOf.

## Verification

```bash
sudo iptables -L DOCKER-USER -v -n
sudo iptables -S SCRAPER-EGRESS
sudo iptables -L SCRAPER-EGRESS-A -v -n
sudo iptables -L SCRAPER-EGRESS-B -v -n
sudo ipset list scraper_firehol_v4 -terse
sudo ip6tables -L DOCKER-USER -v -n
```

Only one DOCKER-USER jump should point at SCRAPER-EGRESS. The anchor selects A or
B; the active chain's counters matter. Verify port rules, threat-set drops and
both bridge interface patterns. Reapplying must retain one hook, without deleting
other administrator rules.

Using an **already cached, trusted** curl image (no download with `--pull=never`):

```bash
docker run --rm --pull=never --network bridge curlimages/curl -4 --connect-timeout 5 https://example.com
docker run --rm --pull=never --network bridge curlimages/curl -4 --connect-timeout 5 http://example.com:3333
sudo journalctl -k --since '2 minutes ago' | rg 'SCRAPER-EGRESS DROP'
```

The second command should time out AND increment the active chain's DROP counter
or produce its rate-limited log. A timeout alone is not proof (the remote port
may simply have no listener). Repeat on a Compose bridge as well. These examples
do not contact a mining pool. Keep the existing host SSH/SSM session open during
verification. No real host firewall changes or privileged-container tests were
performed in the local development validation.

Provider references:
- https://docs.docker.com/engine/network/firewall-iptables/
- https://raw.githubusercontent.com/firehol/blocklist-ipsets/master/firehol_level1.netset
- https://raw.githubusercontent.com/firehol/blocklist-ipsets/master/firehol_abusers_30d.netset
- https://github.com/hoshsadiq/adblock-nocoin-list
