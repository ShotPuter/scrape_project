# Passive proxy screening

Public collection is off by default. `config.py` defaults `PROXY_AUTOCOLLECT`
to false; the primary Compose collector is opt-in (`proxy-collection` profile),
no longer a dependency of scraper/stress, and defaults to Webshare. The source
loader additionally refuses public sources unless `ALLOW_PUBLIC_PROXY_COLLECTION=1`.
Docker services explicitly set that switch to 0. Existing cached proxies are
screened again before transport/browser startup; an old database is not trusted.

## API and configuration

AbuseIPDB offers a free Individual tier advertised at 1,000 IP checks/reports
per day. Register and create a key at https://www.abuseipdb.com/account/api.
Review the provider's terms for your use. No paid CAPTCHA or reputation service
is needed for this implementation.

Put these in the applicable `.env`, `.env.res`, or `.env.camoufox`:

```dotenv
ABUSEIPDB_API_KEY=your_private_key
PROXY_AUTOCOLLECT=false
ALLOW_PUBLIC_PROXY_COLLECTION=0
PROXY_REPUTATION_SCORE_LIMIT=25
PROXY_REPUTATION_MAX_REPORTS=0
PROXY_REPUTATION_CACHE_HOURS=6
```

The conservative default refuses score >=25 OR any report in the preceding
30 days. These are configurable policy thresholds, not infection probabilities.
No reports does not prove safety. Missing keys, timeouts, malformed responses,
API quota exhaustion/HTTP errors, and cache errors fail closed. A cached valid
verdict may be used until its expiry; stale verdicts never bypass API failure.
API failures start a shared 60-second backoff so workers do not hammer the API.

The shared SQLite cache defaults to `data/proxy-reputation.sqlite3`. Override
`PROXY_REPUTATION_CACHE` if required. Keep it on a trusted, writable data volume.
Cache scopes use a hash of the key; raw keys and proxy credentials are not stored.
Only IP, score, report count and timestamp are cached, not report comments.

The API call uses the official HTTPS endpoint directly with normal certificate
verification, an API-key header, no redirects, no environment proxy and a bounded
response size. It does not connect to the candidate. Only the candidate IP is
sent to AbuseIPDB, not the proxy password, company data or documents.

## Local block and connection boundary

Always denied before API use or proxy connection:

```
8.211.0.0/16   8.212.0.0/16   8.213.0.0/16   8.220.0.0/16
47.245.0.0/16  47.251.0.0/16  47.252.0.0/16  110.238.0.0/16
```

Private, loopback, link-local and non-global IPs (including IPv4-mapped IPv6)
are refused too. The eight broad prefixes are a precaution explicitly requested
for this incident, not a claim that every address in those ranges is malicious.
Up to eight hostname DNS answers are screened until one is permitted. Blocked
answers are skipped without connection; HTTP proxy transports are pinned to the
permitted numeric IP to prevent a second DNS resolution changing the endpoint.
If none passes, the whole endpoint is denied.
HTTPS proxy hostnames are refused until SNI-aware pinning is implemented; numeric
HTTPS proxies still require normal certificate validation. Production Bright Data
configuration uses HTTP CONNECT to its gateway with TLS to the destination.

The gate is integrated into collector stage 1/2, common HTTP session factories,
RES HTTP/browser flows, PEP Camoufox batch startup and the CONNECT bridge. RES also
checks the observed exit IP before contacting RES. A residential exit IP cannot
be known before connecting to the screened provider gateway: its discovery uses
the existing IP-check request through that gateway. This is an application guard,
not an OS firewall for unrelated programs or arbitrary external code.

## Existing deployments

Changing Compose profiles does **not** stop an already running collector. When
administrators permit work on the server, stop only this project's collector:

```bash
docker compose --profile proxy-collection -f docker-compose.yml stop proxy-collector
```

Do not remove unrelated containers or start a stopped EC2 instance as part of
this change. Rebuild/recreate the relevant application service after configuring
the key; old images do not contain the new screening code. New Docker builds also
include the pending RES daily-limit and GeoIP changes in the current worktree.
The stopped EC2 deployment was not changed from this workspace.

Tests mock the reputation API and assert that blocked addresses never reach a
transport or browser constructor. No live candidate probes are required for
validation. Public reputation providers can disagree with AWS threat intelligence;
this cannot guarantee absence of GuardDuty alerts or rule out compromise.

## Bulk denylist prefilter

Before the individual AbuseIPDB CHECK (and before a cached individual allow),
the gate now checks a local snapshot of Spamhaus Project DROP v4/v6 and the
AbuseIPDB BLACKLIST. A hit blocks the connection without an individual API call.
A miss is **not an allow**: the original CHECK/report-count policy still applies.
The eight incident ranges remain the first gate, ahead of any feed request.

Sources:
- https://www.spamhaus.org/drop/drop_v4.json
- https://www.spamhaus.org/drop/drop_v6.json
- https://api.abuseipdb.com/api/v2/blacklist?limit=10000

Spamhaus Project attribution, date, copyright, terms and original JSONL are
retained in SQLite `blocklist_snapshots`. AbuseIPDB original JSON, including
`meta.generatedAt`, is retained too. The free BLACKLIST contains up to 10,000
high-score entries; no subscriber-only confidence filter is requested. It does
not cover all IPs that would fail the strict individual report-count policy.
Blacklist downloads have their own provider quota; they do not use CHECK calls.

Defaults, also forwarded by Compose:

```dotenv
PROXY_BLOCKLIST_REFRESH_HOURS=24
PROXY_BLOCKLIST_MAX_AGE_HOURS=48
```

The minimum refresh interval is six hours, max age must be at least the refresh
interval and no greater than seven days. Shared SQLite transactions serialize
updates across processes. A failed refresh leaves the previous snapshot intact
and schedules no retry for six hours. A previous snapshot can be used only until
its maximum age. Missing/expired/invalid snapshots fail closed, before candidate
connection or individual CHECK. Feed requests use direct HTTPS, normal TLS,
no environment proxy/redirect, timeout and response-size limits. API keys are
sent only to AbuseIPDB in a header, not to Spamhaus; database scopes store hashes.

Populate snapshots manually (loads `.env.res`, then `.env`, without overriding
existing environment variables), or let the first screened IP populate them:

```bash
.venv/bin/python -m src.proxy.blocklists refresh
.venv/bin/python -m src.proxy.blocklists status
```

`refresh` respects the shared refresh/backoff intervals; it never forces another
provider request. Automatic free-proxy collection remains disabled. Production
preloads only the IP family's DROP file that it needs; manual refresh loads both.
An unavailable feed is visible as `refresh_error` in status. Even a passed
reputation check is not a guarantee against malicious proxies or AWS findings.

## Screening modes

`PROXY_REPUTATION_MODE=strict` is the default. It checks static ranges, then
fresh local DROP / AbuseIPDB BLACKLIST snapshots, then the individual CHECK
verdict (valid cache first). Individual calls are always the last screening step
before connecting, never a preliminary scan of the entire catalog.

Explicit opt-in:

```dotenv
PROXY_REPUTATION_MODE=blocklist_only
RES_CAPTCHA_TIMEOUT=130
```

`blocklist_only` checks static ranges and the same bulk snapshots, with **zero
individual CHECK requests**, including at observed exit-IP checks and browser
startup. It neither reads nor populates the individual-verdict cache and ignores
individual API backoff; changing back to strict requires a strict verdict again.
An absent IP may be probed under this explicit policy; absence is not a clean-IP
verdict. No silent downgrade or fallback to individual CHECK occurs if a bulk
snapshot is missing or expired: connection is denied instead.

The AbuseIPDB key remains necessary for its bulk BLACKLIST. Its download quota
is separate from individual CHECK quota. Default bulk refresh is once per day.
Unknown/invalid mode values deny connections. Compose forwards the mode to all
services using the shared guard; public discovery remains off unless separately
authorized. RES pilot reports record the selected mode.

## Other reputation services reviewed (2026-11-16)

These services were researched, not automatically added to the gate:

- Spamhaus SBL/XBL via official DNS-query service may add signals; the free DQS
  terms restrict eligibility to small non-commercial users and 100,000 queries
  per day. Public-mirror/DQS access differs from DROP's free download license.
  PBL is an email-sending policy list, not evidence that a proxy is malicious.
  https://www.spamhaus.com/terms-of-use-fair-use-policy-for-free-data-query-service/
  https://www.spamhaus.org/blocklists/policy-blocklist/
- MXToolbox's free API currently includes zero complex (blacklist) queries;
  a free web lookup is not a free bulk API.
  https://mxtoolbox.com/c/products/mxtoolboxapi
- GreyNoise Community supports 50 combined API/Visualizer searches per week;
  API keys on the free tier require a business email. Useful for selective
  investigation rather than wholesale catalog screening.
  https://docs.greynoise.io/docs/using-the-greynoise-community-api
- Max Intel is a browser investigation/aggregation tool. A documented supported
  free bulk reputation API was not found. https://maxintel.org/ip.html
- IP-API.io documents a reputation/security API, but published plans start at
  EUR10/month for 100,000 geo-IP requests. Do not assume a free web checker grants
  unrestricted reputation API usage, or treat proxy/VPN/datacenter flags alone
  as evidence of maliciousness. https://ip-api.io/ https://ip-api.io/api-docs.html

## Mandatory threat feeds and Docker egress

FireHOL Level 1, FireHOL Abusers 30d and NoCoin are now checked in both modes,
before existing bulk lists / individual CHECK. Known mining hostnames are denied
before DNS. These feeds are mandatory and have a separate 6h refresh / 12h maximum
age cache. See [host firewall and threat-feed documentation](docker-egress-firewall.md)
for rule ordering, endpoint exceptions, systemd installation, and verification.
The host firewall is supplied for explicit installation; it is not applied by
Compose or by Python proxy validation.
