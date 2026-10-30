# SEPREC Gazette Scraper

An async Python scraper for Bolivia's Gaceta Electrónica de Comercio. It pulls publication and company metadata from the SEPREC API, saves the original PDFs, renders a PDF for publications that only have text, and keeps an incremental CSV index so reruns don't start from zero.

> **Proxies.** The scraper rotates free public proxies, and that has a cost: they are slow, they die often, and a run can crawl while the pool is being checked or refilled. If you need steady speed, use a paid proxy service. For a small run (one year, one category) `PROXY_MODE=direct` is usually enough and skips proxies entirely.

Set `WEBSHARE_API_TOKEN` in `.env` to use Webshare as the primary proxy provider
without a manual export. Verified public proxies remain a reserve for private
proxy failures or provider API outages. See [Webshare setup](docs/server.md#webshare-primary-proxies).

## What it does

- Discovers publications and downloads them at the same time, through a bounded in-memory queue. Scraping requests share HTTP concurrency and rate limits. Readiness probes share that concurrency cap but bypass pacing; the standalone collector has a separate budget. PDF generation and disk work run outside the event loop.
- Writes each PDF to a temporary file next to the target, calls `fsync`, then `os.replace`. A crash mid-write never leaves a half-written PDF under the final name.
- Skips publications already in the CSV if the saved file exists, matches the recorded size, and has a PDF header and EOF marker. Missing or damaged files are fetched again.
- When the original PDF is missing or fails validation, builds one with ReportLab from the publication text and company metadata. The CSV status tells them apart: `DOWNLOADED` for originals, `GENERATED_PDF` for generated ones.
- Handles flaky networks with timeouts, bounded retries with backoff and jitter, `Retry-After`, proxy rotation, and two transports (`aiohttp`, with `curl_cffi` as a fallback).
- Ignores verified proxies older than `PROXY_DB_MAX_AGE_HOURS`, so a stale database doesn't get reused forever.
- Puts a deadline on proxy warmup and on waiting for the collector. A slow or broken proxy collection can't hang the run.

## Quick start

Python 3.11 or newer.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

# Small run, direct connection, no proxy collection
PROXY_MODE=direct python main.py --category 2 --year 2026

# Incremental run over all categories, using the verified proxy database
python main.py

# Build or refresh the proxy database and exit
python main.py --collect-proxies
```

You don't need any configuration to start. To change defaults, copy `.env.example` to `.env`. Relative paths are resolved from the project root, so it works the same from cron, systemd, Docker or an IDE.

### Docker Compose

For detached overnight collection, an eight-hour stress test, limits, shutdown
and result inspection, see [the server runbook](docs/server.md). Copy
`.env.server.example` to `.env` on a fresh server.

You need Docker Engine or Docker Desktop and Compose v2.24.0 or newer. The container runs as UID/GID 1000:1000. On Linux with different IDs, export `SCRAPER_UID=$(id -u)` and `SCRAPER_GID=$(id -g)` before building.

```bash
mkdir -p data
docker compose build scraper

# Direct run
docker compose run --rm --no-deps -e PROXY_MODE=direct scraper --category 2 --year 2026

# Run with verified proxies
docker compose run --rm scraper

# Only collect proxies
docker compose run --rm --no-deps scraper --collect-proxies
```

PDFs, `publications.csv`, the proxy database and logs end up in `./data`. The image is `python:3.11-slim` with runtime dependencies only and a non-root user. Compose also sets a read-only root filesystem, a writable `/tmp`, dropped capabilities, an init process and capped Docker logs.

## CLI

```bash
python main.py                      # incremental scrape, all categories
python main.py --year 2025          # only 2025
python main.py --category 1         # only INSTRUMENTOS
python main.py --category 2 --year 2026
python main.py --collect-proxies    # build/refresh data/proxies.db and exit
```

| Option | What it does |
| --- | --- |
| `--year YYYY` | Limits collection to 1 January – 31 December of that year |
| `--category all/1/2/3/4` | All, INSTRUMENTOS, CONVOCATORIAS, MEMORIAS, BALANCES |
| `--collect-proxies` | Runs the two-stage proxy collector and exits |

Routing, transports, timeouts, concurrency and proxy settings come from environment variables (see `.env.example`). With `--year`, the scraper scans the whole window and does not use the early stop that unfiltered incremental runs rely on.

Exit codes: `0` for a finished run (even if some items failed), `1` for a failed category run, a fatal startup error or a failed proxy collection, `2` for bad CLI arguments, `130` for Ctrl+C.

## How it works

```text
main.py → CLI
           ├─ SeprecScraper
           │    ├─ SeprecClient → HTTP transport → SEPREC API
           │    ├─ FileStorage → original PDF / ReportLab fallback
           │    └─ PublicationCsv → metadata + incremental checkpoint
           ├─ proxy_bootstrap → freshness check + collector lifecycle
           └─ proxy collector application → src/proxy → two-stage verification → proxies.db
```

1. List categories and page through publication summaries.
2. Compare the IDs with the CSV and check the files already saved.
3. Fetch details for new or damaged publications through a bounded worker queue.
4. Decode and validate the original PDF, or generate one from the text.
5. Save the PDF atomically, then write its row to the CSV right away.

Pydantic models map the API's Spanish field names to English ones. The HTTP layer owns transport, rate limiting and retries. The scraper owns pagination and item budgets. Storage owns file validation and persistence. No browser or Selenium is involved, the scraper talks to the public API directly.

## Proxy package

`src/proxy` owns candidate sources, verification, the SQLite proxy repository and
the in-memory rotation pool. Its core imports without loading application settings
or publication models. `src/network` provides the transports and network budgets
used by both the scraper and the proxy checks.

The application constructs a `CollectorConfig` and a `TargetProbe` and passes them
to `src.proxy.collector.collect`. A target profile contains HTTP requests, payloads
and JSON validators. The SEPREC profile lives in
`src/services/seprec_proxy_probe.py`; the generic checker does not know its API
paths or publication schema. Pool warmup likewise receives its response validator
from the caller.

The standalone SEPREC collector can now also be invoked as a module:

```bash
python -m src.proxy --help
python -m src.proxy --provider public
python -m src.proxy --list-sources
python -m src.proxy --sources monosans,relayglass,proxyscrape --watch
```

`python collect_proxies.py` and `python main.py --collect-proxies` remain supported.
Environment settings are mapped at the application boundary in
`src/services/proxy_collector.py`. Previous public module paths under
`src/client` and `src/storage/proxy_database.py` remain compatibility facades;
new integrations should use `src/proxy` and `src/network`.

The SQLite schema and routing policies are preserved. With a configured database,
the HTTP client reloads fresh verified records every `PROXY_DB_REFRESH_SECONDS`
(default 30; 0 disables reload). Records verified by the collector within
`PROXY_VERIFIED_REUSE_SECONDS` (default 300) enter rotation without another
readiness request. Older candidates are checked in the background.
Removed proxies leave rotation immediately; sessions already leased to requests
close after those requests finish. A runtime exclusion is reset only after a
newer verification is published. A client that starts with no usable proxies can
switch from direct requests to the pool when records appear. `direct` mode does
not watch the database, and `always` still requires usable proxies at startup.

### Source registry and cache

`--provider public` selects ProxyScrape, HProxy, Proxifly, monosans, Relayglass,
Thordata and VPSLab. `--provider all` enables every registered source plus the
optional `free-proxy` package. `--sources name,name` selects an explicit subset;
`PROXY_SOURCES` provides the default selection when no individual provider is
requested. Unknown names fail before downloads begin.

| Name | Input | Cache refresh interval (minutes) |
| --- | --- | --- |
| `proxyscrape` | JSON | 5 |
| `hproxy` | JSON | 30 |
| `proxifly` | JSON | 5 |
| `monosans` | JSON | 60 |
| `relayglass` | JSON | 5 |
| `thordata` | JSON | 360 |
| `vpslab` | TXT by protocol | 15 |
| `thespeedx` | TXT by protocol | 1440 |
| `jetkai` | Advanced JSON | 60 |
| `iplocate` | TXT with protocol prefixes | 30 |
| `gfpcom` | HTTP/SOCKS TXT only | 60 |
| `dpangestuw` | TXT by protocol | 5 |
| `rix4uni` | JSON with protocol flags | 10 |

These are local cache intervals, not an availability guarantee. Large reserve
lists and jetkai are opt-in; upstream timestamps still pass through the age
filter. Provider parsers preserve protocol shards, convert monosans seconds to
milliseconds, expand rix4uni/jetkai protocol entries and distinguish HTTP CONNECT
capability from TLS to the proxy. Only HTTP and SOCKS transports are admitted.

Downloads and parsing are bounded by `PROXY_SOURCE_DOWNLOAD_CONCURRENCY` (3),
`PROXY_SOURCE_MAX_RESPONSE_BYTES` (32 MiB per file, decompressed) and
`PROXY_SOURCE_MAX_RECORDS` (10,000 candidates per file). Metadata filters and the
existing total/per-source verification caps apply afterwards. The record limit
keeps the first records of each file; it does not rank an unlimited upstream list.
JSON decoding and cache I/O run outside the event loop.

Validated source files are cached under `PROXY_CATALOG_CACHE_DIR`. Refresh uses
ETag/Last-Modified when available. An unavailable or invalid source can reuse its
last valid cache for at most `PROXY_SOURCE_CACHE_MAX_AGE_HOURS` (24 hours); a failed
download does not extend that age. Failures are reported per source/protocol and
do not prevent other sources from contributing. An unsuccessful verification
scan preserves the existing proxy database. `PROXY_LIST_PATH` remains available
as an explicit legacy CSV input when `--source '' --provider iplocate` is used.

### Independent collector

Run the collector and scraper in separate terminals:

```bash
LOG_FILE=./data/proxy-collector.log python -m src.proxy --watch --interval 300
PROXY_AUTOCOLLECT=false python main.py --category 2 --year 2026
```

Watch mode waits the configured interval after each scan, so scans never overlap.
It retries failed scans, respects each source's cache interval and publishes each
batch of `PROXY_MIN_READY` eligible proxies (20 by default). Stage 1 feeds its
survivors immediately into stage 2 through a bounded queue; neither stage waits
for the entire candidate list. Fixed worker pools limit the number of tasks,
including when candidate caps are disabled. Each full batch is upserted into
SQLite while both stages continue; the remaining partial batch is included in
the final snapshot when the scan completes. Ctrl+C stops it
with exit code 130. With `PROXY_AUTOCOLLECT=false`, the scraper waits for the
external collector up to the normal startup deadline and does not spawn or stop
that process. Each process still has its own SEPREC request budget.

Compose provides an optional collector service with a separate log file:

```bash
docker compose --profile proxies up -d --build proxy-collector
docker compose run --rm -e PROXY_AUTOCOLLECT=false scraper --category 2 --year 2026
docker compose --profile proxies stop proxy-collector
```

## Proxies

### Download stress test without storing PDFs

Keep the collector running independently, then run a two-hour download test:

```bash
PROXY_AUTOCOLLECT=false PROXY_MODE=always DOWNLOAD_WORKERS=30 \
python main.py --year 2022 --stress-minutes 120 \
  --stress-report data/stress-2022.json
```

`--stress-minutes` enables discard mode. Each PDF response is downloaded in full,
decoded/validated or generated with the normal document path, then released without
writing a PDF or the publication CSV. The test repeats the selected year/categories
after a full pass until the deadline. Repeated downloads count as separate checks.
`--discard-files` alone performs one pass. Identifiers are retained for deduplication
within a pass, then cleared; publication text and PDF payloads are not kept in the
test index. Logs still use the pipeline's usual saved-file counters for documents
successfully processed in memory. This does not exercise filesystem write durability.

For 30-minute snapshots, use a dedicated run directory for the stress report,
proxy database and logs, then start a monitor alongside the test:

```bash
python -m src.services.stress_monitor --run-dir data/my-stress-run
```

The monitor reads `stress-report.json`, `proxies.db` and `scraper.log` in that
directory. It writes `checkpoints.jsonl`, `checkpoints.log` and
`latest-checkpoint.json`, including cumulative/window rates, failures, RSS and
API response-body bytes. Collection and TLS/header traffic are excluded. It
polls every ten seconds; snapshot accuracy is limited by the stress report's
update interval. It writes a final snapshot when the report becomes terminal.
This creates local reports, not chat notifications.

The duration starts after proxy/client initialization. A JSON report is updated
every `METRICS_INTERVAL_SECONDS` and on normal completion, deadline or Ctrl+C. It
contains checked documents/bytes, failures, throughput, HTTP timing percentiles,
and current RSS on Linux. Periodic logs retain the memory/speed history. The main
CSV is neither read nor modified. SIGKILL cannot produce a final report.

`DOWNLOAD_WORKERS` controls document processing workers independently of the shared
HTTP cap (`MAX_CONCURRENT_REQUESTS`). Start by comparing the same input with 15 and
30 workers while keeping the HTTP cap/rate unchanged. At the default 2 HTTP request
starts/second the theoretical ceiling is 120 requests/minute, including search,
retries and probes; adding workers cannot remove that ceiling. Larger queues also
cannot raise this ceiling. Document requests carry whole base64 PDF responses in
JSON, so increasing workers raises memory demand.

### Provider adapters and private proxy lists

Proxy HTTP 429 responses, including readiness probes, exclude the offending URL
until at least its `Retry-After` deadline and pause the shared client budget for
`RATE_LIMIT_PAUSE_SECONDS` (150 seconds by default). Fresh database receipts do not
clear an active rate-limit quarantine. If three distinct URLs receive 429 within
ten minutes, the client suspects a shared limit and uses the longer server delay.
Direct-only requests retain server-directed backoff. Quarantines are local to the
running pool; they are not a persistent blacklist.

Every registered public provider has its own metadata adapter under
`src/proxy/sources/adapters/`. Download caching, candidate normalization, filtering,
the Stage 1/Stage 2 pipeline and database publication remain shared.

For a paid provider exporting static addresses or gateway URLs, create a private
`data/paid-proxies.txt` containing one `http://user:password@host:port` or SOCKS URL
per line (percent-encode special credential characters). Then run:

```bash
PROXY_PAID_FILE=data/paid-proxies.txt python -m src.proxy \
  --sources paid --publish-after 20 --watch --interval 300
```

The `paid` source is explicit and is not included in public `all`. It uses the same
verification pipeline, deduplicates addresses, and stores working credentials in
the local SQLite database. JSON reports redact URL credentials. Private files and
databases must stay in the ignored `data/` directory. Provider-specific API keys,
session rotation and automatic renewal require an adapter for the selected vendor.
For a single rotating gateway, the existing per-host limit still applies; one
gateway URL does not automatically represent many independently leasable proxies.

By default `python main.py` expects verified proxies in `data/proxies.db`. The two-stage collector builds that file:

1. It downloads public proxy lists, normalizes them, and makes a real HTTPS request through each candidate.
2. The survivors make one small publication-search request to SEPREC (page size 10), and the JSON response is validated.

Each stage stops at its first successful transport. If the preferred transport
fails, the other requested compatible transport is tried. Only the transport and
User-Agent that passed stage 2 are stored. By
default stage 2 makes one attempt per transport, with temporary failures eligible
for another scan in watch mode. Explicit `--stage2-attempts` or
`PROXY_STAGE2_ATTEMPTS` can enable immediate retries. If an existing `.env` still
sets that value to 3, change it to 1 to use the reduced retry policy.

On the normal successful path this is one external HTTPS request and one SEPREC
search request. Scraper startup and database reload do not repeat the SEPREC
request for a recent verification. A runtime failure prevents reuse of that same
receipt; recovery requires a real readiness check or a newer collector result.
The stage-2 rate limit counts individual HTTP requests, including custom profiles
with multiple requests. Collector and scraper processes retain separate budgets.

### User-Agent fallback

The collector uses [fake-useragent](https://pypi.org/project/fake-useragent/) to
select real desktop User-Agent strings from the installed package's bundled
dataset. It does not fetch headers over the network or guarantee the newest
browser versions. Generated headers use the browser family selected by
`BROWSER_PROFILE`; this changes the HTTP header, while `curl_cffi` still controls
the TLS/browser impersonation profile.

Stage 1 uses one header per transport. Stage 2 first tries the header that passed
stage 1. After HTTP 403, invalid JSON or an unexpected response shape, it tries
another distinct header, stopping at the first success. A timeout, proxy failure,
429 or server error does not trigger a User-Agent matrix. Each transport tries
at most `PROXY_USER_AGENT_LIMIT` headers (default 3, allowed range 1–10); the
existing HTTP rate limiter applies to every attempt. Explicit retry settings can
increase the requests made with each header.

An explicit `USER_AGENT` is tried first. Setting the limit to 1 disables header
fallback. Generation runs outside the event loop and has a bounded number of
draws; if the dataset is unavailable, an existing/configured header or a static
fallback is used. The successful header is written to SQLite and the report and
reused by scraper sessions, rather than changing on every document request.

```bash
python -m src.proxy --provider all --watch --user-agent-limit 3
```

Run it with `python main.py --collect-proxies` or the standalone `python collect_proxies.py`. It writes `data/proxy-report.json`, and replaces the database only if at least one proxy passes stage 2.

Early publication is enabled for one-shot and watch runs alike. `--publish-after N`
sets the batch size (default `PROXY_MIN_READY`): batches are committed repeatedly,
so an early batch of 5 can later grow to the scraper's startup threshold of 20
without waiting for the full scan. Intermediate batches upsert new records and
preserve other available proxies. The final successful snapshot replaces the
previous scan. Verification timestamps remain tied to the actual check time.

`eligible proxies` in stage-2 progress counts only distinct proxies meeting
`PROXY_STAGE2_MIN_SUCCESSES` and `PROXY_STAGE2_MIN_SUCCESS_RATE`. A `Published ...`
message confirms the commit and includes the absolute database path. Both the
collector and scraper print their paths so differing `--db` / `PROXY_DB_PATH`
settings are visible. An in-memory success counter alone does not confirm a
database write.

### What happens at startup

1. The CLI reads only records newer than `PROXY_DB_MAX_AGE_HOURS`. Set it to `0` to turn the freshness filter off.
2. If there are at least `PROXY_MIN_READY` fresh proxies, scraping starts at once.
3. If there are some but fewer, scraping starts with the smaller pool and logs a warning.
4. If there are none, the CLI starts the collector and waits up to `PROXY_STARTUP_TIMEOUT_SECONDS`.
5. When the deadline passes or the collector exits, scraping starts with whatever fresh proxies were published. With none, it continues over a direct connection. The exception is `PROXY_MODE=always`, which fails instead of quietly going direct.
6. The collector is stopped after the scrape and never delays shutdown by more than `PROXY_COLLECTOR_STOP_TIMEOUT_SECONDS`.

Recently verified database records are available immediately, including when the
pool is below `PROXY_MIN_READY`; older records can warm in the background.
`PROXY_VERIFIED_REUSE_SECONDS=0` disables this shortcut. Legacy CSV/TXT inputs do
not carry collector receipts and still require warmup unless `PROXY_WARMUP=false`.
When no recent receipts are available, warmup is bounded by
`PROXY_WARMUP_TIMEOUT_SECONDS`. If the ready threshold isn't reached in time, the
client starts with the sessions it has and keeps warming in the background. If
none are ready, it goes direct (or raises in `always` mode).

`PROXY_MODE` values:

- `pool` (default): proxies first, direct only when the pool is empty.
- `fallback`: direct first, proxies after a block.
- `always`: every request goes through a proxy.
- `direct`: no proxies.

## Configuration and output

Settings come from environment variables or an optional `.env` file, and invalid values stop the run at startup. The full list is in [.env.example](.env.example).

| Variable | Default | Purpose |
| --- | --- | --- |
| `SEPREC_API_BASE_URL` | `https://servicios.seprec.gob.bo/api` | API root |
| `MAX_CONCURRENT_REQUESTS` | `15` | Shared HTTP cap |
| `HTTP_REQUESTS_PER_SECOND` | `2` | Shared request start rate |
| `REQUEST_TIMEOUT_SECONDS` / `DETAIL_TIMEOUT_SECONDS` | `30` / `90` | Total timeout for API calls / document downloads |
| `RETRY_ATTEMPTS` | `3` | Max attempts per request |
| `PAGE_SIZE` | `50` | Summaries per API page (minimum 10) |
| `PROXY_MODE` | `pool` | Routing policy |
| `PROXY_MIN_READY` | `20` | Proxy readiness threshold |
| `PROXY_DB_MAX_AGE_HOURS` | `24` | Ignore older verified records |
| `PROXY_VERIFIED_REUSE_SECONDS` | `300` | Skip redundant checks for recent collector results; 0 disables reuse |
| `PROXY_STAGE2_ATTEMPTS` | `1` | Attempts per stage-2 transport before moving on |
| `PROXY_USER_AGENT_LIMIT` | `3` | Maximum header variants per transport after a block; 1 disables fallback |
| `PROXY_WARMUP_TIMEOUT_SECONDS` | `90` | Max wait for warmup at startup |
| `PROXY_STARTUP_TIMEOUT_SECONDS` | `180` | Max wait for a fresh database |
| `PROXY_COLLECTOR_STOP_TIMEOUT_SECONDS` | `5` | Grace period when stopping the collector |
| `PDF_STORAGE_DIR` | `./data/pdf` | Where PDFs go |
| `CSV_EXPORT_PATH` | `./data/publications.csv` | Metadata and incremental index |
| `LOG_LEVEL` / `LOG_TO_FILE` | `INFO` / `true` | Log verbosity and rotating file output |

Compose pins the output paths under `/app/data` so they stay on the bind mount.

```text
data/
├── pdf/YYYY/MM/<COMPANY>_<REGISTRATION>_<PUBLICATION_ID>.pdf
├── publications.csv
├── scraper.log
├── proxies.db                # verified proxies
└── proxy-report.json         # collector report
```

Each CSV row has the publication ID, code, title, date, category, company name, current and former registration numbers, an original/generated flag, file path, size, status and timestamps. The CSV stores absolute PDF paths, so keep the storage location the same between incremental runs.

Back up the PDFs and the CSV together. For a large backfill, go one year and one category at a time, estimate disk use from a small sample, and watch free space. The integrity check looks at size and the PDF envelope; it does not parse the PDF structure. Items that failed are retried when pagination reaches them again.

## Development

```bash
python -m pip install -r requirements-dev.txt
python -m pytest
python -m ruff check .
```

The tests cover the models, HTTP behavior against local servers, retries, storage and PDF validation, incremental collection, item budgets, proxy freshness and lifecycle, CLI validation and startup timeouts. They never touch the live SEPREC API.

Container checks:

```bash
docker compose config --quiet
docker compose build scraper
docker compose run --rm scraper --help
docker compose run --rm --no-deps \
  -v "$(pwd)/tests/docker_smoke.py:/smoke.py:ro" \
  -e PYTHONPATH=/app --entrypoint python scraper /smoke.py
```

The smoke test writes only to a fresh `data/smoke-*` directory and leaves it there for you to look at.

## Layout

```text
main.py                     Entry point
config.py                   Settings from the environment, with validation
collect_proxies.py          Standalone proxy collector entry point
src/cli.py                  Argument parsing and run lifecycle
src/client/                 HTTP request orchestration and SEPREC API client
src/network/                Shared transports, sessions, budgets and telemetry
src/proxy/                  Proxy sources, checks, configuration, repository and pool
src/models/                 Pydantic API models and CSV records
src/services/               Scraper, PDF generation, proxy bootstrap and SEPREC collector wiring
src/storage/                Atomic PDF files, CSV index, proxy repository compatibility facade
src/utils/                  Logging, validation, filenames, runtime helpers
tests/                      Regression suite
Dockerfile                  Python 3.11 slim runtime image
docker-compose.yml          One-shot scraper service
```
