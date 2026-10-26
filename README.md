# SEPREC Gazette Scraper

An asynchronous Python scraper for Bolivia's **Gaceta Electrónica de Comercio**.
It collects publication and company metadata from the SEPREC API, saves original
PDFs, renders text-only publications as PDFs, and maintains an incremental CSV index.
Run it once for a backfill or keep it running on a recurring schedule.

## Features

- **Bounded asynchronous collection:** discovery and downloads overlap through an
  in-memory queue, with a shared HTTP concurrency cap and request rate limit.
  Blocking PDF generation and storage work runs outside the event loop.
- **Atomic PDF persistence:** write a temporary sibling, flush and `fsync`, then
  `os.replace` the destination. Interrupted writes do not expose a partial final PDF.
- **Incremental collection without duplicate records:** publication IDs in the CSV
  are skipped when the saved PDF exists, matches its recorded size, and has a PDF
  header and EOF marker. Missing or damaged files are collected again.
- **Automatic PDF generation:** ReportLab renders HTML/text and company metadata
  when an original PDF is absent or fails envelope validation. CSV statuses distinguish
  `DOWNLOADED` originals from `GENERATED_PDF` documents.
- **Resilient networking:** timeouts, bounded retries with backoff and jitter,
  `Retry-After` handling, proxy rotation, and `aiohttp` / `curl_cffi` transports.
- **Operational visibility:** JSON run reports, console and rotating file logs,
  offline storage audits, and an optional recurring scheduler.

## Quick start: three commands

Run these commands from the repository root. Start with a small batch before a
historical backfill. These examples explicitly use a direct connection, bypassing
proxy collection; if SEPREC blocks your IP, use the proxy workflow below.

### Local virtual environment

Requires Python 3.11 or later. On Linux/macOS:

```bash
python3.11 -m venv .venv
source .venv/bin/activate && python -m pip install -r requirements.txt
python main.py --proxy-mode direct --category 2 --year 2026 --max-items 5
```

On Windows, activate with `.venv\Scripts\Activate.ps1` in PowerShell, then install
with `python -m pip install -r requirements.txt` and run the same CLI command.
Configuration is optional: copy `.env.example` to `.env` to customize defaults.

### Docker Compose

Requires Docker Engine / Docker Desktop and Compose **v2.24.0+** (optional `env_file`
support). The container runs as UID/GID **1000:1000** by default. On Linux, if your
account uses different IDs, export `SCRAPER_UID=$(id -u)` and
`SCRAPER_GID=$(id -g)` before building. The existing `data` directory and its files
must be writable by those IDs; a bind mount preserves host permissions.

```bash
mkdir -p data
docker compose build scraper
docker compose run --rm scraper --proxy-mode direct --category 2 --year 2026 --max-items 5
```

PDFs, `publications.csv`, proxy state, and logs remain in **`./data`** after the
container exits or is removed. The image uses `python:3.11-slim`, runtime dependencies
only, and a non-root user. Compose adds a read-only root filesystem, writable `/tmp`,
dropped capabilities, an init process, and bounded Docker logs. No ports are exposed.

## Architecture

```text
main.py → CLI / Scheduler → SeprecScraper
                              ├─ SeprecClient → HTTP transport → SEPREC API
                              ├─ FileStorage → original PDF / ReportLab fallback
                              └─ PublicationCsv → metadata + incremental checkpoint
proxy_tool.py → public candidates → target check → working_proxies.txt
collect_proxies.py → two-stage verification → proxies.db
```

1. Discover categories and paginate publication summaries.
2. Compare IDs with the CSV and verify the corresponding saved files.
3. Fetch new or damaged publication details using a bounded worker queue.
4. Decode and validate the original PDF, or generate a PDF from available text.
5. Save the PDF atomically, then persist its metadata to the CSV immediately.

Pydantic models map the API's Spanish fields to English application fields.
The HTTP layer owns transport, rate limiting, and retries; orchestration owns
pagination and item budgets; storage owns file validation and persistence.
The scraper uses the public API directly and requires no browser or Selenium.

## CLI examples

Examples assume the virtual environment is active. `--proxy-mode direct` skips the
automatic proxy collector. Omit it to use the default verified database workflow.

```bash
# Discover all CLI options
python main.py --help

# Instruments for one calendar year
python main.py --proxy-mode direct --category 1 --year 2025

# Announcements: at most 10 attempted new publications
python main.py --proxy-mode direct --category 2 --max-items 10

# Annual reports and balance sheets
python main.py --proxy-mode direct --category 3 --year 2024 --max-items 20
python main.py --proxy-mode direct --category 4 --year 2025 --max-items 20

# Specific sub-category (requires a single category)
python main.py --proxy-mode direct --category 1 --sub-category 1003 --year 2025 --max-items 5

# At most 3 pages per category, 25 attempted items across all categories
python main.py --proxy-mode direct --category all --max-pages 3 --max-items 25 --concurrency 5

# Re-fetch and overwrite a limited batch, ignoring the incremental checkpoint
python main.py --proxy-mode direct --category 2 --max-items 5 --full

# Offline, read-only integrity report; no API requests
python main.py --audit-storage --audit-output data/storage-audit.json
```

| Option | Behavior |
| --- | --- |
| `--category all/1/2/3/4` | All categories / INSTRUMENTOS / CONVOCATORIAS / MEMORIAS / BALANCES |
| `--year YYYY` | Restrict collection to January 1–December 31 of that year |
| `--max-items N` | Limit attempted new items across categories; failed items also consume the budget |
| `--max-pages N` | Limit API pages per category |
| `--concurrency N` | Shared HTTP concurrency cap; default 15, independent of the rate limit |
| `--transport aiohttp/curl_cffi/auto` | Select HTTP transport policy |
| `--full` | Disable incremental skipping and overwrite existing documents |
| `--schedule` | Run immediately, then repeat after each configured interval |
| `--interval-hours N` | Wait N hours after each completed scheduled job; default 24 |

Already saved intact items do not consume `--max-items`. For an unfiltered
incremental run, three consecutive fully processed pages stop discovery early.
A year filter disables that early stop and scans the selected window.

For Docker, replace `python main.py` with `docker compose run --rm scraper`:

```bash
docker compose run --rm scraper --proxy-mode direct --category 1 --year 2025 --max-items 10
docker compose run --rm scraper --audit-storage
```

Exit codes: **0** for completed runs (including partial item errors), **1** for a
failed category run, fatal startup error, or detected audit problems, **2** for
invalid CLI arguments, and **130** for a keyboard interruption. Inspect JSON
`status` and `items_failed` when partial failures matter.

## Free proxies with proxy_tool.py

The utility collects public HTTP proxy candidates, checks them against the SEPREC
catalog endpoint, and writes responsive endpoints to `data/working_proxies.txt`.
No account or paid proxy subscription is required.

```bash
# Discover and check up to 150 candidates per public source, plus local candidates
python proxy_tool.py update --limit 150

# Show the number of saved endpoints (does not perform a network check)
python proxy_tool.py status

# Recheck the saved list and remove failed endpoints
python proxy_tool.py check

# Use exactly this list; disable upstream refresh to preserve it
python main.py --proxy-file data/working_proxies.txt --no-proxy-source \
  --proxy-mode pool --category 2 --max-items 5
```

`update`, `check`, and `status` accept `--file PATH`. `update` and `check` return
1 when no working proxies remain and write the resulting list, which may be empty.
Free proxies can disappear between verification and download; re-run `update` if
the pool becomes unusable. This utility's catalog check disables certificate
verification; scraper readiness checks still apply their own TLS and API checks.
A utility success does not guarantee a usable scraper session.

The explicit `--proxy-file` skips automatic database collection.
`--no-proxy-source` prevents the default upstream refresh from replacing that file.
`pool` prefers proxies with a direct fallback when no usable session is available;
use `--proxy-mode always` when every request must use a proxy, or `fallback` to try
direct requests first and switch after a block.

Run the same workflow inside the built image by overriding its scraper entrypoint:

```bash
docker compose run --rm --entrypoint python scraper proxy_tool.py update --limit 150
docker compose run --rm --entrypoint python scraper proxy_tool.py status
docker compose run --rm --entrypoint python scraper proxy_tool.py check
docker compose run --rm scraper --proxy-file data/working_proxies.txt \
  --no-proxy-source --proxy-mode pool --category 2 --max-items 5
```

### Automatic verified database workflow

With no proxy overrides, `python main.py` starts `collect_proxies.py` in a separate
process and waits for **20 distinct verified proxies** (`PROXY_MIN_READY`). The
collector checks HTTPS connectivity, then the target catalog and search API.
Successful `(proxy URL, user agent, transport)` combinations are stored in SQLite.
Collection and scraping overlap once the readiness threshold is reached; a run
also waits for the collector to finish before exiting. This can take considerably
longer than the limited direct-connection examples.

If the readiness threshold is not reached, startup fails. The collector writes
`data/proxy-report.json`; a scan with zero verified proxies preserves the existing
database. To reuse a previously collected database without another refresh:

```bash
python main.py --no-refresh-proxies --proxy-db data/proxies.db --max-items 5
```

`--proxy-db` takes precedence over `--proxy-file`. Keep proxy state in `data` so it
survives container replacement. See `.env.example` for collector settings.

## One-shot jobs and scheduled operation

The default image command runs **once**. Compose's `scraper` service has no restart
policy. An opt-in `scheduler` service runs with `--schedule` and restarts unless
explicitly stopped:

```bash
# Start only the recurring service (default proxy/database workflow)
docker compose --profile scheduler up -d scheduler

# Follow logs
docker compose logs -f scheduler

# Stop and remove the service; ./data is retained
docker compose --profile scheduler down
```

To change the schedule interval, set `SCHEDULE_INTERVAL_HOURS=12` in `.env`.
To run the daemon directly without Compose:

```bash
docker run -d --name gaceta-scheduler --restart unless-stopped --init \
  --mount type=bind,source="$(pwd)/data",target=/app/data \
  gaceta-scraper:local --schedule --interval-hours 12
```

Locally: `python main.py --schedule --interval-hours 24`. Each job completes before
the next interval begins; job failures are logged and the scheduler continues.
SIGINT/SIGTERM request a stop after the current job. Compose allows two minutes
before forced termination; increase `stop_grace_period` for long backfills or
proxy collection. Atomic writes and the CSV allow later incremental recovery.

**Run one writer per data directory.** Stop the scheduler before running a one-shot
job against the same CSV. The CSV lock protects tasks within a process; there is
no cross-process writer lock. Use separate data directories for parallel jobs.

There is no HTTP health endpoint. A version command only checks that Python can
start, so it is deliberately not used as a container health check. Monitor job
reports, logs, and the freshness of successful CSV entries.

## Configuration and output

Settings come from environment variables or an optional `.env` file. CLI flags
override their corresponding settings. See [.env.example](.env.example) for the
complete list. Invalid settings fail at startup.

| Variable | Default | Purpose |
| --- | --- | --- |
| `SEPREC_API_BASE_URL` | `https://servicios.seprec.gob.bo/api` | API root |
| `MAX_CONCURRENT_REQUESTS` | `15` | Shared HTTP cap |
| `HTTP_REQUESTS_PER_SECOND` | `2` | Shared request start rate |
| `REQUEST_TIMEOUT_SECONDS` / `DETAIL_TIMEOUT_SECONDS` | `30` / `90` | API / document total timeout |
| `RETRY_ATTEMPTS` | `3` | Maximum attempts per request |
| `PAGE_SIZE` | `50` | Summaries per API page (minimum 10) |
| `PROXY_MODE` | `pool` | Routing policy |
| `PROXY_MIN_READY` | `20` | Proxy readiness threshold |
| `SCHEDULE_INTERVAL_HOURS` | `24` | Delay after each scheduled job |
| `PDF_STORAGE_DIR` | `./data/pdf` | PDF destination |
| `CSV_EXPORT_PATH` | `./data/publications.csv` | Metadata and incremental index |
| `LOG_LEVEL` / `LOG_TO_FILE` | `INFO` / `true` | Log verbosity and rotating file output |

Compose fixes output paths under `/app/data` so they remain on the bind mount.
Use relative `data/...` paths in CLI proxy arguments inside a container. Do not
put host-specific absolute paths into container settings.

```text
data/
├── pdf/YYYY/MM/<COMPANY>_<REGISTRATION>_<PUBLICATION_ID>.pdf
├── publications.csv
├── scraper.log
├── working_proxies.txt       # proxy_tool.py workflow
├── proxies.db               # automatic collector workflow
└── proxy-report.json
```

CSV rows include publication ID, code, title, date, category, company name,
current/former registration numbers, original/generated flag, file path, size,
status, and timestamps. The CSV stores absolute PDF paths: keep the same storage
location for incremental runs. Moving a dataset between local and Docker paths
requires reconciling those paths, otherwise the files are treated as missing.

Back up PDFs and CSV together. For large backfills, collect one year and category
at a time, estimate disk usage from a small sample, and monitor available space.
Integrity checks validate size and the PDF envelope, rather than fully parsing
PDF structure. Failed items are retried when future pagination encounters them.

## Development and validation

```bash
python -m pip install -r requirements-dev.txt
python -m pytest
python -m ruff check .
```

Tests cover models, HTTP behavior with local servers, retries, storage and PDF
validation, incremental collection, item budgets, proxy handling, CLI validation,
and scheduler lifecycle. No live SEPREC connection is needed for the test suite.

Container startup and offline audit checks:

```bash
docker compose config --quiet
docker compose build scraper
docker compose run --rm scraper --help
docker compose run --rm --entrypoint python scraper proxy_tool.py --help
docker compose run --rm scraper --audit-storage
```

For a repeatable acceptance test without external network access, run the local
API fixture inside the production container. It checks original and generated
PDFs, persisted CSV rows, incremental deduplication, the storage audit, non-root
execution, the read-only application directory, and scheduler shutdown on SIGTERM:

```bash
docker compose run --rm --no-deps \
  -v "$(pwd)/tests/docker_smoke.py:/smoke.py:ro" \
  -e PYTHONPATH=/app --entrypoint python scraper /smoke.py
```

The test writes only to a unique `data/smoke-*` directory and leaves its artifacts
for inspection. Stop any scheduler before running validation against its volume.

An audit of an empty directory checks zero records; use the limited live scrape
from Quick start to verify network access and actual PDF output. An API 403 or
Cloudflare challenge can require another network route; changing transport alone
does not solve interactive challenges.

## Project layout

```text
main.py                     Thin executable entry point
config.py                   Environment-based settings and validation
proxy_tool.py               Simple free-proxy update/check/status CLI
collect_proxies.py          Verified SQLite proxy collector entry point
src/cli.py                  Argument parsing and run lifecycle
src/client/                 HTTP transports, retries, rate limits, proxy pool
src/models/                 Pydantic API models and CSV records
src/services/               Scraper, PDF generation, scheduler, proxy verification
src/storage/                Atomic PDF files, CSV index, SQLite proxy state, audit
src/proxy/                  Simple proxy discovery and checking utilities
src/utils/                  Logging, validation, filenames, runtime helpers
tests/                      Automated regression suite
Dockerfile                  Python 3.11 Slim runtime image
docker-compose.yml          One-shot and scheduled services
```
