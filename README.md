# SEPREC Gazette Scraper

An asynchronous Python scraper for Bolivia's **Gaceta Electrónica de Comercio**.
It collects publication and company metadata from the SEPREC API, saves original
PDFs, renders text-only publications as PDFs, and maintains an incremental CSV index.

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
- **Fresh proxy database:** verified proxies older than `PROXY_DB_MAX_AGE_HOURS`
  are ignored instead of being reused indefinitely.
- **Bounded startup:** proxy warming and collector waits always have a deadline.
  A failed or slow proxy collection cannot hang the scraper forever.

## Quick start

Requires Python 3.11 or later.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

# Small direct-connection run (no proxy collection)
PROXY_MODE=direct python main.py --category 2 --year 2026

# Incremental run over every category using the verified proxy database
python main.py

# Collect or refresh the proxy database without scraping
python main.py --collect-proxies
```

Configuration is optional: copy `.env.example` to `.env` to customize defaults.
Relative paths are resolved against the project root, so the scraper can be started
from any working directory (cron, systemd, Docker, IDE, ...).

### Docker Compose

Requires Docker Engine / Docker Desktop and Compose **v2.24.0+**. The container runs
as UID/GID **1000:1000** by default. On Linux, if your account uses different IDs,
export `SCRAPER_UID=$(id -u)` and `SCRAPER_GID=$(id -g)` before building.

```bash
mkdir -p data
docker compose build scraper

# Direct run
docker compose run --rm -e PROXY_MODE=direct scraper --category 2 --year 2026

# Verified-proxy run
docker compose run --rm scraper

# Proxy collection only
docker compose run --rm scraper --collect-proxies
```

PDFs, `publications.csv`, the proxy database, and logs remain in **`./data`**.
The image uses `python:3.11-slim`, runtime dependencies only, and a non-root user.
Compose adds a read-only root filesystem, writable `/tmp`, dropped capabilities,
an init process, and bounded Docker logs.

## CLI

The command line intentionally exposes only the workflows this project needs.

```bash
python main.py                      # incremental scrape of all categories
python main.py --year 2025          # only publications from 2025
python main.py --category 1         # only INSTRUMENTOS
python main.py --category 2 --year 2026
python main.py --collect-proxies    # build/refresh data/proxies.db and exit
```

| Option | Behavior |
| --- | --- |
| `--year YYYY` | Restrict collection to January 1–December 31 of that year |
| `--category all/1/2/3/4` | All / INSTRUMENTOS / CONVOCATORIAS / MEMORIAS / BALANCES |
| `--collect-proxies` | Run the two-stage proxy collector and exit |

Routing, transports, timeouts, concurrency, and proxy tuning come from environment
variables (see `.env.example`). Already saved intact items are skipped during an
incremental run. A year filter scans the selected window and disables the early
stop used for unfiltered incremental runs.

Exit codes: **0** for completed runs (including partial item errors), **1** for a
failed category run, fatal startup error, or failed proxy collection, **2** for
invalid CLI arguments, and **130** for a keyboard interruption.

## Architecture

```text
main.py → CLI
           ├─ SeprecScraper
           │    ├─ SeprecClient → HTTP transport → SEPREC API
           │    ├─ FileStorage → original PDF / ReportLab fallback
           │    └─ PublicationCsv → metadata + incremental checkpoint
           ├─ proxy_bootstrap → freshness check + bounded collector lifecycle
           └─ proxy_collector → two-stage verification → proxies.db
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

## Verified proxy database

By default `python main.py` expects verified proxies in `data/proxies.db`.
The database is produced by the two-stage collector:

1. download public proxy lists, normalise metadata, and make a real HTTPS request
   through each candidate;
2. check survivors against the exact catalog and publication-search endpoints
   used by the scraper.

Run the collector explicitly with `python main.py --collect-proxies` (or the
standalone `python collect_proxies.py`). It writes `data/proxy-report.json` and
only replaces the database when at least one proxy passes stage 2.

### Startup flow

1. The CLI reads only records newer than `PROXY_DB_MAX_AGE_HOURS`
   (set it to `0` to disable the freshness filter deliberately).
2. If enough fresh proxies exist (`PROXY_MIN_READY`), scraping starts immediately.
3. If some fresh proxies exist, scraping starts with that smaller pool and logs a
   warning.
4. If none exist, the CLI starts the collector and waits at most
   `PROXY_STARTUP_TIMEOUT_SECONDS`.
5. After the deadline (or if the collector exits), scraping starts with whatever
   fresh proxies were published. With none, the run continues directly, except in
   `PROXY_MODE=always`, where it fails instead of silently bypassing proxies.
6. The collector is always terminated after the scrape; it never blocks shutdown
   for longer than `PROXY_COLLECTOR_STOP_TIMEOUT_SECONDS`.

Proxy warming itself is bounded by `PROXY_WARMUP_TIMEOUT_SECONDS`. If the warmup
threshold is not reached in time, the client starts with the ready sessions and
continues warming in the background; if none are ready it falls back to direct
requests (or raises in `always` mode).

`PROXY_MODE` values:

- `pool` (default): proxies first, direct only when the pool is empty;
- `fallback`: direct first, proxies after a block;
- `always`: every request must use a proxy;
- `direct`: never use proxies.

## Configuration and output

Settings come from environment variables or an optional `.env` file. Invalid
settings fail at startup. See [.env.example](.env.example) for the complete list.

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
| `PROXY_DB_MAX_AGE_HOURS` | `24` | Ignore older verified records |
| `PROXY_WARMUP_TIMEOUT_SECONDS` | `90` | Maximum startup warmup wait |
| `PROXY_STARTUP_TIMEOUT_SECONDS` | `180` | Maximum wait for a fresh database |
| `PROXY_COLLECTOR_STOP_TIMEOUT_SECONDS` | `5` | Collector shutdown grace period |
| `PDF_STORAGE_DIR` | `./data/pdf` | PDF destination |
| `CSV_EXPORT_PATH` | `./data/publications.csv` | Metadata and incremental index |
| `LOG_LEVEL` / `LOG_TO_FILE` | `INFO` / `true` | Log verbosity and rotating file output |

Compose fixes output paths under `/app/data` so they remain on the bind mount.

```text
data/
├── pdf/YYYY/MM/<COMPANY>_<REGISTRATION>_<PUBLICATION_ID>.pdf
├── publications.csv
├── scraper.log
├── proxies.db                # verified proxy database
└── proxy-report.json         # collector report
```

CSV rows include publication ID, code, title, date, category, company name,
current/former registration numbers, original/generated flag, file path, size,
status, and timestamps. The CSV stores absolute PDF paths: keep the same storage
location for incremental runs.

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
validation, incremental collection, item budgets, proxy freshness and lifecycle,
CLI validation, and startup timeouts. No live SEPREC connection is needed.

Container startup and smoke checks:

```bash
docker compose config --quiet
docker compose build scraper
docker compose run --rm scraper --help
docker compose run --rm --no-deps \
  -v "$(pwd)/tests/docker_smoke.py:/smoke.py:ro" \
  -e PYTHONPATH=/app --entrypoint python scraper /smoke.py
```

The smoke test writes only to a unique `data/smoke-*` directory and leaves its
artifacts for inspection.

## Project layout

```text
main.py                     Thin executable entry point
config.py                   Environment-based settings and validation
collect_proxies.py          Standalone verified-proxy collector entry point
src/cli.py                  Minimal argument parsing and run lifecycle
src/client/                 HTTP transports, retries, rate limits, proxy pool
src/models/                 Pydantic API models and CSV records
src/services/               Scraper, PDF generation, proxy bootstrap/collection
src/storage/                Atomic PDF files, CSV index, SQLite proxy state
src/utils/                  Logging, validation, filenames, runtime helpers
tests/                      Automated regression suite
Dockerfile                  Python 3.11 Slim runtime image
docker-compose.yml          One-shot scraper service
```
