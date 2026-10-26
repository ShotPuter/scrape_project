# SEPREC Gazette Scraper

An async Python scraper for Bolivia's Gaceta Electrónica de Comercio. It pulls publication and company metadata from the SEPREC API, saves the original PDFs, renders a PDF for publications that only have text, and keeps an incremental CSV index so reruns don't start from zero.

> **Proxies.** The scraper rotates free public proxies, and that has a cost: they are slow, they die often, and a run can crawl while the pool is being checked or refilled. If you need steady speed, use a paid proxy service. For a small run (one year, one category) `PROXY_MODE=direct` is usually enough and skips proxies entirely.

## What it does

- Discovers publications and downloads them at the same time, through a bounded in-memory queue. HTTP concurrency and request rate are capped globally. PDF generation and disk work run outside the event loop.
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

You need Docker Engine or Docker Desktop and Compose v2.24.0 or newer. The container runs as UID/GID 1000:1000. On Linux with different IDs, export `SCRAPER_UID=$(id -u)` and `SCRAPER_GID=$(id -g)` before building.

```bash
mkdir -p data
docker compose build scraper

# Direct run
docker compose run --rm -e PROXY_MODE=direct scraper --category 2 --year 2026

# Run with verified proxies
docker compose run --rm scraper

# Only collect proxies
docker compose run --rm scraper --collect-proxies
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
           └─ proxy_collector → two-stage verification → proxies.db
```

1. List categories and page through publication summaries.
2. Compare the IDs with the CSV and check the files already saved.
3. Fetch details for new or damaged publications through a bounded worker queue.
4. Decode and validate the original PDF, or generate one from the text.
5. Save the PDF atomically, then write its row to the CSV right away.

Pydantic models map the API's Spanish field names to English ones. The HTTP layer owns transport, rate limiting and retries. The scraper owns pagination and item budgets. Storage owns file validation and persistence. No browser or Selenium is involved, the scraper talks to the public API directly.

## Proxies

By default `python main.py` expects verified proxies in `data/proxies.db`. The two-stage collector builds that file:

1. It downloads public proxy lists, normalizes them, and makes a real HTTPS request through each candidate.
2. The ones that survive are checked against the same catalog and publication-search endpoints the scraper uses.

Run it with `python main.py --collect-proxies` or the standalone `python collect_proxies.py`. It writes `data/proxy-report.json`, and replaces the database only if at least one proxy passes stage 2.

### What happens at startup

1. The CLI reads only records newer than `PROXY_DB_MAX_AGE_HOURS`. Set it to `0` to turn the freshness filter off.
2. If there are at least `PROXY_MIN_READY` fresh proxies, scraping starts at once.
3. If there are some but fewer, scraping starts with the smaller pool and logs a warning.
4. If there are none, the CLI starts the collector and waits up to `PROXY_STARTUP_TIMEOUT_SECONDS`.
5. When the deadline passes or the collector exits, scraping starts with whatever fresh proxies were published. With none, it continues over a direct connection. The exception is `PROXY_MODE=always`, which fails instead of quietly going direct.
6. The collector is stopped after the scrape and never delays shutdown by more than `PROXY_COLLECTOR_STOP_TIMEOUT_SECONDS`.

Warmup is bounded too, by `PROXY_WARMUP_TIMEOUT_SECONDS`. If the ready threshold isn't reached in time, the client starts with the sessions it has and keeps warming in the background. If none are ready, it goes direct (or raises in `always` mode).

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
src/client/                 HTTP transports, retries, rate limits, proxy pool
src/models/                 Pydantic API models and CSV records
src/services/               Scraper, PDF generation, proxy bootstrap and collection
src/storage/                Atomic PDF files, CSV index, SQLite proxy state
src/utils/                  Logging, validation, filenames, runtime helpers
tests/                      Regression suite
Dockerfile                  Python 3.11 slim runtime image
docker-compose.yml          One-shot scraper service
```