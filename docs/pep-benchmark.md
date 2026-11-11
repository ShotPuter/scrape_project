# Local PEP benchmark: Webshare + Telegram

## Server without a monitor: 50 links and diagnostic logs

With installed Google Chrome, Xvfb, xvfb-run, xauth and the project dependencies:

```bash
bash scripts/run_pep_50.sh
```

The launcher uses one native Chrome under Xvfb, direct networking, the first
50 unique consultation links, a two-second global pacing interval, 40-second
operation timeouts and a 60-minute worker budget. It removes the physical
desktop's display/Wayland environment. It does not enable Chrome headless mode.
For a previously verified CA bundle, set `PEP_CA_BUNDLE=/absolute/path/bundle.pem`.
For another worker budget, set `PEP_MAX_MINUTES=120`.

The default directory is `data/pep-benchmark-50-xvfb`. Repeat the command to resume
that sample; do not delete its database or browser profile. A different workbook
and directory may be provided as positional arguments. The launcher preserves
the benchmark's exit code even when console output is piped through `tee`.

Outputs:

* `console-<timestamp>-<pid>.log`: unbuffered console output for this invocation.
* `xvfb-<timestamp>-<pid>.log`: virtual display startup/errors; an empty file alone
  does not prove that every browser action succeeded.
* `events.jsonl`: append-only events across invocations, grouped by `run_id`.
  Includes job/discovery/document start and completion, timing, browser attempts,
  HTTP statuses, failure stages, quarantine reasons and sanitized exception types.
  Links and document IDs use hash references, not source URLs or personal names.
* `benchmark.json`: latest summary, including `jobs`, `pending_links`,
  `completed_links_percent`, `failure_stages`, `rejection_reasons` and throughput.
* `run-<id>.json`, `state.sqlite3`, `records.json`, validated PDFs and manifests:
  final reports, resume state and private source-level details.

```bash
tail -f data/pep-benchmark-50-xvfb/events.jsonl
python3 -m json.tool data/pep-benchmark-50-xvfb/benchmark.json
```

Interpretation:

| Evidence | Meaning |
| --- | --- |
| `discovery_failed_http_403` | Source rejected HTTP discovery |
| `discovery_failed_network` + `error_type` / `curl_code` | HTTP transport failure; codes 6/7/28 indicate DNS/connect/timeout |
| `browser_warmup_http_403` | Browser's initial landing-page visit was forbidden |
| `browser_rejected_http_302` + `site_verification_notice` | Site explicitly refused reCAPTCHA; not a display crash |
| `browser_session_blocked` | A later document was not submitted because the browser breaker was already open |
| `browser_download_timeout` | No successful download or explicit HTTP refusal arrived within the operation budget |
| `browser_failed_tls` | Browser certificate verification failed |
| `download_failed_http_404` | Supplied historical document URL was not found |

Neither a raw 302 nor a generic timeout proves the exact reCAPTCHA score. Tokens,
cookies and arbitrary exception messages are not logged. The score cannot be
recovered without access to the target server's verification result.

### Refusal observed in run `03a16fef7f99`

HTTP discovery succeeded and found two documents. The first native form POST
received 302 plus an explicit verification notice. The second document was
marked `browser_session_blocked` without submission; 49 links stayed pending.
TLS was repaired successfully. This was an incomplete run, not 50 failed links.

A code difference from the successful browser probe was identified: the probe
visited the public landing page in Chrome before the consultation, whereas the
benchmark previously visited it only through its separate HTTP client.
The benchmark now performs that initial navigation in the **same browser**,
once per browser lifetime, lazily when a missing document requires Chrome.
`browser_warmup_http_status` is recorded per document and
`browser_warmup_enabled` identifies the updated benchmark configuration.
This aligns the flows; it is not proof that the missing visit caused Google's
refusal, nor a guarantee that the next run will be accepted. Explicit verification
refusals still stop the session; no forced retries or proxy rotation were added.

Run in tmux or a managed service if disconnecting SSH. Do not start another
process against the same output directory. A complete 50-link result requires
`status: COMPLETED` and `completed_links: 50`, not merely a high download ratio
among the documents that happened to be discovered.

## Run

The existing `.env` is loaded using the project's configuration. Required:

```dotenv
WEBSHARE_API_TOKEN=...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

Do not put tokens or proxy credentials on the command line. The benchmark
uses Webshare's authenticated direct proxy inventory. Up to two distinct
proxy addresses are selected by default, with a stable address and separate
persistent browser profile per worker. There is no direct download fallback,
free-proxy collection, or automatic rotation after a verification refusal.
The certificate bootstrap can contact the source directly to establish the
verified CA bundle; source discovery/download workers then use their proxies.

```bash
.venv/bin/python -m src.sites.pep.benchmark docs/DECLARACIONES_PEP.xlsx \
  --count 100 --workers 2 --notify-interval 120 --max-minutes 60
```

Installed Google Chrome and the existing Python dependencies are required.
Workers run headless: no active monitor/display is required, but the computer
must not suspend. The browser's authenticated proxy is configured through
Playwright, not a credential-bearing process command line. Both HTTP and
browser traffic of each worker use its assigned proxy. HTTPS verification
remains enabled; the TLS bootstrap/repair from the regular PEP command is reused.

If Webshare sessions are rejected, compare a single direct visible browser
before attempting a larger pool:

```bash
.venv/bin/python -m src.sites.pep.benchmark docs/DECLARACIONES_PEP.xlsx \
  --network direct --headed --count 1 --workers 1 \
  --output data/pep-benchmark-direct-visible-1 --max-minutes 3 \
  --ca-bundle data/pep-benchmark-100/ca-bundle.pem
```

Direct mode does not fetch Webshare inventory and uses one native Chrome
session. Omit `--headed` for a headless comparison, using another output
directory. Changing `--network` in an already tagged benchmark directory is
rejected to avoid mixing proxy and direct results. None of these settings
guarantees acceptance of reCAPTCHA; do not retry a rejected 100-link run as
a way to make the site's verification disappear.

`--ca-bundle` reuses a previously validated bundle, avoiding a new certificate
authority download when comparing output directories. Startup TLS/provider
errors have sanitized `failure_details` in `benchmark.json` and a console
message; exception text from arbitrary proxy/network libraries is not logged.

Telegram is only enabled if both named settings are populated in the current
environment or `.env`. An old Telegram setup in another directory/service is
not automatically available here. A space after a line-continuation backslash
breaks a multi-line shell command: the backslash must be the last character.

The sample is **100 unique consultation links**, not 100 Excel rows or PDFs.
The first N links in workbook order are saved as a stable selection in SQLite;
one link may contain many documents or no documents. Repeated source rows are
preserved. Output is separate from the normal collection:

```text
data/pep-benchmark-100/
  benchmark.json       latest live/final metrics
  run-<id>.json        final metrics for each invocation
  state.sqlite3       source rows, fixed sample, checkpoints
  records.json        all workbook rows with known documents and pending states
  pdf/                verified PDFs
  archives/           original ZIP responses
  manifests/          per-document file hashes and sizes
  profiles/           private persistent worker profiles
```

Telegram and console report start, progress every 120 seconds by default, and
the final state (including interruptions and errors). Messages include finished
links/target, complete links, links with errors, downloaded/discovered documents,
new downloads, unresolved documents, PDF count, elapsed time and throughput.
Missing Telegram credentials disable only Telegram. Delivery failures do not
stop collection and do not expose the token in logs.

## Resume and compare

Repeat exactly the same command to resume the sample. Existing files are
validated and counted as reused rather than new downloads. `benchmark.json`
counts discovered document IDs once even if the same document appears under
multiple source rows. Per-run speed counts **new** downloads only, not cached
files. Each document is checkpointed before the next one is processed.

For an independent cold test, use a new directory:

```bash
.venv/bin/python -m src.sites.pep.benchmark docs/DECLARACIONES_PEP.xlsx \
  --output data/pep-benchmark-cold-2 --count 100 --workers 2
```

Do not change the workbook or sample size in an existing output directory.
A file lock rejects concurrent runs using the same directory/profile.

## Failure and completeness semantics

- `COMPLETED`: every selected link completed discovery and all its discovered
  PDFs are validated locally. This still does not prove publication of every
  declaration for every year of office.
- `INCOMPLETE`: item errors or quarantined/unavailable workers left gaps.
- `TIME_LIMIT`: the worker phase reached `--max-minutes`; checkpoints remain.
- `INTERRUPTED`: cancellation/Ctrl+C; checkpoints remain.
- `FAILED`: a startup/runtime failure such as unavailable Webshare inventory.

Explicit CAPTCHA refusals quarantine that browser session rather than changing
its IP and repeatedly submitting. The other independent worker can continue;
if no workers remain, unattempted sample links stay pending. Network/TLS/provider
failures do not become successful empty results. Known missing legacy PDFs are
reported with HTTP statuses. There is no promise that all 100 links will pass
site verification. `coverage_note` reminds that failed discovery may conceal
documents: downloaded/discovered alone is not proof of full coverage.

Exit codes: 0 for `COMPLETED`, 1 for incomplete/failed/time-limited results,
130 for Ctrl+C. Report files, workbooks, credentials and profiles must not be
committed. The benchmark has no S3 operations.
