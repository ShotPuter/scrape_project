# PEP local collection

> Current persistent comparison: [native Chrome / nodriver](pep-stability.md).
> Historical single-document observations: [experiment archive](pep-experiments-history.md).
> Native headed Chrome, including Xvfb, downloaded a document; a fresh native
> headless session was rejected by reCAPTCHA. Earlier headless successes below
> are historical observations, not a reliable recommendation for full runs.

## Recommended automatic mode: HTTP + one native Chrome session

One-command native Chrome mode (requires installed Google Chrome; with
`--headless` no display is required). The pipeline opens and closes its own
dedicated browser automatically:

```bash
python -m src.sites.pep docs/DECLARACIONES_PEP.xlsx --native-chrome --headless --limit 3 --workers 2 --timeout 30
```

### Persistent nodriver fallback (Xvfb, no physical monitor)

For the most stable local download path, use the persistent nodriver backend as
the browser fallback behind HTTP discovery/download. It owns one Chrome profile
and serializes protected file downloads; it does not solve or bypass CAPTCHA.
Install `requirements-browser.txt`, then run as a non-root user with Chrome and
Xvfb installed:

```bash
xvfb-run -a .venv/bin/python -m src.sites.pep docs/DECLARACIONES_PEP.xlsx \
  --browser-engine nodriver --workers 1 --delay 3 --timeout 45
```

The source workbook is read locally and is not re-downloaded. Valid local files
are reused, discovery and each document are checkpointed in SQLite, transient
failures can be resumed, and an explicit CAPTCHA refusal stops further browser
submissions for that run. Use the same output/profile directory to resume. The
default output is `data/pep`; the private persistent profile is
`data/pep/browser-profile`. Do not run two processes against that output/profile
at once. `--browser-engine nodriver` requires `--workers 1`, cannot attach to
CDP, and does not support browser-headless mode (Xvfb provides the display).

The first-party “Download PEP Declarations” bulk report was verified to return
an XLSX export with per-person consultation links, not a ZIP of declaration
PDFs. It is not used to overwrite or re-fetch the input workbook. No public
direct PDF URL or unauthenticated API was found. See
[network investigation](pep-network-analysis.md).

The dedicated profile defaults to `data/pep/browser-profile`; override with
`--browser-profile`. A random local debugging port is allocated for the owned
Chrome process. The user's regular Chrome profile is not touched. Remove
`--limit 3` only after verifying a small batch in your environment.

See [the Network investigation](pep-network-analysis.md) for modern versus
historical download endpoints, headless testing and partial-run diagnostics.
Headless mode was tested with DISPLAY and WAYLAND_DISPLAY removed. It uses a
stable User-Agent matching installed Chrome; the site's edge rejected the
default HeadlessChrome User-Agent. This is not a guarantee against future
CAPTCHA refusals. The computer must remain awake; turning the monitor off is
different from suspending the computer.

HTTP discovery does not need a browser. Downloads of CAPTCHA-protected forms
need the site's native JavaScript. A verified end-to-end run using an ordinary
Chrome connected over local CDP automatically downloaded both PDFs from the
first consultation URL, extracted their ZIPs and completed the SQLite job.
No stealth plugins, CAPTCHA services or user-agent/proxy rotation were used.

Start a dedicated Chrome (do not expose the debugging port to the network):

```bash
google-chrome --remote-debugging-port=9223 --remote-debugging-address=127.0.0.1 --user-data-dir="$PWD/data/pep/native-chrome" --no-first-run --no-default-browser-check about:blank
```

In another terminal, first run a small batch:

```bash
python -m src.sites.pep docs/DECLARACIONES_PEP.xlsx --cdp-url http://127.0.0.1:9223 --limit 3 --workers 2 --timeout 40
```

Remove `--limit 3` for the whole workbook. Before HTTP jobs start, the pipeline
checks TLS once and reuses `<output>/ca-bundle.pem` if available. If the server
chain fails verification and no explicit `--ca-bundle` was supplied, it fetches
the source's known missing Sectigo intermediate from the authority's published
AIA location, validates it using OpenSSL against existing trusted roots, then
atomically saves a bundle and repeats the HTTPS check with verification enabled.
OpenSSL must be installed. An untrusted certificate is never installed; failure
aborts startup without modifying source jobs. An explicitly supplied CA bundle
is never silently replaced. `--import-only` does not contact the network.

The HTTP queue is bounded and shares one rate limiter. Only one browser page
is used for protected documents, serialized and reused for the next document
on the same page. Previously completed discovery is reused for partial jobs.
Every PDF is checked on resume; validated local files do not need a browser.
The browser starts/connects only if a missing PDF requires CAPTCHA. The
pipeline closes only its own page when attached via CDP, leaving the user's
browser running. Keep the dedicated profile private and do not share it.

Alternatively, let Playwright launch a dedicated persistent Chrome profile:

```bash
python -m src.sites.pep docs/DECLARACIONES_PEP.xlsx --hybrid --headed --limit 1
```

This is distinct from the verified externally started Chrome/CDP session;
availability depends on the site accepting that session. No mode guarantees
future CAPTCHA acceptance. Three consecutive browser failures disable further
browser attempts for the run. TLS/HTTP-403 discovery failures also stop further
queued work, leaving unattempted jobs pending rather than sending thousands
of doomed requests. Incomplete documents have explicit statuses and failure
stage diagnostics. Retry after fixing the cause; do not respond to blocks by
increasing concurrency.

The earlier ambiguous browser selector has been fixed: the portal has two
forms with the same declaration ID (asset and tax), so selecting by ID alone
matched both. The downloader now scopes the ID to the asset-report form.

This command is separate from the existing SEPREC CLI. It imports the second
sheet of a manually downloaded workbook, keeps every input row and collects
Ley 2013 results by unique consultation URL. Data is private; do not commit
the workbook, SQLite state, JSON or downloaded documents.

Install the dependencies (Chromium is not needed for default HTTP discovery):

```bash
python -m pip install -r requirements.txt
```

First verify the input and export all rows without contacting the site:

```bash
python -m src.sites.pep docs/DECLARACIONES_PEP.xlsx --import-only
```

Try a single unique link before attempting a larger run:

```bash
python -m src.sites.pep docs/DECLARACIONES_PEP.xlsx --limit 1 --workers 1
```

The default full pipeline reads the existing workbook (never downloads Excel),
discovers declarations and attempts local PDF downloads. To only discover:

```bash
python -m src.sites.pep docs/DECLARACIONES_PEP.xlsx --discover-only --limit 1
```

HTTP collection uses asynchronous `curl_cffi` with a stable Chrome profile,
Spanish language headers, a shared session, two workers and a global one-second
request interval. Transient HTTP responses (429 and selected 5xx) receive up to
three attempts with bounded backoff. Forbidden responses are not retried.
The queue is bounded; SQLite jobs are read in batches, not all queued in memory.
No proxy or user-agent rotation is performed. Redirects are not followed, and
only same-host HTTPS consultation and pagination URLs are accepted.

Successful discovery-only jobs have status `discovered`, not `completed`; documents have
status `discovered`, `local_path: null` and `s3_uri: null`. No PDF is downloaded
in discovery-only mode. Subsequent discovery-only runs skip successfully discovered jobs.
Partial discoveries are checkpointed and retried; existing document records
are retained, including previously downloaded files.

Full collection reuses discovered job metadata, validates existing local PDFs
and repairs damaged/missing files when downloading is possible. Downloads are
stream-read with a 50 MiB application limit and bounded by the worker count;
only PDF header/EOF-validated content is atomically saved. File metadata includes
SHA-256 and byte size. Each finished document is checkpointed in SQLite.
`records.json` preserves all source rows; `summary.json` gives job status counts.
No S3 writes occur.

Forms that explicitly require a CAPTCHA token are marked `captcha_required`
without submitting a fake token. Their job stays `partial`, not `completed`.
For forms not requiring CAPTCHA, the HTTP downloader submits the document ID;
redirects, HTTP errors and non-PDF responses are never accepted as a PDF.
`--download-browser` switches to the existing browser collection path for
normal site interactions. It is not a CAPTCHA bypass or a guaranteed solution.

## Verified live behavior (2026-11-02)

The source omits its Sectigo RSA **Organization** Validation intermediate and
serves a different intermediate. Downloading the correct intermediate from its
AIA location and verifying it against system trusted roots fixes HTTP TLS
validation. A verified local bundle is saved in `data/pep/ca-bundle.pem` (not
committed). Use `--ca-bundle data/pep/ca-bundle.pem` for HTTP collection.

Clicking the native declaration buttons in the connected desktop browser
successfully generated downloads after executing the site's JavaScript and
reCAPTCHA. The response is **REPORTE.zip**, not a standalone PDF. Its contents
include `DECLARACION_CONFLICTO_INTERES.pdf` and `FORMULARIO.xlsx`. Despite its
filename, the inspected PDF's heading describes an asset/income and conflict
of interest declaration under Ley 2013. Two real PDFs from the first person's
link are saved in `data/pep/pdf/`, with archives under `data/pep/archives/` and
metadata in `data/pep/records.json`.

The downloader now supports PDF and ZIP responses. Archives are retained,
all PDF members are validated and saved using generated filenames (never ZIP
member paths), and `files` lists each PDF. Entry count and uncompressed byte
limits protect against archive expansion. The initial browser success was in
the connected browser MCP, not proof of unattended local Playwright success.
To test an installed Chrome with the local browser workflow:

```bash
python -m src.sites.pep docs/DECLARACIONES_PEP.xlsx --download-browser --browser-channel chrome --headed --limit 1 --workers 1
```

TLS verification is always enabled. If the source's certificate chain cannot
be verified, jobs are marked `discovery_failed_tls`. Supply a trusted PEM CA
bundle with `--ca-bundle /path/to/trusted-ca-bundle.pem` if appropriate; obtain
certificates from a trusted authority, not from an unverified connection.

Browser downloading remains an explicit opt-in and has not been proven to
work against the live site's CAPTCHA. To attempt it:

```bash
python -m playwright install chromium
python -m src.sites.pep docs/DECLARACIONES_PEP.xlsx --limit 1 --workers 1 --download-browser --headed
```

Outputs are under `data/pep/`: `state.sqlite3` tracks jobs, `records.json`
contains every source row and associated results, and `pdf/` holds validated
local PDFs. The JSON is written atomically, with `s3_uri: null`; no AWS calls
are made. Re-running retries incomplete jobs and reuses valid downloaded PDFs.
Use a new output directory for a different workbook. `--limit` limits unique
links processed in a run, not the rows included in the JSON. Browser mode uses
a one-second delay after downloads and between pages. Increase concurrency
only with permission and respect for the source's limits.

The browser clicks the original download button and does not bypass CAPTCHA.
TLS certificate verification is enabled. A 403, TLS failure, CAPTCHA failure,
unrecognised page or unsupported download is reported as incomplete; rerunning
does not guarantee the site will permit automated downloads. A zero-document
result is only accepted when the site explicitly reports no declarations.
Exit code 1 means one or more attempted jobs remain incomplete. Pending jobs
are normal in a limited or import-only run. Interruptions can leave the JSON
behind the SQLite state; rerun the command to regenerate it.

The portal calls the download a Ley 2013 declaration, which may include more
than asset declarations. Year semantics and the relation of each declaration
to each office period have not yet been verified. `publication_year` is only
the year of publication; it must not be interpreted as the year covered by a
declaration. Do not treat a completed job as evidence of a complete set for
every year of office until these issues have been checked against real PDFs.
