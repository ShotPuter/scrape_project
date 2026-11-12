# Experimental RES audio pilot

This is a one-CVE experiment with local Whisper recognition. Production collectors
continue using their existing CAPTCHA handler. No account or paid CAPTCHA API is
needed; Webshare or Bright Data credentials and an available proxy are required.

Use a separate **Python 3.12+** environment, with Google Chrome and Xvfb installed:

```bash
python3.12 -m venv data/res-audio-venv
data/res-audio-venv/bin/python -m pip install -r requirements-res-audio-pilot.txt
HF_HUB_DISABLE_XET=1 data/res-audio-venv/bin/python -m scripts.res_audio_pilot \
  --engine botonomus --proxy-host 107.174.128.95 --cve ACuKDHhqsvAd
```

The first run downloads Whisper `base` to `data/res-audio-models`. Recognition runs
on CPU in a worker thread. Model downloads and the Webshare control API use normal
network access; **all RES, Google challenge and audio traffic uses the browser proxy**.
There is no direct fallback. This pilot checks one endpoint from the first 100
Webshare inventory records, requires its egress IP to match, and checks it again
after the attempt. It does not rotate the IP within a CAPTCHA session.

Botonomus 0.4.1 is used with its **Patchright driver**, not the native CDP driver:
the native API does not expose the frame/download APIs needed by this pilot.
The browser runs on its own virtual X display, so no physical monitor is needed.
For comparison, `--engine patchright` uses our existing launcher directly.
`--engine camoufox` requires the additional Camoufox dependencies and browser cache.

Audio is fetched inside the Google iframe using the browser's session, with a
20-second timeout and a 2 MiB size check. The downloaded recording stays in memory.
The solver attempts at most three rounds, stops if Google declines audio, and
accepts only a checked anchor. The normal RES verifier response and validated PDF
download are still required for `status=downloaded`.

Each run saves `report.json`, a screenshot and HTML beneath a new timestamped
`data/res-audio-pilot/` directory. Diagnostics may contain session information;
keep them private. Successfully downloaded PDFs retain the existing manifest and
SHA-256 validation. This experiment does not modify the main collection database.

The audio solver is an independent implementation inspired by the approach in
[recaptcha-v2-audio-solver](https://github.com/saifyxpro/recaptcha-v2-audio-solver).
It does not copy that repository's Selenium implementation. Audio availability and
acceptance on RES are not guaranteed; an unavailable or declined task remains a
CAPTCHA failure, never a completed download.

## Local trial, 2026-11-12

`data/res-audio-pilot/20261112T121406Z/report.json` records a real Botonomus 0.4.1
run with its Patchright adapter, Chrome on Xvfb, and Webshare `107.174.128.95`.
The RES form returned HTTP 200. Google declined the audio challenge with a
"try again later / automated queries" message before providing a recording.
Whisper therefore did not receive a live audio sample and no PDF was downloaded.
Browser egress was the same before and after. The screenshot and frame text confirm
the refusal; this is not a recognition failure or a completed CAPTCHA.

The host's `/usr/bin/python3.12` also failed a minimal `asyncio.run()` check and
could not import `ctypes` because of an undefined symbol. An isolated CPython
3.12.16 installed under `data/res-python/` resolved these environment failures.
The working test environment is `data/res-audio-python312/`; rerun locally with:

```bash
HF_HUB_DISABLE_XET=1 data/res-audio-python312/bin/python -m scripts.res_audio_pilot \
  --engine botonomus --proxy-host 107.174.128.95 --cve ACuKDHhqsvAd
```

## Bright Data comparison

Use the existing `BRIGHTDATA_PROXY_SERVER`, `BRIGHTDATA_PROXY_USERNAME` and
`BRIGHTDATA_PROXY_PASSWORD` in `.env.camoufox`:

```bash
HF_HUB_DISABLE_XET=1 data/res-audio-python312/bin/python -m scripts.res_audio_pilot \
  --engine botonomus --proxy-provider brightdata \
  --cve ACuKDHhqsvAd --output data/res-audio-brightdata
```

This mode currently supports the Botonomus/Patchright adapter. A sticky session
suffix is added when the configured username does not already contain one.
An HTTPS preflight through that same upstream proxy obtains the expected egress;
the browser must match it before visiting RES and retain it after the attempt.
`--proxy-host` applies only to Webshare. The Bright Data zone's residential/ISP/
datacenter classification is not inferred from the hostname.

Bright Data's local CA file is checked against its pinned SHA-256 fingerprint.
The HTTP preflight trusts that CA alongside the normal certificate bundle. The
test Chrome process receives a certificate exception restricted to that CA's
public-key hash (`--ignore-certificate-errors-spki-list`), without a global TLS
exception or changes to the machine's trust store. Credentials remain in the
owned loopback tunnel, never in browser command-line arguments or reports.

The pilot pins PyAV 14.2.0: PyAV 19.0.2 rejected the `metadata_errors` argument
used by faster-whisper 1.2.1. The replacement was checked by decoding a generated
local WAV through faster-whisper before another live attempt.

### Bright Data result, 2026-11-12

`data/res-audio-brightdata/20261112T122127Z/report.json` records a successful
live audio solution and PDF download for VERTIKAL GROUP, CVE `ACuKDHhqsvAd`.
The browser used Bright Data IP `193.111.185.134` throughout. Google supplied a
35,119-byte recording; Whisper base transcribed it in about one second and the
checkbox was accepted on the first round. RES returned the document and Chrome
downloaded 953,685 bytes.

The adapter omitted the Playwright download event, so the original helper timed
out despite Chrome having finished. The actual file was recovered using this
owned profile's completed download history, its URL's CVE, and its byte counts.
The PDF passed marker/EOF validation and its extracted text contained VERTIKAL.
Its SHA-256 is `cfe63e7e056ba4414c2a905387a04205527022cd778e2f0d38628d562b84cbca`.
The report retains the event timeout alongside the successful recovery.

The pilot now explicitly sets a private download directory and observes Chrome's
CDP download events. Recovery requires both a completed download and an exact
match to the document URL returned by the verifier, followed by PDF validation.
Unit tests cover this recovery. An additional local replay browser test was not
executed because automatic approval review rejected the proposed replay through
a proxy as a potential document disclosure. The later successful Webshare trial
described below verified the CDP fallback against a real RES download.

This is one confirmed success, not a reliability measurement. Other attempts
encountered the initial decoder incompatibility, a form timeout on another IP,
and a Bright Data preflight proxy error. No failed attempt is counted as a PDF.

## Ten-company search and download trial

```bash
HF_HUB_DISABLE_XET=1 data/res-audio-python312/bin/python -m scripts.res_audio_batch \
  --limit 10 --workers 2 --output data/res-audio-batch-10
```

The immutable selection includes VERTIKAL GROUP as a control and nine companies
with RUTs from the existing database, excluding previously confirmed empty RES
results. Two isolated browser/proxy sessions run concurrently. Each company is
searched through RES with the local audio solver; at most one returned CVE is
downloaded. Other returned CVEs remain in the report. Empty search results are
kept rather than replaced by different companies. JSON checkpoints and per-company
results permit continuation; completed trial records are skipped on a normal rerun.

The 2026-11-12 batch hit proxy preflight errors for all ten selected companies.
An independent diagnostic returned `401 Auth Failed (code: ip_blacklisted)` from
Bright Data. There were no RES requests, CAPTCHA tests or PDFs in this batch;
it provides no measurement of CAPTCHA stability. The report records the blocker.
Code now stops scheduling further requests when this provider refusal is detected.

After Bright Data access is restored, retry the same selection with:

```bash
HF_HUB_DISABLE_XET=1 data/res-audio-python312/bin/python -m scripts.res_audio_batch \
  --limit 10 --workers 2 --output data/res-audio-batch-10 --retry-preflight
```

This preserves the previous setup results before retrying preflight failures.
Neither this command nor the single-CVE pilot changes provider account settings.

### Alternative proxy

The batch also supports Webshare with an explicit endpoint:

```bash
HF_HUB_DISABLE_XET=1 data/res-audio-python312/bin/python -m scripts.res_audio_batch \
  --limit 10 --workers 1 --proxy-provider webshare --proxy-host 191.101.41.136 \
  --output data/res-audio-batch-10-webshare
```

The 2026-11-12 single-CVE trial on this IP solved an audio challenge in 1.42 seconds,
received RES's document URL and automatically saved a validated 953,686-byte PDF
using the CDP fallback. See
`data/res-audio-webshare-alternative/20261112T123929Z/report.json`.
An earlier alternative Webshare endpoint did not provide a usable audio recording.
These results do not imply that every endpoint can solve the CAPTCHA.

Batch testing exposed a race when the checkbox became checked after the initial
two-second wait: the solver tried clicking the now-disabled audio button. The
solver now rechecks checkbox acceptance while finding/waiting for that button
and after a click failure. The pre-fix diagnostic run is preserved separately in
`data/res-audio-batch-10-webshare-before-race-fix/`.

The search pilot also waits for DOM readiness: search inputs start hidden and
their option-change handlers are registered at DOM ready. Merely waiting for the
select element after navigation commit allowed option changes to occur before
those handlers existed. That diagnostic run is retained separately under
`data/res-audio-batch-10-webshare-before-form-ready-fix/`.

### Final alternative-proxy sample

`data/res-audio-batch-10-webshare-final/report.json.first-pass` preserves the full
ten-company first pass after both readiness fixes. Eight audio challenges were
explicitly declined by Google. Two recordings were transcribed and accepted
(1.09 and 2.18 seconds), then both searches hit response-body eviction during the
site's synchronous AJAX redirect. No PDFs were obtained in that batch.

The experimental batch now captures original RES search/detail JSON at the CDP
Fetch response stage before allowing the response to continue unchanged. Other
traffic is not intercepted. `lookup` has an optional response reader; production
callers retain their original behavior. Tests cover original-payload preservation,
base64 decoding, release of paused responses and rejection of malformed data.

Only those two technical failures were retried, preserving prior attempts. One
retry could not locate the CAPTCHA anchor; the other could not fetch the Webshare
inventory. Neither reached a new accepted challenge, so the new search response
capture remains unverified on a successful live search. The current report and
CSV include those final statuses plus the prior attempts. `--retry-errors` retries
technical failures only, not all CAPTCHA refusals.

The successful single-CVE PDF is separate evidence and is not counted among the
batch's ten company searches. This sample demonstrates intermittent audio success,
not stable operation of the full search-to-PDF pipeline on this proxy.

## HTTP pilot: readiness, connection retries and local PDFs

`scripts/res_free_pilot.sh` uses a browser for genuine CAPTCHA tokens and sends
the search and verification requests through `src/sites/res/http_flow.py`.
Both transports must report the same live proxy exit IP. For Bright Data, a
changed preflight IP is recorded; a browser/HTTP IP mismatch still rejects the
session. There is no direct-network fallback.

The token form waits for navigation commit and a visible CAPTCHA anchor within
`--timeout`. It does not wait for the entire document's `DOMContentLoaded` event
or fill browser search inputs: this pilot submits the RUT or CVE through HTTP.
Browser-driven collectors that submit the actual form retain their DOM wait.
Only this token page skips RES-hosted scripts, styles, images and fonts: stalled
legacy UI assets can otherwise stop HTML parsing before the CAPTCHA markup.
The original RES document and all Google/gstatic widget, challenge and audio
requests are preserved. This filter is not used by browser-driven collectors.
Form-readiness failures save private HTML/screenshot diagnostics under the output
directory and remain failures, rather than empty company results.

HTTP connect/pool timeouts are capped at 12 seconds; the read timeout follows
`--timeout`. The transport retries connection establishment once, before any
HTTP request bytes are sent, including transient DNS/connect failures. A local
test injects a temporary DNS error and verifies a single POST reaches the server.
An idempotent GET additionally receives one retry after a connection failure or
HTTP 502/503/504. HTTP 403/429 is not retried. POST requests are never replayed
automatically: a connection failure goes back to the company queue for a fresh
token and proxy, bounded by `--company-attempts` per invocation.

Valid local PDF manifests are checked before any network request, even if the
codigo cache is missing. A transient PDF connection failure preserves the cached
codigo for the next attempt. Continue using the same `--output` and state file;
do not pass `--refresh` when reusing completed searches. Use a database containing
only unfinished companies to restrict a retry to that selection. Historic
`report-<timestamp>.json` files remain alongside the new report.

### Retry of the remaining seven companies, 2026-11-12

The first readiness-only attempt still stopped while parsing the document head;
its interrupted checkpoints remain in `report-20261112T155925Z.json`. After the
RES asset filter, `data/res-free-brightdata-10/report-20261112T160313Z.json` records
all seven companies without a form-readiness timeout and five new PDFs.
Constructora Piedra Preciosa's saved CVE was then resumed after the connection
retry fix (`report-20261112T161031Z.json`), producing the sixth new PDF.

The combined sample has nine validated PDFs for ten companies. Extracted company
names and RUTs match all nine documents. Artesania Autoctona remains unfinished:
Google provided an audio recording, but local recognition returned an empty
transcript. This is a CAPTCHA-solving failure, not a connection timeout or an
explicit Google audio refusal. The three original PDFs retained their hashes,
sizes and modification times. `before-connection-fix/` preserves their snapshot
and the previous summary/archive; `results.csv`, `summary.json` and
`verified-pdfs.zip` contain the consolidated results.
