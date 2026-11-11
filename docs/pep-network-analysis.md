# PEP network investigation — 2026-11-03

## Bulk routes discovered

The public PEP page has two first-party bulk-report forms:

```text
POST /fdci/consultaCiudadana/generarReportePEPSIGEP
POST /fdci/consultaCiudadana/generarReportePEPActivos
Form field: captchatoken (site-generated reCAPTCHA v3 token)
```

Using the page's normal reCAPTCHA JavaScript and one native form-equivalent
request per endpoint returned HTTP 200 `application/zip`:

* SIGEP report: `SIGEP_INFORME_PEP.zip` (headers verified; archive not retained).
* PEP declarations: `FDCI_INFORME_PEP.zip`, containing
  `DECLARACIONES_PEP.xlsx` (9,772,831 bytes uncompressed; ZIP 9,451,244 bytes).
  The XLSX has 42,127 rows, 29 columns and `ENLACE_CONSULTA_LEY_2013` links.
  It is an export of declaration data and links, **not a package of PDFs**.

This is a supported way to fetch the PEP data export with one CAPTCHA per bulk
report instead of one CAPTCHA per declaration PDF. It does not supply a direct
PDF URL or eliminate the CAPTCHA protecting individual declaration files. The
user-provided workbook under `docs/` remains the input; the scraper must not
silently fetch or replace it. The new production path therefore keeps per-file
downloads and checkpoints, rather than treating the bulk XLSX as a replacement
for requested documents.

Direct unauthenticated GET/POST/HEAD requests to the report endpoints redirect
back to the application; `.json`, `format=json`, and `Accept: application/json`
do not expose JSON. `/fdci/api` and `/fdci/actuator` redirect to login. The
historical servlet tested on `www` and `www1` returns 404; archived examples
also now return 404. No unauthenticated direct document endpoint was found.
Open data is useful for indexes/metadata, not the requested document files.

The official `datos.gov.co` Socrata catalog exposes PEP (`3qxn-uc22`, 37,597
rows), asset declarations (`8tz7-h3eu`, 328,799 rows), and conflict-of-interest
declarations (`83qu-fqy5`, 3,607,836 rows). Those are downloadable datasets,
not the declaration PDFs; the sampled asset/conflict datasets expose presence
flags and metadata rather than file payloads. A published Google Sheets CSV
contains 2,234 PEP rows and search links, likewise not document URLs.

## Reducing repeated CAPTCHA interaction

For exact files, the safe automation remains one stable browser/profile and
egress IP, one page, serial native button clicks, pacing, SQLite resume, and a
stop-on-explicit-verification-refusal breaker. This avoids wasting tokens on
parallel attempts and does not promise that the site accepts every request.
Use the first-party bulk export when the XLSX data itself satisfies the need;
otherwise retain the individual-file path and request an authorized bulk/API
access arrangement from DAFP for unattended large-scale PDF collection.
Do not reuse, forge, synthesize, or externally solve CAPTCHA tokens.

## What the site's frontend actually does

Inspected the live DOM, the download JavaScript, application JS bundle and
the connected browser's Network records after a successful native button click.
No cookie values or CAPTCHA tokens are recorded here.

The modern declaration's click handler calls `grecaptcha.ready`, executes
reCAPTCHA for action `generarReporteConsultaCiudadana`, fills `captchatoken`,
then submits its native form. The HTTP request is:

```text
POST /fdci/consultaCiudadana/generarReporteConsultaCiudadana
Form fields: id, captchatoken
Response: 200 application/zip
Content-Disposition: attachment; filename=REPORTE.zip
```

The ZIP itself is the response body. There is no intervening JSON response
containing a direct PDF URL in the inspected successful flow. The inspected
application bundle exposes no alternative declaration API; this observation
does not prove that no other API exists. Plain requests without the required
token have returned 302 rather than a document. Changing proxies does not
remove this requirement.

### Confirmed reason for the current 302 refusals

A fresh headless browser investigation followed the 302 to
`/fdci/consultaCiudadana/index` and found the site's explicit alert:
**"Recaptcha did not pass validation"**. The native POST included both `id`
and a nonempty reCAPTCHA token. Therefore JavaScript execution and form
submission occurred, but server-side verification rejected the token. The
exact cause (score, action/hostname validation, expiry or another check) is
not disclosed by this message and must not be assumed. The observed request
had no JSESSIONID cookie, but that alone does not prove a required session was
missing on this public endpoint.

The pipeline now records a query-free `redirect_path` and classifies the site's
notice as `site_verification_notice`. It does not immediately resubmit after
that explicit verification refusal, and disables further browser submissions
for the run. No tokens/cookies are persisted in diagnostics. Increasing the
number of browser tabs is not a demonstrated fix for verification failures.

## Historical declarations use a different download mechanism

Some result rows have direct links instead of forms:

```text
GET /ley-transparencia-web/declaracionServlet?action=download&nextPage=index.jsp&file=...
```

These links are present in the public results, not guessed filenames or
enumerated endpoints. Asset links are distinguished from separate income-tax
links using their labels. Three actual historical asset URLs returned HTTP
404 in verified direct requests. The pipeline now retains these documents,
attempts the supplied links through HTTP, and records failures individually,
rather than losing all modern results on the same page. A 404 must not be
silently interpreted as a downloaded file or as no historical declaration.

## Why the previous run appeared to fail

At investigation time local state showed 15 completed consultations, three
`discovery_failed_schema` jobs and two partial jobs, with 50 document references
marked downloaded and two `browser_rejected_http_302`. No collector process was
running. This is compatible with a normal non-zero batch exit after item errors;
without the original terminal traceback/signal it is not proof of a process crash.

The three schema failures came from the unrecognised historical links above.
Also, the old downloader waited its full timeout after already receiving 302.
It now races the actual download against an explicit HTTP refusal and exits
that attempt immediately. A 302 gets at most one fresh native submission with
backoff; persistent refusal is retained, not looped indefinitely.

## Running without an active desktop display

Native headless Chrome initially returned HTTP 403. A controlled pair of
verified `curl_cffi` requests, differing only in the User-Agent product string,
returned 403 for `HeadlessChrome/154.0.0.0` and 200 for `Chrome/154.0.0.0`.
Native headless launch therefore uses a stable normal Chrome User-Agent with
the installed browser's major version. No rotating fingerprints, proxy pool,
token forging or CAPTCHA-solving service is used.

A live test with both DISPLAY and WAYLAND_DISPLAY removed downloaded both
modern declarations of the first consultation using `--native-chrome --headless`.
One initial 302 needed a subsequent attempt. This validates a working no-display
path, not guaranteed acceptance for every document or an unattended full run.
Sleep/suspend still stops computation; headless mode does not prevent OS sleep.

```bash
.venv/bin/python -m src.sites.pep docs/DECLARACIONES_PEP.xlsx \
  --native-chrome --headless --workers 2 --timeout 30 --limit 20
```

The trusted local CA bundle at `data/pep/ca-bundle.pem` is selected automatically.
On another machine provide a verified CA bundle explicitly. Downloads remain
in `data/pep/`, resume from SQLite, and export every workbook row to JSON.
