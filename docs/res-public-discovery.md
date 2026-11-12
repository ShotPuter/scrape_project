# Public RES discovery audit — 2026-11-12

Playwright MCP was not exposed in this session. Inspection used the local async
Patchright browser through Webshare `191.101.41.136`; browser egress matched before
and after. No CAPTCHA was submitted during this audit. Public CKAN metadata and
small file/header samples were fetched through the same proxy.

Raw browser evidence, HTML and JSON:
`data/res-discovery-audit/20261112T133435Z/`.

## Sitemap and document directory

| URL path on www.registrodeempresasysociedades.cl | Observed result |
| --- | --- |
| `/robots.txt` | HTTP 404 |
| `/sitemap.xml` | HTTP 404 |
| `/sitemap_index.xml` | HTTP 404 |
| `/sitemap.aspx` | Redirect to `Error404.html`; final HTTP 200 is an error page |
| `/Certificados/` | HTTP 200 generic navigation/footer page, no document listing |

No advertised sitemap or complete certificate catalogue was found in the inspected
navigation. These checks do not prove that no sitemap exists anywhere on the host.

## Forms and their public behavior

[Verificar Certificados](https://www.registrodeempresasysociedades.cl/VerificarCertificados.aspx)
requires a CVE and reCAPTCHA response. Its inline JavaScript posts `texto` and
`Response` to `VerificarCertificados.aspx/Busqueda`, receives a document `Link`, then
navigates to it. This is a form endpoint, not a discovered bulk-list API.

[Buscar Actuaciones](https://www.registrodeempresasysociedades.cl/BuscarActuaciones2.aspx)
offers company RUT, company name, action date and registration date. Its UI options
map to backend `tipoBusqueda` values 1, 3, 4 and 6 respectively. Dates are single-day
inputs. A date query might reduce per-company discovery requests, but this audit
did not submit one or verify completeness/pagination of its results.

[Obtener Certificados](https://www.registrodeempresasysociedades.cl/ObtenerCertificadosHome2.aspx)
redirected the clean browser to `Login.aspx`. It did not expose an anonymous
certificate directory.

## Official company CSVs and CKAN API

The official [RES dataset](https://datos.gob.cl/dataset/registro-de-empresas-y-sociedades)
contains companies incorporated under the simplified regime, not all companies
ever registered in Chile. `package_show` returned 14 annual resources, 2013–2026;
the 2026 resource is partial through 2026-09-30.

Verified public APIs, with no API key or CAPTCHA:

```text
https://datos.gob.cl/api/3/action/package_show?id=registro-de-empresas-y-sociedades
https://datos.gob.cl/api/3/action/datastore_search?resource_id=71c8e355-226a-461e-809a-870c2275a178&limit=3
```

The [2025 resource](https://datos.gob.cl/dataset/registro-de-empresas-y-sociedades/resource/71c8e355-226a-461e-809a-870c2275a178)
has a 29,713,181-byte downloadable CSV and an active datastore. The metadata marks
that datastore complete for its source file. The API reported **202,406 records**;
this audit did not independently deduplicate them or count the entire CSV.

Its fields are ID, RUT, Razon Social, action/registration/SII approval dates, year,
month, tax/social municipality and region, company type, action type, and capital.
There is **no CVE or PDF URL field**. `Codigo de sociedad` is a company-type code
(the inspected examples contain `SpA`), not a document verification code.

Month filtering was verified with query parameters:

```json
{"resource_id":"71c8e355-226a-461e-809a-870c2275a178","filters":"{\"Mes\":\"Enero\"}","limit":1}
```

The January 2025 filter reported **16,708 records**. `Mes` uses Spanish names,
not numeric month strings. A ranged GET of the CSV returned HTTP 206 and its real
semicolon-delimited header; only the first 4 KiB was saved.

## Already indexed PDFs

Search engines index some full document URLs. One actual public result at
`/Certificados/Escritura.aspx?codigo=...&cve=AClGDrY3Da8M` returned HTTP 200,
`application/pdf` and a `%PDF-` prefix through a fresh HTTP proxy session without
cookies or a CAPTCHA submission. Only the first bytes were inspected.
See `indexed-pdf-probe.json` for the complete source URL and result.

This proves that an already known complete URL can be usable directly. It does
not establish search-index completeness, guaranteed future validity, or a method
for deriving the extra `codigo` parameter from a CVE. No CVEs or access parameters
were enumerated or guessed.

## Pipeline implication

Official CSV/API data can seed the company queue by year/month and RUT, replacing
DeQuienes for discovery of companies within this dataset's coverage. CVE discovery
and document retrieval remain separate stages. Existing indexed full PDF URLs can
be an additional source with limited coverage. A complete PDF archive or an open
API listing all certificates was not found.
