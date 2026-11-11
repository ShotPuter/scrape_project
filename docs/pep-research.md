# PEP source investigation

The intended source is https://www.funcionpublica.gov.co/fdci/consultaCiudadana/consultaPEP.
On 2026-11-02 the page returned HTTP 502 through the available web fetcher; DNS
resolution was unavailable from the execution sandbox. Consequently the actual
workbook, second sheet, links, document listing and download mechanism have not
been inspected. Do not assume an endpoint, a workbook layout beyond the requested
column, or that HTTP alone will work.

A published sample of the PEP field descriptions says that
`ENLACE_CONSULTA_LEY_2013` links, when applicable, to search results for asset and
income declarations, conflicts of interest and tax declarations. It describes
employment dates and identity fields, and also sensitive family and financial
information. See https://hospitalespecialcubara.gov.co/static/archivos/Formato%20DECLARACIONES_PEP.pdf.
This sample is not a substitute for inspecting the current workbook.

When the source becomes reachable: download the workbook, inspect sheet names,
headers and hyperlink targets, then trace several person links and PDF downloads
(including multiple years, no link, no results and more than one document type).
Record whether a session, JavaScript, pagination or other interaction is needed;
check service limits. Never commit downloaded personal records or credentials.
