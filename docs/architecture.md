# Scraper boundaries

`src/sites/seprec` owns the SEPREC client and scraping workflow. The old
`src.client.seprec_client` and `src.services.scraper` modules remain aliases for
compatibility with existing imports and patches. Existing SEPREC models,
storage, CLI and supporting services are still coupled to that source; their
migration should be incremental and preserve existing entry points.

`src/sites/pep` implements the separate PEP workbook, discovery, document and
browser workflows. The benchmark supports native CDP and an explicitly selected
persistent nodriver backend. SQLite stores jobs, documents and bounded attempt
history; the second pass selects only retryable errors. See [the project index](project-index.md)
and [the stability test](pep-stability.md).
Shared network and proxy infrastructure remains in `src/network` and
`src/proxy`, with the existing client layer retained for compatibility. Reuse
components only after checking for SEPREC assumptions; no generic scraper API is
imposed before the second source is understood.
