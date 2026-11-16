# Public proxy sources

The registry now includes **26 named feeds**, plus the optional `free-proxy`
loader: 13 existing feeds and 13 added repositories. All download as data; no
upstream scripts or packages are installed or executed for the new feeds.

New names: `shiftytr`, `roosterkid`, `hideip`, `clarketm`, `maximilianfeix`,
`mmpx12`, `aliilapro`, `murongpig`, `zaeem20`, `vakhov`, `prxchk`, `hookzof`,
`sunny9577`. See `src/proxy/sources/registry.py` for exact official URLs and refresh
intervals. Protocol-specific HTTP, SOCKS4 and SOCKS5 files preserve their labels;
an explicit scheme inside a mixed file takes precedence. HTTPS capability of an
HTTP CONNECT proxy is not treated as TLS to the proxy itself.

`--provider public` selects the broad catalog except the known stale Jetkai
archive. `--sources all` selects all registered feeds and the optional
`free-proxy` loader. The collector still deduplicates endpoints and merges source
provenance. One failing or empty source does not discard other sources.
Per-file byte/time/concurrency bounds, conditional cache and source refresh
intervals remain active. The record limit now defaults to 100,000 per file
(`PROXY_SOURCE_MAX_RECORDS`), rather than silently cutting large files at 10,000.
This is a bounded catalog, not a guarantee of every public proxy on the Internet.

Inspect sources without enabling collection:

```bash
.venv/bin/python -m src.services.proxy_collector --list-sources
```

For an explicitly requested one-shot collector run, set
`ALLOW_PUBLIC_PROXY_COLLECTION=1` **for that command only**, then use
`--sources all`; omit `--watch`. Existing `.env` loading supplies the reputation
key. `--limit 0 --max-per-source 0` removes the post-filter candidate/source caps,
but does not remove file-size/record limits or reputation-provider quotas.
Choose a finite verification limit when the individual CHECK budget is small.
The full collector also uses the configured target verification stage; catalog
size is not a count of live or RES-compatible proxies.

Automatic public collection remains off in default configuration and Compose.
Nothing here enables a background collector or weakens the reputation gate:
incident ranges, Spamhaus DROP, bulk AbuseIPDB blacklist, then a valid individual
CHECK/cache verdict must pass before connecting. Unknown or expired individual
verdicts cannot be replaced by absence from a bulk list.

## Live source validation, 2026-11-16

One-shot data downloads requested 61 registered files and the optional loader.
25/26 registered source names yielded candidates; HideIP files were empty.
`free-proxy` returned no additional candidates. With a 100,000-record per-file
cap, the merged catalog held 542,795 endpoint records / 155,355 distinct hosts.
These are **unverified candidates**, including large archive feeds and duplicates
across protocols. No candidate connections or individual reputation API requests
were made in this source-validation run.

Artifacts are under `data/proxy-source-expansion-20261116/`; the timestamped
`fetch-report.json` contains per-source counts/errors, `sources/` holds downloads
and cache metadata, and `candidates.jsonl` preserves merged source provenance.
