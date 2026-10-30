# Running on a server

Use Docker Engine with Compose v2.24 or newer. The default limits allow two
containers with up to 2 GiB RAM and two CPUs each. Configure these limits in `.env`
for the server. Actual throughput depends on the connection and working proxies.
The free proxy pool can shrink and stall downloads; a running container alone
does not mean useful throughput.

Docker installs exact runtime versions from `requirements.lock`. The broader
`requirements.txt` ranges remain available for development; update and validate
the lock when upgrading production dependencies.

## Prepare

```bash
cp .env.server.example .env
mkdir -p data
# Run as the non-root user who owns the data directory.
export SCRAPER_UID=$(id -u) SCRAPER_GID=$(id -g)
docker compose config --quiet
docker compose build
```

For an existing data directory, set DATA_DIR and ensure that its owner matches
SCRAPER_UID/SCRAPER_GID. Do not copy the development `.env`, proxy credentials,
or downloaded files into the image. The build context excludes them.

The collector runs separately, writes `data/proxies.db`, and refreshes verified
proxies while the scraper runs. Startup waits up to 15 minutes for proxies;
`PROXY_MODE=always` prevents an automatic direct connection when the pool is empty.
The collector is bounded to 2,000 candidates and 1,000 per source per scan by
default. Increase those settings only after inspecting collection speed.

## Webshare primary proxies

Set `WEBSHARE_API_TOKEN` in `.env` to your API key from the Webshare dashboard.
No manually exported proxy file is required. Leave `PROXY_SOURCES=all` for the
free reserve. The key is an API key, not a proxy-list download token.

The collector retrieves the paginated inventory with username/password
authentication, validates its proxies against the target, and stores the working
combinations in the shared SQLite database. One verified Webshare proxy is enough
to unblock startup. Public proxies are also verified in the background so that
the reserve is already available when needed.

Downloads prefer working private proxies. Busy private proxies retain priority;
they do not cause routine spillover to the public pool. If private proxies fail
or enter cooldown/quarantine and no private session remains usable, requests use
the verified public reserve. Fresh provider verification can restore private
proxies, and downloads prefer them again. Global rate-limit pauses still apply.
An API error, expired key, or empty Webshare inventory leaves public-source
collection available. The collector retries the inventory on later scans.

Use `WEBSHARE_PROXY_MODE=direct` for standard proxy lists. For rotating residential
plans, set it to `backbone`; connections then use `p.webshare.io:80` with the
returned credentials. See the [Webshare list API](https://apidocs.webshare.io/proxy-list/list)
and [connection documentation](https://apidocs.webshare.io/proxy-connection).
Token and authenticated inventory responses are not cached or logged; persistent
SQLite contains the proxy credentials needed by the scraper. Keep that data
directory private. No live Webshare account is required for the automated tests.

## Eight-hour stress test

```bash
docker compose --profile stress up -d stress
docker compose logs --tail 100 -f stress proxy-collector
```

Set STRESS_MINUTES, STRESS_YEAR and STRESS_CATEGORY in `.env` before starting.
The stress deadline starts after proxy startup and client initialization; allow
extra time for that startup. For a new run, archive `data/stress` and recreate
the stress container. Never run two stress jobs against the same report path.

The test downloads and validates full PDF responses without saving PDFs or
updating the production publication index. It repeats the selection until its
deadline; repeated documents count again. Disk persistence and S3 delivery are
not exercised. The report is written every minute and on graceful shutdown:

```bash
cat data/stress/stress-report.json
# Optional, if jq is installed:
jq '{status, duration_seconds, documents_checked, items_failed,
     files_per_minute, rss_kib, http_metrics}' data/stress/stress-report.json
docker compose ps -a
docker inspect --format '{{.State.ExitCode}} OOM={{.State.OOMKilled}}' \
  "$(docker compose ps -a -q stress)"
```

Expected successful final status: `DURATION_REACHED`, process exit code 0.
Check `items_failed` even when the exit code is zero. Check document-counter
growth between minute reports to detect a depleted proxy pool. API body-byte
metrics exclude the collector, headers and TLS overhead. Logs rotate both in
Docker and in the application's persistent directory.

## Real collection with PDF persistence

Start a selected year/category in the background, independently of SSH:

```bash
docker compose run -d --name seprec-night scraper --year 2022 --category 1
docker logs --tail 100 -f seprec-night
```

Compose starts the collector dependency automatically. This is one complete
pass, with no eight-hour deadline. To collect all dates and categories instead:

```bash
docker compose up -d scraper
docker compose logs --tail 100 -f scraper proxy-collector
```

PDFs go to `data/pdf`, and metadata/checkpoints to `data/publications.csv`.
With our sample average of about 3.5 MiB/PDF, 24,000 documents need about 83 GiB
for PDFs alone; actual category sizes vary. Monitor free disk space before and
during a real collection. Keep CSV and PDFs together; stored paths use `/app/data`
inside the container and remain consistent across server runs. Do not run two
writers against the same production CSV. Stress and real collection have
separate output paths but still share the server's bandwidth and proxy pool;
run one workload at a time when measuring throughput.

An interrupted collection can be rerun with the same arguments and persistent
data. Valid indexed files are skipped. After a partial full-site pass, rerun
individual years to sweep gaps: the unfiltered incremental mode stops after
three consecutive pages of already saved publications. Inspect item errors in
the final reports; a zero process exit code does not imply zero item failures.

## Stop and resume

```bash
# Stops Compose services gracefully and preserves all bind-mounted data.
docker compose --profile stress stop
# One-off containers created with `compose run` must be stopped separately:
docker stop --time 120 seprec-night
docker inspect --format '{{.State.ExitCode}} OOM={{.State.OOMKilled}}' seprec-night
```

The image uses SIGINT so asyncio can cancel work, flush the final stress report,
and close the CSV/database. Allow up to two minutes for shutdown. Use stop rather
than pause for overnight jobs; paused processes retain connections and their
internal clocks keep advancing. The collector restarts after daemon/server
restarts; scraper and stress jobs deliberately do not restart automatically.
Rerun real collection explicitly to resume from its index. Stress starts with
new counters and overwrites its report.

The container `seprec-night` stays available for inspection after exit. After
checking its result, remove it with `docker rm seprec-night` before reusing that
name. The collector keeps running after a job finishes; stop it when finished:

```bash
docker compose stop proxy-collector
```

S3 upload is not implemented in this deployment. It prepares local PDF files
and metadata for a separate transfer step; no AWS credentials are required.
