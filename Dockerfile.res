ARG CAMOUFOX_ASSETS=download
FROM python:3.11-slim-bookworm AS runtime
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 XDG_CACHE_HOME=/opt/camoufox-cache
WORKDIR /app
COPY docker/res-requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip pip install --timeout 120 --retries 5 -r /tmp/requirements.txt
RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources \
    && printf '%s\n' 'Acquire::Retries "5";' 'Acquire::https::Timeout "60";' \
       'APT::Update::Error-Mode "any";' > /etc/apt/apt.conf.d/80-network-retries \
    && python -m playwright install-deps firefox \
    && apt-get update && apt-get install -y --no-install-recommends \
       xvfb xauth ca-certificates libdbus-glib-1-2 libgl1-mesa-dri libegl1 libgbm1 fonts-liberation \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 scraper \
    && useradd --uid 1000 --gid scraper --create-home scraper \
    && mkdir -p /app/data /opt/camoufox-cache \
    && chown -R scraper:scraper /app/data /opt/camoufox-cache \
       /usr/local/lib/python3.11/site-packages/fpgen
USER scraper
FROM runtime AS assets-download
RUN timeout 3600 python -m camoufox fetch

# Optional BuildKit named contexts contain public browser/model assets only.
FROM runtime AS assets-cache
COPY --from=browser-assets --chown=scraper:scraper / /opt/camoufox-cache/camoufox/browsers/
COPY --from=model-assets --chown=scraper:scraper / /usr/local/lib/python3.11/site-packages/fpgen/data/
RUN --network=none touch /opt/camoufox-cache/camoufox/.0.5_FLAG \
    && python -c "from camoufox.fpgen_model import ensure_fpgen_model; from camoufox.pkgman import launch_path; ensure_fpgen_model(); print(launch_path())"

FROM assets-${CAMOUFOX_ASSETS} AS final
COPY src ./src
STOPSIGNAL SIGINT
ENTRYPOINT ["python", "-u", "-m", "src.sites.res.server"]
CMD ["pipeline", "--limit", "100", "--browsers", "5", "--tabs-per-browser", "2", "--proxy-provider", "webshare"]
