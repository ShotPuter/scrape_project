# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Cache dependencies separately from application code. No build tools in runtime.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

ARG SCRAPER_UID=1000
ARG SCRAPER_GID=1000
RUN groupadd --gid "${SCRAPER_GID}" scraper \
    && useradd --uid "${SCRAPER_UID}" --gid scraper --create-home scraper \
    && mkdir -p /app/data \
    && chown scraper:scraper /app/data

COPY config.py main.py collect_proxies.py proxy_tool.py ./
COPY src ./src

USER scraper:scraper
VOLUME ["/app/data"]

# Exec form forwards signals to Python; scheduling is explicitly opt-in.
ENTRYPOINT ["python", "main.py"]
CMD []
