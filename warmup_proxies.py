"""Check every CSV proxy against SEPREC and retain up to 50 verified entries."""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import os
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import RequestException

from config import settings
from src.models.publication import PublicationSearchFilter

SUPPORTED_PROXY_SCHEMES = {"http", "https", "socks4", "socks4a", "socks5", "socks5h"}
WORKING_STATUS = "working"
DEFAULT_KEEP = 50
DEFAULT_CONCURRENCY = 25
DEFAULT_TIMEOUT = 10.0
PROGRESS_EVERY = 50
CATALOG_PATH = "publicaciones/catalogos/lista"
SEARCH_PATH = "publicaciones"
SEARCH_PAGE_SIZE = 10
SEARCH_CATEGORY = "1"
BACKUP_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S%fZ"
EXIT_SUCCESS = 0
EXIT_INSUFFICIENT_PROXIES = 2


def proxy_url(row: dict) -> str:
    address = row.get("proxy", "").strip() or f"{row.get('ip', '')}:{row.get('port', '')}"
    url = address if "://" in address else f"{row.get('protocol', 'http').strip().lower()}://{address}"
    parsed = urlsplit(url)
    if parsed.scheme not in SUPPORTED_PROXY_SCHEMES:
        raise ValueError("Unsupported proxy protocol")
    if not parsed.hostname or not parsed.port:
        raise ValueError("Missing proxy host or port")
    return url


async def check_proxy(url: str, request_timeout: float) -> tuple[str, float]:
    """Require successful catalog and search responses, with TLS verification
    enabled."""
    started = time.monotonic()
    try:
        async with AsyncSession(
            proxy=url, impersonate=settings.browser_profile, headers=settings.default_headers,
            timeout=request_timeout, max_clients=1, trust_env=False,
        ) as session:
            async with asyncio.timeout(request_timeout * 2 + 2):
                status = await _check_catalog(session)
                if status is not None:
                    return status, _elapsed(started)
                status = await _check_search(session)
                if status is not None:
                    return status, _elapsed(started)
        return WORKING_STATUS, _elapsed(started)
    except RequestException as exc:
        return f"curl_{exc.code}", _elapsed(started)
    except TimeoutError:
        return "timeout", _elapsed(started)
    except (ValueError, UnicodeError):
        return "invalid_json", _elapsed(started)


async def _check_catalog(session: AsyncSession) -> str | None:
    response = await session.post(f"{settings.api_base_url}/{CATALOG_PATH}", json={"codigo": ""})
    if not 200 <= response.status_code < 300:
        return f"http_{response.status_code}"
    if not _catalog_ready(response.json()):
        return "invalid_catalog"
    return None


async def _check_search(session: AsyncSession) -> str | None:
    filters = PublicationSearchFilter(page_size=SEARCH_PAGE_SIZE, category=SEARCH_CATEGORY)
    response = await session.post(f"{settings.api_base_url}/{SEARCH_PATH}", json=filters.to_api_payload())
    if not 200 <= response.status_code < 300:
        return f"http_{response.status_code}"
    if not _search_ready(response.json()):
        return "invalid_search"
    return None


def _catalog_ready(data: object) -> bool:
    return isinstance(data, dict) and data.get("finalizado") is True and isinstance(data.get("datos"), list)


def _search_ready(data: object) -> bool:
    if not isinstance(data, dict) or data.get("finalizado") is not True:
        return False
    payload = data.get("datos")
    return isinstance(payload, dict) and isinstance(payload.get("filas"), list) and "total" in payload


def _elapsed(started: float) -> float:
    return time.monotonic() - started


async def scan(rows: list[dict], concurrency: int, request_timeout: float) -> list[dict]:
    queue: asyncio.Queue[tuple[int, str] | None] = asyncio.Queue()
    results = _collect_candidates(rows, queue)

    async def worker() -> None:
        while True:
            item = await queue.get()
            try:
                if item is None:
                    return
                index, url = item
                status, seconds = await check_proxy(url, request_timeout)
                results.append({"row": index, "status": status, "seconds": round(seconds, 3)})
                _report_progress(results, len(rows))
            finally:
                queue.task_done()

    tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]
    try:
        for _ in tasks:
            queue.put_nowait(None)
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return results


def _collect_candidates(rows: list[dict], queue: asyncio.Queue[tuple[int, str] | None]) -> list[dict]:
    results: list[dict] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        try:
            url = proxy_url(row)
        except ValueError:
            results.append({"row": index, "status": "invalid_address", "seconds": 0})
            continue
        if url in seen:
            results.append({"row": index, "status": "duplicate", "seconds": 0})
            continue
        seen.add(url)
        queue.put_nowait((index, url))
    return results


def _report_progress(results: list[dict], total: int) -> None:
    if len(results) % PROGRESS_EVERY and len(results) != total:
        return
    counts = Counter(item["status"] for item in results)
    print(f"Checked {len(results)}/{total}, working={counts[WORKING_STATUS]}, outcomes={dict(counts)}", flush=True)


def save_results(path: Path, original: bytes, fields: list[str], rows: list[dict], results: list[dict], keep: int) -> dict:
    """Back up the source and atomically replace it only after a completed scan."""
    if path.read_bytes() != original:
        raise RuntimeError("Source CSV changed during scan; refusing to overwrite it")

    stamp = datetime.now(UTC).strftime(BACKUP_TIMESTAMP_FORMAT)
    backup = _write_backup(path, original, stamp)
    selected = _fastest_working(results, keep)
    report = {
        "checked_at": datetime.now(UTC).isoformat(),
        "source_rows": len(rows),
        "selected": len(selected),
        "outcomes": dict(Counter(item["status"] for item in results)),
        "backup": str(backup),
        "results": sorted(results, key=lambda item: item["row"]),
    }
    report_path = path.with_name(f"{path.name}.{stamp}.report.json")
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    _write_selected_csv(path, stamp, fields, rows, selected)
    return {key: value for key, value in report.items() if key != "results"} | {"report": str(report_path)}


def _write_backup(path: Path, original: bytes, stamp: str) -> Path:
    backup = path.with_name(f"{path.name}.{stamp}.bak")
    with backup.open("xb") as handle:
        handle.write(original)
    return backup


def _fastest_working(results: list[dict], keep: int) -> list[dict]:
    working = (item for item in results if item["status"] == WORKING_STATUS)
    return sorted(working, key=lambda item: item["seconds"])[:keep]


def _write_selected_csv(path: Path, stamp: str, fields: list[str], rows: list[dict], selected: list[dict]) -> None:
    temp = path.with_name(f"{path.name}.{stamp}.tmp")
    try:
        with temp.open("x", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for item in selected:
                row = dict(rows[item["row"]])
                if "alive" in row:
                    row["alive"] = "true"
                writer.writerow(row)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", type=Path, nargs="?", default=Path("free-proxy-list.csv"))
    parser.add_argument("--keep", type=int, default=DEFAULT_KEEP)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    args = parser.parse_args()
    if args.keep < 1 or args.concurrency < 1 or args.timeout <= 0:
        parser.error("keep, concurrency and timeout must be positive")

    original = args.file.read_bytes()
    reader = csv.DictReader(io.StringIO(original.decode("utf-8-sig")))
    rows = list(reader)
    if not rows or not reader.fieldnames:
        parser.error("CSV contains no candidates")

    print(f"Checking all {len(rows)} rows; concurrency={args.concurrency}, keep={args.keep}", flush=True)
    results = asyncio.run(scan(rows, args.concurrency, args.timeout))
    summary = save_results(args.file, original, reader.fieldnames, rows, results, args.keep)
    print(json.dumps(summary, indent=2), flush=True)
    return EXIT_SUCCESS if summary["selected"] == args.keep else EXIT_INSUFFICIENT_PROXIES


if __name__ == "__main__":
    raise SystemExit(main())
