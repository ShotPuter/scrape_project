"""Persistent, rotating JSON logs and consistent counters for PEP runs."""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from collections import Counter
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path


def report_totals(report: dict) -> dict:
    jobs = report["jobs"]
    counts = Counter(job["status"] for job in jobs)
    documents = [doc for job in jobs for doc in job["documents"]]
    downloaded = sum(doc["status"] == "downloaded" for doc in documents)
    empty = sum(job["status"] == "completed" and not job["documents"] for job in jobs)
    return {
        "total": len(jobs),
        "processed": sum(counts.get(s, 0) for s in ("completed", "partial", "failed", "interrupted")),
        "completed": counts.get("completed", 0) - empty,
        "empty": empty,
        "partial": counts.get("partial", 0),
        "failed": counts.get("failed", 0) + counts.get("interrupted", 0),
        "pending": counts.get("pending", 0),
        "running": counts.get("running", 0),
        "downloaded": downloaded,
        "not_downloaded": len(documents) - downloaded,
        "pdf": report.get("pdf_count", 0),
    }


class EventLog:
    def __init__(self, directory: Path, *, secrets=(), max_bytes=10 * 1024 * 1024, backups=3):
        self.secrets = tuple(
            value
            for value in (
                *secrets,
                *(
                    os.getenv(key, "")
                    for key in (
                        "BRIGHTDATA_PROXY_PASSWORD",
                        "BRIGHTDATA_PROXY_USERNAME",
                        "WEBSHARE_API_TOKEN",
                        "TELEGRAM_BOT_TOKEN",
                    )
                ),
            )
            if value
        )
        self.logger = logging.Logger(f"pep.{directory}", level=logging.INFO)
        file_handler = RotatingFileHandler(directory / "run.log", maxBytes=max_bytes, backupCount=backups, encoding="utf-8")
        (directory / "run.log").chmod(0o600)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(logging.StreamHandler(sys.stdout))
        self.previous = (0.0, 0, 0)

    def clean(self, value):
        if isinstance(value, dict):
            return {
                key: "[redacted]"
                if any(part in str(key).lower() for part in ("password", "token", "cookie", "authorization"))
                else self.clean(item)
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [self.clean(item) for item in value]
        if isinstance(value, str):
            for secret in sorted(self.secrets, key=len, reverse=True):
                value = value.replace(secret, "[redacted]")
            return re.sub(r"(https?://)[^/\s@]+@", r"\1[redacted]@", value)
        return value

    def emit(self, event: str, **fields) -> None:
        entry = self.clean({"at": datetime.now(UTC).isoformat(), "event": event, **fields})
        self.logger.info(json.dumps(entry, ensure_ascii=False))

    def summary(self, event: str, report: dict, *, interval=False, **fields) -> None:
        stats = report_totals(report)
        seconds = report.get("seconds", 0)
        minutes = seconds / 60
        info = {
            "status": report["status"],
            "elapsed_seconds": seconds,
            **stats,
            "links_per_minute": round(stats["processed"] / minutes, 2) if minutes else 0,
            "declarations_per_minute": round(stats["downloaded"] / minutes, 2) if minutes else 0,
            "stop_reason": report.get("stop_reason"),
            **fields,
        }
        if interval:
            old_seconds, old_links, old_docs = self.previous
            window = max(0, seconds - old_seconds)
            info.update(
                interval_seconds=round(window, 2),
                interval_processed=stats["processed"] - old_links,
                interval_downloaded=stats["downloaded"] - old_docs,
                interval_declarations_per_minute=(round((stats["downloaded"] - old_docs) * 60 / window, 2) if window else 0),
            )
            self.previous = (seconds, stats["processed"], stats["downloaded"])
        self.emit(event, **info)

    def close(self) -> None:
        for handler in list(self.logger.handlers):
            handler.close()
            self.logger.removeHandler(handler)
