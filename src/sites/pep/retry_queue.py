"""SQLite job-attempt history and a bounded retry policy shared by benchmark passes."""

from __future__ import annotations

import json
from datetime import UTC, datetime

TRANSIENT_HTTP = {429, 500, 502, 503, 504}


def retryable(status: str, documents: list[dict]) -> bool:
    if any(doc.get("rejection_reason") in {"site_verification_notice", "site_limit_notice"} for doc in documents):
        return False
    if status in {"interrupted", "worker_error", "discovery_failed_network"}:
        return True
    if status.startswith("discovery_failed_http_"):
        return status.rsplit("_", 1)[-1] in {str(code) for code in TRANSIENT_HTTP}
    if status != "partial":
        return False
    for doc in documents:
        state = doc.get("status", "")
        if state in {"download_failed_network", "browser_download_timeout", "local_file_invalid"}:
            return True
        if state == "browser_download_failed" and doc.get("error_type") in {
            "TimeoutError", "ConnectionError", "ConnectionClosed", "WebSocketException",
        }:
            return True
        if state == "browser_rejected_http_302" and doc.get("rejection_reason") not in {
            "site_verification_notice", "site_limit_notice",
        }:
            return True
        if state.rsplit("_", 1)[-1] in {str(code) for code in TRANSIENT_HTTP}:
            return True
    return False


def prepare_attempts(db) -> None:
    db.executescript("""
        CREATE TABLE IF NOT EXISTS benchmark_attempts (
            id INTEGER PRIMARY KEY, link TEXT NOT NULL, run_id TEXT NOT NULL,
            started_at TEXT NOT NULL, finished_at TEXT, status TEXT NOT NULL,
            UNIQUE(link, run_id)
        );
        CREATE INDEX IF NOT EXISTS benchmark_attempts_link ON benchmark_attempts(link);
    """)
    # run() holds the output lock: unfinished rows cannot belong to a live peer.
    with db:
        db.execute("UPDATE benchmark_attempts SET status='interrupted',finished_at=? WHERE finished_at IS NULL",
                   (datetime.now(UTC).isoformat(),))
        db.execute("UPDATE jobs SET status='interrupted' WHERE status='processing'")


def attempts_for(db, link: str) -> int:
    return db.execute("SELECT count(*) FROM benchmark_attempts WHERE link=?", (link,)).fetchone()[0]


def eligible_links(db, pass_kind: str, max_attempts: int) -> list[str]:
    selected = []
    for link, status, raw in db.execute(
        "SELECT j.link,j.status,j.result FROM jobs j JOIN benchmark_selection s ON s.link=j.link ORDER BY s.rowid"
    ):
        if status == "completed":
            if pass_kind == "all":
                selected.append(link)  # Validate locally without consuming another attempt.
            continue
        if attempts_for(db, link) >= max_attempts:
            continue
        if status == "pending":
            if pass_kind == "all":
                selected.append(link)
        elif retryable(status, json.loads(raw)):
            selected.append(link)
    return selected


def begin_attempt(db, link: str, run_id: str) -> None:
    with db:
        db.execute("INSERT INTO benchmark_attempts(link,run_id,started_at,status) VALUES (?,?,?,'processing')",
                   (link, run_id, datetime.now(UTC).isoformat()))


def finish_attempt(db, link: str, run_id: str, status: str) -> None:
    with db:
        db.execute("UPDATE benchmark_attempts SET status=?,finished_at=? WHERE link=? AND run_id=?",
                   (status, datetime.now(UTC).isoformat(), link, run_id))
