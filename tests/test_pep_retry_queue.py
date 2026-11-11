import json
import sqlite3

from src.sites.pep.retry_queue import (
    attempts_for,
    begin_attempt,
    eligible_links,
    finish_attempt,
    prepare_attempts,
    retryable,
)


def database():
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE jobs (link TEXT PRIMARY KEY,status TEXT NOT NULL,result TEXT NOT NULL);
        CREATE TABLE benchmark_selection (link TEXT PRIMARY KEY);
    """)
    prepare_attempts(db)
    return db


def test_queue_only_retries_transient_failures_and_caps_attempts():
    db = database()
    rows = [
        ("pending", "pending", []), ("complete", "completed", []),
        ("network", "discovery_failed_network", []),
        ("timeout", "partial", [{"status": "browser_download_timeout"}]),
        ("captcha", "partial", [{"status": "browser_rejected_http_302", "rejection_reason": "site_verification_notice"}]),
        ("missing", "partial", [{"status": "download_failed_http_404"}]),
    ]
    for link, status, docs in rows:
        db.execute("INSERT INTO jobs VALUES (?,?,?)", (link, status, json.dumps(docs)))
        db.execute("INSERT INTO benchmark_selection VALUES (?)", (link,))
    assert eligible_links(db, "all", 2) == ["pending", "complete", "network", "timeout"]
    assert eligible_links(db, "errors", 2) == ["network", "timeout"]
    for run_id in ("run1", "run2"):
        begin_attempt(db, "network", run_id)
        finish_attempt(db, "network", run_id, "discovery_failed_network")
    assert attempts_for(db, "network") == 2
    assert eligible_links(db, "errors", 2) == ["timeout"]
    db.close()


def test_interrupted_attempt_is_kept_in_history_and_recovered():
    db = database()
    db.execute("INSERT INTO jobs VALUES ('job','processing','[]')")
    db.execute("INSERT INTO benchmark_selection VALUES ('job')")
    begin_attempt(db, "job", "old-run")
    prepare_attempts(db)
    assert db.execute("SELECT status FROM benchmark_attempts").fetchone()[0] == "interrupted"
    assert db.execute("SELECT status FROM jobs").fetchone()[0] == "interrupted"
    assert eligible_links(db, "errors", 2) == ["job"]
    assert not retryable("partial", [{"status": "download_failed_tls"}])
    db.close()
