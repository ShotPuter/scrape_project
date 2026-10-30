"""Periodic read-only snapshots of a running discard test."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sqlite3
import time
from datetime import datetime
from pathlib import Path

from src.utils.atomic import atomic_write_json


def build_snapshot(report: dict, previous: dict | None) -> dict:
    previous = previous or {}
    elapsed = report.get("duration_seconds", 0)
    window = max(elapsed - previous.get("duration_seconds", 0), 0.001)
    documents = report.get("documents_checked", 0)
    metrics = report.get("http_metrics", {})
    before_metrics = previous.get("http_metrics", {})
    return {
        "captured_at": datetime.now().astimezone().isoformat(),
        **report,
        "window_seconds": round(window, 2),
        "window_documents": documents - previous.get("documents_checked", 0),
        "window_files_per_minute": round((documents - previous.get("documents_checked", 0)) * 60 / window, 2),
        "window_failures": report.get("items_failed", 0) - previous.get("items_failed", 0),
        "api_body_bytes_total": metrics.get("response_bytes_total", 0),
        "api_body_bytes_window": metrics.get("response_bytes_total", 0) - before_metrics.get("response_bytes_total", 0),
        "pdf_bytes_window": report.get("pdf_bytes_checked", 0) - previous.get("pdf_bytes_checked", 0),
        "traffic_scope": "scraper API response bodies; excludes collector, headers, TLS and proxy overhead",
    }


def include_baseline(report: dict, baseline: dict) -> dict:
    """Keep test totals continuous when a controlled restart changes the policy."""
    combined = dict(report)
    for key in (
        "duration_seconds",
        "documents_checked",
        "original_files",
        "generated_files",
        "items_failed",
        "pdf_bytes_checked",
    ):
        combined[key] = report.get(key, 0) + baseline.get(key, 0)
    combined["files_per_minute"] = round(combined["documents_checked"] * 60 / max(combined["duration_seconds"], 0.001), 2)
    metrics = dict(report.get("http_metrics", {}))
    for key, value in baseline.get("http_metrics", {}).items():
        if isinstance(value, (int, float)):
            metrics[key] = metrics.get(key, 0) + value
    combined["http_metrics"] = metrics
    combined["baseline_included"] = True
    combined["phase_duration_seconds"] = report.get("duration_seconds", 0)
    combined["timing_percentiles_scope"] = "current phase only"
    return combined


def attach_pool_data(snapshot: dict, directory: Path) -> None:
    try:
        connection = sqlite3.connect((directory / "proxies.db").resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
        try:
            snapshot["verified_urls_in_db"] = connection.execute(
                "SELECT COUNT(DISTINCT proxy_url) FROM verified_proxies WHERE is_active=1"
            ).fetchone()[0]
        finally:
            connection.close()
    except sqlite3.Error as exc:
        snapshot["proxy_db_error"] = type(exc).__name__
    try:
        log = (directory / "scraper.log").read_text()
        sizes = re.findall(r"Removed failed proxy from rotation; (\d+) remain", log)
        snapshot["last_logged_pool_size"] = int(sizes[-1]) if sizes else None
    except OSError:
        pass


def write_snapshot(directory: Path, snapshot: dict) -> None:
    with (directory / "checkpoints.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(snapshot, ensure_ascii=False) + "\n")
    atomic_write_json(directory / "latest-checkpoint.json", snapshot)
    text = (
        f"{snapshot['captured_at']} | {snapshot['status']}\n"
        f"Elapsed: {snapshot['duration_seconds'] / 60:.1f} min\n"
        f"Documents: {snapshot['documents_checked']}; window: {snapshot['window_documents']}\n"
        f"Speed: {snapshot['files_per_minute']:.1f}/min overall; "
        f"{snapshot['window_files_per_minute']:.1f}/min window\n"
        f"Failures: {snapshot['items_failed']}; window: {snapshot['window_failures']}\n"
        f"API bodies: {snapshot['api_body_bytes_total'] / 1073741824:.3f} GiB total; "
        f"{snapshot['api_body_bytes_window'] / 1073741824:.3f} GiB window\n"
        f"PDF bytes checked: {snapshot['pdf_bytes_checked'] / 1073741824:.3f} GiB\n"
        f"RSS: {snapshot.get('rss_kib')} KiB; verified DB URLs: {snapshot.get('verified_urls_in_db')}\n"
        f"Traffic scope: {snapshot['traffic_scope']}\n"
    )
    with (directory / "checkpoints.log").open("a", encoding="utf-8") as stream:
        stream.write(text + "\n")
    print(text, flush=True)


def stop_owned_collector(directory: Path) -> None:
    try:
        pid = int((directory / "collector.pid").read_text())
        command = Path(f"/proc/{pid}/cmdline").read_bytes().decode().replace("\0", " ")
        if "src.proxy" in command and str(directory.resolve()) in command:
            os.kill(pid, signal.SIGTERM)
    except (OSError, ValueError):
        pass


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--interval-seconds", type=float, default=1800)
    parser.add_argument("--stop-collector", action="store_true")
    parser.add_argument("--baseline-report", type=Path)
    parser.add_argument("--exit-file", type=Path)
    args = parser.parse_args()
    if not 0 < args.interval_seconds < float("inf"):
        parser.error("interval must be finite and positive")
    previous = None
    baseline = json.loads(args.baseline_report.read_text()) if args.baseline_report else None
    next_checkpoint = args.interval_seconds
    try:
        while True:
            try:
                report = json.loads((args.run_dir / "stress-report.json").read_text())
            except (OSError, ValueError):
                if (args.exit_file or (args.run_dir / "scraper.exit")).exists():
                    raise RuntimeError("Scraper exited without a readable stress report") from None
                time.sleep(10)
                continue
            if baseline is not None:
                report = include_baseline(report, baseline)
            final = report.get("status") != "RUNNING"
            if final or report.get("duration_seconds", 0) >= next_checkpoint:
                snapshot = build_snapshot(report, previous)
                attach_pool_data(snapshot, args.run_dir)
                write_snapshot(args.run_dir, snapshot)
                previous = report
                next_checkpoint = (int(report["duration_seconds"] // args.interval_seconds) + 1) * args.interval_seconds
            if final:
                return
            time.sleep(10)
    finally:
        if args.stop_collector:
            stop_owned_collector(args.run_dir)


if __name__ == "__main__":
    main()
