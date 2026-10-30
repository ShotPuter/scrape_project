"""Checkpoint deltas preserve total traffic independently of percentiles."""

from src.network.budget import RequestMetrics
from src.services.stress_monitor import build_snapshot, include_baseline


def test_total_response_bytes_is_not_replaced_by_percentiles():
    metrics = RequestMetrics()
    for size in (100, 200):
        metrics.record({"response_bytes": size, "ok": True})
    result = metrics.snapshot()
    assert result["response_bytes_total"] == 300
    assert isinstance(result["response_bytes"], dict)


def test_checkpoint_counts_only_window_deltas():
    previous = {
        "duration_seconds": 1800,
        "documents_checked": 100,
        "items_failed": 1,
        "pdf_bytes_checked": 1000,
        "http_metrics": {"response_bytes_total": 1500},
    }
    current = {
        "duration_seconds": 3600,
        "documents_checked": 250,
        "items_failed": 3,
        "pdf_bytes_checked": 3000,
        "http_metrics": {"response_bytes_total": 4500},
    }
    snapshot = build_snapshot(current, previous)
    assert snapshot["window_documents"] == 150
    assert snapshot["window_files_per_minute"] == 5
    assert snapshot["api_body_bytes_window"] == 3000
    assert snapshot["pdf_bytes_window"] == 2000
    assert snapshot["window_failures"] == 2


def test_baseline_keeps_totals_but_not_old_status_or_percentiles():
    baseline = {
        "status": "INTERRUPTED",
        "duration_seconds": 600,
        "documents_checked": 10,
        "http_metrics": {"response_bytes_total": 200, "body_read_ms": {"p50": 99}},
    }
    report = {
        "status": "RUNNING",
        "duration_seconds": 1200,
        "documents_checked": 20,
        "http_metrics": {"response_bytes_total": 300, "body_read_ms": {"p50": 5}},
    }
    combined = include_baseline(report, baseline)
    assert combined["status"] == "RUNNING"
    assert combined["documents_checked"] == 30
    assert combined["duration_seconds"] == 1800
    assert combined["http_metrics"]["response_bytes_total"] == 500
    assert combined["http_metrics"]["body_read_ms"] == {"p50": 5}
