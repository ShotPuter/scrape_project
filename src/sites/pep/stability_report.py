"""Summarize persistent native/nodriver stability runs offline."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.utils.atomic import atomic_write_json


def summarize(root: Path) -> dict:
    results = []
    for engine in ("nodriver", "native"):
        directory = root / engine
        latest = directory / "benchmark.json"
        if not latest.exists():
            results.append({"engine": engine, "status": "not_run"})
            continue
        report = json.loads(latest.read_text())
        runs = [json.loads(path.read_text()) for path in directory.glob("run-*.json")]
        runs.sort(key=lambda item: item["started_at"])
        first = next((run for run in runs if run.get("pass_kind", "all") == "all"), report)
        results.append({
            "engine": engine, "status": report["status"], "sample_sha256": report.get("sample_sha256"),
            "selected_links": report["selected_unique_links"], "first_pass_completed_links": first["completed_links"],
            "first_pass_finished_links": first["finished_links"], "final_completed_links": report["completed_links"],
            "additional_completed_after_first_pass": max(0, report["completed_links"] - first["completed_links"]),
            "pending_links": report["pending_links"], "job_attempts_total": report.get("job_attempts_total", 0),
            "documents_downloaded": report["unique_documents_downloaded"],
            "documents_discovered": report["unique_documents_discovered"],
            "pdf_files": report["pdf_files"], "document_statuses": report["document_statuses"],
            "failure_stages": report["failure_stages"], "rejection_reasons": report["rejection_reasons"],
            "stop_reason": report["stop_reason"], "runs": len(runs),
            "elapsed_seconds_all_runs": round(sum(run["elapsed_seconds"] for run in runs), 2),
            "adapter_revisions": sorted({run.get("browser_adapter_revision") or "not_recorded" for run in runs}),
            "passes": [{key: run.get(key) for key in (
                "run_id", "pass_kind", "status", "completed_links", "new_downloads_this_run",
                "reused_documents_this_run", "elapsed_seconds", "stop_reason", "browser_adapter_revision",
            )} for run in runs],
        })
    digests = [row.get("sample_sha256") for row in results]
    result = {"results": results, "same_sample": bool(all(digests) and len(set(digests)) == 1),
              "coverage_note": "Small ordered sample; pending/failed discovery may conceal documents. No guarantee."}
    atomic_write_json(root / "comparison.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    report = summarize(args.directory)
    for row in report["results"]:
        print(f"{row['engine']}: {row.get('first_pass_completed_links', 0)} -> "
              f"{row.get('final_completed_links', 0)}/{row.get('selected_links', 0)} complete; {row['status']}")
    print(f"Same sample: {report['same_sample']}; report: {args.directory / 'comparison.json'}")
    raise SystemExit(0 if report["same_sample"] and all(row["status"] == "COMPLETED" for row in report["results"]) else 1)


if __name__ == "__main__":
    main()
