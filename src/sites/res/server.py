"""Container entry point: search/download/status/export with existing worker CLI options."""

from __future__ import annotations

import argparse
import asyncio
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument(
        "stage",
        choices=[
            "track",
            "benchmark",
            "free-stream",
            "pipeline",
            "collect",
            "discover",
            "verify-burst",
            "bulk-run",
            "documents",
            "search",
            "download",
            "download-ready",
            "status",
            "export",
            "telegram-test",
        ],
    )
    args, remainder = parser.parse_known_args()
    if args.stage == "bulk-run":
        from src.sites.res.bulk_pipeline import main as bulk_pipeline_main

        raise SystemExit(bulk_pipeline_main(remainder))
    if args.stage == "discover":
        from src.sites.res.bulk import main as bulk_main

        raise SystemExit(bulk_main(remainder))
    if args.stage == "verify-burst":
        from src.sites.res.burst_verifier import main as burst_main

        raise SystemExit(burst_main(remainder))
    if args.stage in {"documents", "download-ready"}:
        from src.sites.res.ready_download import main as download_ready_main

        if args.stage == "documents":
            from src.sites.res.bulk import DEFAULT_DB, DEFAULT_RUN

            raise SystemExit(download_ready_main(remainder, default_db=DEFAULT_DB, default_run=DEFAULT_RUN))
        raise SystemExit(download_ready_main(remainder))
    if args.stage == "track":
        from src.sites.res.tracking import main as tracking_main

        raise SystemExit(tracking_main(remainder))
    if args.stage == "telegram-test":
        if remainder:
            parser.error("telegram-test takes no additional options")
        from src.sites.res.telegram import test_delivery

        raise SystemExit(asyncio.run(test_delivery()))
    if args.stage == "free-stream":
        from src.sites.res.free_stream import main as stream_main

        raise SystemExit(stream_main(remainder))
    if args.stage == "benchmark":
        from src.sites.res.benchmark import main as benchmark_main

        raise SystemExit(benchmark_main(remainder))
    if args.stage == "pipeline":
        from src.sites.res.pipeline import main as worker_main
    elif args.stage == "collect":
        from src.sites.dequienes.companies import main as worker_main
    elif args.stage == "download":
        from src.sites.res.download import main as worker_main
    else:
        from src.sites.res.__main__ import main as worker_main

    sys.argv = [sys.argv[0], *remainder]
    if args.stage in {"status", "export"}:
        sys.argv.extend(["--action", args.stage])
    worker_main()


if __name__ == "__main__":
    main()
