"""Container entry point: search/download/status/export with existing worker CLI options."""

from __future__ import annotations

import argparse
import asyncio
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument(
        "stage", choices=["benchmark", "pipeline", "collect", "search", "download", "status", "export", "telegram-test"]
    )
    args, remainder = parser.parse_known_args()
    if args.stage == "telegram-test":
        if remainder:
            parser.error("telegram-test takes no additional options")
        from src.sites.res.telegram import test_delivery

        raise SystemExit(asyncio.run(test_delivery()))
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
