#!/usr/bin/env python3
"""Standalone proxy collection task. Writes verified proxies to SQLite."""

from src.utils.runtime import require_pydantic_v2


def main() -> int:
    require_pydantic_v2()
    from src.services.proxy_collector import main as collect_main

    return collect_main()


if __name__ == "__main__":
    raise SystemExit(main())
