#!/usr/bin/env python3
"""Thin executable entry point. Use ``python main.py --help`` for usage."""

from src.utils.runtime import require_pydantic_v2


def main() -> int:
    require_pydantic_v2()
    from src.cli import main as cli_main

    return cli_main()


if __name__ == "__main__":
    raise SystemExit(main())
