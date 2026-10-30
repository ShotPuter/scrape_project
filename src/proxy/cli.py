"""SEPREC collector command; application wiring is loaded only when invoked."""

from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    from src.utils.runtime import require_pydantic_v2

    require_pydantic_v2()
    from src.services.proxy_collector import main as collector_main

    return collector_main(argv)
