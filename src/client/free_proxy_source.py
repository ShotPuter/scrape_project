"""Compatibility imports; use src.proxy.sources.free_proxy for new code."""

from src.proxy.sources.free_proxy import (
    FREE_PROXY_SOURCES,
    INSTALL_HINT,
    load_free_proxy_candidates,
    normalise_proxy_address,
)

__all__ = ["FREE_PROXY_SOURCES", "INSTALL_HINT", "load_free_proxy_candidates", "normalise_proxy_address"]
