"""Compatibility facade for the SEPREC proxy checks."""

from __future__ import annotations

from typing import Any

from config import settings
from src.proxy.checker import (
    CheckStats,
    Stage1Result,
    Stage2Result,
    classify_exception,
    classify_status,
    compatible_transports,
    retry_delay_for,
    should_retry,
    verify_stage1 as _verify_stage1,
    verify_stage2 as _verify_stage2,
)
from src.services.seprec_proxy_probe import seprec_target_probe


async def verify_stage1(proxy_url: str, **kwargs: Any) -> Stage1Result:
    defaults = {
        "check_url": settings.proxy_stage1_check_url,
        "timeout": settings.proxy_stage1_timeout,
        "connect_timeout": settings.proxy_stage1_connect_timeout,
        "headers": settings.default_headers,
        "impersonate": settings.browser_profile,
    }
    defaults.update({key: value for key, value in kwargs.items() if value is not None})
    return await _verify_stage1(proxy_url, **defaults)


async def verify_stage2(proxy_url: str, **kwargs: Any) -> Stage2Result:
    kwargs.pop("catalog_path", None)  # legacy argument; verification now checks search only
    target = seprec_target_probe(
        kwargs.pop("api_base_url", None) or settings.api_base_url, kwargs.pop("search_path", "publicaciones")
    )
    defaults = {
        "attempts": settings.proxy_stage2_attempts,
        "timeout": settings.proxy_stage2_timeout,
        "connect_timeout": settings.proxy_stage2_connect_timeout,
        "retry_delay": settings.proxy_stage2_retry_delay,
        "max_retry_delay": settings.proxy_stage2_max_retry_delay,
        "headers": settings.default_headers,
        "impersonate": settings.browser_profile,
    }
    defaults.update({key: value for key, value in kwargs.items() if value is not None})
    return await _verify_stage2(proxy_url, target=target, **defaults)


__all__ = [
    "CheckStats",
    "Stage1Result",
    "Stage2Result",
    "classify_exception",
    "classify_status",
    "compatible_transports",
    "retry_delay_for",
    "should_retry",
    "verify_stage1",
    "verify_stage2",
]
