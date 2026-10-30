"""Compatibility imports; use src.network.budget for new code."""

from src.network.budget import (
    NetworkBudget,
    RequestMetrics,
    TimeoutPolicy,
    attempt_context,
    mark,
    proxy_id,
    trace_config,
    work_context,
)

__all__ = [
    "NetworkBudget",
    "RequestMetrics",
    "TimeoutPolicy",
    "attempt_context",
    "mark",
    "proxy_id",
    "trace_config",
    "work_context",
]
