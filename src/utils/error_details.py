"""Credential-free exception summaries shared by reports and console output."""

from __future__ import annotations

import asyncio
import os
import re
from urllib.parse import urlsplit, urlunsplit

from src.utils.proxy_url import redact_proxy_credentials


def safe_message(value, *, limit=800):
    text = redact_proxy_credentials(str(value))
    text = re.sub(r"(https?://api\.telegram\.org/bot)[^/\s]+", r"\1***", text)
    for key in ("WEBSHARE_API_TOKEN", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
        secret = os.getenv(key, "").strip()
        if secret:
            text = text.replace(secret, "***")

    def clean_url(match):
        value = match.group(0)
        try:
            parts = urlsplit(value)
            return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
        except ValueError:
            return "[invalid URL]"

    text = re.sub(r"https?://[^\s\"'<>]+", clean_url, text)
    return text[:limit]


def exception_details(exc):
    details = {"type": type(exc).__name__, "message": safe_message(exc)}
    leaves = []

    def visit(error, depth=0):
        if depth > 4 or len(leaves) >= 5:
            return
        if isinstance(error, BaseExceptionGroup):
            for child in error.exceptions:
                visit(child, depth + 1)
        elif error.__cause__ is not None:
            leaves.append({"type": type(error).__name__, "message": safe_message(error)})
            visit(error.__cause__, depth + 1)
        else:
            leaves.append({"type": type(error).__name__, "message": safe_message(error)})

    visit(exc)
    details["causes"] = leaves
    details["summary"] = "; ".join(
        dict.fromkeys(f"{leaf['type']}: {leaf['message'] or 'Operation cancelled'}" for leaf in leaves)
    )[:800]
    return details


def failure_state(exc):
    if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)):
        return "interrupted"
    if isinstance(exc, BaseExceptionGroup):
        states = {failure_state(child) for child in exc.exceptions}
        for state in ("blocked", "captcha_required", "failed", "interrupted"):
            if state in states:
                return state
    return getattr(exc, "state", "failed")
