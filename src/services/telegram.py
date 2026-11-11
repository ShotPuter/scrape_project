"""Shared Telegram transport without application configuration imports."""

from __future__ import annotations

import aiohttp


async def send_telegram(session: aiohttp.ClientSession, token: str, chat_id: str, text: str) -> None:
    # Token-bearing URLs and remote error payloads must never enter logs.
    try:
        async with session.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text[:4000]},
            allow_redirects=False,
        ) as response:
            result = await response.json()
            if response.status != 200 or not isinstance(result, dict) or not result.get("ok"):
                raise RuntimeError("Telegram rejected notification; check credentials and chat access")
    except (aiohttp.ClientError, TimeoutError, ValueError):
        raise RuntimeError("Telegram notification unavailable") from None
