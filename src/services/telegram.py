"""Shared Telegram transport without application configuration imports."""

from __future__ import annotations

import socket

import aiohttp


class TelegramDeliveryError(RuntimeError):
    """An API failure described without the bot token, chat ID or remote payload."""


def telegram_error_summary(error: Exception) -> str:
    if isinstance(error, TelegramDeliveryError):
        return str(error)
    return "unexpected delivery error"


async def _request(session, token, method, payload):
    # Never propagate token-bearing URLs or untrusted descriptions to callers.
    try:
        async with session.post(
            f"https://api.telegram.org/bot{token}/{method}", json=payload, allow_redirects=False
        ) as response:
            status = response.status
            if status != 200:
                raise TelegramDeliveryError(f"{method}: HTTP {status}")
            result = await response.json()
            if not isinstance(result, dict) or result.get("ok") is not True:
                raise TelegramDeliveryError(f"{method}: invalid Telegram response")
    except TimeoutError:
        raise TelegramDeliveryError(f"{method}: request timed out") from None
    except aiohttp.ClientSSLError:
        raise TelegramDeliveryError(f"{method}: TLS error") from None
    except aiohttp.ClientConnectorError as error:
        reason = "DNS resolution failed" if isinstance(error.os_error, socket.gaierror) else "connection failed"
        raise TelegramDeliveryError(f"{method}: {reason}") from None
    except (aiohttp.ClientError, ValueError):
        raise TelegramDeliveryError(f"{method}: invalid response or network error") from None


async def send_telegram(session: aiohttp.ClientSession, token: str, chat_id: str, text: str) -> None:
    await _request(session, token, "sendMessage", {"chat_id": chat_id, "text": text[:4000]})


async def check_telegram(session: aiohttp.ClientSession, token: str, chat_id: str) -> None:
    """Check bot credentials and whether the configured chat is reachable."""
    await _request(session, token, "getMe", {})
    await _request(session, token, "getChat", {"chat_id": chat_id})
