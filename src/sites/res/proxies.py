"""Random Webshare assignment per persistent browser, with credential-free state."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import os
import secrets
from urllib.parse import unquote, urlsplit

from src.utils.atomic import atomic_write_json


class ProxySetupError(ValueError):
    """Sanitized error suitable for reports."""


def proxy_provider(args):
    provider = getattr(args, "proxy_provider", "direct")
    if provider not in {"auto", "direct", "webshare"}:
        raise ProxySetupError("proxy-provider must be auto, direct or webshare")
    if provider in {"auto", "webshare"}:
        from dotenv import load_dotenv

        load_dotenv()
    if provider == "auto":
        return "webshare" if os.getenv("WEBSHARE_API_TOKEN", "").strip() else "direct"
    return provider


def webshare_options(candidate):
    parts = urlsplit(candidate.url)
    if parts.scheme != "http" or not parts.hostname or not parts.port or not parts.username or not parts.password:
        raise ProxySetupError("Webshare returned an invalid HTTP proxy")
    host = f"[{parts.hostname}]" if ":" in parts.hostname else parts.hostname
    server = f"http://{host}:{parts.port}"
    proxy_id = hashlib.sha256(server.encode()).hexdigest()[:24]
    return (
        proxy_id,
        parts.hostname,
        {"server": server, "username": unquote(parts.username), "password": unquote(parts.password)},
    )


class ProxyRoutes:
    def __init__(self, profiles, provider="direct"):
        self.profiles, self.provider = profiles, provider
        self.lock = asyncio.Lock()
        self.assignments = None
        self.public = [{"browser": i + 1, "provider": provider, "verified": False} for i in range(len(profiles))]

    async def for_browser(self, index):
        if self.provider == "direct":
            return None
        async with self.lock:
            if self.assignments is None:
                self.assignments = await self._prepare()
            proxy_id, options = self.assignments[index]
            manifest = self.profiles[index] / "res-proxy.json"
            atomic_write_json(manifest, {"provider": "webshare", "proxy_id": proxy_id})
            self.public[index].update(proxy_id=proxy_id)
            return dict(options)

    async def _prepare(self):
        from src.proxy.sources.webshare import load_webshare_candidates

        # Existing loader bounds pagination, validates endpoints, and redacts provider errors.
        try:
            candidates = await load_webshare_candidates(os.getenv("WEBSHARE_API_TOKEN", ""), mode="direct")
        except Exception as exc:
            raise ProxySetupError(f"Webshare inventory unavailable ({type(exc).__name__}); check token/network") from None
        inventory, hosts = {}, set()
        for candidate in candidates:
            proxy_id, host, options = webshare_options(candidate)
            if host not in hosts:
                hosts.add(host)
                inventory[proxy_id] = options
        # Reserve every persisted slot before choosing new proxies, so resumes cannot collide.
        assignments = [None] * len(self.profiles)
        used = set()
        for index, profile in enumerate(self.profiles):
            manifest = profile / "res-proxy.json"
            if not manifest.exists():
                continue
            try:
                if manifest.stat().st_size > 4096:
                    raise ValueError("oversized assignment")
                saved = json.loads(manifest.read_text())
                proxy_id = saved["proxy_id"]
                if saved["provider"] != "webshare" or proxy_id not in inventory or proxy_id in used:
                    raise ValueError("unavailable or duplicate assignment")
            except (OSError, ValueError, KeyError, TypeError) as exc:
                raise ProxySetupError(
                    f"Cannot reuse Webshare assignment for browser {index + 1}; check its res-proxy.json and inventory"
                ) from exc
            assignments[index] = (proxy_id, inventory[proxy_id])
            used.add(proxy_id)
        available = [proxy_id for proxy_id in inventory if proxy_id not in used]
        if len(available) < assignments.count(None):
            raise ProxySetupError(
                f"Webshare needs {len(self.profiles)} distinct proxy addresses; available: {len(inventory)}"
            )
        secrets.SystemRandom().shuffle(available)
        for index, assignment in enumerate(assignments):
            if assignment is None:
                proxy_id = available.pop()
                assignments[index] = (proxy_id, inventory[proxy_id])
        return assignments

    async def verify(self, context, index):
        if self.provider == "direct":
            return
        page = await context.new_page()
        try:
            response = await page.goto("https://api.ipify.org", wait_until="domcontentloaded", timeout=30000)
            if response is None or response.status != 200:
                raise ProxySetupError("Webshare browser IP check failed")
            address = str(ipaddress.ip_address((await page.locator("body").inner_text()).strip()))
            self.public[index].update(verified=True, exit_ip=address)
        except Exception:
            raise ProxySetupError(f"Webshare browser {index + 1} could not verify its proxy connection") from None
        finally:
            await page.close()
