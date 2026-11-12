"""Owned asynchronous browser sessions and per-browser virtual displays."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import ssl
import subprocess
from contextlib import AsyncExitStack, asynccontextmanager


@asynccontextmanager
async def launch_context(profile, *, proxy=None, engine="patchright", headed=False, pdf=False, proxy_ca=None):
    profile.mkdir(parents=True, exist_ok=True, mode=0o700)
    ca_flag = None
    if proxy_ca is not None:
        pem = proxy_ca.read_text(encoding="ascii")
        pinned = "DB8548F8A5B1166536920CCD0473840F7FDBAF165DEDF907B7B52361ABC87B60"
        if hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem)).hexdigest().upper() != pinned:
            raise ValueError("Bright Data CA fingerprint mismatch")

        def configure_ca():
            subprocess.run(
                ["openssl", "verify", "-CAfile", str(proxy_ca), str(proxy_ca)], check=True, capture_output=True, timeout=10
            )
            if engine == "camoufox":
                database = "sql:" + str(profile.resolve())
                if not (profile / "cert9.db").exists():
                    subprocess.run(
                        ["certutil", "-N", "-d", database, "--empty-password"], check=True, capture_output=True, timeout=10
                    )
                subprocess.run(
                    [
                        "certutil",
                        "-A",
                        "-d",
                        database,
                        "-n",
                        "Bright Data 44445 CA",
                        "-t",
                        "C,,",
                        "-i",
                        str(proxy_ca.resolve()),
                    ],
                    check=True,
                    capture_output=True,
                    timeout=10,
                )
                return None
            key = subprocess.run(
                ["openssl", "x509", "-in", str(proxy_ca), "-pubkey", "-noout"], check=True, capture_output=True, timeout=10
            ).stdout
            spki = subprocess.run(
                ["openssl", "pkey", "-pubin", "-outform", "DER"], input=key, check=True, capture_output=True, timeout=10
            ).stdout
            return "--ignore-certificate-errors-spki-list=" + base64.b64encode(hashlib.sha256(spki).digest()).decode()

        ca_flag = await asyncio.to_thread(configure_ca)
    if engine == "camoufox":
        from camoufox.async_api import AsyncCamoufox

        options = {
            "headless": False if headed else "virtual",
            "os": "linux",
            "locale": "es-CL",
            "persistent_context": True,
            "user_data_dir": str(profile.resolve()),
        }
        if proxy:
            options["proxy"] = proxy
        if pdf:
            options.update(accept_downloads=True, firefox_user_prefs={"pdfjs.disabled": True})
        async with AsyncCamoufox(**options) as context:
            yield context
        return
    if engine != "patchright":
        raise ValueError("Unknown browser engine")
    from patchright.async_api import async_playwright

    from src.sites.pep.proxy_tunnel import ProxyTunnel

    # Chromium's PDF viewer otherwise consumes inline PDFs instead of downloading them.
    preferences = profile / "Default" / "Preferences"
    preferences.parent.mkdir(parents=True, exist_ok=True)
    if pdf:
        from src.utils.atomic import atomic_write_json

        data = json.loads(preferences.read_text()) if preferences.exists() else {}
        data.setdefault("plugins", {})["always_open_pdf_externally"] = True
        atomic_write_json(preferences, data)
    async with AsyncExitStack() as stack:
        environment = dict(os.environ)
        if not headed:
            display = await asyncio.create_subprocess_exec(
                "Xvfb",
                "-displayfd",
                "1",
                "-screen",
                "0",
                "1440x1000x24",
                "-nolisten",
                "tcp",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )

            async def stop_display():
                if display.returncode is None:
                    display.terminate()
                    try:
                        await asyncio.wait_for(display.wait(), 5)
                    except TimeoutError:
                        display.kill()
                        await display.wait()

            stack.push_async_callback(stop_display)
            number = (await asyncio.wait_for(display.stdout.readline(), 10)).decode().strip()
            if not number.isdecimal():
                raise RuntimeError("Xvfb did not provide a display")
            environment["DISPLAY"] = ":" + number
            environment.pop("WAYLAND_DISPLAY", None)
        browser_proxy = None
        if proxy:
            tunnel = ProxyTunnel(proxy, timeout=60)
            stack.push_async_callback(tunnel.close)
            browser_proxy = {"server": await tunnel.start()}
        playwright = await stack.enter_async_context(async_playwright())
        context = await playwright.chromium.launch_persistent_context(
            str(profile.resolve()),
            channel="chrome",
            headless=False,
            no_viewport=True,
            chromium_sandbox=True,
            proxy=browser_proxy,
            env=environment,
            accept_downloads=True,
            args=["--disable-quic", "--force-webrtc-ip-handling-policy=disable_non_proxied_udp", "--window-size=1440,1000"]
            + ([ca_flag] if ca_flag else []),
        )
        stack.push_async_callback(context.close)
        yield context
