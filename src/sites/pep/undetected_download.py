"""Experimental UC launcher with the existing CDP download/checkpoint contract.

UC owns Chrome and ChromeDriver; nodriver only attaches to its loopback CDP
endpoint. The site still generates and validates its own reCAPTCHA v3 token.
No OCR solver, token replacement, proxy rotation, or TLS bypass is used.
"""

from __future__ import annotations

import asyncio
import re
import shutil
import subprocess
from contextlib import suppress

from src.sites.pep.nodriver_download import NodriverDownloader


class UndetectedDownloader(NodriverDownloader):
    ADAPTER_REVISION = "undetected-chromedriver-cdp-v1"
    DOWNLOAD_METHOD = "browser_undetected_chromedriver"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.driver = None
        self.launch_task = None

    def create_driver(self, arguments: list[str]):
        try:
            import undetected_chromedriver as uc
        except ImportError:
            raise ValueError("Install requirements-browser-uc.txt to use the undetected engine") from None

        class SandboxedOptions(uc.ChromeOptions):
            def add_argument(self, argument):
                # UC 3.5.5 adds --no-sandbox even when no_sandbox=False.
                # Keep the same sandbox policy as the nodriver backend.
                if argument != "--no-sandbox":
                    super().add_argument(argument)

        executable = shutil.which("google-chrome")
        version = subprocess.run([executable, "--version"], capture_output=True, text=True, check=True, timeout=10)
        match = re.search(r"\b(\d+)\.", version.stdout)
        if not match:
            raise ValueError("Cannot determine installed Chrome's major version")
        options = SandboxedOptions()
        for argument in arguments:
            options.add_argument(argument)
        options.add_argument("--remote-debugging-address=127.0.0.1")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--start-maximized")
        options.set_capability("acceptInsecureCerts", False)
        return uc.Chrome(
            options=options, browser_executable_path=executable, user_data_dir=str(self.profile.resolve()),
            headless=False, no_sandbox=False, use_subprocess=True, version_main=int(match.group(1)),
        )

    async def launch_browser(self, arguments: list[str]):
        import nodriver

        # Keep the task alive on cancellation so close() can reap a late launch.
        self.launch_task = asyncio.create_task(asyncio.to_thread(self.create_driver, arguments))
        self.driver = await asyncio.shield(self.launch_task)
        host, port = self.driver.options.debugger_address.rsplit(":", 1)
        if host != "127.0.0.1":
            raise ValueError("UC debugging endpoint must be loopback-only")
        return await nodriver.start(host=host, port=int(port), headless=False,
                                    user_data_dir=str(self.profile.resolve()), sandbox=True)

    async def close(self) -> None:
        try:
            if self.launch_task is not None and self.driver is None:
                # Launcher may fail before creating Chrome.
                with suppress(Exception):
                    self.driver = await asyncio.shield(self.launch_task)
            try:
                if self.browser:
                    try:
                        await self.browser.aclose()
                    finally:
                        # Attached nodriver must not later stop UC-owned Chrome.
                        from nodriver.core.util import get_registered_instances

                        get_registered_instances().discard(self.browser)
            finally:
                if self.driver:
                    await asyncio.to_thread(self.driver.quit)
        finally:
            if self.tunnel:
                await self.tunnel.close()
            self.driver = None
            self.launch_task = None
            self.browser = None
            self.process = None
            self.page = None
            self.loaded_url = None
            self.loaded_at = None
            self.pointer_position = None
            self.session_initialized = False
            self.warmup_http_status = None
