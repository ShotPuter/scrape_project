"""Shared browser pool, global pacing and durable progress for RES workers."""

from __future__ import annotations

import asyncio
import math
import time
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass

from src.sites.dequienes.companies import exclusive_profile
from src.sites.res.proxies import ProxyRoutes, ProxySetupError
from src.utils.error_details import safe_message


@dataclass(frozen=True)
class BrowserLayout:
    browsers: int = 1
    tabs_per_browser: int = 1

    @property
    def workers(self):
        return self.browsers * self.tabs_per_browser

    def slots(self):
        return [(browser, tab) for browser in range(self.browsers) for tab in range(self.tabs_per_browser)]


def browser_layout(args):
    browsers = getattr(args, "browsers", 1)
    tabs = getattr(args, "tabs_per_browser", None)
    legacy = getattr(args, "workers", None)
    if tabs is not None and legacy is not None:
        raise ValueError("Use --tabs-per-browser or its legacy alias --workers, not both")
    tabs = tabs if tabs is not None else (legacy if legacy is not None else 1)
    if type(browsers) is not int or type(tabs) is not int or min(browsers, tabs) < 1:
        raise ValueError("browsers and tabs-per-browser must be positive integers")
    return BrowserLayout(browsers, tabs)


def add_browser_arguments(parser):
    parser.add_argument("--browser-engine", choices=["patchright", "camoufox"], default="patchright")
    parser.add_argument("--browsers", type=int, default=1, help="Independent browser instances")
    parser.add_argument("--tabs-per-browser", type=int, help="Worker tabs per browser (default: 1)")
    parser.add_argument("--workers", type=int, help="Legacy alias for --tabs-per-browser")
    parser.add_argument("--checkpoint-interval", type=float, default=15, help="Seconds between progress snapshots")
    parser.add_argument(
        "--proxy-provider",
        choices=["auto", "direct", "webshare"],
        default="auto",
        help="auto uses Webshare when WEBSHARE_API_TOKEN is set (also reads .env)",
    )
    parser.add_argument("--telegram-interval-minutes", type=float, help="Telegram status interval (env or default: 5)")
    parser.add_argument("--no-telegram", action="store_true", help="Disable Telegram for this invocation")


def validate_runtime(args):
    browser_layout(args)
    interval = getattr(args, "checkpoint_interval", 15)
    if not math.isfinite(interval) or interval < 1:
        raise ValueError("checkpoint-interval must be finite and at least 1 second")
    telegram_interval = getattr(args, "telegram_interval_minutes", None)
    if telegram_interval is not None and (not math.isfinite(telegram_interval) or telegram_interval < 1):
        raise ValueError("telegram-interval-minutes must be finite and at least 1 minute")


class BrowserStartError(RuntimeError):
    pass


class BrowserPool:
    """Lazy, independent persistent contexts. Tabs remain alive until pool shutdown."""

    def __init__(
        self, profile, layout, *, headed=False, pdf=False, proxy_provider="direct", on_ready=None, engine="camoufox"
    ):
        self.layout, self.headed, self.pdf = layout, headed, pdf
        self.engine = engine
        if engine not in {"patchright", "camoufox"}:
            raise ValueError("Unknown browser engine")
        if engine == "patchright":
            profile = profile.with_name(profile.name + "-patchright")
        self.on_ready = on_ready
        # Browser 1 keeps the old profile path; other slots have stable sibling profiles.
        self.profiles = [profile] + [
            profile.with_name(f"{profile.name}-browser-{index + 1}") for index in range(1, layout.browsers)
        ]
        self.stack = AsyncExitStack()
        self.routes = ProxyRoutes(self.profiles, proxy_provider)
        # Camoufox's native fingerprint setup is not safe to run concurrently in one process.
        # Serialize startup only; all launched browsers continue working concurrently.
        self.launch_lock = asyncio.Lock()
        self.locks = [asyncio.Lock() for _ in self.profiles]
        self.pages = {}
        self.errors = {}

    async def __aenter__(self):
        await self.stack.__aenter__()
        return self

    async def __aexit__(self, *args):
        return await self.stack.__aexit__(*args)

    async def page(self, browser, tab, *, stop=None):
        if not (0 <= browser < self.layout.browsers and 0 <= tab < self.layout.tabs_per_browser):
            raise ValueError("Invalid browser/tab slot")
        async with self.locks[browser], self.launch_lock:
            if stop is not None and stop.is_set():
                return None
            if browser in self.errors:
                raise self.errors[browser]
            if browser not in self.pages:
                profile = self.profiles[browser]
                try:
                    from src.sites.res.engines import launch_context

                    self.stack.enter_context(exclusive_profile(profile))
                    proxy = await self.routes.for_browser(browser)
                    context = await self.stack.enter_async_context(
                        launch_context(profile, proxy=proxy, engine=self.engine, headed=self.headed, pdf=self.pdf)
                    )
                    for extra in list(context.pages)[1:]:
                        await extra.close()
                    pages = [context.pages[0] if context.pages else await context.new_page()]
                    for _ in range(1, self.layout.tabs_per_browser):
                        pages.append(await context.new_page())
                    await self.routes.verify(context, browser)
                    self.pages[browser] = pages
                    print(
                        f"RES browser {browser + 1}/{self.layout.browsers}: {len(pages)} tabs; profile {profile}", flush=True
                    )
                    if self.on_ready is not None:
                        self.on_ready(browser)
                except Exception as exc:
                    error = BrowserStartError(
                        f"RES browser {browser + 1}: {exc}"
                        if isinstance(exc, ProxySetupError)
                        else f"Cannot start RES browser {browser + 1} ({self.engine}): "
                        f"{type(exc).__name__}: {safe_message(exc)}"
                    )
                    self.errors[browser] = error
                    raise error from exc
            return self.pages[browser][tab]


async def wait_turn(store, source, rate, stop):
    delay = store.request_delay(source, rate)
    if delay > 0:
        with suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=delay)
    return not stop.is_set()


def progress_snapshot(store, report):
    row = store.db.execute("SELECT claimed FROM runs WHERE run_id=?", (report["run_id"],)).fetchone()
    report["updated_at"] = time.time()
    report["progress"] = {
        "claimed": row[0] if row else 0,
        "statuses": dict(
            store.db.execute(
                "SELECT status,count(*) FROM jobs j JOIN run_jobs r ON j.id=r.job_id WHERE r.run_id=? GROUP BY status",
                (report["run_id"],),
            )
        ),
    }


def run_incomplete(report):
    return bool(report.get("stop_reason")) or any(
        report["progress"]["statuses"].get(status, 0)
        for status in ("pending", "running", "retry_wait", "failed", "blocked", "captcha_required")
    )


@asynccontextmanager
async def checkpoints(save, interval):
    async def periodic():
        while True:
            await asyncio.sleep(interval)
            save()

    save()
    async with asyncio.TaskGroup() as group:
        task = group.create_task(periodic())
        try:
            yield
        finally:
            task.cancel()
