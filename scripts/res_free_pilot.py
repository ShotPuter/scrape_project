"""Free RES pilot: a local browser mints one reCAPTCHA token, plain HTTP does the rest.

No paid CAPTCHA solver is required. Configured proxies may incur provider charges. The local cascade is:

    native click  ->  local VLM grid  ->  audio + local Whisper  ->  operator

A browser is used only to obtain a token for one PageMethod call. Everything else
(search, company details, verification, PDF download) is plain HTTP. The `codigo`
returned by verification is cached, so valid links can be downloaded again with a
bare GET. Expired links require another verification.

Examples:
    scripts/res_free_pilot.sh --limit 5 --headed --operator-wait 300
    scripts/res_free_pilot.sh --limit 5 --vlm-model qwen2.5vl:7b --audio-model small
    scripts/res_free_pilot.sh --cve ACuKDHhqsvAd
    scripts/res_free_pilot.sh --limit 0 --http-only      # re-download from the codigo cache
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import ipaddress
import json
import os
import sqlite3
import ssl
import time
from collections import Counter, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

import httpx
from dotenv import load_dotenv

from src.proxy.sources.webshare import WebshareError, load_webshare_candidates
from src.sites.dequienes.source import RES_URL, validate_cve
from src.sites.res import http_flow
from src.sites.res.audio import LocalAudioSolver
from src.sites.res.engines import launch_context
from src.sites.res.pdf import cached_pdf, persist_pdf
from src.sites.res.source import FORM_URL, HOST, SearchIssue, match_actions
from src.sites.res.vlm_grid import LocalGridSolver, bframe
from src.utils.atomic import atomic_write_json
from src.utils.error_details import safe_message
from src.utils.proxy_url import normalize_proxy_url

DOWNLOADED = {"downloaded", "reused", "downloaded_from_cache"}
DECLINED = ".rc-doscaptcha-header-text"
POLICY_ERRORS = {"captcha_required", "blocked", "proxy_exhausted", "proxy_failed", "proxy_ip_mismatch"}


@dataclass(frozen=True, slots=True)
class ProxyEndpoint:
    """One egress IP shared by the browser and the plain-HTTP session."""

    url: str | None = field(repr=False)
    browser: dict | None = field(repr=False)
    label: str
    expected_ip: str | None = None
    ca_file: Path | None = None
    dynamic_ip: bool = False


DIRECT = ProxyEndpoint(None, None, "direct")


def endpoint_from_url(url: str, expected_ip: str | None = None) -> ProxyEndpoint:
    """Build matching httpx and Camoufox proxy settings without leaking userinfo."""
    normalized = normalize_proxy_url(url)
    if normalized is None:
        raise ValueError("Invalid proxy URL")
    parts = urlsplit(normalized)
    host = f"[{parts.hostname}]" if ":" in (parts.hostname or "") else parts.hostname
    browser = {"server": f"http://{host}:{parts.port}"}
    if parts.username:
        browser["username"] = unquote(parts.username)
        browser["password"] = unquote(parts.password or "")
    return ProxyEndpoint(normalized, browser, f"{parts.hostname}:{parts.port}", expected_ip)


def load_proxy_file(path: Path) -> list[ProxyEndpoint]:
    """Residential/static list: one ``host:port`` or proxy URL per line."""
    if not path.exists():
        raise ValueError(f"Proxy list not found: {path}")
    endpoints = []
    seen = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        endpoint = endpoint_from_url(value)
        if endpoint.url not in seen:
            seen.add(endpoint.url)
            endpoints.append(endpoint)
    return endpoints


async def load_proxy_endpoints(args) -> list[ProxyEndpoint]:
    if args.proxy_provider == "direct":
        return [DIRECT]
    if args.proxy_provider == "brightdata":
        if getattr(args, "lazy_proxies", False):
            args.proxy_setup_failures = []
            return [None] * args.proxy_limit
        from scripts.res_audio_pilot import ProxyPreflightError, brightdata_proxy

        endpoints = []
        args.proxy_setup_failures = []
        for index in range(args.proxy_limit or 1):
            try:
                settings, expected, _ = await asyncio.to_thread(brightdata_proxy, args.output)
            except ProxyPreflightError as exc:
                if exc.code == "ip_blacklisted":
                    raise
                args.proxy_setup_failures.append({"slot": index + 1, "code": exc.code})
                say(f"Bright Data session {index + 1}: {exc.code}; skipped")
                continue
            except (ValueError, FileNotFoundError):
                raise
            except Exception as exc:
                args.proxy_setup_failures.append({"slot": index + 1, "error_type": type(exc).__name__})
                say(f"Bright Data session {index + 1}: {type(exc).__name__}; skipped")
                continue
            parts = urlsplit(settings["server"])
            url = (
                parts.scheme
                + "://"
                + quote(settings["username"], safe="")
                + ":"
                + quote(settings["password"], safe="")
                + "@"
                + parts.netloc
            )
            endpoint = endpoint_from_url(url, expected)
            endpoints.append(
                ProxyEndpoint(
                    endpoint.url, endpoint.browser, endpoint.label, expected, Path("docker/brightdata-proxy-ca.crt"), True
                )
            )
    elif args.proxy_provider == "residential":
        endpoints = load_proxy_file(args.proxy_file)
    elif args.proxy_provider == "webshare":
        token = os.getenv("WEBSHARE_API_TOKEN", "").strip()
        if not token:
            raise ValueError("WEBSHARE_API_TOKEN is required for --proxy-provider webshare")
        try:
            candidates = await load_webshare_candidates(token, mode="direct")
        except WebshareError as exc:
            raise ValueError(str(exc)) from exc
        endpoints = [
            endpoint_from_url(candidate.url, expected_ip=urlsplit(candidate.url).hostname) for candidate in candidates
        ]
    else:
        raise ValueError("Unknown proxy provider")
    if args.proxy_limit:
        endpoints = endpoints[: args.proxy_limit]
    if not endpoints:
        raise ValueError("The proxy provider returned no usable endpoints")
    return endpoints


async def resolve_proxy(endpoint, args):
    if endpoint is not None:
        return endpoint
    single = copy.copy(args)
    single.lazy_proxies, single.proxy_limit = False, 1
    try:
        return (await load_proxy_endpoints(single))[0]
    finally:
        args.proxy_setup_failures.extend(getattr(single, "proxy_setup_failures", []))


def http_exit_ip(http: httpx.Client) -> str:
    response = http_flow.get(http, "https://api.ipify.org?format=json", timeout=20)
    response.raise_for_status()
    return str(ipaddress.ip_address(response.json()["ip"]))


async def browser_exit_ip(page) -> str:
    await page.goto("https://api.ipify.org?format=json", wait_until="commit", timeout=30000)
    return str(ipaddress.ip_address(json.loads(await page.locator("body").inner_text())["ip"]))


def say(message):
    print(message, flush=True)


def anchor(page):
    return page.frame_locator('iframe[title="reCAPTCHA"]').locator("#recaptcha-anchor")


async def open_token_form(page, url, budget_seconds):
    """Only the genuine widget must be ready; HTTP submits company/CVE fields."""
    started = time.monotonic()
    timeout_ms = int(budget_seconds * 1000)
    try:
        response = await page.goto(url, wait_until="commit", timeout=timeout_ms)
        if not response or response.status != 200:
            status = response.status if response else 0
            raise SearchIssue("blocked" if status in {403, 429} else "proxy_failed", f"Form HTTP {status}")
        if page.url != url:
            raise SearchIssue("blocked", "Unexpected redirect from the RES token form")
        remaining = max(1, timeout_ms - int((time.monotonic() - started) * 1000))
        await anchor(page).wait_for(state="visible", timeout=remaining)
        if page.url != url:
            raise SearchIssue("blocked", "RES token form redirected while loading")
    except SearchIssue:
        raise
    except Exception as exc:
        raise SearchIssue("proxy_failed", f"RES widget readiness failed ({type(exc).__name__})") from exc


async def token_asset_route(route):
    """Avoid legacy RES UI parser blockers; keep original form and Google widget."""
    request = route.request
    if urlsplit(request.url).hostname == HOST and request.resource_type in {
        "script",
        "stylesheet",
        "image",
        "font",
        "media",
    }:
        await route.abort()
    else:
        await route.continue_()


async def save_form_diagnostic(page, args):
    root = args.output / "diagnostics"
    root.mkdir(exist_ok=True, mode=0o700)
    prefix = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    try:
        (root / (prefix + ".html")).write_text(await page.content(), encoding="utf-8")
        await page.screenshot(path=str(root / (prefix + ".png")), timeout=3000)
    except Exception:
        pass


async def bframe_has(page, selector):
    frame = bframe(page)
    if frame is None:
        return False
    try:
        locator = frame.locator(selector)
        if not await locator.count():
            return False
        return await locator.first.is_visible()
    except Exception:
        return False


async def grid_ready(page):
    """A real tile grid; the bframe itself pre-exists and means nothing."""
    return await bframe_has(page, "td.rc-imageselect-tile")


async def wait_for(page, selector, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if await bframe_has(page, selector):
            return bframe(page)
        await asyncio.sleep(0.25)
    return None


async def read_token(page, seconds=10.0):
    """The token is the only thing the browser is kept for; never click the site button."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        token = ""
        try:
            token = await page.locator("#g-recaptcha-response").first.input_value()
        except Exception:
            token = ""
        if not token:
            try:
                token = await page.evaluate(
                    "() => { try { return window.grecaptcha.getResponse(); } catch (e) { return ''; } }"
                )
            except Exception:
                token = ""
        token = (token or "").strip()
        if len(token) >= 40:
            return token
        await asyncio.sleep(0.5)
    raise SearchIssue("captcha_required", "The page did not expose a reCAPTCHA token")


async def fill_search(page, company):
    await page.locator("#ddlTipoRegistro").select_option("1")
    rut = (company.get("rut") or "").strip()
    await page.locator("#ddlTipoBusqueda").select_option("1" if rut else "2")
    await page.locator("#txtRut" if rut else "#txtRSocial").fill(rut or company["company_name"].strip())


async def fill_verify(page, cve):
    await page.locator("#boxer_content_txtCve").fill(cve)


class Cascade:
    """Try the cheapest local stage first and record what happened at every step."""

    def __init__(self, page, args, vlm_model, audio, counters, records, proxy_label=""):
        self.page = page
        self.args = args
        self.audio = audio
        self.counters = counters
        self.records = records
        self.proxy_label = proxy_label
        self.vlm = None
        if vlm_model:
            self.vlm = LocalGridSolver(vlm_model, endpoint=args.vlm_url, rounds=args.vlm_rounds, events=[])
        self.events = []
        self.last_stage = None
        self.minted = 0

    async def accepted(self):
        try:
            return await anchor(self.page).get_attribute("aria-checked", timeout=3000) == "true"
        except Exception:
            return False

    async def native(self):
        try:
            await anchor(self.page).click(timeout=15000)
        except Exception as exc:
            raise SearchIssue("captcha_required", "reCAPTCHA anchor is not clickable") from exc
        # The checkbox can take a couple of seconds to resolve, and the audio/verify
        # UI may render briefly even when the click eventually passes.
        deadline = time.monotonic() + self.args.native_wait
        while time.monotonic() < deadline:
            if await self.accepted():
                return
            if await grid_ready(self.page) or await bframe_has(self.page, DECLINED):
                break
            await asyncio.sleep(0.5)
        raise SearchIssue("captcha_required", "A visual challenge is required")

    async def vlm_stage(self):
        frame = await wait_for(self.page, "td.rc-imageselect-tile", min(self.args.timeout, 20))
        if frame is None:
            raise SearchIssue("captcha_required", "No visual tile grid for the local VLM")
        if not await self.vlm.solve(self.page):
            raise SearchIssue("captcha_required", "Local VLM did not pass the challenge")

    async def audio_stage(self):
        frame = await wait_for(self.page, "#recaptcha-audio-button", min(self.args.timeout, 15))
        if frame is None:
            raise SearchIssue("captcha_required", "Audio challenge was not offered")
        await self.audio(
            self.page, timeout_ms=int(self.args.timeout * 1000), captcha_seconds=self.args.audio_seconds, click_anchor=False
        )

    async def operator_stage(self):
        say("  OPERATOR: solve the CAPTCHA in the open browser window")
        deadline = time.monotonic() + self.args.operator_wait
        while time.monotonic() < deadline:
            if await self.accepted():
                return
            await asyncio.sleep(0.5)
        raise SearchIssue("captcha_required", "Operator did not solve the CAPTCHA in time")

    async def run_stages(self):
        stages = [("native", self.native)]
        if self.vlm is not None:
            stages.append(("vlm", self.vlm_stage))
        if self.audio is not None:
            stages.append(("audio", self.audio_stage))
        if self.args.operator_wait > 0:
            stages.append(("operator", self.operator_stage))
        failures = []
        for name, stage in stages:
            try:
                await stage()
            except SearchIssue as exc:
                self.events.append({"stage": name, "status": exc.status, "error": safe_message(exc)})
                failures.append(name)
            except Exception as exc:
                self.events.append({"stage": name, "status": "error", "error": safe_message(exc)})
                failures.append(name)
            else:
                if await self.accepted():
                    self.events.append({"stage": name, "status": "accepted"})
                    self.last_stage = name
                    return
                self.events.append({"stage": name, "status": "not_accepted"})
                failures.append(name)
        raise SearchIssue("captcha_required", "No stage accepted the challenge: " + ", ".join(failures))

    async def mint(self, url, fill=None, *, attempts=None):
        """Open the form, pass the local cascade, return a single-use token."""
        if self.args.solves_per_proxy and self.minted >= self.args.solves_per_proxy:
            raise SearchIssue("proxy_exhausted", "The per-proxy token budget is exhausted")
        attempts = attempts if attempts is not None else self.args.mint_attempts
        for attempt in range(1, attempts + 1):
            self.events = []
            vlm_start = len(self.vlm.events) if self.vlm is not None else 0
            audio_start = len(self.audio.events) if self.audio is not None else 0
            self.audio_event_start = audio_start
            started = time.monotonic()
            try:
                await open_token_form(self.page, url, self.args.timeout)
            except SearchIssue as exc:
                self.events.append({"stage": "form", "status": exc.status, "error": safe_message(exc)})
                await save_form_diagnostic(self.page, self.args)
                raise
            if fill is not None:
                await fill(self.page)
            try:
                await self.run_stages()
                token = await read_token(self.page)
            except SearchIssue:
                if attempt >= attempts:
                    raise
                say("  no local stage passed; reloading the form")
                await asyncio.sleep(self.args.delay)
                continue
            self.minted += 1
            self.counters["tokens"] += 1
            seconds = round(time.monotonic() - started, 2)
            say(f"  token via {self.last_stage} in {seconds}s")
            record = {
                "url": urlsplit(url).path,
                "stage": self.last_stage,
                "seconds": seconds,
                "proxy": self.proxy_label,
                "stages": list(self.events),
            }
            if self.vlm is not None:
                record["vlm_events"] = self.vlm.events[vlm_start:]
            if self.audio is not None:
                record["audio_events"] = self.audio.events[audio_start:]
            self.records.append(record)
            await asyncio.sleep(self.args.delay)
            return token
        raise SearchIssue("captcha_required", "Could not mint a token")


class HttpOnlyCascade:
    """Cached codigos only: any cache miss is reported instead of opening a browser."""

    def __init__(self, counters, records):
        self.counters = counters
        self.records = records
        self.events = []
        self.minted = 0

    async def mint(self, url, fill=None, **_):
        raise SearchIssue("captcha_required", "HTTP-only mode: no cached codigo for this item")


def load_state(path: Path) -> dict:
    if not path.exists():
        return {"cves": {}, "companies": {}}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except ValueError:
        return {"cves": {}, "companies": {}}
    if not isinstance(state, dict):
        return {"cves": {}, "companies": {}}
    state.setdefault("cves", {})
    state.setdefault("companies", {})
    return state


def save_state(path: Path, state: dict) -> None:
    atomic_write_json(path, state)


def load_companies(database: Path, limit: int) -> list[dict]:
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute("SELECT company_id, company_name, rut FROM companies ORDER BY company_name").fetchall()
    companies = [dict(row) for row in rows]
    return companies[:limit] if limit else companies


def load_audio(args):
    if not args.audio_model:
        return None, None
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        return None, "faster-whisper is not installed: .venv/bin/pip install faster-whisper"
    model = WhisperModel(
        args.audio_model,
        device=args.audio_device,
        compute_type=args.audio_compute,
        download_root=os.getenv("RES_AUDIO_MODEL_PATH", "data/res-audio-models"),
        cpu_threads=int(os.getenv("RES_AUDIO_THREADS", "2")),
    )
    return LocalAudioSolver(model, rounds=args.audio_rounds), None


def ollama_models(url):
    try:
        response = httpx.get(f"{url.rstrip('/')}/api/tags", timeout=3.0)
        response.raise_for_status()
        return [str(item.get("name", "")) for item in response.json().get("models", [])]
    except Exception:
        return None


def document_summary(saved: dict) -> dict:
    return {
        "cve": saved.get("cve"),
        "status": saved.get("status", "downloaded"),
        "size_bytes": saved.get("size_bytes"),
        "local_path": saved.get("local_path"),
        "download_url": saved.get("download_url"),
        "reused": saved.get("reused", False),
    }


async def ensure_document(cascade, http, cve, company_id, state, args, output, counters):
    """Cached codigo -> free GET; otherwise one browser token and one verify POST."""
    reused = cached_pdf(output, cve)
    if reused:
        counters["reused"] += 1
        return document_summary(reused)
    record = state["cves"].get(cve)
    if record:
        try:
            data = measured(counters, "download", http_flow.download, http, record["link"])
        except SearchIssue as exc:
            if exc.status in {"blocked", "proxy_failed", "proxy_ip_mismatch"}:
                raise
            say(f"  cached codigo for {cve} is stale ({safe_message(exc)}); re-verifying")
            state["cves"].pop(cve, None)
            save_state(args.state_path, state)
        else:
            saved = persist_pdf(output, cve, data, record["link"])
            counters["downloaded"] += 1
            return document_summary(saved)

    token = await cascade.mint(RES_URL)
    cve, codigo, link = measured(counters, "verify", http_flow.verify, http, cve, token)
    state["cves"][cve] = {"cve": cve, "codigo": codigo, "link": link, "company_id": company_id, "verified_at": time.time()}
    save_state(args.state_path, state)
    counters["codigos"] += 1
    say(f"  codigo saved for {cve}")
    data = measured(counters, "download", http_flow.download, http, link)
    saved = persist_pdf(output, cve, data, link)
    counters["downloaded"] += 1
    return document_summary(saved)


def measured(counters, stage, function, *args, **kwargs):
    started = time.monotonic()
    try:
        return function(*args, **kwargs)
    finally:
        counters[f"http_{stage}_seconds"] += time.monotonic() - started
        counters[f"http_{stage}_calls"] += 1


def cached_company_result(company, state, args, output):
    cached = state["companies"].get(company["company_id"])
    if not cached or args.refresh or "cves" not in cached:
        return None
    status = cached.get("status", "completed")
    if status in {"not_found", "no_cve_found", "ambiguous_match"}:
        return {**company, "status": status, "search_status": status, "documents": []}
    cves = cached["cves"][: args.max_documents] if args.max_documents else cached["cves"]
    docs = [cached_pdf(output, cve) for cve in cves]
    if status == "completed" and cves and all(docs):
        return {
            **company,
            "status": "downloaded",
            "search_status": status,
            "documents": [document_summary(doc) for doc in docs],
        }
    return None


async def process_company(cascade, http, company, state, args, output, counters):
    company_id = company["company_id"]
    cached = state["companies"].get(company_id)
    if cached and not args.refresh and "cves" in cached:
        cves = cached["cves"]
        status = cached.get("status", "completed")
        say(f"  cached search: {status}, {len(cves)} CVE(s), no CAPTCHA")
    else:
        token = await cascade.mint(FORM_URL)
        link = measured(
            counters, "search", http_flow.search, http, token=token, rut=company.get("rut"), name=company["company_name"]
        )
        actions = measured(counters, "details", http_flow.details, http, link)
        status, matched = match_actions(actions, company)
        cves = sorted({action["cve"] for action in matched if action["cve"]})
        state["companies"][company_id] = {
            "company_name": company["company_name"],
            "rut": company.get("rut"),
            "status": status,
            "cves": cves,
            "searched_at": time.time(),
        }
        save_state(args.state_path, state)
    result = {
        "company_id": company_id,
        "company_name": company["company_name"],
        "rut": company.get("rut"),
        "search_status": status,
        "documents": [],
    }
    if status not in {"completed", "no_cve_found"}:
        result["status"] = status
        return result
    selected = cves[: args.max_documents] if args.max_documents else cves
    for cve in selected:
        if args.http_only and cve not in state["cves"]:
            continue
        try:
            document = await ensure_document(cascade, http, cve, company_id, state, args, output, counters)
        except SearchIssue as exc:
            counters["document_failures"] += 1
            result["documents"].append({"cve": cve, "status": exc.status, "error": safe_message(exc)})
            if exc.status in {"blocked", "proxy_exhausted", "proxy_failed"}:
                raise
            continue
        result["documents"].append(document)
    downloaded = [doc for doc in result["documents"] if doc.get("status") in DOWNLOADED]
    result["status"] = "downloaded" if downloaded else (status if not result["documents"] else "documents_failed")
    return result


def new_stats(index: int, endpoint: ProxyEndpoint) -> dict:
    return {
        "index": index,
        "label": endpoint.label,
        "expected_ip": endpoint.expected_ip,
        "http_ip": None,
        "browser_ip": None,
        "status": "preparing",
        "tokens": 0,
        "companies": 0,
        "challenges": 0,
        "error": None,
    }


@asynccontextmanager
async def endpoint_session(index, endpoint, args, counters, token_records, audio, vlm_model, stats):
    """One proxy: verify both transports share its IP, then yield a ready cascade."""
    cascade = None
    verify = True
    if endpoint.ca_file:
        verify = ssl.create_default_context()
        verify.load_verify_locations(cafile=str(endpoint.ca_file))
    http = http_flow.new_session(timeout=args.timeout, proxy=endpoint.url, verify=verify)
    try:
        await asyncio.to_thread(http_flow.prepare, http)
        stats["http_ip"] = await asyncio.to_thread(http_exit_ip, http)
        stats["preflight_ip_changed"] = bool(endpoint.expected_ip and stats["http_ip"] != endpoint.expected_ip)
        if endpoint.expected_ip and stats["http_ip"] != endpoint.expected_ip and not endpoint.dynamic_ip:
            raise SearchIssue(
                "proxy_ip_mismatch", f"Proxy {endpoint.label} exits as {stats['http_ip']}, not {endpoint.expected_ip}"
            )
        if args.http_only:
            cascade = HttpOnlyCascade(counters, token_records)
            stats["status"] = "ready"
            yield cascade, http
            return
        profile = args.profile
        if endpoint.url is not None:
            profile = args.profile / "workers" / f"w{index % args.profile_slots:02d}"
        async with launch_context(
            profile,
            engine=args.engine,
            headed=args.headed,
            proxy=endpoint.browser,
            **({"proxy_ca": endpoint.ca_file} if endpoint.ca_file else {}),
        ) as context:
            page = await context.new_page()
            try:
                stats["browser_ip"] = await browser_exit_ip(page)
                if stats["browser_ip"] != stats["http_ip"]:
                    raise SearchIssue(
                        "proxy_ip_mismatch", f"Browser exits as {stats['browser_ip']} but HTTP exits as {stats['http_ip']}"
                    )
                cascade = Cascade(page, args, vlm_model, audio, counters, token_records, proxy_label=endpoint.label)
                await page.route("**/*", token_asset_route)
                stats["status"] = "ready"
                yield cascade, http
            finally:
                await page.close()
    finally:
        stats["tokens"] = cascade.minted if cascade is not None else 0
        http.close()


def failure_result(company, exc, cascade):
    return {
        "company_id": company["company_id"],
        "company_name": company["company_name"],
        "rut": company.get("rut"),
        "status": getattr(exc, "status", "failed"),
        "error": safe_message(exc),
        "stages": list(cascade.events) if cascade is not None else [],
        "audio_events": list(cascade.audio.events[getattr(cascade, "audio_event_start", 0) :])
        if getattr(cascade, "audio", None) is not None
        else [],
        "documents": [],
    }


async def drain(cascade, http, queue, deferred, state, args, counters, report, stats, outcomes, runtime):
    """Work through the queue on one proxy until its budget or challenge streak ends."""
    challenge_streak = 0

    def checkpoint_attempt(company, attempt, result):
        report.setdefault("attempts", []).append(
            {
                "company_id": company["company_id"],
                "company_name": company["company_name"],
                "attempt": attempt,
                "proxy_index": stats["index"],
                "seconds": round(time.monotonic() - attempt_started, 3),
                "result": result,
            }
        )
        report["companies"] = list(outcomes.values())
        report["statuses"] = dict(Counter(item["status"] for item in outcomes.values()))
        report["progress"] = {
            "completed_companies": len(outcomes),
            "tokens": counters["tokens"],
            "downloaded": counters["downloaded"],
            "reused": counters["reused"],
        }
        atomic_write_json(args.output / f"report-{report['run_id']}.json", report)

    while queue:
        if runtime["stop"]:
            stats["status"] = "stopped"
            return
        if not args.http_only and args.solves_per_proxy and cascade.minted >= args.solves_per_proxy:
            stats["status"] = "budget"
            return
        company, attempts = queue.popleft()
        attempt_started = time.monotonic()
        say(f"[{stats['index'] + 1}] {company['company_name']} (try {attempts + 1}/{args.company_attempts})")
        result = None
        try:
            result = await process_company(cascade, http, company, state, args, args.output, counters)
        except SearchIssue as exc:
            if exc.status != "proxy_exhausted":
                checkpoint_attempt(company, attempts + 1, failure_result(company, exc, cascade))
            if exc.status == "proxy_exhausted":
                queue.appendleft((company, attempts))
                stats["status"] = "budget"
                return
            if (
                exc.status in {"captcha_required", "proxy_failed", "proxy_ip_mismatch", "retry_wait"}
                and attempts + 1 < args.company_attempts
            ):
                deferred.append((company, attempts + 1))
                runtime["failures"] += 1
                if runtime["failures"] >= args.max_failures:
                    runtime["stop"], stats["status"] = True, "stopped"
                    return
                stats["challenges"] += 1
                say(f"  {exc.status}: requeued for a fresh proxy")
                challenge_streak += 1
                if args.challenges_per_proxy and challenge_streak >= args.challenges_per_proxy:
                    stats["status"] = "challenged"
                    return
                continue
            counters["company_failures"] += 1
            result = failure_result(company, exc, cascade)
        except Exception as exc:
            counters["company_failures"] += 1
            result = failure_result(company, exc, cascade)
            checkpoint_attempt(company, attempts + 1, result)
        else:
            checkpoint_attempt(company, attempts + 1, result)
            document_challenge = any(document.get("status") == "captcha_required" for document in result["documents"])
            if document_challenge and attempts + 1 < args.company_attempts:
                deferred.append((company, attempts + 1))
                runtime["failures"] += 1
                if runtime["failures"] >= args.max_failures:
                    runtime["stop"], stats["status"] = True, "stopped"
                    return
                stats["challenges"] += 1
                say("  captcha_required while verifying: requeued for a fresh proxy")
                challenge_streak += 1
                if args.challenges_per_proxy and challenge_streak >= args.challenges_per_proxy:
                    stats["status"] = "challenged"
                    return
                continue
            challenge_streak = 0
        outcomes[company["company_id"]] = result
        stats["companies"] += 1
        counters["company_status:" + result["status"]] += 1
        save_state(args.state_path, state)
        report["companies"] = list(outcomes.values())
        report["statuses"] = dict(Counter(item["status"] for item in outcomes.values()))
        report["progress"] = {
            "completed_companies": len(outcomes),
            "tokens": counters["tokens"],
            "downloaded": counters["downloaded"],
            "reused": counters["reused"],
        }
        atomic_write_json(args.output / f"report-{report['run_id']}.json", report)
        if result["status"] in {
            "captcha_required",
            "blocked",
            "failed",
            "retry_wait",
            "proxy_exhausted",
            "proxy_failed",
            "proxy_ip_mismatch",
            "documents_failed",
        }:
            runtime["failures"] += 1
            if runtime["failures"] >= args.max_failures:
                say(f"stopping after {runtime['failures']} consecutive failures; state is saved")
                runtime["stop"] = True
        else:
            runtime["failures"] = 0
        if result["status"] in {"captcha_required", "proxy_failed", "proxy_ip_mismatch", "failed"} or any(
            doc.get("status") == "captcha_required" for doc in result.get("documents", [])
        ):
            stats["challenges"] += 1
            challenge_streak += 1
            if args.challenges_per_proxy and challenge_streak >= args.challenges_per_proxy:
                stats["status"] = "challenged"
                return
    stats["status"] = "done"


async def scan(proxies, args, state, counters, report, audio, vlm_model, token_records):
    if args.cve:
        await scan_single(proxies, args, state, counters, report, audio, vlm_model, token_records)
        return
    companies = load_companies(args.db, args.limit)
    if not companies:
        say("no companies selected")
        return
    selected = []
    skipped = 0
    for company in companies:
        if args.http_only and not state["companies"].get(company["company_id"], {}).get("cves"):
            skipped += 1
            continue
        selected.append(company)
    if skipped:
        counters["skipped"] = skipped
        say(f"skipped {skipped} company(ies) without a cached search (HTTP-only)")
    if not selected:
        say("nothing to do")
        return
    say(f"{len(selected)} company(ies), {len(proxies)} proxy endpoint(s), state={args.state_path}")

    queue = deque((company, 0) for company in selected)
    deferred = deque()
    outcomes = {}
    runtime = {"failures": 0, "stop": False}
    for index, slot in enumerate(proxies):
        if not queue or runtime["stop"]:
            break
        stats = new_stats(index, slot or ProxyEndpoint(None, None, "brightdata"))
        report["proxies"].append(stats)
        try:
            endpoint = await resolve_proxy(slot, args)
            stats.update(new_stats(index, endpoint))
            async with endpoint_session(index, endpoint, args, counters, token_records, audio, vlm_model, stats) as (
                cascade,
                http,
            ):
                atomic_write_json(args.output / f"report-{report['run_id']}.json", report)
                await drain(cascade, http, queue, deferred, state, args, counters, report, stats, outcomes, runtime)
        except SearchIssue as exc:
            stats["status"] = exc.status
            stats["error"] = safe_message(exc)
            runtime["failures"] += 1
        except Exception as exc:
            stats["status"] = getattr(exc, "status", "proxy_failed")
            stats["error"] = safe_message(exc)
            runtime["failures"] += 1
            if stats["status"] == "proxy_auth_failed":
                runtime["stop"] = True
        if runtime["failures"] >= args.max_failures:
            runtime["stop"] = True
        if deferred:
            # Give every selected company its first attempt before spending routes on retries.
            queue.extend(deferred)
            deferred.clear()
        say(
            f"proxy {index + 1}/{len(proxies)} {stats['label']}: {stats['status']}, "
            f"{stats['tokens']} token(s), {stats['companies']} company(ies)"
        )

    leftover = "stopped" if runtime["stop"] else "proxy_exhausted"
    while queue:
        company, _ = queue.popleft()
        outcomes.setdefault(
            company["company_id"],
            {
                "company_id": company["company_id"],
                "company_name": company["company_name"],
                "rut": company.get("rut"),
                "status": leftover,
                "error": "No usable proxy remained before this company was processed",
                "documents": [],
            },
        )
    report["companies"] = [outcomes[company["company_id"]] for company in selected if company["company_id"] in outcomes]


async def scan_single(proxies, args, state, counters, report, audio, vlm_model, token_records):
    cve = validate_cve(args.cve)
    say(f"single CVE {cve}")
    for index, slot in enumerate(proxies):
        stats = new_stats(index, slot or ProxyEndpoint(None, None, "brightdata"))
        report["proxies"].append(stats)
        cascade = None
        try:
            endpoint = await resolve_proxy(slot, args)
            stats.update(new_stats(index, endpoint))
            async with endpoint_session(index, endpoint, args, counters, token_records, audio, vlm_model, stats) as (
                cascade,
                http,
            ):
                document = await ensure_document(cascade, http, cve, None, state, args, args.output, counters)
                report["documents"] = [document]
                report["status"] = document["status"]
                report.pop("error", None)
                stats["status"] = document["status"]
                return
        except SearchIssue as exc:
            stats["status"] = exc.status
            stats["error"] = safe_message(exc)
            report["status"] = exc.status
            report["error"] = safe_message(exc)
            report["stages"] = list(cascade.events) if cascade is not None else []
            report["audio_events"] = list(cascade.audio.events) if getattr(cascade, "audio", None) is not None else []
            if exc.status not in POLICY_ERRORS:
                return
        except Exception as exc:
            stats["status"] = "proxy_failed"
            stats["error"] = safe_message(exc)
            report["status"] = "failed"
            report["error"] = safe_message(exc)
            return
        say(f"proxy {index + 1}/{len(proxies)} {stats['label']}: {stats['status']}; trying the next endpoint")


async def run(args):
    started = time.monotonic()
    load_dotenv(".env.res")
    load_dotenv(".env")
    args.output.mkdir(parents=True, exist_ok=True)
    args.state_path = args.state or args.output / "state.json"
    state = load_state(args.state_path)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    counters = Counter()
    token_records = []
    report = {
        "run_id": run_id,
        "started_at": datetime.now(UTC).isoformat(),
        "output": str(args.output),
        "state": str(args.state_path),
        "engine": "none" if args.http_only else args.engine,
        "headed": args.headed,
        "proxy_provider": args.proxy_provider,
        "proxy_limit": args.proxy_limit,
        "solves_per_proxy": args.solves_per_proxy,
        "challenges_per_proxy": args.challenges_per_proxy,
        "company_attempts": args.company_attempts,
        "cascade": ["http"]
        if args.http_only
        else ["native"]
        + (["vlm"] if args.vlm_model else [])
        + (["audio"] if args.audio_model else [])
        + (["operator"] if args.operator_wait > 0 else []),
        "companies": [],
        "proxies": [],
        "statuses": {},
        "tokens": token_records,
    }

    try:
        proxies = await load_proxy_endpoints(args)
    except ValueError as exc:
        say(f"cannot load proxies: {safe_message(exc)}")
        return 2
    report["proxy_setup_failures"] = getattr(args, "proxy_setup_failures", [])
    if proxies == [DIRECT]:
        say("proxy provider: direct egress")
    elif args.proxy_provider == "brightdata" and getattr(args, "lazy_proxies", False):
        say(f"proxy provider: brightdata, budget up to {len(proxies)} sessions (created on demand)")
    else:
        say(f"proxy provider: {args.proxy_provider}, {len(proxies)} endpoint(s)")

    audio = None
    if not args.http_only:
        audio, audio_error = await asyncio.to_thread(load_audio, args)
        if audio_error:
            say(f"audio stage disabled: {audio_error}")

    vlm_model = "" if args.http_only else args.vlm_model
    if vlm_model:
        models = await asyncio.to_thread(ollama_models, args.vlm_url)
        if models is None:
            say(f"vlm stage disabled: no Ollama answer at {args.vlm_url}")
            vlm_model = ""
        elif models and not any(name.split(":")[0] == vlm_model.split(":")[0] for name in models):
            say(f"vlm stage disabled: {vlm_model} is not among {', '.join(models[:8])}")
            vlm_model = ""
        else:
            say(f"vlm stage ready: {vlm_model} at {args.vlm_url}")

    if not args.http_only and args.operator_wait > 0 and not args.headed:
        say("warning: --operator-wait without --headed; the browser window is not visible")

    if args.http_only:
        say("HTTP-only mode: cached codigos only, no browser")
    await scan(proxies, args, state, counters, report, audio, vlm_model, token_records)

    report["finished_at"] = datetime.now(UTC).isoformat()
    report["elapsed_seconds"] = round(time.monotonic() - started, 3)
    report["http_metrics"] = {key: round(value, 3) for key, value in counters.items() if key.startswith("http_")}
    if report["companies"]:
        report["statuses"] = dict(Counter(item["status"] for item in report["companies"]))
        report["documents"] = [doc for item in report["companies"] for doc in item.get("documents", [])]
    else:
        report["statuses"] = {report.get("status", "unknown"): 1}
    report["totals"] = {
        "companies": len(report["companies"]),
        "tokens": counters["tokens"],
        "codigos": counters["codigos"],
        "downloaded": counters["downloaded"],
        "reused": counters["reused"],
        "document_failures": counters["document_failures"],
        "company_failures": counters["company_failures"],
        "skipped": counters["skipped"],
        "proxies": len(report["proxies"]),
        "usable_proxies": sum(
            1 for item in report["proxies"] if item["status"] in {"done", "budget", "challenged", "downloaded", "reused"}
        ),
    }
    report["tokens"] = token_records
    atomic_write_json(args.output / f"report-{run_id}.json", report)
    totals = report["totals"]
    say(
        f"done: {totals['downloaded']} downloaded, {totals['reused']} reused, "
        f"{totals['codigos']} codigos, {totals['tokens']} tokens, {totals['document_failures']} document failures, "
        f"{totals['usable_proxies']}/{totals['proxies']} usable proxies"
    )
    say(f"report: {args.output / f'report-{run_id}.json'}")
    return 0 if totals["downloaded"] or totals["reused"] else 1


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=Path("data/dequienes-companies-2025/companies.sqlite3"))
    parser.add_argument("--limit", type=int, default=10, help="Companies to scan; 0 means all")
    parser.add_argument("--cve", help="Process a single CVE without a company search")
    parser.add_argument("--max-documents", type=int, default=3, help="Documents per company; 0 means all")
    parser.add_argument("--output", type=Path, default=Path("data/res-free-pilot"))
    parser.add_argument("--state", type=Path, help="Codigo cache; defaults to <output>/state.json")
    parser.add_argument("--profile", type=Path, default=Path("data/res-free-pilot/profile"))
    parser.add_argument("--profile-slots", type=int, default=4, help="Browser profile directories reused across proxies")
    parser.add_argument("--engine", choices=("camoufox", "patchright"), default="camoufox")
    parser.add_argument("--http-only", action="store_true", help="Never launch a browser; download cached codigos only")
    parser.add_argument("--proxy-provider", choices=("direct", "webshare", "residential", "brightdata"), default="webshare")
    parser.add_argument("--proxy-file", type=Path, default=Path("data/residential-proxies.txt"))
    parser.add_argument("--proxy-limit", type=int, default=0, help="Use at most this many proxy endpoints; 0 means all")
    parser.add_argument("--lazy-proxies", action="store_true", help="Create Bright Data sessions only when needed")
    parser.add_argument("--solves-per-proxy", type=int, default=0, help="Rotate after this many tokens; 0 means unlimited")
    parser.add_argument(
        "--challenges-per-proxy",
        type=int,
        default=2,
        help="Rotate after this many challenge requeues; 0 disables challenge rotation",
    )
    parser.add_argument("--company-attempts", type=int, default=2, help="Tries per company across fresh proxies")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--native-wait", type=float, default=6.0, help="Seconds to wait for a free checkbox pass")
    parser.add_argument("--operator-wait", type=float, default=0.0, help="Seconds to wait for a manual solve")
    parser.add_argument("--delay", type=float, default=2.0, help="Seconds between token mints")
    parser.add_argument("--mint-attempts", type=int, default=1, help="Clicks per token before giving up")
    parser.add_argument("--vlm-model", default="", help="Ollama vision model, e.g. qwen2.5vl:7b")
    parser.add_argument("--vlm-url", default="http://127.0.0.1:11434")
    parser.add_argument("--vlm-rounds", type=int, default=2)
    parser.add_argument("--audio-model", default="base", help="faster-whisper size; empty disables the stage")
    parser.add_argument("--audio-device", default="cpu")
    parser.add_argument("--audio-compute", default="int8")
    parser.add_argument("--audio-rounds", type=int, default=3)
    parser.add_argument("--audio-seconds", type=float, default=180.0)
    parser.add_argument("--refresh", action="store_true", help="Ignore cached company searches")
    parser.add_argument("--max-failures", type=int, default=20, help="Stop after this many consecutive failures")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        say("interrupted; the codigo cache is already on disk")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
