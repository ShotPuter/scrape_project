"""Single-CVE experiment: Botonomus + local Whisper, always through a proxy."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import ipaddress
import json
import os
import secrets
import ssl
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from dotenv import dotenv_values, load_dotenv

from src.sites.res.audio import LocalAudioSolver
from src.sites.res.certificates import VERIFY_PATH, certificate_link, download_certificate
from src.sites.res.engines import launch_context
from src.sites.res.pdf import persist_pdf, read_pdf
from src.sites.res.source import HOST
from src.utils.atomic import atomic_write_json
from src.utils.error_details import safe_message


def webshare_proxy(host):
    token = os.getenv("WEBSHARE_API_TOKEN", "").strip()
    if not token:
        raise ValueError("WEBSHARE_API_TOKEN is required; direct access is disabled")
    request = Request(
        "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=100",
        headers={"Authorization": "Token " + token},
    )

    class NoRedirect(HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None

    with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=30) as response:
        data = response.read(2 * 1024 * 1024 + 1)
    if len(data) > 2 * 1024 * 1024:
        raise ValueError("Proxy inventory exceeded size limit")
    for row in json.loads(data).get("results", []):
        if row.get("valid") is False or row.get("proxy_address") != host:
            continue
        ipaddress.ip_address(host)
        return {"server": f"http://{host}:{int(row['port'])}", "username": row["username"], "password": row["password"]}
    raise ValueError("Requested proxy is unavailable in the first inventory page")


class ProxyPreflightError(ValueError):
    status = "proxy_auth_failed"

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.status = "proxy_auth_failed" if code == "ip_blacklisted" else "proxy_failed"


def brightdata_proxy(output):
    """Pinned provider CA and sticky username; never disable TLS verification globally."""
    config = dotenv_values(".env.camoufox")
    # Containers receive credentials through their environment, never image layers.
    config.update(
        {
            key: os.environ[key]
            for key in ("BRIGHTDATA_PROXY_SERVER", "BRIGHTDATA_PROXY_USERNAME", "BRIGHTDATA_PROXY_PASSWORD")
            if key in os.environ
        }
    )
    username = config.get("BRIGHTDATA_PROXY_USERNAME", "")
    password = config.get("BRIGHTDATA_PROXY_PASSWORD", "")
    server = config.get("BRIGHTDATA_PROXY_SERVER", "http://brd.superproxy.io:44445")
    parts = urlsplit(server)
    if not username or not password or parts.scheme != "http" or not parts.hostname or not parts.port:
        raise ValueError("Valid Bright Data proxy settings are required in the environment or .env.camoufox")
    if "-session-" not in username:
        username += "-session-resaudio" + secrets.token_hex(8)
    certificate = Path("docker/brightdata-proxy-ca.crt")
    der = ssl.PEM_cert_to_DER_cert(certificate.read_text(encoding="ascii"))
    pinned = "DB8548F8A5B1166536920CCD0473840F7FDBAF165DEDF907B7B52361ABC87B60"
    if hashlib.sha256(der).hexdigest().upper() != pinned:
        raise ValueError("Bright Data CA fingerprint mismatch")
    subprocess.run(
        ["openssl", "verify", "-CAfile", str(certificate), str(certificate)], check=True, capture_output=True, timeout=10
    )
    key = subprocess.run(
        ["openssl", "x509", "-in", str(certificate), "-pubkey", "-noout"], check=True, capture_output=True, timeout=10
    ).stdout
    spki = subprocess.run(
        ["openssl", "pkey", "-pubin", "-outform", "DER"], input=key, check=True, capture_output=True, timeout=10
    ).stdout
    import certifi
    import requests

    bundle = output / "proxy-ca-bundle.pem"
    bundle.write_bytes(Path(certifi.where()).read_bytes() + b"\n" + certificate.read_bytes())
    proxy_url = parts.scheme + "://" + quote(username, safe="") + ":" + quote(password, safe="") + "@" + parts.netloc
    with requests.Session() as session:
        session.trust_env = False
        try:
            response = session.get(
                "https://api.ipify.org?format=json", proxies={"https": proxy_url}, verify=str(bundle), timeout=30
            )
        except requests.exceptions.ProxyError as exc:
            if "ip_blacklisted" in str(exc):
                raise ProxyPreflightError("ip_blacklisted", "Bright Data rejected this client IP (ip_blacklisted)") from None
            raise ProxyPreflightError("proxy_connect_failed", "Bright Data proxy connection failed") from None
        response.raise_for_status()
        expected = str(ipaddress.ip_address(response.json()["ip"]))
    return (
        {"server": server, "username": username, "password": password},
        expected,
        "--ignore-certificate-errors-spki-list=" + base64.b64encode(hashlib.sha256(spki).digest()).decode(),
    )


async def browser_ip(context):
    page = await context.new_page()
    try:
        await page.goto("https://api.ipify.org?format=json", wait_until="domcontentloaded", timeout=30000)
        return str(ipaddress.ip_address(json.loads(await page.locator("body").inner_text())["ip"]))
    finally:
        await page.close()


class ChromeDownloads:
    """Observe actual Chrome completion when its adapter omits Playwright events."""

    def __init__(self, directory):
        self.directory = directory
        self.urls = {}
        self.completed = set()

    def started(self, event):
        self.urls[str(uuid.UUID(event["guid"]))] = event["url"]

    def progress(self, event):
        if event["state"] == "completed":
            self.completed.add(str(uuid.UUID(event["guid"])))

    def recover(self, cve, expected_url, output):
        if not expected_url:
            return None
        for guid in self.completed:
            if self.urls.get(guid) == expected_url:
                return persist_pdf(output, cve, read_pdf(self.directory / guid), expected_url)
        return None


async def attempt(context, args, output, report, model, downloads=None):
    report["phase"] = "proxy_verification"
    report["exit_ip_before"] = await browser_ip(context)
    if report["exit_ip_before"] != report["expected_ip"]:
        raise ValueError("Browser proxy egress differs from the expected proxy IP")
    report["proxy_verified"] = True
    print("Browser proxy verified", flush=True)
    page = await context.new_page()
    responses = report.setdefault("responses", [])
    observations = []

    async def inspect_verifier(response):
        try:
            report["verifier_document_url"] = certificate_link(await response.json())
            report["verifier_accepted"] = True
        except Exception as exc:
            report["verifier_error"] = safe_message(exc)

    def observe(response):
        from urllib.parse import urlsplit

        parts = urlsplit(response.url)
        if parts.hostname == HOST:
            responses.append(
                {"path": parts.path, "status": response.status, "content_type": response.headers.get("content-type", "")}
            )
            if parts.path == VERIFY_PATH:
                observations.append(asyncio.create_task(inspect_verifier(response)))

    page.on("response", observe)

    class ReadyPage:
        def __getattr__(self, name):
            return getattr(page, name)

        async def goto(self, url, **kwargs):
            kwargs["wait_until"] = "commit"
            response = await page.goto(url, **kwargs)
            await page.locator("#boxer_content_txtCve").wait_for(state="visible", timeout=45000)
            return response

    try:
        report["phase"] = "certificate_form"
        solver = LocalAudioSolver(model, events=report.setdefault("audio_events", []))
        report["certificate"] = await download_certificate(
            ReadyPage(), args.cve, output, timeout_ms=60000, captcha_seconds=120, captcha_solver=solver
        )
        report["status"] = "downloaded"
        report["phase"] = "complete"
    except Exception as exc:
        report.update(status=getattr(exc, "status", "failed"), error_type=type(exc).__name__, error=safe_message(exc))
        if observations:
            await asyncio.gather(*observations, return_exceptions=True)
        if downloads:
            recovered = downloads.recover(args.cve, report.get("verifier_document_url"), output)
            if recovered:
                report["download_event_error"] = report.pop("error")
                report.pop("error_type", None)
                report.update(status="downloaded", phase="complete", certificate=recovered, download_transport="cdp")
    finally:
        page.remove_listener("response", observe)
        if observations:
            await asyncio.gather(*observations, return_exceptions=True)
        try:
            await page.screenshot(path=str(output / "verify.png"), timeout=5000)
            (output / "verify.html").write_text(await page.content(), encoding="utf-8")
            report["frames"] = []
            for frame in page.frames:
                if "/recaptcha/" in frame.url:
                    report["frames"].append((await frame.locator("body").inner_text(timeout=2000))[:1500])
        except Exception as exc:
            report["diagnostics_error"] = type(exc).__name__
        report["exit_ip_after"] = await browser_ip(context)
        report["ip_unchanged"] = report["exit_ip_after"] == report["exit_ip_before"]
        if not report["ip_unchanged"]:
            report["status"] = "proxy_changed"


async def main(args):
    load_dotenv(".env.res")
    load_dotenv(".env")
    output = args.output / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output.mkdir(parents=True, mode=0o700)
    report = {
        "engine": args.engine,
        "status": "preparing",
        "cve": args.cve,
        "model": args.model,
        "provider": args.proxy_provider,
        "started_at": datetime.now(UTC).isoformat(),
    }
    if args.engine == "botonomus":
        report["driver"] = "patchright"
    print("Diagnostics:", output, flush=True)
    try:
        report["phase"] = "proxy_inventory"
        browser_args = ("--disable-quic",)
        if args.proxy_provider == "brightdata":
            if args.engine != "botonomus":
                raise ValueError("Bright Data audio pilot currently requires --engine botonomus")
            proxy, report["expected_ip"], ca_flag = await asyncio.to_thread(brightdata_proxy, output)
            browser_args += (ca_flag,)
            report["network_type"] = "unconfirmed"
            report["tls_trust"] = "pinned_brightdata_ca"
        else:
            proxy = await asyncio.to_thread(webshare_proxy, args.proxy_host)
            report["expected_ip"] = args.proxy_host
        from faster_whisper import WhisperModel

        report["phase"] = "local_model"
        model = await asyncio.to_thread(
            WhisperModel, args.model, device="cpu", compute_type="int8", download_root="data/res-audio-models", cpu_threads=4
        )
        print("Local Whisper ready", flush=True)
        report["phase"] = "browser_startup"
        async with asyncio.timeout(300):
            if args.engine == "botonomus":
                from botonomus import Botonomus, BrowserConfig

                from src.sites.pep.proxy_tunnel import ProxyTunnel

                profile = output / "profiles" / "pilot"
                atomic_write_json(profile / "Default" / "Preferences", {"plugins": {"always_open_pdf_externally": True}})
                tunnel = ProxyTunnel(proxy, timeout=60)
                try:
                    local_proxy = await tunnel.start()
                    config = BrowserConfig(
                        profile_root=profile.parent,
                        browser="chrome",
                        persona="off",
                        driver="patchright",
                        virtual_display=True,
                        proxy=local_proxy,
                        extra_args=browser_args,
                    )
                    async with (
                        Botonomus(config=config, max_instances=1) as browser,
                        browser.open(profile="pilot") as session,
                    ):
                        # Botonomus attaches with no_defaults=True; explicitly enable downloads
                        # in its existing default context, without creating a new proxy session.
                        directory = output / "browser-downloads"
                        directory.mkdir(mode=0o700)
                        downloads = ChromeDownloads(directory)
                        cdp = await session.context.browser.new_browser_cdp_session()
                        cdp.on("Browser.downloadWillBegin", downloads.started)
                        cdp.on("Browser.downloadProgress", downloads.progress)
                        await cdp.send(
                            "Browser.setDownloadBehavior",
                            {"behavior": "allowAndName", "downloadPath": str(directory.resolve()), "eventsEnabled": True},
                        )
                        await attempt(session.context, args, output, report, model, downloads)
                finally:
                    await tunnel.close()
            else:
                async with launch_context(output / "profile", engine=args.engine, proxy=proxy, pdf=True) as context:
                    await attempt(context, args, output, report, model)
    except Exception as exc:
        # Inventory errors may echo authorization headers; only print the exception type here.
        report.update(status=getattr(exc, "status", "failed"), error_type=type(exc).__name__)
        if isinstance(exc, ProxyPreflightError):
            report.update(error_code=exc.code, error=str(exc))
    finally:
        atomic_write_json(output / "report.json", report)
        print(
            json.dumps({k: v for k, v in report.items() if k not in {"responses", "frames"}}, ensure_ascii=False), flush=True
        )
        print("Saved:", output / "report.json", flush=True)
    return 0 if report["status"] == "downloaded" else 2


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("botonomus", "patchright", "camoufox"), default="botonomus")
    parser.add_argument("--proxy-provider", choices=("webshare", "brightdata"), default="webshare")
    parser.add_argument("--proxy-host", default="107.174.128.95")
    parser.add_argument("--cve", default="ACuKDHhqsvAd")
    parser.add_argument("--model", choices=("tiny", "base", "small"), default="base")
    parser.add_argument("--output", type=Path, default=Path("data/res-audio-pilot"))
    raise SystemExit(asyncio.run(main(parser.parse_args())))
