"""Bounded HTTP discovery; no browser, CAPTCHA submission or TLS bypass."""

from __future__ import annotations

import asyncio
import random
from pathlib import Path
from urllib.parse import urlsplit

from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import CertificateVerifyError, RequestException

from src.sites.pep.source import RESULT_PATH, parse_results, safe_url


class TLSPreflightError(ValueError):
    pass


class DiscoveryClient:
    def __init__(self, *, workers: int, delay: float, timeout: int, ca_bundle: Path | None = None,
                 proxy: str | None = None, accept_language: str = "es-CO,es;q=0.9") -> None:
        self.session = AsyncSession(
            impersonate="chrome",
            headers={"Accept-Language": accept_language},
            verify=str(ca_bundle.resolve()) if ca_bundle else True,
            timeout=timeout,
            max_clients=workers,
            trust_env=False,
            proxy=proxy,
        )
        self.delay = delay
        self.lock = asyncio.Lock()
        self.next_request = 0.0
        self.ca_bundle = ca_bundle.resolve() if ca_bundle else None
        self.explicit_ca = ca_bundle is not None
        self.last_failure: dict = {}

    async def preflight(self, output: Path) -> None:
        """Check once before jobs and repair only when no CA bundle was supplied."""
        url = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/consultaPEP"
        cached = output / "ca-bundle.pem"
        if not self.explicit_ca and cached.is_file():
            self.ca_bundle = cached.resolve()
            self.session.verify = str(self.ca_bundle)
        try:
            await self.session.get(url, allow_redirects=False)
            return
        except CertificateVerifyError as exc:
            if self.explicit_ca:
                raise TLSPreflightError(
                    f"TLS verification failed with CA bundle {self.ca_bundle}. "
                    "Check its validity and your system clock; do not disable verification."
                ) from exc
        except RequestException as exc:
            raise TLSPreflightError(
                f"Source connectivity check failed ({type(exc).__name__}); no jobs were attempted."
            ) from exc
        from src.sites.pep.tls import prepare_ca_bundle

        try:
            print("PEP TLS: missing certificate chain; validating Sectigo intermediate against trusted roots...", flush=True)
            self.ca_bundle = await prepare_ca_bundle(output / "ca-bundle.pem")
            self.session.verify = str(self.ca_bundle)
            await self.session.get(url, allow_redirects=False)
        except (ValueError, OSError, RequestException) as exc:
            raise TLSPreflightError(
                "Cannot establish verified TLS. Check the system clock/network or supply a valid --ca-bundle. "
                f"Diagnostic: {type(exc).__name__}. No jobs were attempted."
            ) from exc
        print(f"PEP TLS: verification succeeded; saved CA bundle {self.ca_bundle}", flush=True)

    async def close(self) -> None:
        await self.session.close()

    async def pace(self) -> None:
        async with self.lock:
            loop = asyncio.get_running_loop()
            await asyncio.sleep(max(0, self.next_request - loop.time()))
            self.next_request = loop.time() + self.delay

    async def discover(self, url: str) -> tuple[str, list[dict]]:
        self.last_failure = {}
        documents: dict[str, dict] = {}
        visited: set[str] = set()
        try:
            while url:
                url = safe_url(url)
                if url in visited:
                    return "discovery_failed_pagination_loop", list(documents.values())
                visited.add(url)
                for attempt in range(3):
                    await self.pace()
                    response = await self.session.get(url, allow_redirects=False)
                    if response.status_code not in {429, 500, 502, 503, 504} or attempt == 2:
                        break
                    retry_after = response.headers.get("Retry-After", "")
                    pause = min(60, int(retry_after)) if retry_after.isdecimal() else 2 ** attempt + random.random()
                    await asyncio.sleep(pause)
                if response.status_code != 200:
                    return f"discovery_failed_http_{response.status_code}", list(documents.values())
                if urlsplit(safe_url(response.url)).path != RESULT_PATH:
                    return "discovery_failed_redirect", list(documents.values())
                entries, next_url = await asyncio.to_thread(parse_results, response.text, url)
                for doc in entries:
                    doc.update({"source_page": url, "status": "discovered", "local_path": None,
                                "s3_uri": None, "publication_year": doc["publication_date"][:4] or None})
                    documents.setdefault(doc["id"], doc)
                url = next_url
            return "discovered", list(documents.values())
        except CertificateVerifyError as exc:
            self.last_failure = {"error_type": type(exc).__name__}
            return "discovery_failed_tls", list(documents.values())
        except RequestException as exc:
            self.last_failure = {"error_type": type(exc).__name__}
            code = getattr(exc, "code", None)
            if isinstance(code, int):
                self.last_failure["curl_code"] = int(code)
            return "discovery_failed_network", list(documents.values())
        except ValueError as exc:
            self.last_failure = {"error_type": type(exc).__name__}
            return "discovery_failed_schema", list(documents.values())
