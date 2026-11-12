"""Plain-HTTP RES flow: PageMethod search/verify and cached certificate links.

The browser is only needed to obtain a reCAPTCHA token. Both site steps are ASP.NET
PageMethods, so the rest of the pipeline runs without a browser, profiles or Xvfb.
"""

from __future__ import annotations

import json
import re
import time
from urllib.parse import parse_qs, urlsplit

import httpx

from src.sites.dequienes.source import RES_URL, res_url, validate_cve
from src.sites.res.source import DETAILS_PATH, FORM_URL, HOST, SEARCH_PATH, SearchIssue, parse_actions, search_link

VERIFY_URL = RES_URL
VERIFY_PATH = "/VerificarCertificados.aspx/Busqueda"
JSON_HEADERS = {"Content-Type": "application/json; charset=utf-8", "X-Requested-With": "XMLHttpRequest"}
REFERERS = {SEARCH_PATH: FORM_URL, DETAILS_PATH: f"https://{HOST}/Busqueda/ActuacionDetalle.aspx", VERIFY_PATH: VERIFY_URL}
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0"
CODIGO_PATTERN = re.compile(r"[0-9A-Fa-f]{2}(?:-[0-9A-Fa-f]{2}){15}")
TRANSIENT_HTTP = {502, 503, 504}


def new_session(timeout=30.0, proxy=None, verify=True) -> httpx.Client:
    """No redirects: a 302 from Escritura.aspx is an invalid codigo, not a fallback."""
    return httpx.Client(
        timeout=httpx.Timeout(timeout, connect=min(timeout, 12.0), pool=min(timeout, 12.0)),
        follow_redirects=False,
        # Only connection establishment is retried, before any HTTP request is sent.
        # Read/write errors and HTTP responses must never replay a CAPTCHA POST.
        transport=httpx.HTTPTransport(proxy=proxy, verify=verify, trust_env=False, retries=1),
        trust_env=False,
        headers={"User-Agent": USER_AGENT, "Accept-Language": "es-CL,es;q=0.9"},
    )


def get(session: httpx.Client, url: str, **kwargs):
    """One retry for an idempotent GET; never replay a single-use-token POST."""
    for attempt in range(2):
        try:
            response = session.get(url, **kwargs)
        except httpx.TransportError as exc:
            if attempt == 1:
                raise SearchIssue("proxy_failed", f"HTTP GET connection failed ({type(exc).__name__})") from exc
        else:
            if response.status_code not in TRANSIENT_HTTP or attempt == 1:
                return response
            response.close()
        time.sleep(0.5)
    raise AssertionError("GET attempt limit")


def prepare(session: httpx.Client) -> None:
    """Warm the session so the PageMethod carries the same ASP.NET cookies the form does."""
    for url in (FORM_URL, VERIFY_URL):
        response = get(session, url)
        if response.status_code != 200:
            status = (
                "blocked"
                if response.status_code in {403, 429}
                else "proxy_failed"
                if response.status_code in TRANSIENT_HTTP
                else "retry_wait"
            )
            raise SearchIssue(status, f"Form HTTP {response.status_code}")


def _post(session: httpx.Client, path: str, body: dict) -> dict:
    headers = {**JSON_HEADERS, "Origin": f"https://{HOST}", "Referer": REFERERS.get(path, FORM_URL)}
    try:
        response = session.post(f"https://{HOST}{path}", json=body, headers=headers)
    except httpx.TransportError as exc:
        raise SearchIssue("proxy_failed", f"RES POST connection failed ({type(exc).__name__}); token not replayed") from exc
    if response.status_code != 200:
        status = (
            "blocked"
            if response.status_code in {403, 429}
            else "proxy_failed"
            if response.status_code in TRANSIENT_HTTP
            else "retry_wait"
        )
        raise SearchIssue(status, f"{path} HTTP {response.status_code}")
    try:
        return response.json()
    except ValueError as exc:
        raise SearchIssue("retry_wait", f"{path} did not return JSON") from exc


def search(session: httpx.Client, *, token: str, rut: str | None = None, name: str | None = None) -> str:
    """One token per company; returns the ActuacionDetalle link carrying the codehash."""
    texto = (rut or name or "").strip()
    if not texto:
        raise SearchIssue("failed", "Search needs a RUT or a company name")
    body = {"tipoRegistro": "1", "tipoBusqueda": "1" if rut else "3", "texto": texto, "Response": token}
    return search_link(_post(session, SEARCH_PATH, body))


def details(session: httpx.Client, link: str) -> list[dict]:
    """No CAPTCHA: the codehash from the search authorizes the whole action list."""
    query = parse_qs(urlsplit(link).query)
    try:
        tipo, param, codehash = query["TipoBusqueda"][0], query["ParamBusqueda1"][0], query["codigo"][0]
    except (KeyError, IndexError) as exc:
        raise SearchIssue("retry_wait", "Details link is missing its codehash") from exc
    body = {"tipoBusqueda": tipo, "ParamBusqueda1": param, "codehash": codehash}
    return parse_actions(_post(session, DETAILS_PATH, body))


def verify(session: httpx.Client, cve: str, token: str) -> tuple[str, str, str]:
    """One token per document. Returns the CVE, codigo and server-issued URL."""
    cve = validate_cve(cve)
    payload = _post(session, VERIFY_PATH, {"Response": token, "texto": cve})
    body = payload.get("d")
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except ValueError as exc:
            raise SearchIssue("retry_wait", "Verifier returned an invalid envelope") from exc
    if not isinstance(body, dict):
        raise SearchIssue("retry_wait", "Verifier returned an unexpected payload")
    if body.get("ErrorMessage"):
        raise SearchIssue("captcha_required", str(body["ErrorMessage"])[:300])
    if not isinstance(body.get("Link"), str) or not body["Link"]:
        raise SearchIssue("retry_wait", "Verifier did not return a document link")
    link = res_url(body["Link"])
    query = parse_qs(urlsplit(link).query)
    if query.get("cve") != [cve]:
        raise SearchIssue("retry_wait", "Verifier returned a link for a different CVE")
    codigo = query.get("codigo", [""])[0]
    if not CODIGO_PATTERN.fullmatch(codigo):
        raise SearchIssue("retry_wait", "Verifier returned an unexpected codigo")
    return cve, codigo.upper(), link


def download(session: httpx.Client, link: str) -> bytes:
    """Try the server-issued link; callers handle expiry by re-verifying."""
    response = get(session, res_url(link))
    if response.status_code != 200:
        status = (
            "blocked"
            if response.status_code in {403, 429}
            else "proxy_failed"
            if response.status_code in TRANSIENT_HTTP
            else "retry_wait"
        )
        raise SearchIssue(status, f"Certificate HTTP {response.status_code}")
    content_type = response.headers.get("content-type", "").split(";")[0].strip().lower()
    if content_type != "application/pdf":
        raise SearchIssue("retry_wait", f"Certificate content-type is {content_type or 'absent'}")
    return response.content
