"""Download the PDF returned by the normal Verificar Certificados form."""

from __future__ import annotations

from urllib.parse import urlsplit

from src.sites.dequienes.source import RES_URL, res_url, validate_cve
from src.sites.res.browser import solve_checkbox
from src.sites.res.pdf import persist_pdf, read_pdf
from src.sites.res.source import HOST, SearchIssue, unpack

VERIFY_PATH = "/VerificarCertificados.aspx/Busqueda"


def certificate_link(payload):
    body = unpack(payload)
    if not isinstance(body, dict) or not isinstance(body.get("Link"), str) or not body["Link"]:
        raise SearchIssue("retry_wait", "Verifier did not return a document link")
    try:
        return res_url(body["Link"])
    except ValueError as exc:
        raise SearchIssue("retry_wait", "Unexpected certificate URL") from exc


async def download_certificate(page, cve, output, *, timeout_ms=60000, captcha_seconds=30):
    cve = validate_cve(cve)
    response = await page.goto(RES_URL, wait_until="domcontentloaded", timeout=timeout_ms)
    if not response or response.status != 200:
        status = response.status if response else 0
        raise SearchIssue("blocked" if status in {403, 429} else "retry_wait", f"Verifier form HTTP {status}")
    if res_url(page.url) != RES_URL:
        raise SearchIssue("blocked", "Unexpected verifier form redirect")
    await page.locator("#boxer_content_txtCve").fill(cve)
    await solve_checkbox(page, timeout_ms=timeout_ms, captcha_seconds=captcha_seconds)

    def is_verification(candidate):
        parts = urlsplit(candidate.url)
        return parts.scheme == "https" and parts.hostname == HOST and parts.path == VERIFY_PATH

    # Firefox downloads inline PDFs with pdfjs.disabled=True. Both listeners must precede the click.
    async with page.expect_download(timeout=timeout_ms) as download_event:
        async with page.expect_response(is_verification, timeout=timeout_ms) as verification_event:
            await page.locator("#boxer_content_lnkValidarCVE").click(timeout=timeout_ms)
        verification = await verification_event.value
        if verification.status != 200:
            raise SearchIssue(
                "blocked" if verification.status in {403, 429} else "retry_wait", f"Verifier HTTP {verification.status}"
            )
        link = certificate_link(await verification.json())
    download = await download_event.value
    try:
        if res_url(download.url) != link:
            raise SearchIssue("retry_wait", "Download URL does not match the verifier response")
        if await download.failure():
            raise SearchIssue("retry_wait", "Browser could not complete the PDF download")
        path = await download.path()
        if path is None:
            raise SearchIssue("retry_wait", "Browser did not save the PDF")
        try:
            data = read_pdf(path)
        except ValueError as exc:
            raise SearchIssue("retry_wait", str(exc)) from exc
        return persist_pdf(output, cve, data, link)
    finally:
        await download.delete()
