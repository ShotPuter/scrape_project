"""Use the normal RES form, checkbox and backend responses; never forge CAPTCHA tokens."""

from __future__ import annotations

import asyncio
from time import monotonic
from urllib.parse import urlsplit

from src.sites.res.source import (
    DETAILS_PATH,
    FORM_URL,
    HOST,
    SEARCH_PATH,
    SearchIssue,
    match_actions,
    parse_actions,
    search_link,
)


async def solve_checkbox(page, *, timeout_ms=60000, captcha_seconds=30):
    anchor = page.frame_locator('iframe[title="reCAPTCHA"]').locator("#recaptcha-anchor")
    try:
        await anchor.click(timeout=min(timeout_ms, 15000))
        deadline = monotonic() + captcha_seconds
        while await anchor.get_attribute("aria-checked") != "true":
            if monotonic() >= deadline:
                raise SearchIssue("captcha_required", "CAPTCHA requires operator interaction")
            await asyncio.sleep(0.25)
    except SearchIssue:
        raise
    except Exception as exc:
        raise SearchIssue("captcha_required", "CAPTCHA checkbox was not available") from exc


async def lookup(page, company, *, timeout_ms=60000, captcha_seconds=30):
    query = company.get("rut") or company["company_name"]
    result = {**company, "query": query, "source_url": FORM_URL, "events": []}
    response = await page.goto(FORM_URL, wait_until="domcontentloaded", timeout=timeout_ms)
    if not response or response.status != 200:
        status = response.status if response else 0
        raise SearchIssue("blocked" if status in {403, 429} else "retry_wait", f"Form HTTP {status}")
    if urlsplit(page.url).hostname != HOST or urlsplit(page.url).scheme != "https":
        raise SearchIssue("blocked", "Unexpected form redirect")
    await page.locator("#ddlTipoRegistro").select_option("1")
    await page.locator("#ddlTipoBusqueda").select_option("1" if company.get("rut") else "2")
    await page.locator("#txtRut" if company.get("rut") else "#txtRSocial").fill(query)
    await solve_checkbox(page, timeout_ms=timeout_ms, captcha_seconds=captcha_seconds)

    def is_response(candidate, path):
        parts = urlsplit(candidate.url)
        return parts.scheme == "https" and parts.hostname == HOST and parts.path == path

    # Install both listeners before clicking: the details request starts during the redirect.
    async with page.expect_response(lambda r: is_response(r, DETAILS_PATH), timeout=timeout_ms) as details_event:
        async with page.expect_response(lambda r: is_response(r, SEARCH_PATH), timeout=timeout_ms) as search_event:
            await page.locator("#btnBuscar").click(timeout=timeout_ms)
        search_response = await search_event.value
        if search_response.status != 200:
            raise SearchIssue(
                "blocked" if search_response.status in {403, 429} else "retry_wait", f"Search HTTP {search_response.status}"
            )
        search_link(await search_response.json())
    details_response = await details_event.value
    if details_response.status != 200:
        raise SearchIssue(
            "blocked" if details_response.status in {403, 429} else "retry_wait",
            f"Action list HTTP {details_response.status}",
        )
    actions = parse_actions(await details_response.json())
    status, matched = match_actions(actions, company)
    result.update(status=status, events=matched, res_cves=sorted({e["cve"] for e in matched if e["cve"]}))
    if status == "ambiguous_match":
        result["candidates"] = [{"company_name": a["company_name"], "rut": a["rut"]} for a in actions]
    return result
