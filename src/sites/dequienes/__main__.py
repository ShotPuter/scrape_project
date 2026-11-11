"""Camoufox discovery and RES verification using the site's normal UI."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
from pathlib import Path

from src.sites.dequienes.source import RES_URL, parse_legal, validate_cve, verification_result
from src.sites.res.pdf import MAX_PDF_BYTES, pdf_metadata
from src.utils.atomic import atomic_write_bytes, atomic_write_json, atomic_write_text


async def snapshot(page, output: Path, name: str) -> None:
    atomic_write_text(output / f"{name}.html", await page.content())
    await page.screenshot(path=str(output / f"{name}.png"), full_page=True)


async def download(context, doc: dict, args) -> dict:
    cve = validate_cve(doc["cve"])
    # Hash avoids collisions between case-sensitive CVEs on case-insensitive disks.
    relative = f"pdf/{hashlib.sha256(cve.encode()).hexdigest()}.pdf"
    target = args.output / relative
    if target.exists() and target.stat().st_size <= MAX_PDF_BYTES:
        try:
            return {**doc, **pdf_metadata(target.read_bytes()), "status": "downloaded", "local_path": relative}
        except ValueError:
            pass
    page = await context.new_page()
    try:
        await page.goto(RES_URL, wait_until="domcontentloaded")
        await page.locator("#boxer_content_txtCve").fill(cve)
        if not args.interactive:
            return {**doc, "status": "captcha_required"}
        print(f"RES {cve}: solve CAPTCHA in the browser, then press Enter here.", flush=True)
        await asyncio.to_thread(input)
        async with page.expect_response(
            lambda r: r.url.split("?")[0].endswith("/VerificarCertificados.aspx/Busqueda")
        ) as event:
            await page.locator("#boxer_content_lnkValidarCVE").click()
        response = await event.value
        if response.status != 200:
            raise ValueError(f"RES verification HTTP {response.status}")
        url = verification_result(await response.json())
        # Follow the link returned by the official verifier with the same session.
        result = await context.request.get(url, max_redirects=0)
        if result.status != 200:
            raise ValueError(f"PDF HTTP {result.status}; redirect or viewer needs inspection")
        length = result.headers.get("content-length")
        if length and int(length) > MAX_PDF_BYTES:
            raise ValueError("PDF exceeds size limit")
        data = await result.body()
        metadata = pdf_metadata(data)
        atomic_write_bytes(target, data)
        return {**doc, **metadata, "status": "downloaded", "local_path": relative, "download_url": url}
    finally:
        await page.close()


async def run(args) -> int:
    args.output.mkdir(parents=True, exist_ok=True)
    records = [{"cve": cve, "source": "RES", "status": "pending"} for cve in args.cve]
    report = {"engine": "camoufox", "status": "starting", "documents": records}
    try:
        if args.html:
            records.extend(parse_legal(args.html.read_text(encoding="utf-8"), args.company_name, args.rut))
        if args.html and args.discover_only and not args.query:
            report["status"] = "discovered"
            return 0
        from camoufox.async_api import AsyncCamoufox

        async with AsyncCamoufox(
            headless=False if args.interactive else "virtual",
            os="linux",
            locale="es-CL",
            persistent_context=True,
            user_data_dir=str(args.profile.resolve()),
            accept_downloads=True,
        ) as context:
            context.set_default_timeout(args.timeout * 1000)
            if args.query:
                page = await context.new_page()
                await page.goto("https://dequienes.cl/", wait_until="domcontentloaded")
                await page.locator("input[type=search]").wait_for(state="visible")
                if args.interactive:
                    print("Sign into DeQuienes in this browser if needed, then press Enter.", flush=True)
                    await asyncio.to_thread(input)
                await page.locator("input[type=search]").fill(args.query)
                await page.get_by_role("button", name="Buscar", exact=True).click()
                # Wait for either the authentication gate or actual search results.
                await page.wait_for_function("""() => {
                    const text = document.body.innerText;
                    return text.includes('Podrás seguir buscando') || /RUT\\s*:/i.test(text)
                        || /sin resultados|no se encontraron/i.test(text);
                }""")
                await snapshot(page, args.output, "search")
                text = await page.locator("body").inner_text()
                if "Podrás seguir buscando" in text:
                    report["status"] = "login_required"
                    print("DeQuienes requires login. Repeat with --interactive using the same --profile.", flush=True)
                    return 1
                if not args.interactive:
                    report["status"] = "company_selection_required"
                    print("Search loaded; use --interactive to select the company and open Legal.", flush=True)
                    return 1
                print("Open the required company and its Legal tab, then press Enter.", flush=True)
                await asyncio.to_thread(input)
                await snapshot(page, args.output, "legal")
                records.extend(parse_legal(await page.content(), args.company_name, args.rut))
                await page.close()
            if not args.discover_only:
                for index, doc in enumerate(records):
                    if doc.get("source") != "RES":
                        continue
                    try:
                        records[index] = await download(context, doc, args)
                    except Exception as exc:
                        records[index] = {**doc, "status": "download_failed", "error": str(exc)}
                    atomic_write_json(args.output / "records.json", records)
            report["status"] = (
                "discovered"
                if args.discover_only
                else ("completed" if all(d["status"] == "downloaded" for d in records) else "partial")
            )
            return int(report["status"] == "partial")
    except Exception as exc:
        report.update(status="failed", error=str(exc))
        print(f"DeQuienes: {exc}", flush=True)
        return 1
    finally:
        report["documents"] = records
        atomic_write_json(args.output / "records.json", records)
        atomic_write_json(args.output / "report.json", report)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query", help="Company name or Chilean RUT")
    parser.add_argument("--cve", action="append", default=[], type=validate_cve)
    parser.add_argument("--html", type=Path, help="Saved Legal tab HTML")
    parser.add_argument("--company-name", default="")
    parser.add_argument("--rut", default="")
    parser.add_argument("--output", type=Path, default=Path("data/dequienes"))
    parser.add_argument("--profile", type=Path, default=Path("data/dequienes-profile"))
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--interactive", action="store_true", help="Visible browser for login, selection and CAPTCHA")
    parser.add_argument("--discover-only", action="store_true")
    args = parser.parse_args()
    if not (args.query or args.cve or args.html):
        parser.error("Provide --query, --cve or --html")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    try:
        raise SystemExit(asyncio.run(run(args)))
    except KeyboardInterrupt:
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
