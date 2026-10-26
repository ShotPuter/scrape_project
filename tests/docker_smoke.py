"""Container acceptance checks using only loopback HTTP and an isolated bind mount."""

import asyncio
import base64
import csv
import hashlib
import io
import json
import os
import sys
import tempfile
from pathlib import Path

from aiohttp import web
from reportlab.pdfgen.canvas import Canvas


async def main():
    assert os.getuid() != 0, "Container must run without root"
    try:
        await asyncio.to_thread(Path("/app/write-probe").write_text, "unexpected")
    except OSError:
        pass
    else:
        raise AssertionError("Application directory must be read-only")

    output = Path(tempfile.mkdtemp(prefix="smoke-", dir="/app/data"))
    stream = io.BytesIO()
    canvas = Canvas(stream)
    canvas.drawString(72, 720, "Original PDF fixture")
    canvas.save()
    original = stream.getvalue()
    details = {}
    for identifier in ("original", "text"):
        details[identifier] = {
            "id": identifier,
            "titulo": "Container acceptance fixture",
            "codigo": identifier + "/2026",
            "fechaPublicacion": "2026-10-23",
            "empresa": {"razonSocial": "Fixture Company", "matricula": "12345"},
            "textoCompleto": "<p>Text publication with Spanish accents: publicación.</p>",
            "documento": base64.b64encode(original).decode() if identifier == "original" else None,
        }

    async def catalog(request):
        return web.json_response({"finalizado": True, "datos": []})

    async def search(request):
        payload = await request.json()
        rows = list(details.values()) if payload["pagina"] == 1 else []
        return web.json_response({"finalizado": True, "datos": {"total": 2, "filas": rows}})

    async def detail(request):
        return web.json_response({"finalizado": True, "datos": details[request.match_info["id"]]})

    app = web.Application()
    app.router.add_post("/api/publicaciones/catalogos/lista", catalog)
    app.router.add_post("/api/publicaciones", search)
    app.router.add_get("/api/publicaciones/{id}", detail)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    env = {
        **os.environ,
        "SEPREC_API_BASE_URL": f"http://127.0.0.1:{port}/api",
        "PDF_STORAGE_DIR": str(output / "pdf"),
        "CSV_EXPORT_PATH": str(output / "publications.csv"),
        "LOG_FILE": str(output / "scraper.log"),
        "HTTP_REQUESTS_PER_SECOND": "0",
        "PAGE_DELAY_SECONDS": "0",
        "RETRY_ATTEMPTS": "1",
        "LOG_TO_FILE": "true",
    }
    args = ["--proxy-mode", "direct", "--category", "2", "--year", "2026", "--max-items", "2", "--max-pages", "1"]

    async def execute(*arguments):
        process = await asyncio.create_subprocess_exec(
            sys.executable, "main.py", *arguments, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), 30)
        except BaseException:
            process.kill()
            await process.wait()
            raise
        assert process.returncode == 0, (stdout.decode(), stderr.decode())
        return stdout.decode()

    try:
        first = await execute(*args)
        assert '"items_scraped": 2' in first, first
        with (output / "publications.csv").open(encoding="utf-8-sig") as handle:
            records = list(csv.DictReader(handle))
        assert len(records) == 2
        assert {r["status"] for r in records} == {"DOWNLOADED", "GENERATED_PDF"}
        snapshots = {}
        for record in records:
            path = Path(record["file_path"])
            data = await asyncio.to_thread(path.read_bytes)
            assert data.startswith(b"%PDF") and b"%%EOF" in data[-2048:]
            if record["id"] == "original":
                assert data == original
            stat = await asyncio.to_thread(path.stat)
            snapshots[path] = (hashlib.sha256(data).hexdigest(), stat.st_mtime_ns)
        second = await execute(*args)
        assert '"items_scraped": 0' in second and '"items_skipped": 2' in second, second
        with (output / "publications.csv").open(encoding="utf-8-sig") as handle:
            assert len(list(csv.DictReader(handle))) == 2
        for path, snapshot in snapshots.items():
            data = await asyncio.to_thread(path.read_bytes)
            stat = await asyncio.to_thread(path.stat)
            assert (hashlib.sha256(data).hexdigest(), stat.st_mtime_ns) == snapshot
        assert not await asyncio.to_thread(lambda: list(output.rglob("*.tmp")))
        audit = await execute("--audit-storage")
        assert '"checked": 2' in audit and '"invalid": 0' in audit, audit

        process = await asyncio.create_subprocess_exec(
            sys.executable, "main.py", *args,
            "--schedule", "--interval-hours", "24",
            env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        try:
            for _ in range(100):
                if "Next recurring scrape iteration scheduled" in (output / "scraper.log").read_text():
                    break
                await asyncio.sleep(0.1)
            else:
                raise AssertionError("Scheduler did not reach interval wait")
            process.terminate()
            stdout, stderr = await asyncio.wait_for(process.communicate(), 10)
            assert process.returncode == 0, (stdout.decode(), stderr.decode())
            assert "Scheduler execution loop stopped cleanly" in (output / "scraper.log").read_text()
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
        print(
            json.dumps(
                {
                    "result": "PASS",
                    "uid": os.getuid(),
                    "original_pdfs": 1,
                    "generated_pdfs": 1,
                    "csv_rows": 2,
                    "duplicate_rows": 0,
                    "audit_invalid": 0,
                    "scheduler_sigterm": "clean",
                    "output": str(output),
                },
                indent=2,
            )
        )
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
