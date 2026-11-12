"""Shared SQLite ledger and offline import of RES benchmark checkpoints."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import sqlite3
import time
import uuid
from pathlib import Path

from src.sites.res.pdf import cached_pdf, certificate_directory, pdf_metadata, read_pdf
from src.utils.atomic import atomic_write_json
from src.utils.error_details import safe_message


class TrackingStore:
    def __init__(self, path, run_id, *, worker=None, invocation=None):
        self.path, self.run_id = Path(path).resolve(), run_id
        self.base = self.path.parent
        self.worker, self.invocation = worker, invocation
        self.base.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY, config_json TEXT NOT NULL, status TEXT NOT NULL,
                created_at REAL NOT NULL, updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS companies (
                company_id TEXT PRIMARY KEY, company_name TEXT NOT NULL, rut TEXT,
                search_status TEXT NOT NULL DEFAULT 'pending', searched_at REAL, updated_at REAL NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS companies_rut ON companies(rut) WHERE rut IS NOT NULL;
            CREATE TABLE IF NOT EXISTS run_companies (
                run_id TEXT REFERENCES runs(run_id), company_id TEXT REFERENCES companies(company_id),
                status TEXT NOT NULL DEFAULT 'pending', worker TEXT, error TEXT, updated_at REAL NOT NULL,
                PRIMARY KEY(run_id,company_id)
            );
            CREATE INDEX IF NOT EXISTS run_companies_status ON run_companies(run_id,status);
            CREATE TABLE IF NOT EXISTS documents (
                cve TEXT PRIMARY KEY COLLATE BINARY, status TEXT NOT NULL DEFAULT 'pending',
                codigo TEXT, download_url TEXT, verified_at REAL, error TEXT, updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS company_documents (
                company_id TEXT REFERENCES companies(company_id), cve TEXT REFERENCES documents(cve),
                action_json TEXT, discovered_at REAL NOT NULL, PRIMARY KEY(company_id,cve)
            );
            CREATE INDEX IF NOT EXISTS company_documents_cve ON company_documents(cve);
            CREATE TABLE IF NOT EXISTS document_files (
                cve TEXT PRIMARY KEY REFERENCES documents(cve), local_path TEXT NOT NULL,
                sha256 TEXT NOT NULL, size_bytes INTEGER NOT NULL, downloaded_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS attempts (
                attempt_id TEXT PRIMARY KEY, run_id TEXT REFERENCES runs(run_id),
                company_id TEXT REFERENCES companies(company_id), invocation_id TEXT, worker TEXT,
                attempt_number INTEGER, started_at REAL, finished_at REAL, seconds REAL,
                status TEXT NOT NULL, error TEXT
            );
            CREATE INDEX IF NOT EXISTS attempts_company ON attempts(company_id,started_at);
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY, run_id TEXT REFERENCES runs(run_id),
                company_id TEXT REFERENCES companies(company_id), cve TEXT REFERENCES documents(cve),
                worker TEXT, stage TEXT NOT NULL, status TEXT NOT NULL, error TEXT, at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS events_company ON events(company_id,at);
            PRAGMA user_version=1;
        """)

    def close(self):
        self.db.close()

    def register_run(self, config, companies):
        now = time.time()
        with self.db:
            self.db.execute(
                """INSERT INTO runs VALUES (?,?, 'prepared',?,?)
                ON CONFLICT(run_id) DO UPDATE SET config_json=excluded.config_json,updated_at=excluded.updated_at""",
                (self.run_id, json.dumps(config), now, now),
            )
            for company in companies:
                self.db.execute(
                    """INSERT INTO companies(company_id,company_name,rut,updated_at) VALUES (?,?,?,?)
                    ON CONFLICT(company_id) DO UPDATE SET company_name=excluded.company_name,rut=excluded.rut""",
                    (company["company_id"], company["company_name"], company.get("rut"), now),
                )
                self.db.execute(
                    "INSERT OR IGNORE INTO run_companies(run_id,company_id,updated_at) VALUES (?,?,?)",
                    (self.run_id, company["company_id"], now),
                )

    def run_status(self, status):
        with self.db:
            self.db.execute("UPDATE runs SET status=?,updated_at=? WHERE run_id=?", (status, time.time(), self.run_id))

    def company_status(self, company_id, status, error=None):
        with self.db:
            self.db.execute(
                "UPDATE run_companies SET status=?,error=?,worker=COALESCE(?,worker),updated_at=? "
                "WHERE run_id=? AND company_id=?",
                (status, safe_message(error) if error else None, self.worker, time.time(), self.run_id, company_id),
            )

    def search_result(self, company_id, cached, actions=()):
        when = cached.get("searched_at") or time.time()
        with self.db:
            self.db.execute(
                "UPDATE companies SET search_status=?,searched_at=?,updated_at=? "
                "WHERE company_id=? AND (searched_at IS NULL OR searched_at<=?)",
                (cached.get("status", "completed"), when, time.time(), company_id, when),
            )
            for cve in cached.get("cves", []):
                self.db.execute("INSERT OR IGNORE INTO documents(cve,updated_at) VALUES (?,?)", (cve, time.time()))
                action = next((a for a in actions if a.get("cve") == cve), None)
                self.db.execute(
                    """INSERT INTO company_documents VALUES (?,?,?,?)
                    ON CONFLICT(company_id,cve) DO UPDATE SET action_json=COALESCE(excluded.action_json,action_json)""",
                    (company_id, cve, json.dumps(action, ensure_ascii=False) if action else None, when),
                )

    def document_status(self, cve, status, company_id=None, *, error=None, record=None):
        now, message = time.time(), safe_message(error) if error else None
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO documents(cve,updated_at) VALUES (?,?)", (cve, now))
            self.db.execute("UPDATE documents SET status=?,error=?,updated_at=? WHERE cve=?", (status, message, now, cve))
            if record:
                self.db.execute(
                    "UPDATE documents SET codigo=?,download_url=?,verified_at=? WHERE cve=?",
                    (
                        record.get("codigo"),
                        record.get("link") or record.get("download_url"),
                        record.get("verified_at", now),
                        cve,
                    ),
                )
            self.db.execute(
                "INSERT INTO events(run_id,company_id,cve,worker,stage,status,error,at) VALUES (?,?,?,?,?,?,?,?)",
                (self.run_id, company_id, cve, self.worker, "document", status, message, now),
            )

    def file_result(self, saved, directory, company_id=None):
        cve = saved["cve"]
        relative = (Path(directory) / saved["local_path"]).resolve().relative_to(self.base).as_posix()
        # Manifests are accepted only after the same validation used by normal resume.
        verified = cached_pdf(Path(directory), cve)
        if not verified or verified["local_path"] != saved["local_path"]:
            raise ValueError("Cannot track a PDF without a valid manifest and matching SHA-256")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO documents(cve,updated_at) VALUES (?,?)", (cve, time.time()))
            self.db.execute(
                """INSERT INTO document_files VALUES (?,?,?,?,?) ON CONFLICT(cve) DO UPDATE SET
                local_path=excluded.local_path,sha256=excluded.sha256,size_bytes=excluded.size_bytes,
                downloaded_at=excluded.downloaded_at""",
                (cve, relative, verified["sha256"], verified["size_bytes"], verified["downloaded_at"]),
            )
            self.db.execute(
                "UPDATE documents SET status='downloaded',download_url=?,error=NULL,updated_at=? WHERE cve=?",
                (verified["download_url"], time.time(), cve),
            )

    def begin_attempt(self, company_id, number):
        attempt_id = uuid.uuid4().hex
        with self.db:
            self.db.execute(
                """INSERT INTO attempts(attempt_id,run_id,company_id,invocation_id,worker,attempt_number,
                started_at,status) VALUES (?,?,?,?,?,?,?,'running')""",
                (attempt_id, self.run_id, company_id, self.invocation, self.worker, number, time.time()),
            )
        self.company_status(company_id, "running")
        return attempt_id

    def finish_attempt(self, attempt_id, result, seconds):
        error = safe_message(result.get("error", "")) or None
        with self.db:
            self.db.execute(
                "UPDATE attempts SET finished_at=?,seconds=?,status=?,error=? WHERE attempt_id=?",
                (time.time(), seconds, result["status"], error, attempt_id),
            )
        self.company_status(result["company_id"], result["status"], error)

    def existing_file(self, cve):
        row = self.db.execute(
            "SELECT f.*,d.download_url FROM document_files f JOIN documents d USING(cve) WHERE cve=?", (cve,)
        ).fetchone()
        if not row:
            return None
        result = dict(row)
        path = (self.base / result["local_path"]).resolve()
        if not path.is_relative_to(self.base):
            return None
        try:
            metadata = pdf_metadata(read_pdf(path))
        except (ValueError, OSError):
            return None
        return result if all(result[k] == v for k, v in metadata.items()) else None

    def restore_worker(self, directory, companies):
        """Recover JSON compatibility checkpoints from SQLite without website requests."""
        directory = Path(directory)
        state_path = directory / "state.json"
        state = json.loads(state_path.read_text()) if state_path.exists() else {"companies": {}, "cves": {}}
        restored = False
        for company in companies:
            row = self.db.execute("SELECT * FROM companies WHERE company_id=?", (company["company_id"],)).fetchone()
            if not row or row["search_status"] in {"pending", "searching"}:
                continue
            docs = self.db.execute(
                "SELECT d.* FROM documents d JOIN company_documents cd USING(cve) WHERE cd.company_id=? ORDER BY cve",
                (company["company_id"],),
            ).fetchall()
            state["companies"][company["company_id"]] = {
                **company,
                "status": row["search_status"],
                "searched_at": row["searched_at"],
                "cves": [d["cve"] for d in docs],
            }
            for doc in docs:
                if doc["codigo"] and doc["download_url"]:
                    state["cves"][doc["cve"]] = {
                        "cve": doc["cve"],
                        "codigo": doc["codigo"],
                        "link": doc["download_url"],
                        "verified_at": doc["verified_at"],
                        "company_id": company["company_id"],
                    }
                saved = self.existing_file(doc["cve"])
                if not saved or cached_pdf(directory, doc["cve"]):
                    continue
                relative = certificate_directory(doc["cve"]) / f"{saved['sha256']}.pdf"
                target = directory / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    try:
                        valid = pdf_metadata(read_pdf(target))["sha256"] == saved["sha256"]
                    except (OSError, ValueError):
                        valid = False
                    if not valid:
                        target.unlink()
                if not target.exists():
                    try:
                        os.link(self.base / saved["local_path"], target)
                    except OSError:
                        shutil.copyfile(self.base / saved["local_path"], target)
                atomic_write_json(
                    target.parent / "manifest.json", {**saved, "local_path": relative.as_posix(), "status": "downloaded"}
                )
            restored = True
        if restored:
            atomic_write_json(state_path, state)

    def rows(self):
        return [
            dict(r)
            for r in self.db.execute(
                """SELECT c.company_id,c.company_name,c.rut,rc.status,c.search_status,
            rc.error,rc.worker,count(cd.cve) AS cves_found,
            count(CASE WHEN d.status='downloaded' THEN f.cve END) AS pdfs_saved,
            group_concat(CASE WHEN f.cve IS NULL OR d.status!='downloaded' THEN cd.cve END, ', ') AS missing_cves
            FROM run_companies rc JOIN companies c USING(company_id)
            LEFT JOIN company_documents cd USING(company_id) LEFT JOIN documents d USING(cve)
            LEFT JOIN document_files f USING(cve)
            WHERE rc.run_id=? GROUP BY c.company_id ORDER BY c.company_name""",
                (self.run_id,),
            )
        ]

    def reconcile_files(self):
        cves = [
            row[0]
            for row in self.db.execute(
                """SELECT DISTINCT f.cve FROM document_files f
            JOIN company_documents cd USING(cve) JOIN run_companies rc USING(company_id) WHERE rc.run_id=?""",
                (self.run_id,),
            )
        ]
        with self.db:
            for cve in cves:
                if not self.existing_file(cve):
                    self.db.execute(
                        "UPDATE documents SET status='missing_pdf',error=?,updated_at=? WHERE cve=?",
                        ("PDF is missing or its SHA-256 no longer matches", time.time(), cve),
                    )


def import_run(store, root, *, max_documents=0):
    """Idempotent, offline import; existing PDFs are read but never rewritten."""
    root = Path(root).resolve()
    selection = json.loads((root / "selection.json").read_text())
    config = {
        **selection.get("config", {}),
        "max_documents": max_documents,
        "source_url": selection.get("source_url"),
        "resource_id": selection.get("resource_id"),
    }
    store.register_run(config, selection["companies"])
    allowed = {c["company_id"] for c in selection["companies"]}
    for directory in sorted((root / "workers").glob("w*")):
        store.worker = directory.name
        state_path = directory / "state.json"
        state = json.loads(state_path.read_text()) if state_path.exists() else {"companies": {}, "cves": {}}
        for company_id, cached in state["companies"].items():
            if company_id in allowed:
                store.search_result(company_id, cached)
        for cve, record in state["cves"].items():
            company_id = record.get("company_id")
            if store.db.execute("SELECT 1 FROM documents WHERE cve=?", (cve,)).fetchone():
                # A stale checkpoint must not reset a downloaded document's status.
                with store.db:
                    store.db.execute(
                        "UPDATE documents SET codigo=?,download_url=?,verified_at=?,"
                        "status=CASE WHEN status='pending' THEN 'verified' ELSE status END WHERE cve=? "
                        "AND (verified_at IS NULL OR verified_at<=?)",
                        (
                            record.get("codigo"),
                            record.get("link"),
                            record.get("verified_at"),
                            cve,
                            record.get("verified_at") or 0,
                        ),
                    )
        latest = {}
        for report_path in sorted(directory.glob("report-*.json")):
            report = json.loads(report_path.read_text())
            for index, attempt in enumerate(report.get("attempts", [])):
                company_id = attempt["company_id"]
                if company_id not in allowed:
                    continue
                result = attempt["result"]
                key = attempt.get("attempt_id") or f"import:{report_path.relative_to(store.base)}:{index}"
                with store.db:
                    store.db.execute(
                        """INSERT OR IGNORE INTO attempts(attempt_id,run_id,company_id,invocation_id,
                        worker,attempt_number,seconds,status,error) VALUES (?,?,?,?,?,?,?,?,?)""",
                        (
                            key,
                            store.run_id,
                            company_id,
                            report.get("run_id"),
                            directory.name,
                            attempt.get("attempt"),
                            attempt.get("seconds"),
                            result["status"],
                            safe_message(result.get("error", "")) or None,
                        ),
                    )
                latest[company_id] = result
                for doc in result.get("documents", []):
                    if doc.get("cve") and store.db.execute("SELECT 1 FROM documents WHERE cve=?", (doc["cve"],)).fetchone():
                        with store.db:
                            store.db.execute(
                                "UPDATE documents SET status=?,error=? WHERE cve=? AND status!='downloaded'",
                                (doc["status"], safe_message(doc.get("error", "")) or None, doc["cve"]),
                            )
            latest.update({r["company_id"]: r for r in report.get("companies", []) if r["company_id"] in allowed})
        for company_id in allowed:
            cached = state["companies"].get(company_id)
            if not cached:
                known = store.db.execute("SELECT * FROM companies WHERE company_id=?", (company_id,)).fetchone()
                if known and known["searched_at"] is not None:
                    cached = {
                        "status": known["search_status"],
                        "cves": [
                            r[0]
                            for r in store.db.execute(
                                "SELECT cve FROM company_documents WHERE company_id=? ORDER BY cve", (company_id,)
                            )
                        ],
                    }
            if not cached and company_id not in latest:
                continue
            cves = (cached or {}).get("cves", [])
            selected = cves[:max_documents] if max_documents else cves
            saved = []
            for cve in cves:
                manifest = cached_pdf(directory, cve)
                if manifest:
                    store.file_result(manifest, directory, company_id)
                if cve in selected:
                    saved.append(bool(manifest or store.existing_file(cve)))
            result = latest.get(company_id, {})
            live = store.db.execute(
                "SELECT status,error FROM attempts WHERE run_id=? AND company_id=? "
                "AND finished_at IS NOT NULL AND status NOT IN ('interrupted','proxy_exhausted') "
                "ORDER BY finished_at DESC LIMIT 1",
                (store.run_id, company_id),
            ).fetchone()
            if live:
                result = {**result, **dict(live)}
            status = result.get("status", (cached or {}).get("status", "pending"))
            if selected and all(saved):
                status = "downloaded"
            elif any(saved):
                status = "partial"
            elif status == "downloaded":
                status = "missing_pdf"
            store.company_status(company_id, status, None if status == "downloaded" else result.get("error"))
    store.reconcile_files()
    # Running attempts left by an interrupted old process are retained in the ledger.
    with store.db:
        store.db.execute(
            "UPDATE attempts SET status='interrupted',finished_at=? WHERE run_id=? AND status='running'",
            (time.time(), store.run_id),
        )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/res-benchmark"))
    parser.add_argument("--run-id", default="res-2025-01-100")
    parser.add_argument("--db", type=Path)
    parser.add_argument("--action", choices=["sync", "status", "errors", "companies", "documents"], default="status")
    parser.add_argument("--company", help="RUT or company name substring")
    parser.add_argument("--max-documents", type=int, default=0)
    args = parser.parse_args(argv)
    if Path(args.run_id).name != args.run_id or args.run_id in {".", ".."}:
        parser.error("Invalid run-id")
    root = (args.output / args.run_id).resolve()
    path = args.db or args.output / "tracking.sqlite3"
    if args.action != "sync" and not path.exists():
        parser.error("Tracking database not found; run --action sync first")
    store = TrackingStore(path, args.run_id)
    try:
        if args.action == "sync":
            with (root / ".lock").open("a") as lock:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    parser.error("Run is active; use status without sync")
                import_run(store, root, max_documents=args.max_documents)
                summary_path = root / "summary.json"
                summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
                store.run_status(summary.get("state", "imported"))
        rows = store.rows()
        if args.company:
            rows = [r for r in rows if args.company.casefold() in (r["company_name"] + " " + (r["rut"] or "")).casefold()]
        if args.action == "documents":
            result = [
                dict(row)
                for company in rows
                for row in store.db.execute(
                    """SELECT d.*,f.local_path,f.sha256,
                f.size_bytes FROM company_documents cd JOIN documents d USING(cve) LEFT JOIN document_files f USING(cve)
                WHERE cd.company_id=? ORDER BY cve""",
                    (company["company_id"],),
                )
            ]
        elif args.action in {"companies", "errors"}:
            result = [
                r for r in rows if args.action != "errors" or r["status"] not in {"downloaded", "not_found", "no_cve_found"}
            ]
        else:
            counts = {"pdfs": 0, "cves": 0}
            if rows:
                placeholders = ",".join("?" for _ in rows)
                counts = dict(
                    store.db.execute(
                        f"""SELECT count(DISTINCT cd.cve) AS cves,
                    count(DISTINCT CASE WHEN d.status='downloaded' THEN f.cve END) AS pdfs
                    FROM company_documents cd JOIN documents d USING(cve) LEFT JOIN document_files f USING(cve)
                    WHERE cd.company_id IN ({placeholders})""",
                        [r["company_id"] for r in rows],
                    ).fetchone()
                )
            result = {
                "database": str(store.path),
                "run_id": args.run_id,
                "companies": len(rows),
                "statuses": {
                    status: sum(r["status"] == status for r in rows) for status in sorted({r["status"] for r in rows})
                },
                **counts,
            }
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
