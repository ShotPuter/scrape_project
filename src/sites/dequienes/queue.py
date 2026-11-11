"""SQLite WAL queues: atomic leases, fenced acknowledgements and legacy migration."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

QUEUES = {"public_discovery", "company_lookup", "res_company_lookup", "certificate_download"}
TERMINAL = {
    "completed",
    "no_cve_found",
    "needs_login",
    "captcha_required",
    "blocked",
    "failed",
    "not_found",
    "ambiguous_match",
}


class LostLease(ValueError):
    """The worker no longer owns this attempt; its result must not be committed."""


def normalized_rut(raw: str) -> str | None:
    value = re.sub(r"[.\s]", "", raw).upper()
    if not re.fullmatch(r"\d{7,8}-[0-9K]", value):
        return None
    number, check = value.split("-")
    total = sum(int(digit) * (2 + i % 6) for i, digit in enumerate(reversed(number)))
    calculated = 11 - total % 11
    expected = "0" if calculated == 11 else "K" if calculated == 10 else str(calculated)
    return value if check == expected else None


class QueueStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS publications (
                source_url TEXT PRIMARY KEY, month TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
                company_name TEXT, name_key TEXT, rut TEXT, publication_title TEXT, publication_date TEXT, error TEXT
            );
            CREATE TABLE IF NOT EXISTS pipeline_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS companies (
                company_id TEXT PRIMARY KEY, company_name TEXT NOT NULL, name_key TEXT NOT NULL,
                rut TEXT, identity_kind TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS company_sources (
                company_id TEXT REFERENCES companies(company_id), source_url TEXT REFERENCES publications(source_url),
                PRIMARY KEY(company_id,source_url)
            );
            CREATE TABLE IF NOT EXISTS documents (
                cve TEXT PRIMARY KEY COLLATE BINARY, status TEXT NOT NULL DEFAULT 'candidate'
            );
            CREATE TABLE IF NOT EXISTS document_files (
                cve TEXT PRIMARY KEY REFERENCES documents(cve), local_path TEXT NOT NULL,
                sha256 TEXT NOT NULL, size_bytes INTEGER NOT NULL, download_url TEXT NOT NULL,
                downloaded_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS certificate_download_results (
                job_id INTEGER PRIMARY KEY REFERENCES jobs(id), cve TEXT REFERENCES documents(cve),
                status TEXT NOT NULL, result_json TEXT NOT NULL, checked_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS document_sources (
                cve TEXT REFERENCES documents(cve), source_url TEXT REFERENCES publications(source_url),
                company_id TEXT REFERENCES companies(company_id), evidence TEXT NOT NULL,
                PRIMARY KEY(cve,source_url,evidence)
            );
            CREATE TABLE IF NOT EXISTS code_candidates (
                source_url TEXT REFERENCES publications(source_url), code TEXT, kind TEXT, evidence TEXT,
                PRIMARY KEY(source_url,code,kind,evidence)
            );
            CREATE TABLE IF NOT EXISTS publication_scans (
                source_url TEXT PRIMARY KEY REFERENCES publications(source_url), status TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY, queue TEXT NOT NULL, job_key TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3, available_at REAL NOT NULL DEFAULT 0,
                lease_owner TEXT, lease_token TEXT, lease_until REAL, error TEXT,
                UNIQUE(queue,job_key)
            );
            CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(queue,status,available_at);
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY, queue TEXT NOT NULL, max_items INTEGER,
                claimed INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS run_jobs (
                run_id TEXT REFERENCES runs(run_id), job_id INTEGER REFERENCES jobs(id),
                PRIMARY KEY(run_id,job_id)
            );
            CREATE TABLE IF NOT EXISTS job_attempts (
                token TEXT PRIMARY KEY, job_id INTEGER REFERENCES jobs(id), run_id TEXT REFERENCES runs(run_id),
                owner TEXT NOT NULL, started_at REAL NOT NULL, finished_at REAL, status TEXT NOT NULL, error TEXT
            );
            CREATE TABLE IF NOT EXISTS company_lookup_results (
                job_id INTEGER PRIMARY KEY REFERENCES jobs(id), company_id TEXT REFERENCES companies(company_id),
                status TEXT NOT NULL, source_url TEXT NOT NULL, result_json TEXT NOT NULL, checked_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS account_document_sources (
                cve TEXT REFERENCES documents(cve), company_id TEXT REFERENCES companies(company_id),
                source_url TEXT NOT NULL, PRIMARY KEY(cve,company_id,source_url)
            );
            CREATE TABLE IF NOT EXISTS res_lookup_results (
                job_id INTEGER PRIMARY KEY REFERENCES jobs(id), company_id TEXT REFERENCES companies(company_id),
                status TEXT NOT NULL, result_json TEXT NOT NULL, checked_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS res_document_sources (
                cve TEXT REFERENCES documents(cve), company_id TEXT REFERENCES companies(company_id),
                source_url TEXT NOT NULL, action_date TEXT NOT NULL, action_type TEXT NOT NULL,
                attention_number TEXT NOT NULL, PRIMARY KEY(cve,company_id,attention_number)
            );
            CREATE TABLE IF NOT EXISTS request_budgets (
                source TEXT PRIMARY KEY, next_at REAL NOT NULL
            );
        """)
        with self.transaction():
            if not self.db.execute("SELECT 1 FROM pipeline_meta WHERE key='schema_version'").fetchone():
                columns = [
                    "source_url",
                    "month",
                    "status",
                    "company_name",
                    "name_key",
                    "rut",
                    "publication_title",
                    "publication_date",
                ]
                rows = self.db.execute(f"SELECT {','.join(columns)} FROM publications").fetchall()
                for row in rows:
                    item = dict(zip(columns, row, strict=True))
                    done = item["status"] == "collected"
                    self._enqueue("public_discovery", item["source_url"], status="completed" if done else "pending")
                    if done and item["company_name"]:
                        self._company(item)
                        self.db.execute(
                            "INSERT OR IGNORE INTO publication_scans VALUES (?,?)", (item["source_url"], "not_scanned")
                        )
                self.db.execute("INSERT INTO pipeline_meta VALUES ('schema_version','1')")

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def close(self):
        self.db.close()

    def _enqueue(self, queue, key, *, status="pending", max_attempts=3):
        if queue not in QUEUES:
            raise ValueError("Unknown queue")
        self.db.execute(
            "INSERT OR IGNORE INTO jobs(queue,job_key,status,max_attempts) VALUES (?,?,?,?)",
            (queue, key, status, max_attempts),
        )

    def add_publications(self, month: str, urls: list[str], max_attempts=3):
        with self.transaction():
            for url in urls:
                self.db.execute("INSERT OR IGNORE INTO publications(source_url,month) VALUES (?,?)", (url, month))
                collected = self.db.execute("SELECT status FROM publications WHERE source_url=?", (url,)).fetchone()[0]
                self._enqueue(
                    "public_discovery",
                    url,
                    status="completed" if collected == "collected" else "pending",
                    max_attempts=max_attempts,
                )

                self.db.execute(
                    """UPDATE jobs SET max_attempts=?,status=CASE WHEN attempts>=? THEN 'failed' ELSE status END
                    WHERE queue='public_discovery' AND job_key=? AND status IN ('pending','retry_wait')""",
                    (max_attempts, max_attempts, url),
                )

    def retry_errors(self, queue="public_discovery") -> int:
        if queue not in QUEUES:
            raise ValueError("Unknown queue")
        with self.transaction():
            cursor = self.db.execute(
                """UPDATE jobs SET status='pending',attempts=0,error=NULL,available_at=0
                WHERE queue=? AND status IN ('failed','blocked')""",
                (queue,),
            )
            return cursor.rowcount

    def rescan_legacy(self) -> int:
        """Explicitly requeue pre-pipeline pages; preserve their collected metadata."""
        with self.transaction():
            cursor = self.db.execute("""UPDATE jobs SET status='pending',attempts=0,error=NULL,available_at=0
                WHERE queue='public_discovery' AND status='completed' AND job_key IN
                (SELECT source_url FROM publication_scans WHERE status='not_scanned')""")
            return cursor.rowcount

    def start_run(self, queue: str, limit: int | None = None, *, run_id: str | None = None) -> str:
        if queue not in QUEUES or (limit is not None and (type(limit) is not int or limit < 0)):
            raise ValueError("Invalid queue or run limit")
        run_id = run_id or uuid.uuid4().hex
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", run_id):
            raise ValueError("Invalid run ID")
        with self.transaction():
            self.db.execute(
                "INSERT OR IGNORE INTO runs(run_id,queue,max_items,created_at) VALUES (?,?,?,?)",
                (run_id, queue, limit, time.time()),
            )
            stored = self.db.execute("SELECT queue,max_items FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if stored != (queue, limit):
                raise ValueError("Existing run has a different queue or limit")
        return run_id

    def _recover(self, now):
        expired = self.db.execute(
            "SELECT lease_token FROM jobs WHERE status='running' AND lease_until<=?", (now,)
        ).fetchall()
        for (token,) in expired:
            self.db.execute("UPDATE job_attempts SET status='expired',finished_at=? WHERE token=?", (now, token))
        self.db.execute(
            """UPDATE jobs SET status=CASE WHEN attempts<max_attempts THEN 'retry_wait' ELSE 'failed' END,
            lease_owner=NULL,lease_token=NULL,lease_until=NULL,available_at=?,error='lease expired'
            WHERE status='running' AND lease_until<=?""",
            (now, now),
        )

    def claim(self, run_id: str, owner: str, *, lease_seconds=180, max_in_flight=4, months=None, keys=None, now=None):
        if lease_seconds <= 0 or max_in_flight < 1:
            raise ValueError("Invalid lease or concurrency")
        now = time.time() if now is None else now
        with self.transaction():
            self._recover(now)
            run = self.db.execute("SELECT queue,max_items,claimed FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if not run:
                raise ValueError("Unknown run")
            queue, limit, claimed = run
            running = self.db.execute("SELECT count(*) FROM jobs WHERE queue=? AND status='running'", (queue,)).fetchone()[0]
            if running >= max_in_flight:
                return None
            query = """SELECT id,job_key,attempts FROM jobs WHERE queue=? AND status IN ('pending','retry_wait')
                AND available_at<=? AND attempts<max_attempts
                AND (EXISTS(SELECT 1 FROM run_jobs WHERE run_id=? AND job_id=jobs.id) OR ? IS NULL OR ? < ?)"""
            params = [queue, now, run_id, limit, claimed, limit]
            if months is not None:
                if not months:
                    return None
                query += (
                    f" AND job_key IN (SELECT source_url FROM publications WHERE month IN ({','.join('?' for _ in months)}))"
                )
                params.extend(months)
            if keys is not None:
                if not keys:
                    return None
                query += f" AND job_key IN ({','.join('?' for _ in keys)})"
                params.extend(keys)
            row = self.db.execute(query + " ORDER BY id LIMIT 1", params).fetchone()
            if not row:
                return None
            job_id, key, attempts = row
            token = uuid.uuid4().hex
            self.db.execute(
                """UPDATE jobs SET status='running',attempts=attempts+1,lease_owner=?,lease_token=?,
                lease_until=?,error=NULL WHERE id=?""",
                (owner, token, now + lease_seconds, job_id),
            )
            reservation = self.db.execute("INSERT OR IGNORE INTO run_jobs VALUES (?,?)", (run_id, job_id))
            if reservation.rowcount:
                self.db.execute("UPDATE runs SET claimed=claimed+1 WHERE run_id=?", (run_id,))
            self.db.execute(
                "INSERT INTO job_attempts(token,job_id,run_id,owner,started_at,status) VALUES (?,?,?,?,?,?)",
                (token, job_id, run_id, owner, now, "running"),
            )
            return {"id": job_id, "key": key, "token": token, "attempt": attempts + 1}

    def _owned(self, job, now):
        row = self.db.execute(
            "SELECT 1 FROM jobs WHERE id=? AND lease_token=? AND status='running' AND lease_until>?",
            (job["id"], job["token"], now),
        ).fetchone()
        if not row:
            raise LostLease("Attempt expired or belongs to another worker")

    def heartbeat(self, job, lease_seconds=180, *, now=None):
        now = time.time() if now is None else now
        with self.transaction():
            self._owned(job, now)
            self.db.execute("UPDATE jobs SET lease_until=? WHERE id=?", (now + lease_seconds, job["id"]))

    def _finish(self, job, status, now, error=None, retry_seconds=0):
        self._owned(job, now)
        if status not in TERMINAL | {"retry_wait"}:
            raise ValueError("Invalid completion status")
        if status == "retry_wait":
            attempts, maximum = self.db.execute("SELECT attempts,max_attempts FROM jobs WHERE id=?", (job["id"],)).fetchone()
            if attempts >= maximum:
                status = "failed"
        self.db.execute(
            """UPDATE jobs SET status=?,error=?,available_at=?,lease_owner=NULL,lease_token=NULL,
            lease_until=NULL WHERE id=?""",
            (status, error, now + retry_seconds, job["id"]),
        )
        self.db.execute(
            "UPDATE job_attempts SET status=?,error=?,finished_at=? WHERE token=?", (status, error, now, job["token"])
        )

    def finish(self, job, status="completed", *, error=None, retry_seconds=0, now=None):
        now = time.time() if now is None else now
        with self.transaction():
            self._finish(job, status, now, error, retry_seconds)

    def _company(self, item, *, enqueue_lookup=True):
        rut = normalized_rut(item.get("rut") or "")
        key = "rut:" + rut if rut else "name:" + hashlib.sha256(item["name_key"].encode()).hexdigest()
        self.db.execute(
            "INSERT OR IGNORE INTO companies VALUES (?,?,?,?,?)",
            (key, item["company_name"], item["name_key"], rut, "rut" if rut else "name_candidate"),
        )
        self.db.execute("INSERT OR IGNORE INTO company_sources VALUES (?,?)", (key, item["source_url"]))
        if enqueue_lookup:
            self._enqueue("company_lookup", key)
        return key

    def finish_publication(self, job, item: dict, candidates: list[dict], *, now=None):
        now = time.time() if now is None else now
        if item["source_url"] != job["key"]:
            raise ValueError("Publication does not match leased job")
        with self.transaction():
            self._owned(job, now)
            self.db.execute(
                """UPDATE publications SET status='collected',company_name=:company_name,
                name_key=:name_key,rut=:rut,publication_title=:publication_title,publication_date=:publication_date,
                error=NULL WHERE source_url=:source_url""",
                item,
            )
            has_res_reference = any(candidate["kind"] == "res_reference" for candidate in candidates)
            company = self._company(item, enqueue_lookup=not has_res_reference)
            for candidate in candidates:
                code, kind, evidence = candidate["code"], candidate["kind"], candidate["evidence"]
                if kind not in {"res_reference", "ambiguous", "diario_oficial"}:
                    raise ValueError("Unknown code candidate kind")
                self.db.execute(
                    "INSERT OR IGNORE INTO code_candidates VALUES (?,?,?,?)", (item["source_url"], code, kind, evidence)
                )
                if kind == "res_reference":
                    from src.sites.dequienes.source import validate_cve

                    validate_cve(code)
                    self.db.execute("INSERT OR IGNORE INTO documents(cve) VALUES (?)", (code,))
                    self.db.execute(
                        "INSERT OR IGNORE INTO document_sources VALUES (?,?,?,?)",
                        (code, item["source_url"], company, evidence),
                    )
                    self._enqueue("certificate_download", code)
            self.db.execute("INSERT OR REPLACE INTO publication_scans VALUES (?,?)", (item["source_url"], "scanned"))
            self._finish(job, "completed", now)

    def finish_company_lookup(self, job, result: dict):
        now = time.time()
        with self.transaction():
            self._owned(job, now)
            queue, key = self.db.execute("SELECT queue,job_key FROM jobs WHERE id=?", (job["id"],)).fetchone()
            if queue != "company_lookup" or key != result["company_id"]:
                raise ValueError("Lookup result does not match leased company")
            status = result["status"]
            for event in result.get("events", []):
                if event.get("source", "").strip().upper() != "RES":
                    continue
                from src.sites.dequienes.source import validate_cve

                cve = validate_cve(event["cve"])
                self.db.execute("INSERT OR IGNORE INTO documents(cve) VALUES (?)", (cve,))
                self.db.execute(
                    "INSERT OR IGNORE INTO account_document_sources VALUES (?,?,?)", (cve, key, result["source_url"])
                )
                self._enqueue("certificate_download", cve)
            self.db.execute(
                "INSERT OR REPLACE INTO company_lookup_results VALUES (?,?,?,?,?,?)",
                (job["id"], key, status, result["source_url"], json.dumps(result, ensure_ascii=False), now),
            )
            self._finish(job, status, now, result.get("error"))

    def seed_res_jobs(self, keys=None, max_attempts=3):
        with self.transaction():
            if keys is None:
                keys = [row[0] for row in self.db.execute("SELECT company_id FROM companies ORDER BY company_id")]
            for key in keys:
                if not self.db.execute("SELECT 1 FROM companies WHERE company_id=?", (key,)).fetchone():
                    raise ValueError("Unknown company")
                self._enqueue("res_company_lookup", key, max_attempts=max_attempts)
                self.db.execute(
                    """UPDATE jobs SET max_attempts=?,status=CASE WHEN attempts>=? THEN 'failed' ELSE status END
                    WHERE queue='res_company_lookup' AND job_key=? AND status IN ('pending','retry_wait')""",
                    (max_attempts, max_attempts, key),
                )

    def retry_res_jobs(self, run_id=None):
        return self.retry_stage_jobs("res_company_lookup", run_id=run_id)

    def retry_stage_jobs(self, queue, *, run_id=None, keys=None):
        if queue not in {"res_company_lookup", "certificate_download"}:
            raise ValueError("Unsupported retry queue")
        with self.transaction():
            sql = """UPDATE jobs SET status='pending',attempts=0,error=NULL,available_at=0
                WHERE queue=? AND status IN ('failed','blocked','captcha_required')"""
            parameters = [queue]
            if run_id is not None:
                sql += " AND id IN (SELECT job_id FROM run_jobs WHERE run_id=?)"
                parameters.append(run_id)
            if keys is not None:
                sql += f" AND job_key IN ({','.join('?' for _ in keys)})"
                parameters.extend(keys)
            return self.db.execute(sql, parameters).rowcount

    def prepare_certificate_jobs(self, keys=None, max_attempts=3):
        with self.transaction():
            if keys is None:
                keys = [row[0] for row in self.db.execute("SELECT cve FROM documents ORDER BY cve")]
            for cve in keys:
                if not self.db.execute("SELECT 1 FROM documents WHERE cve=?", (cve,)).fetchone():
                    raise ValueError(f"CVE is not present in the document queue: {cve}")
                self._enqueue("certificate_download", cve, max_attempts=max_attempts)
                self.db.execute(
                    """UPDATE jobs SET max_attempts=?,status=CASE WHEN attempts>=? THEN 'failed' ELSE status END
                    WHERE queue='certificate_download' AND job_key=? AND status IN ('pending','retry_wait')""",
                    (max_attempts, max_attempts, cve),
                )

    def finish_certificate(self, job, result, *, now=None):
        from pathlib import PurePosixPath

        from src.sites.dequienes.source import res_url, validate_cve
        from src.sites.res.pdf import MAX_PDF_BYTES

        now = time.time() if now is None else now
        with self.transaction():
            self._owned(job, now)
            queue, key = self.db.execute("SELECT queue,job_key FROM jobs WHERE id=?", (job["id"],)).fetchone()
            if queue != "certificate_download" or validate_cve(result["cve"]) != key:
                raise ValueError("Certificate result does not match leased CVE")
            status = result["status"]
            if status == "downloaded":
                path = PurePosixPath(result["local_path"])
                if (
                    path.is_absolute()
                    or ".." in path.parts
                    or path.suffix != ".pdf"
                    or not re.fullmatch(r"[a-f0-9]{64}", result["sha256"])
                    or not 0 < result["size_bytes"] <= MAX_PDF_BYTES
                ):
                    raise ValueError("Invalid PDF metadata")
                self.db.execute(
                    "INSERT OR REPLACE INTO document_files VALUES (?,?,?,?,?,?)",
                    (
                        key,
                        result["local_path"],
                        result["sha256"],
                        result["size_bytes"],
                        res_url(result["download_url"]),
                        result["downloaded_at"],
                    ),
                )
                self.db.execute("UPDATE documents SET status='downloaded' WHERE cve=?", (key,))
            elif status not in {"retry_wait", "failed", "blocked", "captcha_required"}:
                raise ValueError("Invalid certificate completion status")
            self._finish(
                job,
                "completed" if status == "downloaded" else status,
                now,
                result.get("error"),
                result.get("retry_seconds", 30),
            )
            if status != "downloaded":
                result["status"] = self.db.execute("SELECT status FROM jobs WHERE id=?", (job["id"],)).fetchone()[0]
            self.db.execute(
                "INSERT OR REPLACE INTO certificate_download_results VALUES (?,?,?,?,?)",
                (job["id"], key, result["status"], json.dumps(result, ensure_ascii=False), now),
            )

    def finish_res_lookup(self, job, result):
        from src.sites.dequienes.source import validate_cve

        now = time.time()
        with self.transaction():
            self._owned(job, now)
            queue, key = self.db.execute("SELECT queue,job_key FROM jobs WHERE id=?", (job["id"],)).fetchone()
            if queue != "res_company_lookup" or result["company_id"] != key:
                raise ValueError("RES result does not match leased company")
            events = result.get("events", [])
            if result["status"] == "completed" and not any(e.get("cve") for e in events):
                raise ValueError("Successful RES lookup must contain a CVE")
            if result["status"] != "completed" and any(e.get("cve") for e in events):
                raise ValueError("CVE found but result is not completed")
            for event in events:
                if not event.get("cve"):
                    continue
                cve = validate_cve(event["cve"])
                self.db.execute("INSERT OR IGNORE INTO documents(cve) VALUES (?)", (cve,))
                self.db.execute(
                    "INSERT OR IGNORE INTO res_document_sources VALUES (?,?,?,?,?,?)",
                    (cve, key, result["source_url"], event["date"], event["event_type"], event["attention_number"]),
                )
                self._enqueue("certificate_download", cve)
            self._finish(job, result["status"], now, result.get("error"), result.get("retry_seconds", 30))
            result["status"] = self.db.execute("SELECT status FROM jobs WHERE id=?", (job["id"],)).fetchone()[0]
            self.db.execute(
                "INSERT OR REPLACE INTO res_lookup_results VALUES (?,?,?,?,?)",
                (job["id"], key, result["status"], json.dumps(result, ensure_ascii=False), now),
            )

    def request_delay(self, source: str, rate_per_minute: float, *, now=None) -> float:
        if rate_per_minute <= 0:
            raise ValueError("Rate must be positive")
        now = time.time() if now is None else now
        with self.transaction():
            row = self.db.execute("SELECT next_at FROM request_budgets WHERE source=?", (source,)).fetchone()
            slot = max(now, row[0] if row else now)
            self.db.execute("INSERT OR REPLACE INTO request_budgets VALUES (?,?)", (source, slot + 60 / rate_per_minute))
        return slot - now

    def status(self) -> dict:
        return {
            "queues": {
                queue: dict(self.db.execute("SELECT status,count(*) FROM jobs WHERE queue=? GROUP BY status", (queue,)))
                for queue in sorted(QUEUES)
            },
            "companies": self.db.execute("SELECT count(*) FROM companies").fetchone()[0],
            "cve_candidates": dict(self.db.execute("SELECT kind,count(*) FROM code_candidates GROUP BY kind")),
            "documents": self.db.execute("SELECT count(*) FROM documents").fetchone()[0],
            "publication_scans": dict(self.db.execute("SELECT status,count(*) FROM publication_scans GROUP BY status")),
        }
