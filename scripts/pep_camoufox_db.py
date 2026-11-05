"""SQLite state storage and explicit JSON export for Camoufox runs."""

from __future__ import annotations

import argparse
import json
import sqlite3
import threading
import time
from contextlib import closing, contextmanager
from pathlib import Path

from scripts.pep_camoufox_export import write_export
from src.utils.atomic import atomic_write_json


class StateDB:
    def __init__(self, path: Path, batch_size: int = 500):
        self.path, self.batch_size = path, batch_size
        self.pending, self.saved = {}, {}
        self.lock = threading.RLock()
        self.last_commit = time.monotonic()
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS links (
                    url TEXT PRIMARY KEY, n INTEGER NOT NULL, status TEXT NOT NULL,
                    payload TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS links_status ON links(status);
                CREATE TABLE IF NOT EXISTS documents (
                    url TEXT NOT NULL, id TEXT NOT NULL, status TEXT NOT NULL,
                    local_path TEXT, payload TEXT NOT NULL,
                    PRIMARY KEY (url, id), FOREIGN KEY (url) REFERENCES links(url)
                );
                CREATE INDEX IF NOT EXISTS documents_status ON documents(status);
            """)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=60)
        try:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA synchronous=NORMAL")
            with db:
                yield db
        finally:
            db.close()

    def stage(self, jobs):
        with self.lock:
            self._stage(jobs)

    def _stage(self, jobs):
        for job in jobs:
            payload = json.dumps(job, ensure_ascii=False, separators=(",", ":"))
            if payload != self.saved.get(job["url"]):
                self.pending[job["url"]] = (job["n"], job["status"], payload)
                self.saved[job["url"]] = payload

    def flush(self, *, force=False, metadata=None):
        with self.lock:
            return self._flush(force=force, metadata=metadata)

    def _flush(self, *, force=False, metadata=None):
        if not force and len(self.pending) < self.batch_size:
            return 0
        count = 0
        # Every transaction contains at most batch_size links, with all their documents.
        while self.pending:
            batch = list(self.pending.items())[: self.batch_size]
            with self.connect() as db:
                for url, (number, status, payload) in batch:
                    db.execute(
                        """INSERT INTO links(url,n,status,payload) VALUES(?,?,?,?)
                        ON CONFLICT(url) DO UPDATE SET n=excluded.n,status=excluded.status,
                        payload=excluded.payload,updated_at=CURRENT_TIMESTAMP""",
                        (url, number, status, payload),
                    )
                    db.execute("DELETE FROM documents WHERE url=?", (url,))
                    db.executemany(
                        "INSERT INTO documents(url,id,status,local_path,payload) VALUES(?,?,?,?,?)",
                        [
                            (url, str(doc["id"]), doc["status"], doc.get("local_path"), json.dumps(doc, ensure_ascii=False))
                            for doc in json.loads(payload).get("documents", [])
                        ],
                    )
            for url, _ in batch:
                del self.pending[url]
            count += len(batch)
        if metadata is not None:
            with self.connect() as db:
                db.execute(
                    "INSERT OR REPLACE INTO metadata VALUES('report',?)",
                    (json.dumps({key: value for key, value in metadata.items() if key != "jobs"}, ensure_ascii=False),),
                )
        self.last_commit = time.monotonic()
        return count


def read_report(path: Path):
    uri = path.resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=60)) as db:
        db.execute("BEGIN")
        metadata = db.execute("SELECT payload FROM metadata WHERE key='report'").fetchone()
        report = json.loads(metadata[0]) if metadata else {"status": "interrupted"}
        report["jobs"] = [json.loads(row[0]) for row in db.execute("SELECT payload FROM links ORDER BY n")]
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("--stats", action="store_true", help="Print SQL status counts without exporting JSON")
    args = parser.parse_args()
    if args.database.is_dir():
        candidates = list(args.database.glob("run-*/state.sqlite3"))
        if not candidates:
            parser.error("No run databases found in this directory")
        args.database = max(candidates, key=lambda path: path.parent.name)
    if args.stats:
        with closing(sqlite3.connect(args.database.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("BEGIN")
            print(
                json.dumps(
                    {
                        "database": str(args.database),
                        **{
                            table: dict(db.execute(f"SELECT status,COUNT(*) FROM {table} GROUP BY status"))
                            for table in ("links", "documents")
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        return
    report = read_report(args.database)
    output = args.database.parent
    atomic_write_json(output / "report.json", report)
    write_export(output, report)
    print(f"Exported {len(report['jobs'])} links to {output / 'politicians.json'}")


if __name__ == "__main__":
    main()
