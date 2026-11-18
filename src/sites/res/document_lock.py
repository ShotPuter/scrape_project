"""Coordinate access to the same certificate across RES worker processes."""

import asyncio
import fcntl
import hashlib
from contextlib import asynccontextmanager


@asynccontextmanager
async def document_lock(database_path, cve):
    # Fixed shards keep the number of lock files bounded for large archives.
    shard = hashlib.sha256(cve.encode()).hexdigest()[:3]
    directory = database_path.parent / (database_path.name + ".document-locks")
    directory.mkdir(exist_ok=True)
    with (directory / shard).open("a") as lock:
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                await asyncio.sleep(0.1)
        yield
