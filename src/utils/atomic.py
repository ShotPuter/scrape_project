"""Atomic filesystem writes shared by caches, reports, and storage indexes."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, TextIO

TEMPORARY_FILE_SUFFIX = ".tmp"


def _create_temporary_file(destination: Path) -> tuple[int, Path]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=TEMPORARY_FILE_SUFFIX, dir=destination.parent
    )
    return descriptor, Path(temp_name)


def _preserve_mode(source: Path, destination: Path) -> None:
    """Keep the permissions of an existing destination across a replace."""
    try:
        mode = destination.stat().st_mode & 0o777
    except OSError:
        return
    os.chmod(source, mode)


@contextmanager
def atomic_text_writer(
    path: Path | str,
    *,
    encoding: str = "utf-8",
    newline: str | None = None,
    preserve_mode: bool = True,
) -> Iterator[TextIO]:
    """Yield a text handle that atomically replaces ``path`` on success.

    The temporary file lives in the destination directory so ``os.replace`` is
    atomic on POSIX.  If the block raises, the destination is left untouched and
    the temporary file is removed.
    """
    destination = Path(path)
    descriptor, temp_path = _create_temporary_file(destination)
    try:
        with os.fdopen(descriptor, "w", encoding=encoding, newline=newline) as handle:
            yield handle
            handle.flush()
            os.fsync(handle.fileno())
        if preserve_mode:
            _preserve_mode(temp_path, destination)
        os.replace(temp_path, destination)
    finally:
        temp_path.unlink(missing_ok=True)


def atomic_write_bytes(path: Path | str, payload: bytes, *, preserve_mode: bool = True) -> None:
    """Atomically replace ``path`` with ``payload``."""
    destination = Path(path)
    descriptor, temp_path = _create_temporary_file(destination)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if preserve_mode:
            _preserve_mode(temp_path, destination)
        os.replace(temp_path, destination)
    finally:
        temp_path.unlink(missing_ok=True)


def atomic_write_text(
    path: Path | str,
    text: str,
    *,
    encoding: str = "utf-8",
    newline: str | None = None,
    preserve_mode: bool = True,
) -> None:
    """Atomically replace ``path`` with ``text``."""
    with atomic_text_writer(path, encoding=encoding, newline=newline, preserve_mode=preserve_mode) as handle:
        handle.write(text)


def atomic_write_json(
    path: Path | str,
    payload: Any,
    *,
    indent: int | None = 2,
    sort_keys: bool = False,
    ensure_ascii: bool = False,
    preserve_mode: bool = True,
) -> None:
    """Atomically serialise ``payload`` as JSON to ``path``."""
    text = json.dumps(payload, indent=indent, sort_keys=sort_keys, ensure_ascii=ensure_ascii)
    atomic_write_text(path, f"{text}\n", preserve_mode=preserve_mode)
