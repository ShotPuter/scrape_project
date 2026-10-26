"""Console and non-blocking rotating-file logging configuration."""

from __future__ import annotations

import atexit
import logging
import queue
import sys
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from pathlib import Path

from config import settings

CONSOLE_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s"
FILE_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(process)d | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_FILE_HANDLERS: dict[Path, RotatingFileHandler] = {}
_ACTIVE_LISTENERS: list[QueueListener] = []


def _shutdown_active_listeners() -> None:
    """Flush and stop background log queue listeners upon process exit."""
    for listener in _ACTIVE_LISTENERS:
        listener.stop()


atexit.register(_shutdown_active_listeners)


def setup_logger(
    name: str = "seprec_scraper", log_level: str | None = None, log_to_file: bool | None = None
) -> logging.Logger:
    """
    Initialize non-blocking logger with console handler and optional rotating file handler.

    Uses QueueHandler and QueueListener to ensure logging operations do not block async I/O.
    """
    effective_level = getattr(logging, (log_level or settings.log_level).upper(), logging.INFO)
    target_logger = logging.getLogger(name)
    target_logger.setLevel(effective_level)

    if target_logger.handlers:
        return target_logger

    target_logger.addHandler(_create_console_handler(effective_level))

    should_write_file = settings.log_to_file if log_to_file is None else log_to_file
    if should_write_file:
        _attach_rotating_file_handler(target_logger)

    configured_handlers = list(target_logger.handlers)
    for handler in configured_handlers:
        target_logger.removeHandler(handler)

    message_queue: queue.SimpleQueue = queue.SimpleQueue()
    target_logger.addHandler(QueueHandler(message_queue))

    queue_listener = QueueListener(message_queue, *configured_handlers, respect_handler_level=True)
    queue_listener.start()
    _ACTIVE_LISTENERS.append(queue_listener)

    return target_logger


def _create_console_handler(level: int) -> logging.Handler:
    """Create stdout stream handler with formatted timestamps and locations."""
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter(CONSOLE_LOG_FORMAT, DATE_FORMAT))
    return console_handler


def _attach_rotating_file_handler(target_logger: logging.Logger) -> None:
    """Attach size-bounded rotating file handler to logger if directories permit."""
    try:
        settings.ensure_directories()
        log_file_path = settings.log_file.resolve()
        existing_handler = _FILE_HANDLERS.get(log_file_path)

        if existing_handler is None:
            existing_handler = RotatingFileHandler(
                log_file_path, maxBytes=settings.log_max_bytes, backupCount=settings.log_backup_count, encoding="utf-8"
            )
            existing_handler.setLevel(logging.NOTSET)
            existing_handler.setFormatter(logging.Formatter(FILE_LOG_FORMAT, DATE_FORMAT))
            _FILE_HANDLERS[log_file_path] = existing_handler

        target_logger.addHandler(existing_handler)
    except Exception as error:  # noqa: BLE001 - logging setup must never crash the application
        target_logger.warning("Could not initialize rotating file logger", extra={"error": str(error)})
