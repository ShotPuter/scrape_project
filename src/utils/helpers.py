"""Filename sanitization, date parsing, base64 validation, and domain exceptions."""

from __future__ import annotations

import base64
import binascii
import re
import unicodedata

DEFAULT_FALLBACK_FILENAME = "UNKNOWN"
MAXIMUM_FILENAME_LENGTH = 80
PDF_HEADER_MAGIC = b"%PDF"
PDF_TRAILER_MAGIC = b"%%EOF"
ILLEGAL_FILENAME_CHARACTERS = re.compile(r'[/\\:*?"<>|\x00-\x1f]+')
WHITESPACE_PATTERN = re.compile(r"\s+")
YEAR_MONTH_PATTERN = re.compile(r"(\d{4})[-/](\d{2})")
TRAILER_SEARCH_WINDOW_BYTES = 2048


class SeprecError(Exception):
    """Base exception for all domain and scraper runtime errors."""


class SeprecAPIError(SeprecError):
    """Raised when SEPREC REST API returns unexpected HTTP or business errors."""

    def __init__(self, message: str, status_code: int | None = None, details: dict | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.details = details or {}


class SeprecRateLimitError(SeprecAPIError):
    """Raised when upstream API or Cloudflare gateway enforces rate limits."""


class StorageError(SeprecError):
    """Raised when filesystem write, read, or base64 decoding fails."""


class PDFGenerationError(SeprecError):
    """Raised when HTML/text rendering into PDF document fails."""


def sanitize_filename(
    text: str | None, default: str = DEFAULT_FALLBACK_FILENAME, max_length: int = MAXIMUM_FILENAME_LENGTH
) -> str:
    """
    Sanitize arbitrary text string for safe cross-platform filesystem use.

    Normalizes Unicode to NFC, replaces forbidden control/special characters
    with whitespace, converts multiple spaces to underscores, and truncates length.
    """
    if not text:
        return default

    normalized_text = unicodedata.normalize("NFC", text)
    cleaned_characters = ILLEGAL_FILENAME_CHARACTERS.sub(" ", normalized_text)
    sanitized_text = WHITESPACE_PATTERN.sub("_", cleaned_characters).strip(" ._-")

    if not sanitized_text:
        return default

    return sanitized_text[:max_length]


def build_pdf_filename(company_name: str | None, matricula: str | None, publication_id: str) -> str:
    """
    Construct safe filename matching pattern: <company>_<matricula>_<id>.pdf.

    Including publication_id guarantees uniqueness across filings from the same company.
    """
    safe_company = sanitize_filename(company_name, default="COMPANY")
    safe_matricula = sanitize_filename(matricula, default="NO_MATRICULA", max_length=20)
    safe_identifier = sanitize_filename(str(publication_id), default="0", max_length=20)
    return f"{safe_company}_{safe_matricula}_{safe_identifier}.pdf"


def extract_period(publication_date: str | None, formatted_publication_date: str | None = None) -> str:
    """
    Extract YYYY/MM partition directory from ISO or formatted date strings.
    Validates month boundaries (01..12). Defaults to 'unknown' if not parsable.
    """
    for raw_date_candidate in (formatted_publication_date, publication_date):
        if not raw_date_candidate:
            continue

        match = YEAR_MONTH_PATTERN.search(raw_date_candidate)
        if match and 1 <= int(match.group(2)) <= 12:
            return f"{match.group(1)}/{match.group(2)}"

    return "unknown"


def decode_base64_pdf(payload: str) -> bytes:
    """
    Decode and validate a base64-encoded PDF payload.

    Ensures the payload contains a valid '%PDF' header and '%%EOF' trailer marker.
    """
    if not payload:
        raise StorageError("Empty document payload")

    clean_payload = payload.split(",", 1)[1] if payload.startswith("data:") and "," in payload else payload

    try:
        decoded_bytes = base64.b64decode(clean_payload, validate=False)
    except (binascii.Error, ValueError) as error:
        raise StorageError(f"Invalid base64 document payload: {error}") from error

    if not decoded_bytes.startswith(PDF_HEADER_MAGIC):
        raise StorageError("Decoded document is not a PDF (missing %PDF header)")

    if PDF_TRAILER_MAGIC not in decoded_bytes[-TRAILER_SEARCH_WINDOW_BYTES:]:
        raise StorageError("Decoded document is incomplete (missing %%EOF marker)")

    return decoded_bytes
