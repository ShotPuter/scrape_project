"""Atomic filesystem persistence for original and generated publication PDFs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from config import settings
from src.models.publication import PublicationDetail
from src.utils.atomic import atomic_write_bytes
from src.utils.helpers import StorageError, build_pdf_filename, decode_base64_pdf, extract_period
from src.utils.logger import setup_logger

logger = setup_logger("file_storage")


@dataclass(frozen=True, slots=True)
class SavedDocument:
    filename: str
    path: Path
    size_bytes: int
    is_original: bool


class FileStorage:
    """Manages filesystem organization, atomic persistence, and PDF validation."""

    def __init__(self, pdf_dir: Path | str | None = None) -> None:
        self.pdf_dir = Path(pdf_dir or settings.pdf_storage_dir)
        self.pdf_dir.mkdir(parents=True, exist_ok=True)

    def save_publication_document(self, publication: PublicationDetail) -> SavedDocument:
        """
        Persist publication document atomically to disk.

        If original base64 payload is present, it is validated and saved.
        Otherwise, a synthetic PDF is generated from available text.
        """
        company_info = publication.company
        file_name = build_pdf_filename(
            company_info.company_name if company_info else None,
            company_info.registration_number if company_info else None,
            publication.id,
        )
        period_directory = extract_period(publication.publication_date, publication.formatted_publication_date)
        target_path = self.pdf_dir / period_directory / file_name
        document_bytes, has_original_file = self._load_or_generate_pdf(publication)

        try:
            self._write_atomically(target_path, document_bytes)
        except OSError as error:
            logger.error(
                "Failed to persist publication document to disk",
                extra={"publication_id": publication.id, "error": str(error)},
                exc_info=True,
            )
            raise StorageError(f"Failed to save document for publication {publication.id}: {error}") from error

        return SavedDocument(
            filename=file_name, path=target_path, size_bytes=len(document_bytes), is_original=has_original_file
        )

    def _load_or_generate_pdf(self, publication: PublicationDetail) -> tuple[bytes, bool]:
        """Extract valid original PDF bytes or generate PDF from text content."""
        if publication.document_base64:
            try:
                original_bytes = decode_base64_pdf(publication.document_base64)
                logger.debug(
                    "Decoded original base64 PDF payload",
                    extra={"publication_id": publication.id, "byte_length": len(original_bytes)},
                )
                return original_bytes, True
            except StorageError as error:
                logger.warning(
                    "Original base64 payload corrupted; generating fallback PDF",
                    extra={"publication_id": publication.id, "error": str(error)},
                )

        from src.services.pdf_generator import generate_pdf_for_publication

        generated_bytes = generate_pdf_for_publication(publication)
        logger.debug(
            "Synthesized PDF from text preview content",
            extra={"publication_id": publication.id, "byte_length": len(generated_bytes)},
        )
        return generated_bytes, False

    @staticmethod
    def _write_atomically(destination: Path, payload: bytes) -> None:
        """Write file contents to a tempfile first and rename atomically."""
        atomic_write_bytes(destination, payload)
