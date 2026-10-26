"""Tests for atomic PDF storage, sharding, and fallback."""

from src.models.publication import PublicationDetail
from src.storage.file_storage import FileStorage


def test_saves_original_pdf_and_shards_by_month(tmp_path, detail_pdf_payload):
    storage = FileStorage(tmp_path / "pdf")
    detail = PublicationDetail.model_validate(detail_pdf_payload)

    document = storage.save_publication_document(detail)

    assert document.filename == "CTT_AUREA_S.R.L_711899029_228340.pdf"
    assert document.path == tmp_path / "pdf" / "2026" / "10" / document.filename
    assert document.path.exists()
    assert document.path.read_bytes().startswith(b"%PDF")
    assert document.size_bytes == document.path.stat().st_size
    assert document.is_original is True


def test_generates_pdf_for_text_only_publication(tmp_path, detail_text_payload):
    storage = FileStorage(tmp_path / "pdf")
    detail = PublicationDetail.model_validate(detail_text_payload)

    document = storage.save_publication_document(detail)

    assert document.filename == "INMOBILIARIA_GEMINOSS_S.R.L_422064022_228345.pdf"
    assert document.path.parent == tmp_path / "pdf" / "2026" / "10"
    assert document.path.read_bytes().startswith(b"%PDF")
    assert document.size_bytes > 500
    assert document.is_original is False


def test_malformed_base64_falls_back_to_generated_pdf(tmp_path, detail_text_payload):
    storage = FileStorage(tmp_path / "pdf")
    detail = PublicationDetail.model_validate(detail_text_payload)
    detail.document_base64 = "abc"  # invalid base64 padding

    document = storage.save_publication_document(detail)

    assert document.path.exists()
    assert document.path.read_bytes().startswith(b"%PDF")
    assert document.is_original is False
