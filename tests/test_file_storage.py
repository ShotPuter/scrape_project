"""Tests for atomic PDF storage, sharding, and fallback."""

from src.models.publication import PublicationDetail
from src.storage.file_storage import FileStorage


def test_saves_original_pdf_and_shards_by_month(tmp_path, detail_pdf_payload):
    storage = FileStorage(tmp_path / "pdf")
    detail = PublicationDetail.model_validate(detail_pdf_payload)

    filename, path, size, has_original = storage.save_publication_document(detail)

    assert filename == "CTT_AUREA_S.R.L_711899029_228340.pdf"
    assert path == tmp_path / "pdf" / "2026" / "10" / filename
    assert path.exists()
    assert path.read_bytes().startswith(b"%PDF")
    assert size == path.stat().st_size
    assert has_original is True


def test_generates_pdf_for_text_only_publication(tmp_path, detail_text_payload):
    storage = FileStorage(tmp_path / "pdf")
    detail = PublicationDetail.model_validate(detail_text_payload)

    filename, path, size, has_original = storage.save_publication_document(detail)

    assert filename == "INMOBILIARIA_GEMINOSS_S.R.L_422064022_228345.pdf"
    assert path.parent == tmp_path / "pdf" / "2026" / "10"
    assert path.read_bytes().startswith(b"%PDF")
    assert size > 500
    assert has_original is False


def test_malformed_base64_falls_back_to_generated_pdf(tmp_path, detail_text_payload):
    storage = FileStorage(tmp_path / "pdf")
    detail = PublicationDetail.model_validate(detail_text_payload)
    detail.document_base64 = "abc"  # invalid base64 padding

    _, path, _, has_original = storage.save_publication_document(detail)

    assert path.exists()
    assert path.read_bytes().startswith(b"%PDF")
    assert has_original is False
