"""Tests for Pydantic models and API payload mapping."""

from src.models.publication import PublicationDetail, PublicationSearchFilter, PublicationSummary


def test_parse_publication_summary(summary_payload):
    summary = PublicationSummary.model_validate(summary_payload)

    assert summary.id == "228340"
    assert summary.publication_date == "2026-10-22T12:04:41.000Z"
    assert summary.formatted_publication_date == "2026/10/22"
    assert summary.company is not None
    assert summary.company.company_name == "CTT AUREA S.R.L."
    assert summary.company.registration_number == "711899029"


def test_parse_publication_detail_with_document(detail_pdf_payload):
    detail = PublicationDetail.model_validate(detail_pdf_payload)

    assert detail.id == "228340"
    assert detail.document_base64 is not None
    assert detail.document_base64.startswith("JVBERi0")
    assert detail.full_text is None
    assert detail.formatted_publication_date == "2026/10/22"


def test_parse_text_only_publication(detail_text_payload):
    detail = PublicationDetail.model_validate(detail_text_payload)

    assert detail.document_base64 is None
    assert detail.full_text is not None
    assert "INMOBILIARIA GEMINOSS" in detail.full_text
    assert detail.company.former_registration_number == "00459414"


def test_search_filter_payload_clamps_api_minimums():
    payload = PublicationSearchFilter(page=0, page_size=1).to_api_payload()

    assert payload["pagina"] == 1
    assert payload["limite"] == 10
    assert set(payload) == {
        "fechaInicial",
        "fecha",
        "fechaFinal",
        "categoria",
        "texto",
        "razonSocial",
        "tipoPublicacion",
        "matricula",
        "resumen",
        "limite",
        "pagina",
    }


def test_english_fields_preserve_api_payload(detail_pdf_payload):
    detail = PublicationDetail.model_validate(detail_pdf_payload)

    assert detail.publication_code == detail_pdf_payload["codigo"]
    assert detail.title == detail_pdf_payload["titulo"]
    assert detail.summary == detail_pdf_payload["resumen"]
    assert detail.source_file_path == detail_pdf_payload["archivo"]
    assert detail.model_dump(by_alias=True) == detail_pdf_payload
    assert PublicationDetail.model_validate(detail.model_dump()) == detail


def test_english_search_filters_serialize_to_api_names():
    filters = PublicationSearchFilter(
        page=2,
        page_size=25,
        category="1",
        subcategory="1001",
        date_filter="5",
        start_date="2025-01-01",
        end_date="2025-12-31",
        text="capital",
        company_name="ACME",
        registration_number="123",
        summary="notice",
    )

    assert filters.to_api_payload() == {
        "pagina": 2,
        "limite": 25,
        "categoria": "1",
        "tipoPublicacion": "1001",
        "fecha": "5",
        "fechaInicial": "2025-01-01",
        "fechaFinal": "2025-12-31",
        "texto": "capital",
        "razonSocial": "ACME",
        "matricula": "123",
        "resumen": "notice",
    }


def test_search_filter_rejects_obsolete_python_field_names():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        PublicationSearchFilter(fecha_inicial="2025-01-01")
