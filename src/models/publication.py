"""Domain and data transfer models for SEPREC publications and API payloads."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

STATUS_DOWNLOADED = "DOWNLOADED"
STATUS_GENERATED_PDF = "GENERATED_PDF"
STATUS_FAILED = "FAILED"
MINIMUM_API_PAGE_SIZE = 10
MINIMUM_API_PAGE_NUMBER = 1


def current_utc_iso_timestamp() -> str:
    """Return current UTC timestamp formatted as ISO 8601 string."""
    return datetime.now(UTC).isoformat()


class ApiModel(BaseModel):
    """Base Pydantic model configuring field population by original API alias."""

    model_config = ConfigDict(populate_by_name=True)


class CompanyInfo(ApiModel):
    """Company legal name and commercial registration identifiers."""

    registration_number: str | None = Field(default=None, alias="matricula")
    former_registration_number: str | None = Field(default=None, alias="matriculaAnterior")
    company_name: str | None = Field(default=None, alias="razonSocial")


class CatalogItem(ApiModel):
    """Publication category and subcategory parametric definitions."""

    subcategory_id: int | None = Field(default=None, alias="idParametrica")
    subcategory_name: str | None = Field(default=None, alias="nombreParametrica")
    subcategory_code: str | None = Field(default=None, alias="codigoParametrica")
    category_id: int | None = Field(default=None, alias="idParametricaCategoria")
    category_name: str | None = Field(default=None, alias="nombreParametricaCategoria")
    category_code: str | None = Field(default=None, alias="codigoParametricaCategoria")


class PublicationSummary(ApiModel):
    """Publication card summary retrieved from pagination search lists."""

    id: str
    publication_code: str | None = Field(default=None, alias="codigo")
    title: str | None = Field(default=None, alias="titulo")
    summary: str | None = Field(default=None, alias="resumen")
    publication_date: str | None = Field(default=None, alias="fechaPublicacion")
    formatted_publication_date: str | None = Field(default=None, alias="fechaPublicacionFormato")
    company: CompanyInfo | None = Field(default=None, alias="empresa")


class PublicationDetail(ApiModel):
    """Comprehensive publication detail including text content and base64 document."""

    id: str
    publication_code: str | None = Field(default=None, alias="codigo")
    former_publication_code: str | None = Field(default=None, alias="codigoAnterior")
    title: str | None = Field(default=None, alias="titulo")
    summary: str | None = Field(default=None, alias="resumen")
    full_text: str | None = Field(default=None, alias="textoCompleto")
    publication_date: str | None = Field(default=None, alias="fechaPublicacion")
    formatted_publication_date: str | None = Field(default=None, alias="fechaPublicacionFormato")
    source_file_path: str | None = Field(default=None, alias="archivo")
    document_base64: str | None = Field(default=None, alias="documento")
    company: CompanyInfo | None = Field(default=None, alias="empresa")


class PublicationSearchFilter(ApiModel):
    """
    Search request parameters submitted to the /api/publicaciones endpoint.
    Strictly forbids unexpected payload fields.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    page: int = Field(default=1, alias="pagina")
    page_size: int = Field(default=50, alias="limite")
    category: str = Field(default="", alias="categoria")
    subcategory: str = Field(default="", alias="tipoPublicacion")
    date_filter: str = Field(default="", alias="fecha")
    start_date: str = Field(default="", alias="fechaInicial")
    end_date: str = Field(default="", alias="fechaFinal")
    text: str = Field(default="", alias="texto")
    company_name: str = Field(default="", alias="razonSocial")
    registration_number: str = Field(default="", alias="matricula")
    summary: str = Field(default="", alias="resumen")

    def to_api_payload(self) -> dict[str, Any]:
        """Convert typed parameters into payload dictionary required by SEPREC."""
        payload = self.model_dump(by_alias=True)
        payload["limite"] = max(MINIMUM_API_PAGE_SIZE, self.page_size)
        payload["pagina"] = max(MINIMUM_API_PAGE_NUMBER, self.page)
        return payload


class PublicationRecord(ApiModel):
    """Persisted index record stored in CSV with document path and audit status."""

    id: str
    publication_code: str | None = None
    former_publication_code: str | None = None
    title: str | None = None
    summary: str | None = None
    registration_number: str | None = None
    former_registration_number: str | None = None
    company_name: str | None = None
    publication_date: str | None = None
    formatted_publication_date: str | None = None
    category_code: str | None = None
    category_name: str | None = None
    has_original_file: bool = False
    file_name: str | None = None
    file_path: str | None = None
    file_size_bytes: int | None = None
    status: str = STATUS_DOWNLOADED
    created_at: str = Field(default_factory=current_utc_iso_timestamp)
    updated_at: str = Field(default_factory=current_utc_iso_timestamp)
