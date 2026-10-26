"""PDF generation service for publications that lack original downloadable files."""

from __future__ import annotations

import html
import io
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import HRFlowable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from src.models.publication import PublicationDetail
from src.utils.atomic import atomic_write_bytes
from src.utils.helpers import PDFGenerationError
from src.utils.logger import setup_logger

logger = setup_logger("pdf_generator")

PAGE_MARGIN_POINTS = 40
METADATA_COLUMN_WIDTHS = (1.5 * inch, 2.3 * inch, 1.4 * inch, 1.8 * inch)
HEADER_COLOR = colors.HexColor("#1A365D")
SUBHEADER_COLOR = colors.HexColor("#4A5568")
PRIMARY_ACCENT_COLOR = colors.HexColor("#2B6CB0")
BODY_TEXT_COLOR = colors.HexColor("#2D3748")
TABLE_BACKGROUND_COLOR = colors.HexColor("#F7FAFC")
TABLE_BORDER_COLOR = colors.HexColor("#E2E8F0")
TABLE_TEXT_COLOR = colors.HexColor("#1A202C")
DIVIDER_RULE_COLOR = colors.HexColor("#CBD5E0")
DEFAULT_EMPTY_CONTENT = "Sin contenido adicional disponible para esta publicación."
HTML_BLOCK_TAGS = ("br", "p", "div", "h1", "h2", "h3", "h4", "li")

STYLE_SPECIFICATIONS = {
    "header": (
        "GazetteHeader",
        "Normal",
        {"fontName": "Helvetica-Bold", "fontSize": 14, "leading": 16, "alignment": 1, "textColor": HEADER_COLOR},
    ),
    "sub_header": (
        "GazetteSubHeader",
        "Normal",
        {"fontName": "Helvetica", "fontSize": 9, "leading": 12, "alignment": 1, "textColor": SUBHEADER_COLOR},
    ),
    "title": (
        "PublicationTitle",
        "Heading2",
        {"fontName": "Helvetica-Bold", "fontSize": 12, "leading": 15, "alignment": 1, "textColor": PRIMARY_ACCENT_COLOR},
    ),
    "body": (
        "PublicationBody",
        "Normal",
        {"fontName": "Helvetica", "fontSize": 9.5, "leading": 14, "textColor": BODY_TEXT_COLOR},
    ),
    "table_label": (
        "TableLabel",
        "Normal",
        {"fontName": "Helvetica-Bold", "fontSize": 8.5, "leading": 11, "textColor": BODY_TEXT_COLOR},
    ),
    "table_value": (
        "TableValue",
        "Normal",
        {"fontName": "Helvetica", "fontSize": 8.5, "leading": 11, "textColor": TABLE_TEXT_COLOR},
    ),
}


def clean_html_text(raw_html: str) -> str:
    """Normalize HTML body content into cleanly spaced plain text paragraphs."""
    if not raw_html:
        return ""
    parsed_soup = BeautifulSoup(raw_html, "html.parser")
    for block_element in parsed_soup.find_all(HTML_BLOCK_TAGS):
        block_element.insert_before("\n")
        block_element.append("\n")
    plain_text = html.unescape(parsed_soup.get_text()).replace("\xa0", " ")
    return "".join(char for char in plain_text if char in "\n\t" or ord(char) >= 32).strip()


def build_styles() -> dict[str, ParagraphStyle]:
    """Generate typed ReportLab paragraph styles for publication layout."""
    base_styles = getSampleStyleSheet()
    return {
        style_key: ParagraphStyle(name=name, parent=base_styles[parent_name], **options)
        for style_key, (name, parent_name, options) in STYLE_SPECIFICATIONS.items()
    }


def build_metadata_table(publication: PublicationDetail, styles: dict[str, ParagraphStyle]) -> Table:
    """Construct formatted two-column metadata table for publication details."""
    table = Table(_extract_metadata_rows(publication, styles), colWidths=METADATA_COLUMN_WIDTHS)
    table.setStyle(_create_table_style())
    return table


def _extract_metadata_rows(publication: PublicationDetail, styles: dict[str, ParagraphStyle]) -> list[list[Paragraph]]:
    """Generate table rows populated with company and publication identifiers."""
    company_name, registration_identifier = _extract_company_identifiers(publication)
    label_style = styles["table_label"]
    value_style = styles["table_value"]

    formatted_date = publication.formatted_publication_date or publication.publication_date or "N/A"

    return [
        [
            Paragraph("Empresa / Razón Social:", label_style),
            Paragraph(company_name, value_style),
            Paragraph("Matrícula de Comercio:", label_style),
            Paragraph(registration_identifier, value_style),
        ],
        [
            Paragraph("Código de Publicación:", label_style),
            Paragraph(publication.publication_code or "N/A", value_style),
            Paragraph("Fecha de Publicación:", label_style),
            Paragraph(formatted_date, value_style),
        ],
    ]


def _extract_company_identifiers(publication: PublicationDetail) -> tuple[str, str]:
    """Extract and format legal company name and commercial registration numbers."""
    company = publication.company
    if not company:
        return "N/A", "N/A"

    company_name = company.company_name or "N/A"
    registration = company.registration_number or "N/A"
    if company.former_registration_number:
        registration += f" (Ant: {company.former_registration_number})"

    return company_name, registration


def _create_table_style() -> TableStyle:
    """Construct clean styling rules for ReportLab metadata grid."""
    return TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, -1), TABLE_BACKGROUND_COLOR),
            ("BOX", (0, 0), (-1, -1), 0.5, TABLE_BORDER_COLOR),
            ("INNERGRID", (0, 0), (-1, -1), 0.5, TABLE_BORDER_COLOR),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ]
    )


def build_story(publication: PublicationDetail) -> list[Any]:
    """Assemble complete flowable story elements for document generation."""
    styles = build_styles()
    return [*header_story(publication, styles), *content_story(publication, styles), *footer_story(publication, styles)]


def header_story(publication: PublicationDetail, styles: dict[str, ParagraphStyle]) -> list[Any]:
    """Generate header elements: banners, title, and metadata table."""
    return [
        Paragraph("ESTADO PLURINACIONAL DE BOLIVIA", styles["sub_header"]),
        Paragraph("SERVICIO PLURINACIONAL DE REGISTRO DE COMERCIO (SEPREC)", styles["sub_header"]),
        Paragraph("GACETA ELECTRÓNICA DE COMERCIO", styles["header"]),
        Spacer(1, 10),
        HRFlowable(width="100%", thickness=1.5, color=PRIMARY_ACCENT_COLOR, spaceAfter=15),
        Paragraph(publication.title or "PUBLICACIÓN OFICIAL", styles["title"]),
        Spacer(1, 12),
        build_metadata_table(publication, styles),
        Spacer(1, 15),
    ]


def content_story(publication: PublicationDetail, styles: dict[str, ParagraphStyle]) -> list[Any]:
    """Generate flowable paragraphs for publication body text."""
    body_text = clean_html_text(publication.full_text) if publication.full_text else (publication.summary or "").strip()
    resolved_content = body_text or DEFAULT_EMPTY_CONTENT

    story_elements: list[Any] = []
    for raw_paragraph in resolved_content.split("\n"):
        clean_line = raw_paragraph.strip()
        if clean_line:
            story_elements.append(Paragraph(html.escape(clean_line), styles["body"]))
            story_elements.append(Spacer(1, 6))

    return story_elements


def footer_story(publication: PublicationDetail, styles: dict[str, ParagraphStyle]) -> list[Any]:
    """Generate official footer disclaimer."""
    disclaimer_text = (
        "Documento generado electrónicamente a partir de la publicación "
        f"oficial Nro {publication.id}. "
        "Gaceta Electrónica de Comercio de Bolivia."
    )
    return [
        Spacer(1, 15),
        HRFlowable(width="100%", thickness=0.5, color=DIVIDER_RULE_COLOR, spaceAfter=10),
        Paragraph(disclaimer_text, styles["sub_header"]),
    ]


def generate_pdf_for_publication(
    publication: PublicationDetail, output_dest: Path | str | io.BytesIO | None = None
) -> bytes:
    """
    Render publication details into a structured PDF document.

    Args:
        publication: Source publication with metadata and body text.
        output_dest: Optional file path or in-memory byte buffer.

    Returns:
        Generated PDF payload as bytes.
    """
    memory_buffer = output_dest if isinstance(output_dest, io.BytesIO) else io.BytesIO()
    try:
        document_template = SimpleDocTemplate(
            memory_buffer,
            pagesize=letter,
            rightMargin=PAGE_MARGIN_POINTS,
            leftMargin=PAGE_MARGIN_POINTS,
            topMargin=PAGE_MARGIN_POINTS,
            bottomMargin=PAGE_MARGIN_POINTS,
        )
        document_template.build(build_story(publication))
        rendered_bytes = memory_buffer.getvalue()

        if isinstance(output_dest, (str, Path)):
            destination_path = Path(output_dest)
            atomic_write_bytes(destination_path, rendered_bytes)

        return rendered_bytes

    except Exception as error:  # noqa: BLE001 - reportlab failures are wrapped as a domain error
        logger.error(
            "PDF generation failed for publication",
            extra={"publication_id": publication.id, "error": str(error)},
            exc_info=True,
        )
        raise PDFGenerationError(f"PDF generation failed for ID {publication.id}: {error}") from error


class PDFGenerator:
    """Class interface for PDF generation backward compatibility."""

    @staticmethod
    def generate_pdf_for_publication(
        publication: PublicationDetail, output_dest: Path | str | io.BytesIO | None = None
    ) -> bytes:
        """High-level static API delegating to functional renderer."""
        return generate_pdf_for_publication(publication, output_dest)
