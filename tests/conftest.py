"""Shared pytest configuration and fixtures."""

import os
import sys
from pathlib import Path

# Keep test runs from writing into the project data directory.
os.environ.setdefault("LOG_TO_FILE", "false")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest  # noqa: E402


@pytest.fixture
def summary_payload() -> dict:
    return {
        "id": "228340",
        "codigo": "123231/2026",
        "titulo": "Constitución de Sociedad Comercial S.R.L.",
        "resumen": "ESCRITURA PUBLICA DE CONSTITUCION",
        "fechaPublicacion": "2026-10-22T12:04:41.000Z",
        "empresa": {"matricula": "711899029", "matriculaAnterior": None, "razonSocial": "CTT AUREA S.R.L."},
        "fechaPublicacionFormato": "2026/10/22",
    }


@pytest.fixture
def detail_pdf_payload(summary_payload: dict) -> dict:
    import base64

    payload = dict(summary_payload)
    payload.update(
        {
            "codigoAnterior": None,
            "textoCompleto": None,
            "archivo": "/opt/seprec/sistema/tmp-files/testimonio.pdf",
            "documento": base64.b64encode(b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\ntrailer\n%%EOF\n").decode("ascii"),
        }
    )
    return payload


@pytest.fixture
def detail_text_payload() -> dict:
    return {
        "id": "228345",
        "codigo": "123236/2026",
        "codigoAnterior": None,
        "titulo": "NOTIFICACIÓN DE CAMBIO DE DIRECCIÓN DE LA EMPRESA",
        "resumen": "LA EMPRESA INMOBILIARIA GEMINOSS S.R.L. ANUNCIA EL CAMBIO DE DIRECCIÓN",
        "textoCompleto": (
            "<h3>NOTIFICACIÓN DE CAMBIO DE DIRECCIÓN</h3>"
            "<p>LA EMPRESA INMOBILIARIA GEMINOSS S.R.L.</p>"
            "<p>CON MATRÍCULA DE COMERCIO: 422064022</p>"
        ),
        "fechaPublicacion": "2026-10-22T13:04:50.000Z",
        "archivo": None,
        "empresa": {
            "matricula": "422064022",
            "matriculaAnterior": "00459414",
            "razonSocial": "INMOBILIARIA GEMINOSS S.R.L.",
        },
        "fechaPublicacionFormato": "2026/10/22",
    }
