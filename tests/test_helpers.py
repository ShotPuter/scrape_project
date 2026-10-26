"""Tests for filename, date, and base64 helpers."""

import base64

import pytest

from src.utils.helpers import StorageError, build_pdf_filename, decode_base64_pdf, extract_period, sanitize_filename


def test_sanitize_filename_preserves_spanish_characters():
    assert sanitize_filename("YUJRA SIÑANI SOFIA") == "YUJRA_SIÑANI_SOFIA"
    assert sanitize_filename("  Compañía   Andina S.R.L. ") == "Compañía_Andina_S.R.L"


def test_sanitize_filename_removes_illegal_characters():
    assert sanitize_filename('ACME/SA\\Ltd:*?"<>|') == "ACME_SA_Ltd"
    assert sanitize_filename("", default="COMPANY") == "COMPANY"
    assert sanitize_filename(None, default="COMPANY") == "COMPANY"


def test_sanitize_filename_truncates():
    assert len(sanitize_filename("A" * 200, max_length=80)) == 80


def test_build_pdf_filename_includes_company_registration_and_id():
    assert build_pdf_filename("CTT AUREA S.R.L.", "711899029", "228340") == "CTT_AUREA_S.R.L_711899029_228340.pdf"
    assert build_pdf_filename(None, None, "42") == "COMPANY_NO_MATRICULA_42.pdf"


@pytest.mark.parametrize(
    ("publication", "formatted", "expected"),
    [
        ("2026-10-22T13:04:50.000Z", "2026/10/22", "2026/10"),
        ("2026-10-22T13:04:50.000Z", None, "2026/10"),
        (None, "2026/10/22", "2026/10"),
        (None, None, "unknown"),
        ("not-a-date", None, "unknown"),
        ("2026-13-01", None, "unknown"),
    ],
)
def test_extract_period(publication, formatted, expected):
    assert extract_period(publication, formatted) == expected


def test_decode_base64_pdf_plain_and_data_uri():
    raw = b"%PDF-1.4\ntest\n%%EOF"
    encoded = base64.b64encode(raw).decode("ascii")

    assert decode_base64_pdf(encoded) == raw
    assert decode_base64_pdf(f"data:application/pdf;base64,{encoded}") == raw


@pytest.mark.parametrize("payload", ["", "abc", base64.b64encode(b"hello").decode("ascii")])
def test_decode_base64_pdf_rejects_invalid_payloads(payload):
    with pytest.raises(StorageError):
        decode_base64_pdf(payload)
