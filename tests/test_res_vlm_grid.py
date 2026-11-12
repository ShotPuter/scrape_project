"""Unit tests for the local VLM grid helpers: pure text, no browser or model."""

from __future__ import annotations

import pytest

from src.sites.res.vlm_grid import instruction_object, rejects


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Selecciona todos los cuadros con semáforos.\nHaga clic en Verificar", "semáforos"),
        (
            "Selecciona todos los cuadrados con\ncruces peatonales\nSi no hay ninguna, haz clic en Omitir",
            "cruces peatonales",
        ),
        (
            "Select all squares with\ncrosswalks\nIf there are none, click skip",
            "crosswalks",
        ),
        (
            "Выберите все квадраты с\nпешеходными переходами\nЕсли их нет, нажмите Пропустить",
            "пешеходными переходами",
        ),
        ("Select all images with traffic lights\nClick verify once there are none left", "traffic lights"),
        ("Выберите все изображения с светофорами", "светофорами"),
        ("Selecciona todos los cuadros con semáforos", "semáforos"),
        ("", ""),
    ],
)
def test_instruction_object(text: str, expected: str) -> None:
    assert instruction_object(text) == expected


@pytest.mark.parametrize("answer", ["no", "No.", "false", "nope", "maybe", ""])
def test_rejects_anything_that_is_not_a_clear_yes(answer: str) -> None:
    assert rejects(answer) is True


@pytest.mark.parametrize("answer", ["yes", "Yes.", "sí", "si", "correct", "true"])
def test_accepts_affirmative_answers(answer: str) -> None:
    assert rejects(answer) is False
