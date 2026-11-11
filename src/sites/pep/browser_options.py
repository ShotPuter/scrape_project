"""Explicit browser experiments; no fabricated hardware identities or IP geolocation."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from src.utils.atomic import atomic_write_json


@dataclass(frozen=True)
class BrowserOptions:
    render_mode: str = "native"
    browser_language: str | None = None
    browser_timezone: str | None = None
    mouse_motion: bool = False
    require_webgl: bool = False

    def __post_init__(self):
        if self.render_mode not in {"native", "swiftshader"}:
            raise ValueError("browser-renderer must be native or swiftshader")
        if self.browser_language and not re.fullmatch(r"[a-z]{2,3}(?:-[A-Z]{2})?", self.browser_language):
            raise ValueError("browser-language must be a language tag such as es-CO or en-US")
        if self.browser_timezone:
            try:
                ZoneInfo(self.browser_timezone)
            except (ValueError, ZoneInfoNotFoundError):
                raise ValueError("browser-timezone must be a valid IANA timezone") from None

    @property
    def accept_language(self) -> str | None:
        if not self.browser_language:
            return None
        primary = self.browser_language.split("-", 1)[0]
        return f"{self.browser_language},{primary};q=0.9" if primary != self.browser_language else primary

    def arguments(self) -> list[str]:
        arguments = []
        if self.render_mode == "swiftshader":
            # Deliberately no --enable-unsafe-swiftshader / sandbox bypass.
            arguments.extend(["--use-gl=angle", "--use-angle=swiftshader", "--enable-webgl"])
        if self.browser_language:
            arguments.append(f"--lang={self.browser_language}")
        return arguments

    def snapshot(self) -> dict:
        return asdict(self)

    def prepare_profile(self, profile: Path) -> None:
        if self.browser_language:
            destination = profile / "Default" / "Preferences"
            preferences = json.loads(destination.read_text()) if destination.is_file() else {}
            primary = self.browser_language.split("-", 1)[0]
            languages = ",".join(dict.fromkeys((self.browser_language, primary)))
            preferences.setdefault("intl", {})["accept_languages"] = languages
            atomic_write_json(destination, preferences)


class BrowserEnvironmentError(ValueError):
    """Locally generated diagnostic safe to put in a report."""
