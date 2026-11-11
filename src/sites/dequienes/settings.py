"""Validated configuration for the staged Chile collection pipeline (Python 3.11 TOML)."""

from __future__ import annotations

import math
import tomllib
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path


@dataclass(frozen=True)
class StageSettings:
    browsers: int = 1
    tabs_per_browser: int = 1
    max_items: int | None = None
    max_in_flight: int = 4
    delay_seconds: float = 1.0
    rate_per_minute: float = 60.0
    timeout_seconds: float = 45.0
    max_attempts: int = 3
    lease_seconds: float = 180.0
    retry_seconds: float = 30.0
    proxy_mode: str = "direct"
    transport: str = "browser"
    session_refreshes: int = 2

    def __post_init__(self):
        for name in ("browsers", "tabs_per_browser", "max_in_flight", "max_attempts"):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.max_items is not None and (type(self.max_items) is not int or self.max_items < 0):
            raise ValueError("max_items must be a non-negative integer or omitted")
        for name in ("delay_seconds", "rate_per_minute", "timeout_seconds", "lease_seconds", "retry_seconds"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
            if value < 0 or (name in {"rate_per_minute", "timeout_seconds", "lease_seconds"} and value == 0):
                raise ValueError(f"Invalid {name}")
        if self.transport not in {"browser", "hybrid"}:
            raise ValueError("transport must be browser or hybrid")
        if type(self.session_refreshes) is not int or self.session_refreshes < 0:
            raise ValueError("session_refreshes must be a non-negative integer")
        if self.proxy_mode != "direct":
            raise ValueError("Only direct routing is implemented in stages 1–3; proxy support is a later stage")


@dataclass(frozen=True)
class PipelineSettings:
    year: int | None = None
    months: tuple[str, ...] = ()
    all_months: bool = False
    output: Path = Path("data/dequienes-companies")
    profile: Path = Path("data/dequienes-companies-profile")
    public: StageSettings = field(default_factory=lambda: StageSettings(tabs_per_browser=4, transport="hybrid"))
    account: StageSettings = field(default_factory=lambda: StageSettings(max_in_flight=1))
    res: StageSettings = field(default_factory=lambda: StageSettings(max_in_flight=1))

    def __post_init__(self):
        if self.year is not None and (type(self.year) is not int or not 1 <= self.year <= 9999):
            raise ValueError("year must be between 1 and 9999")
        if type(self.all_months) is not bool:
            raise ValueError("all_months must be boolean")
        if self.all_months and (self.year is not None or self.months):
            raise ValueError("all_months cannot be combined with year/months")
        import argparse

        from src.sites.dequienes.companies import month_value

        for month in self.months:
            try:
                month_value(month)
            except argparse.ArgumentTypeError as exc:
                raise ValueError(str(exc)) from exc
            if month.isdecimal() and self.year is None:
                raise ValueError("Numeric month requires year")
            if self.year and not month.isdecimal() and not month.startswith(f"{self.year:04d}-"):
                raise ValueError("month and year refer to different years")
        if self.public.browsers != 1:
            raise ValueError("Public collection currently supports one browser with configurable tabs")

    def display(self) -> dict:
        result = asdict(self)
        result["output"], result["profile"] = str(self.output), str(self.profile)
        return result


def read_settings(path: Path | None = None) -> PipelineSettings:
    if path is None:
        return PipelineSettings()
    with path.open("rb") as handle:
        raw = tomllib.load(handle)
    if set(raw) - {"selection", "storage", "public", "account", "res"}:
        raise ValueError("Unknown configuration section")
    selection = raw.get("selection", {})
    storage = raw.get("storage", {})
    if set(selection) - {"year", "months", "all_months"} or set(storage) - {"output", "profile"}:
        raise ValueError("Unknown selection/storage setting")
    months = selection.get("months", [])
    if not isinstance(months, list) or any(not isinstance(m, str) for m in months):
        raise ValueError("selection.months must be an array of month strings")
    defaults = PipelineSettings()
    stages = {}
    for name in ("public", "account", "res"):
        try:
            stages[name] = replace(getattr(defaults, name), **raw.get(name, {}))
        except TypeError as exc:
            raise ValueError(f"Unknown setting in {name}") from exc
    return PipelineSettings(
        year=selection.get("year"),
        months=tuple(months),
        all_months=selection.get("all_months", False),
        output=Path(storage.get("output", str(defaults.output))),
        profile=Path(storage.get("profile", str(defaults.profile))),
        **stages,
    )
