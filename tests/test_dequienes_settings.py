import argparse
import json

import pytest

from src.sites.dequienes.pipeline import run_public, settings_from_args
from src.sites.dequienes.settings import StageSettings, read_settings


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_items": -1},
        {"tabs_per_browser": 0},
        {"rate_per_minute": float("nan")},
        {"lease_seconds": 0},
        {"browsers": True},
        {"proxy_mode": "pool"},
    ],
)
def test_invalid_settings_fail_before_browser(kwargs):
    with pytest.raises(ValueError):
        StageSettings(**kwargs)


def test_config_and_cli_precedence(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text("""[selection]
    year=2025
    months=["1"]
    [public]
    max_items=20
    tabs_per_browser=2
    [account]
    max_items=5
    [res]
    max_items=7
    """)
    settings = settings_from_args(argparse.Namespace(config=config, workers=3, limit=0, max_companies=2))
    assert settings.public.tabs_per_browser == 3
    assert settings.public.max_items == 0
    assert settings.account.max_items == 2
    assert settings.res.max_items == 7
    assert settings.months == ("1",)
    assert settings.year == 2025


def test_unknown_config_keys_are_rejected(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text("[public]\ntabz=20\n")
    with pytest.raises(ValueError):
        read_settings(config)


async def test_zero_limit_and_status_do_not_import_browser(tmp_path, monkeypatch):
    import sys

    monkeypatch.setitem(sys.modules, "camoufox.async_api", None)
    args = argparse.Namespace(output=tmp_path, limit=0)
    assert await run_public(args) == 0
    args.action = "status"
    assert await run_public(args) == 0
    assert json.loads((tmp_path / "queues.json").read_text())["documents"] == 0


async def test_offline_import_extracts_and_routes_codes(tmp_path, monkeypatch):
    import sys

    monkeypatch.setitem(sys.modules, "camoufox.async_api", None)
    html = tmp_path / "saved.html"
    html.write_text("""<h1>Company SpA</h1><h2>Publicado el 02/01/2025</h2>
        <p>CVE RES: ACiWqbFTpsmy</p><p>CVE: 123456789</p>""")
    url = "https://dequienes.cl/diario-oficial/2025/01/02/company-spa-1234567"
    args = argparse.Namespace(output=tmp_path / "output", import_publication=[(url, str(html))], limit=1)
    assert await run_public(args) == 0
    assert await run_public(args) == 0
    status = json.loads((args.output / "queues.json").read_text())
    assert status["companies"] == 1
    assert status["documents"] == 1
    assert status["queues"]["certificate_download"] == {"pending": 1}
    assert status["queues"]["public_discovery"] == {"completed": 1}
    assert status["cve_candidates"]["ambiguous"] == 1
