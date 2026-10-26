"""Runtime dependency checks for the executable entry points."""

from __future__ import annotations

import sys


def require_pydantic_v2() -> None:
    """Fail with an actionable message when the wrong interpreter is used."""
    try:
        import pydantic
    except ImportError as exc:
        raise SystemExit(
            "pydantic is not installed for this interpreter:\n"
            f"  {sys.executable}\n"
            "Install the project dependencies or use the project virtualenv:\n"
            "  .venv/bin/python -m pip install -r requirements.txt\n"
            "  .venv/bin/python main.py --help"
        ) from exc

    version = pydantic.VERSION
    if int(version.split(".", 1)[0]) >= 2:
        return
    raise SystemExit(
        f"pydantic {version} is too old for this project (2.x is required):\n"
        f"  interpreter: {sys.executable}\n"
        "Use the project virtualenv instead of the global/conda Python:\n"
        "  .venv/bin/python collect_proxies.py --db data/proxies.db\n"
        "  .venv/bin/python main.py --help\n"
        "Or activate it first:\n"
        "  source .venv/bin/activate"
    )
