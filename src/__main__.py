"""Allow ``python -m src`` as an alternative to ``python main.py``."""

from src.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
