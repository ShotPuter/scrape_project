#!/usr/bin/env bash
set -euo pipefail
login_project_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
export PYTHONPATH="$login_project_root/.camoufox-deps:$login_project_root${PYTHONPATH:+:$PYTHONPATH}"
export XDG_CACHE_HOME="$login_project_root/data/camoufox-cache"
cd -- "$login_project_root"
exec "$login_project_root/.venv/bin/python" -m src.sites.dequienes.login "$@"
