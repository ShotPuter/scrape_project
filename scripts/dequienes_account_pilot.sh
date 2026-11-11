#!/usr/bin/env bash
set -euo pipefail
account_project_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
export PYTHONPATH="$account_project_root/.camoufox-deps:$account_project_root${PYTHONPATH:+:$PYTHONPATH}"
export XDG_CACHE_HOME="$account_project_root/data/camoufox-cache"
cd -- "$account_project_root"
exec "$account_project_root/.venv/bin/python" -m src.sites.dequienes.account_pilot "$@"
