#!/usr/bin/env bash
set -euo pipefail
res_project_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
export PYTHONPATH="$res_project_root/.camoufox-deps:$res_project_root${PYTHONPATH:+:$PYTHONPATH}"
export XDG_CACHE_HOME="$res_project_root/data/camoufox-cache"
cd -- "$res_project_root"
exec "$res_project_root/.venv/bin/python" -m src.sites.res "$@"
