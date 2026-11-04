#!/usr/bin/env bash
set -euo pipefail
camoufox_project_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
export PYTHONPATH="$camoufox_project_root/.camoufox-deps:$camoufox_project_root${PYTHONPATH:+:$PYTHONPATH}"
export XDG_CACHE_HOME="$camoufox_project_root/data/camoufox-cache"
if [ "$#" -eq 0 ]; then set -- version; fi
exec "$camoufox_project_root/.venv/bin/python" -m camoufox "$@"
