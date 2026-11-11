#!/usr/bin/env bash
set -euo pipefail
archive_project_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
export PYTHONPATH="$archive_project_root/.camoufox-deps:$archive_project_root${PYTHONPATH:+:$PYTHONPATH}"
export XDG_CACHE_HOME="$archive_project_root/data/camoufox-cache"
cd -- "$archive_project_root"
exec "$archive_project_root/.venv/bin/python" -m src.sites.dequienes.companies "$@"
