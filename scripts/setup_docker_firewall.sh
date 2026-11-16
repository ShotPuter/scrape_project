#!/usr/bin/env bash
# Run on the HOST, never inside a scraping container. Default: dry-run.
set -euo pipefail
firewall_project_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$firewall_project_root"
export PYTHONPATH="$firewall_project_root${PYTHONPATH:+:$PYTHONPATH}"
exec "${FIREWALL_PYTHON:-python3}" -m src.proxy.docker_firewall "$@"
