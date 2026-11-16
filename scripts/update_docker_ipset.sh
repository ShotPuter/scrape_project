#!/usr/bin/env bash
# Reapply safely: refresh cached FireHOL when due, atomically swap ipset,
# and retain a closed bridge gate when required data is unavailable.
set -euo pipefail
firewall_script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
exec "$firewall_script_dir/setup_docker_firewall.sh" --apply
