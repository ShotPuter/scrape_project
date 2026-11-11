#!/usr/bin/env bash
# Ten identical links per engine, persistent profiles, at most one bounded error pass.
set -euo pipefail
umask 077
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PEP_PYTHON:-$ROOT/.venv/bin/python}"
WORKBOOK="$(realpath -- "${1:-$ROOT/docs/DECLARACIONES_PEP.xlsx}")"
OUTPUT="${2:-$ROOT/data/pep-stability-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
COUNT="${PEP_COUNT:-10}"
if [[ "$(id -u)" == 0 ]]; then
    printf 'Run as a non-root user; Chrome sandbox must remain enabled.\n' >&2
    exit 2
fi
if [[ ! "$COUNT" =~ ^(1[0-9]|20)$ ]]; then
    printf 'PEP_COUNT must be 10..20.\n' >&2
    exit 2
fi
for executable in google-chrome Xvfb xvfb-run xauth flock timeout; do
    command -v "$executable" >/dev/null || { printf 'Missing prerequisite: %s\n' "$executable" >&2; exit 2; }
done
if [[ ! -x "$PYTHON" || ! -f "$WORKBOOK" ]]; then
    printf 'Python/workbook must exist.\n' >&2
    exit 2
fi
mkdir -p -- "$OUTPUT"
OUTPUT="$(cd -- "$OUTPUT" && pwd)"
exec 9>"$OUTPUT/comparison.lock"
flock -n 9 || { printf 'Another stability test owns this output.\n' >&2; exit 2; }
EXTRA=()
if [[ -n "${PEP_CA_BUNDLE:-}" ]]; then EXTRA+=(--ca-bundle "$(realpath -- "$PEP_CA_BUNDLE")"); fi
cd -- "$ROOT"

run_pass() {
    local engine="$1" kind="$2" stamp
    stamp="$(date -u +%Y%m%dT%H%M%SZ)-$$"
    mkdir -p "$OUTPUT/$engine"
    set +e
    env -u DISPLAY -u WAYLAND_DISPLAY -u XDG_RUNTIME_DIR -u DBUS_SESSION_BUS_ADDRESS \
        XDG_SESSION_TYPE=x11 TELEGRAM_BOT_TOKEN= TELEGRAM_CHAT_ID= \
        xvfb-run -a -e "$OUTPUT/$engine/xvfb-$kind-$stamp.log" -s '-screen 0 1920x1080x24 -nolisten tcp' \
        timeout --signal=TERM --kill-after=15s "${PEP_HARD_TIMEOUT:-20m}" \
        "$PYTHON" -u -m src.sites.pep.benchmark "$WORKBOOK" \
        --network direct --headed --browser-engine "$engine" --workers 1 --count "$COUNT" \
        --delay 3 --timeout 40 --notify-interval 30 --max-minutes "${PEP_MAX_MINUTES:-15}" \
        --pass-kind "$kind" --max-job-attempts 2 --single-browser-attempt \
        --output "$OUTPUT/$engine" "${EXTRA[@]}" 2>&1 | tee "$OUTPUT/$engine/console-$kind-$stamp.log"
    local statuses=("${PIPESTATUS[@]}")
    set -e
    if [[ "${statuses[1]}" != 0 ]]; then exit "${statuses[1]}"; fi
    if [[ "${statuses[0]}" != 0 && "${statuses[0]}" != 1 ]]; then
        exit "${statuses[0]}"
    fi
}

for engine in nodriver native; do
    run_pass "$engine" all
    # Never automatically reopen a quarantined browser after a CAPTCHA/403 refusal.
    if "$PYTHON" - "$OUTPUT/$engine/benchmark.json" <<'PY'
import json, sys
from pathlib import Path
path = Path(sys.argv[1])
report = json.loads(path.read_text()) if path.exists() else {}
eligible = (report.get('status') == 'INCOMPLETE'
            and report.get('stop_reason') == 'sample_contains_unresolved_errors'
            and report.get('retryable_links', 0) > 0)
sys.exit(0 if eligible else 1)
PY
    then
        sleep "${PEP_RETRY_DELAY_SECONDS:-30}"
        run_pass "$engine" errors
    fi
done
"$PYTHON" -m src.sites.pep.stability_report "$OUTPUT"
