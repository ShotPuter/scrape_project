#!/usr/bin/env bash
# Run from any directory: bash scripts/run_pep_50.sh [workbook.xlsx] [output-directory]
set -euo pipefail
umask 077

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PEP_PYTHON:-$ROOT/.venv/bin/python}"
WORKBOOK="${1:-$ROOT/docs/DECLARACIONES_PEP.xlsx}"
OUTPUT="${2:-$ROOT/data/pep-benchmark-50-xvfb}"

for executable in google-chrome Xvfb xvfb-run xauth; do
    if ! command -v "$executable" >/dev/null 2>&1; then
        printf 'Missing prerequisite: %s\n' "$executable" >&2
        exit 2
    fi
done
if [[ ! -x "$PYTHON" || ! -f "$WORKBOOK" ]]; then
    printf 'Python executable or input workbook is missing.\n' >&2
    exit 2
fi

# Resolve caller-relative input/output before switching to the project directory.
WORKBOOK="$(realpath -- "$WORKBOOK")"
mkdir -p -- "$OUTPUT"
OUTPUT="$(cd -- "$OUTPUT" && pwd)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)-$$"
CONSOLE="$OUTPUT/console-$STAMP.log"
XVFB_LOG="$OUTPUT/xvfb-$STAMP.log"
EXTRA=()
if [[ -n "${PEP_CA_BUNDLE:-}" ]]; then
    EXTRA+=(--ca-bundle "$(realpath -- "$PEP_CA_BUNDLE")")
fi
cd -- "$ROOT"

# Preserve the Python/xvfb-run exit status instead of reporting tee's success.
set +e
env -u DISPLAY -u WAYLAND_DISPLAY -u XDG_RUNTIME_DIR -u DBUS_SESSION_BUS_ADDRESS \
    XDG_SESSION_TYPE=x11 \
    xvfb-run -a -e "$XVFB_LOG" -s '-screen 0 1920x1080x24 -nolisten tcp' \
    "$PYTHON" -u -m src.sites.pep.benchmark "$WORKBOOK" \
    --network direct --headed --count 50 --workers 1 \
    --delay 2 --timeout 40 --notify-interval 30 \
    --max-minutes "${PEP_MAX_MINUTES:-60}" --output "$OUTPUT" "${EXTRA[@]}" \
    2>&1 | tee -a "$CONSOLE"
STATUSES=("${PIPESTATUS[@]}")
set -e
CODE="${STATUSES[0]}"
if [[ "$CODE" == 0 && "${STATUSES[1]}" != 0 ]]; then
    CODE="${STATUSES[1]}"
fi
printf 'PEP exit code: %s; console: %s; Xvfb: %s\n' "$CODE" "$CONSOLE" "$XVFB_LOG" | tee -a "$CONSOLE"
exit "$CODE"
