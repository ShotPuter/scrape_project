import json
import os
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("exit_code", [0, 1, 130])
def test_launcher_uses_virtual_display_logs_and_preserves_exit_code(tmp_path, monkeypatch, exit_code):
    tools = tmp_path / "bin"
    tools.mkdir()
    for name in ("google-chrome", "Xvfb", "xauth"):
        path = tools / name
        path.write_text("#!/bin/sh\nexit 0\n")
        path.chmod(0o700)
    xvfb = tools / "xvfb-run"
    xvfb.write_text('''#!/bin/sh
while [ "$#" -gt 0 ]; do
    case "$1" in
        -a) shift ;;
        -e|-s) shift 2 ;;
        *) break ;;
    esac
done
export DISPLAY=:199
exec "$@"
''')
    xvfb.chmod(0o700)
    python = tools / "fake-python"
    python.write_text('''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
Path(os.environ['FAKE_CAPTURE']).write_text(json.dumps({
    'args': sys.argv[1:], 'display': os.getenv('DISPLAY'),
    'wayland': os.getenv('WAYLAND_DISPLAY'), 'runtime': os.getenv('XDG_RUNTIME_DIR'),
}))
print('fake benchmark console output', flush=True)
sys.exit(int(os.environ['FAKE_EXIT_CODE']))
''')
    python.chmod(0o700)
    workbook = tmp_path / "input with spaces.xlsx"
    workbook.write_bytes(b"fake-input")
    output = tmp_path / "output with spaces"
    capture = tmp_path / "capture.json"
    monkeypatch.setenv("PATH", f"{tools}:{os.environ['PATH']}")
    monkeypatch.setenv("PEP_PYTHON", str(python))
    monkeypatch.delenv("PEP_CA_BUNDLE", raising=False)
    monkeypatch.setenv("FAKE_CAPTURE", str(capture))
    monkeypatch.setenv("FAKE_EXIT_CODE", str(exit_code))
    monkeypatch.setenv("DISPLAY", ":real-display")
    monkeypatch.setenv("WAYLAND_DISPLAY", "real-wayland")
    monkeypatch.setenv("XDG_RUNTIME_DIR", "/real-desktop")
    script = Path(__file__).resolve().parents[1] / "scripts" / "run_pep_50.sh"
    result = subprocess.run(["bash", str(script), str(workbook), str(output)],
                            cwd=tmp_path, capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == exit_code
    recorded = json.loads(capture.read_text())
    args = recorded["args"]
    for flag, expected in (("--network", "direct"), ("--count", "50"), ("--workers", "1")):
        assert args[args.index(flag) + 1] == expected
    assert "--headed" in args
    assert recorded["display"] == ":199"
    assert recorded["wayland"] is None
    assert recorded["runtime"] is None
    logs = list(output.glob("console-*.log"))
    assert len(logs) == 1
    assert "fake benchmark console output" in logs[0].read_text()
    assert f"PEP exit code: {exit_code}" in logs[0].read_text()
