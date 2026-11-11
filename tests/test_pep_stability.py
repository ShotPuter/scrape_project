import json
import os
import subprocess
import sys
from pathlib import Path


def test_stability_launcher_retries_errors_but_never_reopens_quarantined_engine(tmp_path, monkeypatch):
    tools = tmp_path / "bin"
    tools.mkdir()
    for name in ("google-chrome", "Xvfb", "xauth", "sleep", "flock"):
        path = tools / name
        path.write_text("#!/bin/sh\nexit 0\n")
        path.chmod(0o700)
    identity = tools / "id"
    identity.write_text("#!/bin/sh\nprintf '1000\\n'\n")
    identity.chmod(0o700)
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
    watchdog = tools / "timeout"
    watchdog.write_text('#!/bin/sh\nshift 3\nexec "$@"\n')
    watchdog.chmod(0o700)
    python = tools / "fake-python"
    python.write_text('''#!/usr/bin/env python3
import json, os, subprocess, sys
from pathlib import Path
args = sys.argv[1:]
if args[0] == '-':
    sys.exit(subprocess.run([os.environ['REAL_PYTHON'], *args], input=sys.stdin.read(), text=True, check=False).returncode)
module = args[args.index('-m') + 1]
if module.endswith('stability_report'):
    sys.exit(subprocess.run([os.environ['REAL_PYTHON'], '-m', module, args[-1]], check=False).returncode)
engine = args[args.index('--browser-engine') + 1]
kind = args[args.index('--pass-kind') + 1]
output = Path(args[args.index('--output') + 1])
complete = 10 if kind == 'errors' else 9 if engine == 'nodriver' else 0
report = {'status': 'COMPLETED' if complete == 10 else 'INCOMPLETE',
 'started_at': '2026-11-03T00:0' + ('1' if kind == 'errors' else '0'),
 'pass_kind': kind, 'completed_links': complete, 'selected_unique_links': 10,
 'finished_links': 10 if engine == 'nodriver' else 1, 'pending_links': 0 if engine == 'nodriver' else 9,
 'job_attempts_total': 11 if kind == 'errors' else 10 if engine == 'nodriver' else 1,
 'unique_documents_downloaded': complete, 'unique_documents_discovered': 10,
 'pdf_files': complete, 'document_statuses': {}, 'failure_stages': {}, 'rejection_reasons': {},
 'elapsed_seconds': 1, 'sample_sha256': 'same-sample', 'retryable_links': 1 if engine == 'nodriver' and kind == 'all' else 0,
 'stop_reason': ('sample_contains_unresolved_errors' if engine == 'nodriver' and kind == 'all'
                 else 'workers_unavailable_or_quarantined')}
(output / 'benchmark.json').write_text(json.dumps(report))
(output / ('run-' + kind + '.json')).write_text(json.dumps(report))
assert args[args.index('--count') + 1] == '10'
assert '--single-browser-attempt' in args
assert os.getenv('DISPLAY') == ':199' and os.getenv('WAYLAND_DISPLAY') is None
print(engine, kind, flush=True)
sys.exit(int(complete != 10))
''')
    python.chmod(0o700)
    workbook = tmp_path / "input.xlsx"
    workbook.write_bytes(b"test")
    output = tmp_path / "results"
    monkeypatch.setenv("PATH", f"{tools}:{os.environ['PATH']}")
    monkeypatch.setenv("PEP_PYTHON", str(python))
    monkeypatch.setenv("REAL_PYTHON", sys.executable)
    monkeypatch.delenv("PEP_CA_BUNDLE", raising=False)
    monkeypatch.delenv("PEP_COUNT", raising=False)
    monkeypatch.setenv("WAYLAND_DISPLAY", "real-desktop")
    script = Path(__file__).resolve().parents[1] / "scripts" / "test_pep_stability.sh"
    result = subprocess.run(["bash", str(script), str(workbook), str(output)], cwd=tmp_path,
                            capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 1
    report = json.loads((output / "comparison.json").read_text())
    nodriver, native = report["results"]
    assert report["same_sample"] is True
    assert nodriver["first_pass_completed_links"] == 9
    assert nodriver["final_completed_links"] == 10
    assert nodriver["additional_completed_after_first_pass"] == 1
    assert native["runs"] == 1
    assert not list((output / "native").glob("console-errors-*.log"))
