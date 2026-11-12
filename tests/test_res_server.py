import sys

import pytest

from src.sites.res import __main__ as search, download, server


@pytest.mark.parametrize("stage", ["search", "download", "status", "export"])
def test_container_entrypoint_forwards_flags_and_stage(monkeypatch, stage):
    forwarded = []
    monkeypatch.setattr(search, "main", lambda: forwarded.extend(sys.argv[1:]))
    monkeypatch.setattr(download, "main", lambda: forwarded.extend(sys.argv[1:]))
    monkeypatch.setattr(sys, "argv", ["server", stage, "--output", "data/example", "--browsers", "5"])
    server.main()
    assert forwarded[:4] == ["--output", "data/example", "--browsers", "5"]
    assert forwarded[4:] == (["--action", stage] if stage in {"status", "export"} else [])


def test_benchmark_entrypoint_forwards_flags_and_exit_code(monkeypatch):
    from src.sites.res import benchmark

    seen = []
    monkeypatch.setattr(benchmark, "main", lambda args: seen.extend(args) or 2)
    monkeypatch.setattr(sys, "argv", ["server", "benchmark", "--limit", "150", "--browsers", "2"])
    with pytest.raises(SystemExit) as stopped:
        server.main()
    assert stopped.value.code == 2
    assert seen == ["--limit", "150", "--browsers", "2"]


def test_tracking_command_forwards_read_only_inspection(monkeypatch):
    from src.sites.res import tracking

    seen = []
    monkeypatch.setattr(tracking, "main", lambda args: seen.extend(args) or 0)
    monkeypatch.setattr(sys, "argv", ["server", "track", "--action", "errors", "--run-id", "sample"])
    with pytest.raises(SystemExit) as stopped:
        server.main()
    assert stopped.value.code == 0
    assert seen == ["--action", "errors", "--run-id", "sample"]
