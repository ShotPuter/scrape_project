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
