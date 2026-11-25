"""Health page (M0 stub): read-only, loopback-only, built from files the recorder writes."""
import json
import threading
import urllib.error
import urllib.request

import pytest

from fakes import FakeInfo
from test_recorder_e2e import _cfg, _run

from keel.cli import main
from keel.health import make_server, render, snapshot


def test_snapshot_without_data_says_so(tmp_path):
    snap = snapshot(tmp_path)
    assert snap["state"] == "no data" and snap["status"] is None
    page = render(snap)
    assert "no data" in page and "keel quality" in page


def test_page_after_a_real_recorder_run(tmp_path):
    with FakeInfo() as info:
        cfg = _cfg(tmp_path, info.url, "", references=[])
        _run(cfg, info, 2.0)
    from pathlib import Path

    from keel.quality import write_report

    root = Path(cfg.data_root)

    write_report(root)
    snap = snapshot(root)
    assert snap["status"]["session"]
    assert snap["state"] in ("running", "degraded")  # status written moments ago
    assert snap["quality"]["checks"]
    assert any(e["kind"] == "session_start" for e in snap["events"])
    page = render(snap)
    assert snap["status"]["session"] in page and "Data quality" in page and "<script" not in page
    # stale status is reported as not running
    late = snapshot(root, now_ns=snap["status"]["now_ns"] + 120 * 10**9)
    assert late["state"] == "not running"


def test_values_are_escaped(tmp_path):
    (tmp_path / "meta").mkdir()
    (tmp_path / "meta" / "status.json").write_text(json.dumps({"now_ns": 1, "session": "<img src=x onerror=alert(1)>", "healthy": True}))
    page = render(snapshot(tmp_path))
    assert "<img" not in page and "&lt;img" in page


def test_server_is_loopback_only_and_read_only(tmp_path):
    with pytest.raises(ValueError):
        make_server(tmp_path, host="0.0.0.0", port=0)
    srv = make_server(tmp_path, host="127.0.0.1", port=0)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    try:
        base = "http" + "://127.0.0.1:%d" % srv.server_address[1]
        with urllib.request.urlopen(base + "/") as r:
            assert r.status == 200 and b"Keel" in r.read()
            assert r.headers["X-Frame-Options"] == "DENY"
        with urllib.request.urlopen(base + "/health.json") as r:
            assert json.loads(r.read())["state"] == "no data"
        req = urllib.request.Request(base + "/", data=b"x", method="POST")
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(req)
        assert e.value.code == 405
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(base + "/meta/status.json")
        assert e.value.code == 404  # never serves files from the data folder
    finally:
        srv.shutdown()
        srv.server_close()


def test_cli_health_once_and_refuses_public_bind(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert main(["health", "--once", "h.html"]) == 0
    assert "Keel · Health" in (tmp_path / "h.html").read_text()
    assert main(["health", "--host", "0.0.0.0"]) == 2
