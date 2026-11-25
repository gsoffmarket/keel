import gzip
import json

from keel.recorder.store import RawStore, iter_events, iter_raw, raw_files

NS_2026_11_23_10 = 1795428000 * 10**9  # 2026-11-23 10:00:00Z


def test_lines_are_verbatim_and_rotate_hourly(tmp_path):
    st = RawStore(tmp_path, session="s1")
    raw = '{"channel":"l2Book","data":{"coin":"#436390","x":0.10}}'
    st.write("hl_ws", raw, NS_2026_11_23_10)
    st.write("hl_ws", raw, NS_2026_11_23_10 + 3600 * 10**9)  # next hour
    st.write("hl_rest", "not json", NS_2026_11_23_10, req={"type": "outcomeMeta"})
    st.close()
    files = [p.relative_to(tmp_path).as_posix() for p in raw_files(tmp_path)]
    assert files == [
        "raw/hl_rest/2026-11-23/10Z-s1.jsonl.gz",
        "raw/hl_ws/2026-11-23/10Z-s1.jsonl.gz",
        "raw/hl_ws/2026-11-23/11Z-s1.jsonl.gz",
    ]
    line = gzip.open(tmp_path / files[1]).read().decode()
    assert raw in line  # payload inlined byte-for-byte (0.10 not re-serialised to 0.1)
    recs = list(iter_raw(tmp_path))
    assert [r["seq"] for r in recs] == [3, 1, 2]
    assert recs[0]["msg"] == "not json" and recs[0]["req"] == {"type": "outcomeMeta"}


def test_truncated_file_reads_up_to_last_complete_line(tmp_path):
    st = RawStore(tmp_path, session="s1", flush_every=0)
    for i in range(50):
        st.write("hl_ws", json.dumps({"i": i}), NS_2026_11_23_10)
    st.flush()
    p = raw_files(tmp_path)[0]
    # simulate a crash: chop the file mid-stream without closing the gzip member
    data = p.read_bytes()
    p.write_bytes(data[: len(data) - 7])
    got = [r["msg"]["i"] for r in iter_raw(tmp_path)]
    assert got == list(range(len(got))) and 40 <= len(got) <= 50


def test_new_session_never_appends_to_old_file(tmp_path):
    a = RawStore(tmp_path, session="a")
    a.write("hl_ws", "{}", NS_2026_11_23_10)
    a.close()
    b = RawStore(tmp_path, session="b")
    b.write("hl_ws", "{}", NS_2026_11_23_10)
    b.close()
    assert len(raw_files(tmp_path)) == 2


def test_events(tmp_path):
    st = RawStore(tmp_path, session="s")
    st.event("ws_connected", {"x": 1}, ns=NS_2026_11_23_10)
    st.close()
    ev = list(iter_events(tmp_path))
    assert ev == [{"ns": NS_2026_11_23_10, "session": "s", "kind": "ws_connected", "x": 1}]


def test_payload_newlines_do_not_split_records(tmp_path):
    st = RawStore(tmp_path, session="s1")
    st.write("ref_pm", '[{"title": "a\\nb"},\n {"x": 1}]\n', NS_2026_11_23_10)
    st.write("ref_pm", "[]", NS_2026_11_23_10 + 1)
    st.close()
    recs = list(iter_raw(tmp_path))
    assert len(recs) == 2 and recs[0]["msg"] == [{"title": "a\nb"}, {"x": 1}]


def test_reader_rejoins_records_split_by_old_writer(tmp_path):
    import gzip as gz

    p = tmp_path / "raw" / "ref_pm" / "2026-11-23"
    p.mkdir(parents=True)
    old = b'{"seq":1,"recv_ns":1,"src":"ref_pm","req":null,"msg":[1,2]\n}\n{"seq":2,"recv_ns":2,"src":"ref_pm","req":null,"msg":[]}\n'
    (p / "10Z-old.jsonl.gz").write_bytes(gz.compress(old))
    assert [r["seq"] for r in iter_raw(tmp_path)] == [1, 2]
