"""Unit tests: config, log, classify, queue, ledger, state, delta, evict, startup, validate."""
import asyncio
import json
import os
from datetime import datetime, timedelta, timezone

import pytest

import main as main_mod
from src import config as cfgmod
from src.classify import Kind, classify, find_chat_id, make_entry_id
from src.delta import DeltaTap
from src.evict import enforce_limit
from src.ledger import Ledger
from src.log import Logger
from src.reqqueue import QueueClosed, QueueFull, QueueTimeout, RequestQueue
from src.slotpath import get_arg
from src.startup import run_startup_checks
from src.state import State
from src.validate import check


class _Log:
    def __init__(self): self.lines = []
    def info(self, m): self.lines.append(m)
    warn = error = info


# ---------------------------------------------------------------- config
def test_config_missing_file_is_created_with_defaults(tmp_path):
    p = tmp_path / "config.json"
    cfg = cfgmod.load_config(str(p))
    assert cfg == cfgmod.DEFAULTS and p.exists()


def test_config_docs_mirror_every_key(tmp_path):
    doc = cfgmod.default_document()
    assert set(doc["$docs"]) == set(cfgmod.DEFAULTS)
    assert all(isinstance(v, str) and v for v in doc["$docs"].values())


def test_config_corrupt_file_recreated(tmp_path):
    p = tmp_path / "config.json"
    p.write_text("{not json")
    assert cfgmod.load_config(str(p))["listen_port"] == cfgmod.DEFAULTS["listen_port"]
    assert json.loads(p.read_text())["listen_port"] == cfgmod.DEFAULTS["listen_port"]


def test_config_custom_values_and_partial_file(tmp_path):
    p = tmp_path / "c.json"
    p.write_text(json.dumps({"listen_port": 9999, "queue_limit": 3, "queue_timeout": 5}))
    cfg = cfgmod.load_config(str(p))
    assert cfg["listen_port"] == 9999 and cfg["queue_limit"] == 3 and cfg["queue_timeout"] == 5.0
    assert cfg["upstream_url"] == cfgmod.DEFAULTS["upstream_url"]


@pytest.mark.parametrize("bad", [{"listen_port": "80"}, {"upstream_url": "nope"},
                                 {"bin_padding_percent": 150}, {"queue_limit": -1},
                                 {"save_timeout": True}])
def test_config_invalid_values_rejected(tmp_path, bad):
    p = tmp_path / "c.json"
    p.write_text(json.dumps(bad))
    with pytest.raises(cfgmod.ConfigError):
        cfgmod.load_config(str(p))


def test_generate_config_flag_writes_and_refuses_overwrite(tmp_path, capsys):
    p = tmp_path / "gen.json"
    assert main_mod.main(["--generate-config", "--config-file", str(p)]) == 0
    assert "$docs" in json.loads(p.read_text())
    assert main_mod.main(["--generate-config", "--config-file", str(p)]) == 1


# ------------------------------------------------------------------- log
def test_logger_format_and_verbose_gating(capsys):
    quiet = Logger(verbose=False)
    quiet.info("hidden")
    quiet.error("shown")
    cap = capsys.readouterr()
    assert "hidden" not in cap.out and "ERROR: shown" in cap.err
    Logger(verbose=True).info("visible")
    out = capsys.readouterr().out
    assert out.startswith("[") and "] visible" in out
    datetime.strptime(out[1:20], "%Y-%m-%d %H:%M:%S")


# -------------------------------------------------------------- classify
def test_entry_id_is_md5_of_chat_underscore_model():
    import hashlib
    assert make_entry_id("c", "m") == hashlib.md5(b"c_m").hexdigest()


def test_first_recognized_header_wins(monkeypatch):
    import src.classify as cl
    monkeypatch.setattr(cl, "RECOGNIZED_HEADERS", ["x-a", "x-b"])
    assert find_chat_id({"X-B": "b", "X-A": "a"}) == "a"
    assert find_chat_id({"X-B": "b"}) == "b"
    assert find_chat_id({"other": "x"}) is None


CHAT = json.dumps({"model": "27B", "stream": True}).encode()


@pytest.mark.parametrize("method,path,headers,body,kind", [
    ("POST", "/v1/chat/completions", {"x-session-affinity": "c"}, CHAT, Kind.TRACKED),
    ("POST", "/v1/chat/completions", {}, CHAT, Kind.NOOP),
    ("POST", "/v1/chat/completions", {"x-session-affinity": "c"}, b'{"messages": []}', Kind.NOOP),
    ("POST", "/v1/chat/completions", {"x-session-affinity": "c"}, b"garbage", Kind.NOOP),
    ("POST", "/v1/completions", {"x-session-affinity": "c"}, CHAT, Kind.PLAIN),
    ("POST", "/models/unload", {}, b'{"model": "27B"}', Kind.UNLOAD),
    ("POST", "/models/unload", {}, b"nope", Kind.PLAIN),
    ("GET", "/health", {}, b"", Kind.BYPASS),
    ("GET", "/models/sse", {}, b"", Kind.BYPASS),
    ("GET", "/v1/models", {}, b"", Kind.BYPASS),
    ("POST", "/tokenize", {}, b"{}", Kind.PLAIN),
])
def test_classification_table(method, path, headers, body, kind):
    assert classify(method, path, headers, body).kind is kind


def test_tracked_carries_ids_and_stream_flag():
    c = classify("POST", "/v1/chat/completions", {"x-session-affinity": "c"}, CHAT)
    assert (c.chat_id, c.model_id, c.stream) == ("c", "27B", True)
    assert c.entry_id == make_entry_id("c", "27B")


# ----------------------------------------------------------------- queue
async def test_queue_is_fifo_and_single_flight():
    q = RequestQueue()
    order, active, peak = [], 0, 0

    async def worker(i):
        nonlocal active, peak
        await q.acquire()
        active += 1; peak = max(peak, active)
        order.append(i)
        await asyncio.sleep(0.01)
        active -= 1
        q.release()

    await asyncio.gather(*(worker(i) for i in range(5)))
    assert order == [0, 1, 2, 3, 4] and peak == 1


async def test_queue_limit_counts_waiting_only():
    q = RequestQueue(limit=1)
    await q.acquire()                      # in flight, not counted
    waiter = asyncio.create_task(q.acquire())
    await asyncio.sleep(0)
    with pytest.raises(QueueFull):
        await q.acquire()
    q.release(); await waiter; q.release()


async def test_queue_timeout_covers_waiting_only():
    q = RequestQueue(timeout=0.05)
    await q.acquire()
    with pytest.raises(QueueTimeout):
        await q.acquire()
    assert q.waiting == 0
    q.release()


async def test_queue_zero_means_unlimited():
    q = RequestQueue(timeout=0, limit=0)
    await q.acquire()
    tasks = [asyncio.create_task(q.acquire()) for _ in range(20)]
    await asyncio.sleep(0.05)
    assert q.waiting == 20
    for t in tasks:
        q.release(); await t
    q.release()


async def test_queue_cancelled_waiter_is_removed():
    q = RequestQueue()
    await q.acquire()
    t = asyncio.create_task(q.acquire())
    await asyncio.sleep(0)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert q.waiting == 0
    q.release()
    await asyncio.wait_for(q.acquire(), 0.5)   # slot is free again


async def test_queue_close_rejects_waiters_and_new_requests():
    q = RequestQueue()
    await q.acquire()
    t = asyncio.create_task(q.acquire())
    await asyncio.sleep(0)
    q.close()
    with pytest.raises(QueueClosed):
        await t
    with pytest.raises(QueueClosed):
        await q.acquire()
    q.release()
    await asyncio.wait_for(q.wait_idle(), 0.5)


# ---------------------------------------------------------------- ledger
def test_ledger_roundtrip_and_dates(tmp_path):
    p = str(tmp_path / "l.json")
    led = Ledger(p); led.load()
    led.upsert_saved("a" * 32, "chat", "27B", "/x/aaa.bin")
    led.mark_restored("a" * 32)
    led2 = Ledger(p); led2.load()
    e = led2.get("a" * 32)
    assert e["chat_id"] == "chat" and e["bin_path"] == "/x/aaa.bin"
    assert e["dates"]["created"] and e["dates"]["saved"] and e["dates"]["restored"]
    led2.remove("a" * 32)
    led3 = Ledger(p); led3.load()
    assert led3.entries() == []


@pytest.mark.parametrize("content", ["", "{bad", '{"a": 1}', '[{"entry_id": "x"}]'])
def test_ledger_missing_or_corrupt_recreated_empty(tmp_path, content):
    p = tmp_path / "l.json"
    if content:
        p.write_text(content)
    led = Ledger(str(p)); led.load()
    assert led.entries() == [] and json.loads(p.read_text()) == []


def test_ledger_write_is_atomic(tmp_path, monkeypatch):
    p = tmp_path / "l.json"
    led = Ledger(str(p)); led.load()
    led.upsert_saved("a" * 32, "c", "m", "/x/a.bin")
    before = p.read_text()

    def boom(src, dst): raise OSError("disk gone")
    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError):
        led.upsert_saved("b" * 32, "c", "m", "/x/b.bin")
    monkeypatch.undo()
    assert p.read_text() == before
    assert [f for f in os.listdir(tmp_path) if f.endswith(".tmp")] == []


# ----------------------------------------------------------------- state
def test_state_set_active_and_reset():
    s = State()
    c = classify("POST", "/v1/chat/completions", {"x-session-affinity": "c"}, CHAT)
    s.set_active(c)
    assert s.active_model == "27B" and s.active_chat == c.entry_id and s.active_chat_id == "c"
    noop = classify("POST", "/v1/chat/completions", {}, CHAT)
    s.set_active(noop)
    assert s.active_chat == "" and s.active_model == "27B"
    s.loaded_chats.add("x"); s.tracked_paths["27B"] = "/p"
    s.reset()
    assert (s.active_model, s.active_chat, s.loaded_chats) == ("", "", set())
    assert s.tracked_paths == {"27B": "/p"}     # reset never forgets paths


# ----------------------------------------------------------------- delta
def _sse(*objs):
    return b"".join(b"data: " + (o if isinstance(o, bytes) else json.dumps(o).encode()) + b"\n\n" for o in objs)


def test_delta_sse_split_at_every_byte_boundary():
    raw = _sse({"choices": []}, {"timings": {"prompt_n": 7, "predicted_n": 3}}, b"[DONE]")
    for cut in range(1, len(raw)):
        tap = DeltaTap(); tap.configure("text/event-stream")
        tap.feed(raw[:cut]); tap.feed(raw[cut:])
        assert tap.delta() == 10


def test_delta_non_stream_json_and_missing_timings():
    tap = DeltaTap(); tap.configure("application/json")
    tap.feed(b'{"timings": {"prompt_n": 4, "predicted_n": 6}}')
    assert tap.delta() == 10
    tap = DeltaTap(); tap.configure("application/json"); tap.feed(b'{"usage": {}}')
    assert tap.delta() is None
    tap = DeltaTap(); tap.configure("text/event-stream"); tap.feed(_sse({"choices": []}))
    assert tap.delta() is None


# ----------------------------------------------------------------- evict
def _mk_entry(led, tmp_path, name, size, saved, restored=None, created=None):
    path = tmp_path / f"{name * 32}.bin"
    path.write_bytes(b"x" * size)
    e = led.upsert_saved(name * 32, "c", "m", str(path))
    e["dates"]["saved"] = saved.isoformat()
    e["dates"]["restored"] = restored.isoformat() if restored else None
    e["dates"]["created"] = (created or saved).isoformat()
    led._write()
    return path


def test_evict_noop_under_limit_and_target_when_over(tmp_path):
    led = Ledger(str(tmp_path / "l.json")); led.load()
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    paths = [_mk_entry(led, tmp_path, n, 1024 ** 2, t0 + timedelta(days=i)) for i, n in enumerate("abcd")]
    cfg = {"bin_max_total_gb": 5 / 1024, "bin_padding_percent": 40}   # max 5 MiB, target 3 MiB
    assert enforce_limit(led, {**cfg, "bin_max_total_gb": 10 / 1024}, _Log()) == []   # 4 MiB < 10 MiB
    evicted = enforce_limit(led, {"bin_max_total_gb": 3.5 / 1024, "bin_padding_percent": 40}, _Log())
    # max 3.5 MiB, target 2.1 MiB: delete oldest until <= 2.1 MiB -> two removed
    assert evicted == ["a" * 32, "b" * 32]
    assert not paths[0].exists() and not paths[1].exists() and paths[2].exists()
    assert [e["entry_id"] for e in led.entries()] == ["c" * 32, "d" * 32]


def test_evict_order_uses_latest_activity_null_dates_oldest_and_protects_active(tmp_path):
    led = Ledger(str(tmp_path / "l.json")); led.load()
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    _mk_entry(led, tmp_path, "a", 1000, t0, restored=t0 + timedelta(days=30))   # restored recently
    _mk_entry(led, tmp_path, "b", 1000, t0 + timedelta(days=5))                 # saved day 5
    _mk_entry(led, tmp_path, "c", 1000, t0 + timedelta(days=1))                 # oldest activity
    cfg = {"bin_max_total_gb": 1500 / 1024 ** 3, "bin_padding_percent": 0}
    evicted = enforce_limit(led, cfg, _Log())
    assert evicted == ["c" * 32, "b" * 32]       # a survives: restored later than others were saved
    led2 = Ledger(str(tmp_path / "l2.json")); led2.load()
    _mk_entry(led2, tmp_path, "d", 1000, t0)
    _mk_entry(led2, tmp_path, "e", 1000, t0 + timedelta(days=1))
    protected = enforce_limit(led2, {"bin_max_total_gb": 0, "bin_padding_percent": 0}, _Log(),
                              protect_entry_id="d" * 32)
    assert protected == ["e" * 32] and led2.get("d" * 32) is not None


# --------------------------------------------------------------- startup
def test_startup_routine_cases(tmp_path):
    led = Ledger(str(tmp_path / "l.json")); led.load()
    ok = tmp_path / ("a" * 32 + ".bin"); ok.write_bytes(b"data")
    led.upsert_saved("a" * 32, "c", "m", str(ok))
    led.upsert_saved("b" * 32, "c", "m", str(tmp_path / ("b" * 32 + ".bin")))     # bin missing
    empty = tmp_path / ("c" * 32 + ".bin"); empty.write_bytes(b"")
    led.upsert_saved("c" * 32, "c", "m", str(empty))                              # zero bytes
    orphan = tmp_path / ("d" * 32 + ".bin"); orphan.write_bytes(b"orphan")
    foreign = tmp_path / "manual-save.bin"; foreign.write_bytes(b"mine")
    note = tmp_path / "notes.txt"; note.write_text("keep")
    run_startup_checks(led, _Log())
    assert [e["entry_id"] for e in led.entries()] == ["a" * 32]
    assert ok.exists() and not empty.exists() and not orphan.exists()
    assert foreign.exists() and note.exists()
    led2 = Ledger(str(tmp_path / "l.json")); led2.load()
    assert [e["entry_id"] for e in led2.entries()] == ["a" * 32]


# -------------------------------------------------------------- validate
def _model(args, status="loaded", name="27B"):
    return {"id": name, "status": {"value": status, "args": args}}


def test_get_arg_forms():
    assert get_arg(["--parallel", "1"], "--parallel", "-np") == "1"
    assert get_arg(["-np", "4"], "--parallel", "-np") == "4"
    assert get_arg(["--parallel=2"], "--parallel", "-np") == "2"
    assert get_arg(["--other"], "--parallel", "-np") is None
    assert get_arg(None, "--x") is None


def test_validate_clean_setup_has_no_warnings(tmp_path):
    args = ["--slot-save-path", str(tmp_path), "-np", "1"]
    assert check({"data": [_model(args)]}) == []


def test_validate_each_check(tmp_path):
    good = ["--slot-save-path", str(tmp_path), "-np", "1"]
    assert any("check 1" in w for w in check({"data": [{"id": "x"}]}))
    two = {"data": [_model(good, name="a"), _model(good, name="b")]}
    assert any("check 2" in w for w in check(two))
    assert any("check 3" in w for w in check({"data": [_model(["--slot-save-path", str(tmp_path)])]}))
    assert any("check 3" in w for w in check({"data": [_model(good[:2] + ["-np", "4"])]}))
    assert any("check 4" in w for w in check({"data": [_model(good + ["--sleep-idle-seconds", "60"])]}))
    assert not any("check 4" in w for w in check({"data": [_model(good + ["--sleep-idle-seconds", "0"])]}))
    assert any("check 5" in w for w in check({"data": [_model(["-np", "1"])]}))
    assert any("check 5" in w for w in check({"data": [_model(["--slot-save-path", "/nope/nope", "-np", "1"])]}))


def test_validate_skips_models_without_args():
    assert check({"data": [{"id": "x", "status": {"value": "unloaded"}}]}) == []
