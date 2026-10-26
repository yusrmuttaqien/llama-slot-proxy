"""Integration tests: real proxy app in front of the fake llama-server."""
import asyncio
import json
import os
import time
from datetime import timedelta

import aiohttp
import pytest

import src.pipeline as pipeline
from src.ledger import utcnow
from src.shutdown import graceful_shutdown
from tests.conftest import eid


def chat_calls(fake):
    return [c for c in fake.calls if c["path"] == "/v1/chat/completions"]


# ------------------------------------------------------------ forwarding
async def test_no_upstream_calls_until_first_request(proxy, fake):
    assert fake.calls == []


async def test_get_and_post_forwarded_unchanged(proxy, fake):
    r = await proxy.session.get(proxy.url + "/props?model=27B")
    assert r.status == 200 and (await r.json())["path"] == "/props"
    r = await proxy.session.post(proxy.url + "/tokenize", data=b'{"content": "hi"}',
                                 headers={"Content-Type": "application/json", "X-Custom": "yes"})
    assert r.status == 200
    call = [c for c in fake.calls if c["path"] == "/tokenize"][0]
    assert call["body"] == {"content": "hi"} and call["headers"]["X-Custom"] == "yes"


async def test_sse_arrives_incrementally_and_is_byte_identical(proxy, fake):
    fake.chunk_delay = 0.15
    r = await proxy.chat(stream=True, settle=False)
    times, body = [], b""
    async for chunk in r.content.iter_any():
        times.append(time.monotonic()); body += chunk
    assert r.status == 200 and b"data: [DONE]" in body and body.count(b"data:") == 5
    assert times[-1] - times[0] > 0.25            # not buffered into one blob
    assert r.headers["Content-Type"].startswith("text/event-stream")


async def test_upstream_http_errors_pass_through_unchanged(proxy, fake):
    fake.chat_status = 400
    r = await proxy.chat()
    assert r.status == 400 and (await r.json())["error"]["message"] == "fake chat error"


async def test_upstream_down_gives_502_with_message(proxy_factory):
    p = await proxy_factory(upstream_url="http://127.0.0.1:9")
    r = await p.chat()
    assert r.status == 502
    assert "unreachable" in (await r.json())["error"]["message"]
    assert "upstream unreachable" in p.err_text


async def test_verbose_log_shows_classification(proxy, fake):
    await proxy.chat(chat_id=None)
    await proxy.session.get(proxy.url + "/health")
    await proxy.chat()
    log = proxy.log_text
    assert "NOOP" in log and "BYPASS" in log and "TRACKED" in log


# ----------------------------------------------------------------- queue
async def test_chats_reach_upstream_one_at_a_time(proxy, fake):
    fake.chat_delay = 0.1
    rs = await asyncio.gather(*(proxy.chat(chat_id=f"c{i}") for i in range(3)))
    assert [r.status for r in rs] == [200] * 3
    calls = sorted(chat_calls(fake), key=lambda c: c["t0"])
    for a, b in zip(calls, calls[1:]):
        assert a["t1"] <= b["t0"]


async def test_bypass_requests_do_not_wait_behind_a_slow_chat(proxy, fake):
    fake.chat_delay = 0.5
    slow = asyncio.create_task(proxy.chat())
    await asyncio.sleep(0.1)
    t = time.monotonic()
    r = await proxy.session.get(proxy.url + "/health")
    assert r.status == 200 and time.monotonic() - t < 0.3
    await slow


async def test_sse_events_endpoint_never_blocks_the_queue(proxy, fake):
    events = await proxy.session.get(proxy.url + "/models/sse")      # open forever
    r = await asyncio.wait_for(proxy.chat(), 3)
    assert r.status == 200
    events.close()


async def test_queue_limit_gives_429(proxy_factory, fake):
    p = await proxy_factory(queue_limit=1)
    fake.chat_delay = 0.3
    rs = await asyncio.gather(*(p.chat(chat_id=f"c{i}") for i in range(3)))
    assert sorted(r.status for r in rs) == [200, 200, 429]


async def test_queue_timeout_gives_504_and_waiting_request_never_reaches_upstream(proxy_factory, fake):
    p = await proxy_factory(queue_timeout=0.1)
    fake.chat_delay = 0.5
    rs = await asyncio.gather(p.chat(chat_id="a"), p.chat(chat_id="b"))
    assert sorted(r.status for r in rs) == [200, 504]
    assert len(chat_calls(fake)) == 1


async def test_client_that_leaves_while_waiting_never_reaches_upstream(proxy, fake):
    fake.chat_delay = 0.4
    first = asyncio.create_task(proxy.chat(chat_id="a"))
    await asyncio.sleep(0.1)
    body = {"model": "27B", "messages": []}
    short = aiohttp.ClientTimeout(total=0.1)
    with pytest.raises(asyncio.TimeoutError):
        await proxy.session.post(proxy.url + "/v1/chat/completions", json=body,
                                 headers={"x-session-affinity": "b"}, timeout=short)
    await first
    await asyncio.sleep(0.2)
    assert len(chat_calls(fake)) == 1


async def test_client_disconnect_mid_stream_aborts_upstream_and_frees_queue(proxy, fake):
    fake.chunk_delay = 0.2
    fake.stream_chunks = 20
    r = await proxy.chat(stream=True, settle=False)
    await r.content.readany()
    r.close()
    await asyncio.sleep(0.5)
    assert fake.chat_aborted == 1
    fake.stream_chunks = 1; fake.chunk_delay = 0
    t = time.monotonic()
    r2 = await asyncio.wait_for(proxy.chat(chat_id="other"), 3)
    assert r2.status == 200 and time.monotonic() - t < 2


async def test_non_stream_disconnect_aborts_upstream(proxy, fake):
    fake.chat_delay = 1.0
    with pytest.raises(asyncio.TimeoutError):
        await proxy.session.post(proxy.url + "/v1/chat/completions", json={"model": "27B"},
                                 timeout=aiohttp.ClientTimeout(total=0.2))
    await asyncio.sleep(0.3)
    assert fake.chat_aborted == 1


# -------------------------------------------- first hit and slot-save-path
async def test_first_request_fetches_models_before_forwarding_then_never_again(proxy, fake):
    await proxy.chat(chat_id=None)
    assert fake.summary() == ["models", "chat:27B"]
    await proxy.chat(chat_id=None)
    assert fake.summary() == ["models", "chat:27B", "chat:27B"]
    assert proxy.ctx.state.tracked_paths["27B"] == fake.slot_dir


async def test_failed_fetch_stores_none_and_client_still_served(proxy, fake):
    fake.models_fail = True
    r = await proxy.chat(chat_id=None)
    assert r.status == 200 and proxy.ctx.state.tracked_paths.get("27B") is None
    assert "could not fetch" in proxy.err_text


async def test_setup_warnings_are_printed_without_blocking(proxy, fake):
    fake.args = ["--slot-save-path", fake.slot_dir, "-np", "4", "--sleep-idle-seconds", "30"]
    r = await proxy.chat(chat_id=None)
    assert r.status == 200
    assert "setup check 3" in proxy.err_text and "setup check 4" in proxy.err_text


# --------------------------------------------------------- lazy restore
async def test_new_chat_no_restore_added_to_loaded_then_saved(proxy, fake):
    r = await proxy.chat(chat_id="chatA")
    assert r.status == 200
    assert fake.summary() == ["models", "chat:27B", f"slot:save:27B:{eid()}.bin"]
    assert eid() in proxy.ctx.state.loaded_chats
    assert proxy.ctx.ledger.get(eid())["bin_path"] == fake.bin_path(eid())


async def _seed(proxy, fake, chat="chatA"):
    """Run a chat so a ledger entry + bin exist, then forget runtime state (as after a restart)."""
    await proxy.chat(chat_id=chat)
    fake.calls.clear()
    proxy.ctx.state.reset()


async def test_known_entry_is_restored_before_the_chat_call(proxy, fake):
    await _seed(proxy, fake)
    await proxy.chat()
    assert fake.summary()[:2] == [f"slot:restore:27B:{eid()}.bin", "chat:27B"]
    assert proxy.ctx.ledger.get(eid())["dates"]["restored"]
    assert not any(s.startswith("slot:restore") for s in fake.summary()[2:])


async def test_already_loaded_entry_is_not_restored_again(proxy, fake):
    await _seed(proxy, fake)
    await proxy.chat(); fake.calls.clear()
    await proxy.chat()
    assert not any(s.startswith("slot:restore") for s in fake.summary())


async def test_noop_never_restores_and_clears_active_chat(proxy, fake):
    await _seed(proxy, fake)
    await proxy.chat(chat_id=None)
    assert fake.summary() == ["chat:27B"]
    assert proxy.ctx.state.active_chat == ""


async def test_restore_rejected_drops_entry_still_serves_chat_and_resaves(proxy, fake):
    await _seed(proxy, fake)
    fake.slot_mode["restore"] = "400"
    r = await proxy.chat()
    assert r.status == 200
    assert "rejected" in proxy.err_text
    assert eid() in proxy.ctx.state.loaded_chats
    # the rejected entry was dropped, so the finished request re-creates it (never-saved rule)
    s = fake.summary()
    assert s[0] == f"slot:restore:27B:{eid()}.bin" and s[1] == "chat:27B"
    assert s[2] == f"slot:save:27B:{eid()}.bin"
    fake.slot_mode["restore"] = "ok"
    assert proxy.ctx.ledger.get(eid())["dates"]["restored"] is None   # fresh entry, never restored


async def test_restore_rejected_deletes_the_old_entry_and_bin(proxy, fake):
    await _seed(proxy, fake)
    old_created = proxy.ctx.ledger.get(eid())["dates"]["created"]
    fake.slot_mode["restore"] = "400"
    fake.slot_mode["save"] = "400"          # keep the re-save from recreating it
    await proxy.chat()
    assert proxy.ctx.ledger.get(eid()) is None and not os.path.exists(fake.bin_path(eid()))
    assert old_created


async def test_restore_timeout_keeps_entry_and_serves_chat(proxy_factory, fake):
    p = await proxy_factory(save_timeout=0.2, save_time_threshold=10 ** 6, delta_context_threshold=10 ** 6)
    await _seed(p, fake)
    fake.slot_mode["restore"] = "hang"
    r = await p.chat()
    assert r.status == 200 and p.ctx.ledger.get(eid()) is not None
    assert "timed out" in p.err_text


# ------------------------------------------------------- save triggers
async def test_time_trigger_respects_threshold(proxy_factory, fake, monkeypatch):
    p = await proxy_factory(save_time_threshold=100, delta_context_threshold=10 ** 6)
    await p.chat()
    assert sum(s.startswith("slot:save") for s in fake.summary()) == 1
    await p.chat()                                           # immediately again: no save
    assert sum(s.startswith("slot:save") for s in fake.summary()) == 1
    later = utcnow() + timedelta(seconds=101)
    monkeypatch.setattr(pipeline, "utcnow", lambda: later)
    await p.chat()
    assert sum(s.startswith("slot:save") for s in fake.summary()) == 2


async def test_delta_trigger_uses_timings(proxy_factory, fake):
    p = await proxy_factory(save_time_threshold=10 ** 6, delta_context_threshold=100)
    await p.chat()                                           # first save (no entry yet)
    fake.timings = {"prompt_n": 50, "predicted_n": 40}       # 90 <= 100
    await p.chat(stream=True)
    assert sum(s.startswith("slot:save") for s in fake.summary()) == 1
    fake.timings = {"prompt_n": 80, "predicted_n": 40}       # 120 > 100
    await p.chat(stream=True)
    assert sum(s.startswith("slot:save") for s in fake.summary()) == 2
    fake.timings = {"prompt_n": 80, "predicted_n": 40}
    await p.chat(stream=False)                               # non-stream JSON path
    assert sum(s.startswith("slot:save") for s in fake.summary()) == 3


async def test_save_runs_after_the_chat_and_before_the_next_queued_request(proxy, fake):
    fake.chat_delay = 0.05
    await asyncio.gather(proxy.chat(chat_id="a"), proxy.chat(chat_id="b"))
    s = fake.summary()
    first_chat = s.index("chat:27B")
    assert s[first_chat + 1].startswith("slot:save")          # save sits between the two chats
    assert s[first_chat + 2].startswith("slot:erase")        # erase vacates the slot for the new conversation
    assert s[first_chat + 3] == "chat:27B"


async def test_noop_and_failed_responses_never_save(proxy, fake):
    await proxy.chat(chat_id=None)
    fake.chat_status = 500
    await proxy.chat()
    assert not any(s.startswith("slot:save") for s in fake.summary())


async def test_failing_save_does_not_affect_client_response(proxy, fake):
    fake.slot_mode["save"] = "400"
    r = await proxy.chat()
    assert r.status == 200 and await r.json()
    assert proxy.ctx.ledger.get(eid()) is None
    assert "slot save" in proxy.err_text


async def test_save_timeout_logs_and_queue_continues(proxy_factory, fake):
    p = await proxy_factory(save_timeout=0.2)
    fake.slot_mode["save"] = "hang"
    r = await p.chat()
    assert r.status == 200
    fake.slot_mode["save"] = "ok"
    r2 = await asyncio.wait_for(p.chat(chat_id="b"), 3)
    assert r2.status == 200


async def test_slot_save_path_change_moves_the_bin(proxy_factory, fake, tmp_path):
    p = await proxy_factory(save_time_threshold=0)
    await p.chat()
    old = p.ctx.ledger.get(eid())["bin_path"]
    new_dir = tmp_path / "newslots"; new_dir.mkdir()
    p.ctx.state.tracked_paths["27B"] = str(new_dir)
    fake.slot_dir = str(new_dir)
    await p.chat()
    entry = p.ctx.ledger.get(eid())
    assert entry["bin_path"] == str(new_dir / f"{eid()}.bin") and os.path.exists(entry["bin_path"])
    assert not os.path.exists(old)


async def test_eviction_runs_before_a_save(proxy_factory, fake):
    p = await proxy_factory(bin_max_total_gb=150 / 1024 ** 3, bin_padding_percent=0)
    await p.chat(chat_id="a")                    # 100 bytes
    await p.chat(chat_id="b")                    # 100 <= 150: no eviction yet, total 200
    await p.chat(chat_id="c")                    # 200 > 150: oldest ("a") evicted before c is saved
    ids = {e["entry_id"] for e in p.ctx.ledger.entries()}
    assert ids == {eid("b"), eid("c")} and not os.path.exists(fake.bin_path(eid("a")))


# ---------------------------------------------------------- model change
async def test_model_change_saves_old_model_before_forwarding(proxy_factory, fake):
    p = await proxy_factory(save_time_threshold=10 ** 6, delta_context_threshold=10 ** 6)
    await p.chat(model="27B")
    fake.calls.clear()
    await p.chat(chat_id="chatB", model="9B")
    s = fake.summary()
    assert s[0] == f"slot:save:27B:{eid('chatA', '27B')}.bin" and "chat:9B" in s[1:]
    assert p.ctx.state.active_model == "9B" and eid("chatA", "27B") not in p.ctx.state.loaded_chats


async def test_model_change_without_active_chat_does_not_save(proxy, fake):
    await proxy.chat(chat_id=None, model="27B")
    fake.calls.clear()
    await proxy.chat(chat_id=None, model="9B")
    assert not any(s.startswith("slot:save") for s in fake.summary())


async def test_noop_with_model_id_still_triggers_model_change(proxy_factory, fake):
    p = await proxy_factory(save_time_threshold=10 ** 6, delta_context_threshold=10 ** 6)
    await p.chat(model="27B")
    fake.calls.clear()
    await p.chat(chat_id=None, model="9B")
    assert fake.summary()[0].startswith("slot:save:27B")


# --------------------------------------------------------------- unload
async def test_unload_of_active_model_saves_first_then_resets(proxy_factory, fake):
    p = await proxy_factory(save_time_threshold=10 ** 6, delta_context_threshold=10 ** 6)
    await p.chat()
    fake.calls.clear()
    r = await p.session.post(p.url + "/models/unload", json={"model": "27B"})
    assert r.status == 200 and (await r.json()) == {"success": True}
    assert fake.summary() == [f"slot:save:27B:{eid()}.bin", "unload:27B"]
    st = p.ctx.state
    assert (st.active_model, st.active_chat, st.loaded_chats) == ("", "", set())


async def test_unload_of_other_model_has_no_save_and_keeps_state(proxy, fake):
    await proxy.chat()
    fake.calls.clear()
    await proxy.session.post(proxy.url + "/models/unload", json={"model": "other"})
    assert fake.summary() == ["unload:other"]
    assert proxy.ctx.state.active_chat == eid()


# --------------------------------------------------- errors and shutdown
async def test_unexpected_pipeline_error_gives_500_and_releases_queue(proxy, fake, monkeypatch):
    async def boom(*a, **k): raise RuntimeError("kaboom")
    monkeypatch.setattr(pipeline.restore, "maybe_restore", boom)
    r = await proxy.chat()
    assert r.status == 500 and "kaboom" in (await r.json())["error"]["message"]
    monkeypatch.undo()
    r2 = await asyncio.wait_for(proxy.chat(chat_id="x"), 3)
    assert r2.status == 200


async def test_connection_error_during_slot_call_resets_state(proxy, fake):
    await proxy.chat()
    assert proxy.ctx.state.active_chat == eid()
    await fake.stop()
    r = await proxy.chat(chat_id="other")
    assert r.status == 502
    assert proxy.ctx.state.active_chat == "" and proxy.ctx.state.loaded_chats == set()


async def test_graceful_shutdown_waits_for_inflight_rejects_waiters_and_saves(proxy_factory, fake):
    p = await proxy_factory(save_time_threshold=10 ** 6, delta_context_threshold=10 ** 6)
    await p.chat(chat_id="a")                                   # active chat a, saved once
    fake.calls.clear()
    fake.chat_delay = 0.4
    inflight = asyncio.create_task(p.chat(chat_id="b"))
    await asyncio.sleep(0.1)
    waiting = asyncio.create_task(p.chat(chat_id="c"))
    await asyncio.sleep(0.1)
    shutdown = asyncio.create_task(graceful_shutdown(p.ctx))
    r_in, r_wait = await inflight, await waiting
    await shutdown
    assert r_in.status == 200 and r_wait.status == 503
    late = await p.chat(chat_id="d")
    assert late.status == 503
    saves = [s for s in fake.summary() if s.startswith("slot:save")]
    assert saves[-1] == f"slot:save:27B:{eid('b')}.bin"
    assert fake.summary().count("chat:27B") == 1                 # "c" never reached upstream


async def test_shutdown_with_no_active_chat_does_not_save(proxy, fake):
    await proxy.chat(chat_id=None)
    fake.calls.clear()
    await graceful_shutdown(proxy.ctx)
    assert fake.summary() == []


# --------------------------- upstream without status.args (NInfer style) ---

NINFER_MODELS = {"object": "list", "data": [
    {"id": "27B", "object": "model", "created": 0, "owned_by": "ninfer",
     "max_model_len": 102400, "context_window": 102400, "modalities": {"vision": False}}]}


async def test_ninfer_style_upstream_uses_config_slot_save_path(proxy_factory, fake):
    p = await proxy_factory(slot_save_path=fake.slot_dir)
    fake.models_override = NINFER_MODELS
    r = await p.chat(chat_id="chatA")
    assert r.status == 200
    assert p.ctx.state.tracked_paths["27B"] == fake.slot_dir
    assert f"slot:save:27B:{eid()}.bin" in fake.summary()
    assert p.ctx.ledger.get(eid())["bin_path"] == fake.bin_path(eid())
    assert "setup check 1" in p.err_text and "setup check 6" not in p.err_text


async def test_ninfer_style_upstream_without_config_skips_saves_and_warns(proxy_factory, fake):
    p = await proxy_factory()
    fake.models_override = NINFER_MODELS
    r = await p.chat(chat_id="chatA")
    assert r.status == 200
    assert p.ctx.state.tracked_paths["27B"] is None
    assert not any(s.startswith("slot:save") for s in fake.summary())
    assert "setup check 6" in p.err_text


async def test_discovered_path_wins_over_conflicting_config(proxy_factory, fake):
    p = await proxy_factory(slot_save_path=fake.slot_dir + "-conflict")
    r = await p.chat(chat_id="chatA")   # fake exposes its own --slot-save-path
    assert r.status == 200
    assert p.ctx.state.tracked_paths["27B"] == fake.slot_dir
    assert "differs from discovered" in p.err_text
    assert p.ctx.ledger.get(eid())["bin_path"] == fake.bin_path(eid())


async def test_models_fetch_failure_with_config_still_saves(proxy_factory, fake):
    p = await proxy_factory(slot_save_path=fake.slot_dir)
    fake.models_fail = True
    r = await p.chat(chat_id="chatA")
    assert r.status == 200
    assert "could not fetch" in p.err_text
    assert p.ctx.state.tracked_paths["27B"] == fake.slot_dir
    assert f"slot:save:27B:{eid()}.bin" in fake.summary()


async def test_ninfer_style_restart_restores_from_disk(proxy_factory, fake, tmp_path):
    ledger_path = str(tmp_path / "ledger.json")
    p1 = await proxy_factory(ledger_path=ledger_path, slot_save_path=fake.slot_dir)
    fake.models_override = NINFER_MODELS
    await p1.chat(chat_id="chatA")
    assert os.path.exists(fake.bin_path(eid()))
    await p1.stop()
    fake.calls.clear()

    p2 = await proxy_factory(ledger_path=ledger_path, slot_save_path=fake.slot_dir)
    fake.models_override = NINFER_MODELS
    await p2.chat(chat_id="chatA")
    assert fake.summary()[0:2] == ["models", f"slot:restore:27B:{eid()}.bin"]
