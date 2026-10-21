"""End-to-end scenarios, including real main.py subprocesses (startup, flags, signals)."""
import asyncio
import json
import os
import signal
import socket
import sys
import time

import aiohttp
import pytest

from src.ledger import Ledger
from tests.conftest import eid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ------------------------------------------------------- in-process scenarios
async def test_restart_restores_a_saved_chat_before_its_chat_call(proxy_factory, fake, tmp_path):
    ledger_path = str(tmp_path / "ledger.json")
    p1 = await proxy_factory(ledger_path=ledger_path)
    await p1.chat(chat_id="chatA")
    assert os.path.exists(fake.bin_path(eid()))
    await p1.stop()
    fake.calls.clear()

    p2 = await proxy_factory(ledger_path=ledger_path)           # "restart": new state, same ledger
    await p2.chat(chat_id="chatA")
    s = fake.summary()
    assert s[0] == "models"
    assert s[1:3] == [f"slot:restore:27B:{eid()}.bin", "chat:27B"]


async def test_a_b_a_in_one_run_does_not_restore_the_second_a(proxy, fake):
    await proxy.chat(chat_id="A")
    await proxy.chat(chat_id="B")
    await proxy.chat(chat_id="A")
    assert not any(s.startswith("slot:restore") for s in fake.summary())
    assert proxy.ctx.state.active_chat == eid("A")


async def test_model_switch_then_unload_then_back(proxy_factory, fake):
    p = await proxy_factory(save_time_threshold=10 ** 6, delta_context_threshold=10 ** 6)
    await p.chat(chat_id="A", model="27B")
    await p.chat(chat_id="B", model="9B")                       # saves A (27B) first
    await p.session.post(p.url + "/models/unload", json={"model": "9B"})   # saves B first
    await p.chat(chat_id="A", model="27B")                      # model changed from "" -> restore A
    s = fake.summary()
    assert f"slot:save:27B:{eid('A', '27B')}.bin" in s
    assert f"slot:save:9B:{eid('B', '9B')}.bin" in s
    assert s[-2:] == [f"slot:restore:27B:{eid('A', '27B')}.bin", "chat:27B"]


async def test_upstream_down_then_up_recovers(proxy_factory, fake):
    p = await proxy_factory()
    await p.chat()
    await fake.stop()
    assert (await p.chat(chat_id="B")).status == 502
    await fake.start()
    p.ctx.upstream.base_url = fake.url
    r = await p.chat(chat_id="B")
    assert r.status == 200


# ------------------------------------------------------- real subprocesses
def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _start_proxy(fake, tmp_path, ledger_path=None, extra=()):
    port = _free_port()
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps({"upstream_url": fake.url, "listen_port": port, "save_timeout": 5,
                               "save_time_threshold": 10 ** 6, "delta_context_threshold": 10 ** 6}))
    ledger_path = ledger_path or str(tmp_path / "ledger.json")
    proc = await asyncio.create_subprocess_exec(
        sys.executable, os.path.join(ROOT, "main.py"), "--config-file", str(cfg),
        "--ledger-file", ledger_path, "--verbose", *extra,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=ROOT)
    for _ in range(100):
        line = await asyncio.wait_for(proc.stdout.readline(), 10)
        if b"listening" in line or not line:
            break
    return proc, f"http://127.0.0.1:{port}", ledger_path


def _chat(url, chat="chatA", stream=False):
    return aiohttp.ClientSession().post(
        url + "/v1/chat/completions", json={"model": "27B", "stream": stream, "messages": []},
        headers={"x-session-affinity": chat})


async def test_startup_routine_runs_before_serving(fake, tmp_path):
    ledger_path = str(tmp_path / "ledger.json")
    led = Ledger(ledger_path); led.load()
    good = tmp_path / "slots" / ("a" * 32 + ".bin"); good.write_bytes(b"data")
    led.upsert_saved("a" * 32, "c", "27B", str(good))
    led.upsert_saved("b" * 32, "c", "27B", str(tmp_path / "slots" / ("b" * 32 + ".bin")))   # bin deleted by hand
    orphan = tmp_path / "slots" / ("c" * 32 + ".bin"); orphan.write_bytes(b"orphan")
    proc, url, _ = await _start_proxy(fake, tmp_path, ledger_path)
    try:
        led2 = Ledger(ledger_path); led2.load()
        assert [e["entry_id"] for e in led2.entries()] == ["a" * 32]
        assert good.exists() and not orphan.exists()
        assert fake.calls == []                                     # startup never touched the server
    finally:
        proc.send_signal(signal.SIGTERM)
        await proc.wait()


async def test_sigterm_when_idle_saves_active_chat_and_exits_cleanly(fake, tmp_path):
    proc, url, ledger_path = await _start_proxy(fake, tmp_path)
    async with _chat(url) as r:
        assert r.status == 200
        await r.read()
    await asyncio.sleep(0.3)
    fake.calls.clear()
    proc.send_signal(signal.SIGTERM)
    assert await asyncio.wait_for(proc.wait(), 10) == 0
    assert fake.summary() == [f"slot:save:27B:{eid()}.bin"]
    led = Ledger(ledger_path); led.load()
    assert led.get(eid()) is not None


async def test_sigterm_during_a_stream_lets_it_finish_then_saves(fake, tmp_path):
    fake.chunk_delay = 0.3
    fake.stream_chunks = 5
    proc, url, _ = await _start_proxy(fake, tmp_path)
    session = aiohttp.ClientSession()
    r = await session.post(url + "/v1/chat/completions", json={"model": "27B", "stream": True},
                           headers={"x-session-affinity": "chatS"})
    await r.content.readany()
    proc.send_signal(signal.SIGTERM)
    body = await r.read()
    await session.close()
    assert b"[DONE]" in body                                        # stream was not cut off
    assert await asyncio.wait_for(proc.wait(), 10) == 0
    assert fake.summary()[-1] == f"slot:save:27B:{eid('chatS')}.bin"


async def test_second_signal_forces_immediate_exit(fake, tmp_path):
    fake.chat_delay = 30
    proc, url, _ = await _start_proxy(fake, tmp_path)
    session = aiohttp.ClientSession()
    task = asyncio.create_task(session.post(url + "/v1/chat/completions", json={"model": "27B"},
                                            headers={"x-session-affinity": "chatS"}))
    await asyncio.sleep(0.5)
    proc.send_signal(signal.SIGTERM)
    await asyncio.sleep(0.3)
    assert proc.returncode is None                                  # still waiting for in-flight
    t = time.monotonic()
    proc.send_signal(signal.SIGTERM)
    await asyncio.wait_for(proc.wait(), 5)
    assert time.monotonic() - t < 3 and proc.returncode != 0
    task.cancel()
    await session.close()


async def test_generate_config_flag_runs_as_a_script(tmp_path):
    path = tmp_path / "cfg.json"
    proc = await asyncio.create_subprocess_exec(
        sys.executable, os.path.join(ROOT, "main.py"), "--generate-config", "--config-file", str(path),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=ROOT)
    await proc.wait()
    assert proc.returncode == 0 and "$docs" in json.loads(path.read_text())
