"""Tests for the NInfer catalog simulation: cell assignment, eviction, switch-back
restore, and digest-based erase preconditions.

These tests use a fake upstream with multiple catalog cells (like NInfer's
--host-state-slots 8) to verify that the proxy's vacate-before-new-conversation
logic keeps every conversation in slot 0, and that digest tracking works.
"""
import asyncio
import io

import aiohttp
import pytest
import pytest_asyncio

from src.app import start_server
from src.classify import make_entry_id
from src.config import DEFAULTS
from src.context import build_context
from src.ledger import Ledger
from src.log import Logger
from tests.conftest import Proxy
from tests.fake_upstream import FakeUpstream


@pytest_asyncio.fixture
async def fake8(tmp_path):
    """Fake upstream with 8 catalog cells (like NInfer --host-state-slots 8)."""
    f = FakeUpstream(tmp_path / "slots", catalog_cells=8)
    await f.start()
    yield f
    await f.stop()


@pytest_asyncio.fixture
async def proxy8(tmp_path, fake8):
    cfg = dict(DEFAULTS)
    cfg.update(upstream_url=fake8.url, save_timeout=2.0)
    out, err = io.StringIO(), io.StringIO()
    log = Logger(verbose=True, out=out, err=err)
    ledger = Ledger(str(tmp_path / "ledger.json"), log)
    ledger.load()
    ctx = build_context(cfg, log, ledger)
    runner, site = await start_server(ctx, "127.0.0.1", 0)
    port = site._server.sockets[0].getsockname()[1]
    p = Proxy(ctx, runner, f"http://127.0.0.1:{port}", aiohttp.ClientSession(), out, err)
    yield p
    await p.stop()


def eid(chat, model="27B"):
    return make_entry_id(chat, model)


async def test_new_conversation_lands_in_slot0_with_multi_cell_catalog(proxy8, fake8):
    """With 8 cells, if the proxy didn't vacate, a second conversation would land in
    cell 1. The proxy's erase step ensures it lands in cell 0."""
    await proxy8.chat(chat_id="A")
    assert fake8.catalog.get(0, {}).get("entry_id") == eid("A")

    await proxy8.chat(chat_id="B")
    # B must be in cell 0, not cell 1 (the proxy erased A first).
    assert fake8.catalog.get(0, {}).get("entry_id") == eid("B")
    assert eid("A") not in fake8.catalog.values() and not any(
        v.get("entry_id") == eid("A") for v in fake8.catalog.values())
    # A was evicted (erased) and is not in any cell.
    assert not any(v.get("entry_id") == eid("A") for v in fake8.catalog.values())


async def test_evicted_conversation_is_restored_on_switch_back(proxy8, fake8):
    """A is evicted when B takes slot 0. Switching back to A restores it from disk."""
    await proxy8.chat(chat_id="A")
    await proxy8.chat(chat_id="B")
    # A is evicted; B is in cell 0.
    assert fake8.catalog.get(0, {}).get("entry_id") == eid("B")
    assert not any(v.get("entry_id") == eid("A") for v in fake8.catalog.values())

    # Switch back to A: the proxy restores A from disk.
    await proxy8.chat(chat_id="A")
    s = fake8.summary()
    # The restore should appear after B's save and before A's chat.
    assert any(x.startswith(f"slot:restore:27B:{eid('A')}.bin") for x in s)
    # A is back in cell 0; B is evicted.
    assert fake8.catalog.get(0, {}).get("entry_id") == eid("A")
    assert not any(v.get("entry_id") == eid("B") for v in fake8.catalog.values())


async def test_erase_uses_tracked_digest_as_precondition(proxy8, fake8):
    """The proxy records the post-save digest and passes it as if_digest on the next
    erase, so the erase only succeeds if the slot still holds that session."""
    await proxy8.chat(chat_id="A")
    # After saving A, the proxy records A's digest.
    digest_a = proxy8.ctx.state.active_digest
    assert digest_a, "proxy should have recorded a digest after saving A"

    # Now switch to B: the proxy erases slot 0 with if_digest=digest_a.
    await proxy8.chat(chat_id="B")
    # Find the erase call in the recorded calls and verify it carried the digest.
    erase_calls = [c for c in fake8.calls
                   if c["path"].startswith("/slots/") and c["query"].get("action") == "erase"]
    assert erase_calls, "expected an erase call"
    last_erase = erase_calls[-1]
    assert last_erase["body"].get("if_digest") == digest_a


async def test_erase_mismatch_falls_back_to_unconditional_erase(proxy8, fake8):
    """If the tracked digest is stale (mismatch), the proxy falls back to an
    unconditional erase so the new conversation can still be published to slot 0."""
    await proxy8.chat(chat_id="A")
    # Corrupt the tracked digest so the first erase will mismatch.
    proxy8.ctx.state.active_digest = "bogus-digest"

    await proxy8.chat(chat_id="B")
    # B should still land in cell 0 (the fallback erase worked).
    assert fake8.catalog.get(0, {}).get("entry_id") == eid("B")
    # There should be two erase calls: one with the bogus digest (409), one without.
    erase_calls = [c for c in fake8.calls
                   if c["path"].startswith("/slots/") and c["query"].get("action") == "erase"]
    assert len(erase_calls) == 2
    assert erase_calls[0]["body"].get("if_digest") == "bogus-digest"
    assert erase_calls[1]["body"].get("if_digest") is None


async def test_get_slots_returns_per_cell_state(proxy8, fake8):
    """GET /slots returns one entry per catalog cell with retained/digest info."""
    await proxy8.chat(chat_id="A")
    states = await proxy8.ctx.upstream.slot_states()
    assert len(states) == 8
    cell0 = next(s for s in states if s["id"] == 0)
    assert cell0["retained"] is True
    assert cell0["session_digest"] == proxy8.ctx.state.active_digest
    cell1 = next(s for s in states if s["id"] == 1)
    assert cell1["retained"] is False
    assert cell1["session_digest"] is None


async def test_capacity_error_on_restore_keeps_ledger_entry(proxy8, fake8):
    """If the vacate fails (so the restore still hits a capacity refusal), the proxy
    must keep the ledger entry (the snapshot is valid, just didn't fit this time) rather
    than deleting it. The distinguishing signal: a kept entry retains its original
    'created' date, whereas a dropped-and-recreated entry gets a fresh one. The current
    request falls back to a prefill; the saved state is preserved for a later retry."""
    await proxy8.chat(chat_id="A")
    await proxy8.chat(chat_id="B")
    # A is evicted; B is in cell 0. A's snapshot is on disk.
    assert fake8.catalog.get(0, {}).get("entry_id") == eid("B")
    entry_a = proxy8.ctx.ledger.get(eid("A"))
    assert entry_a is not None
    original_created = entry_a["dates"]["created"]

    # Make the erase fail, so the proxy cannot vacate B before restoring A.
    fake8.slot_mode["erase"] = "400"
    await proxy8.chat(chat_id="A")
    fake8.slot_mode["erase"] = "ok"

    # The restore hit a capacity refusal (cell 0 still holds B). The proxy must keep
    # A's ledger entry (not delete it), so the original 'created' date survives.
    entry_a_after = proxy8.ctx.ledger.get(eid("A"))
    assert entry_a_after is not None, "capacity error must keep the entry"
    assert entry_a_after["dates"]["created"] == original_created, "kept entry retains its created date"
    # The chat was still served (prefill fallback).
    assert any(s.startswith("chat:27B") for s in fake8.summary())
