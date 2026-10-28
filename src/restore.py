"""Lazy restore (spec 6.1) plus slot-vacating before any slot change.

NInfer's /slots/{id} addresses the private continuation-catalog cells, not a fixed
active lane. Two things require the current occupant of slot 0 to be cleared first:

1. A *new* conversation (no saved state) is published by the engine to the first
   *vacant* catalog cell. If slot 0 still holds the previous conversation, the new one
   lands in a different cell, and a later blind save to slot 0 would write the wrong
   session's bytes to the new entry's file.

2. A *restore* of a different conversation must fit in the *free* KV pages. NInfer
   refuses to restore a snapshot whose KV pages exceed the free pool ("session snapshot
   does not fit the free KV capacity; evict other sessions first"). It will not evict
   the current occupant for you, so the proxy must do it.

In both cases the current occupant is already saved to disk (the proxy saves after every
request), so erasing it loses nothing; it is restored from disk later if the user
switches back to it.
"""
import os
from enum import Enum

import aiohttp

from src.upstream import SlotKind


class Outcome(Enum):
    CONTINUE = "continue"
    UPSTREAM_DOWN = "upstream_down"


async def maybe_restore(ctx, c):
    """For a TRACKED request: restore the slot from the ledger when appropriate, or
    vacate the slot for a brand-new conversation.

    - A continuation (the same conversation as the previous request) is already in the
      active slot, so it is neither restored nor erased.
    - A switch to a different conversation that has a saved state is always restored,
      even if that conversation was seen before: the upstream engine may have evicted it
      in the meantime, and the on-disk snapshot is then the only remaining copy. Before
      restoring, the current occupant of slot 0 is erased so its KV pages are free for
      the snapshot (NInfer refuses a restore that does not fit the free pool).
    - A brand-new conversation (no saved state) cannot be restored. We instead erase the
      current occupant of slot 0 so the engine publishes the new conversation into slot 0
      (the first vacant cell), keeping the proxy's fixed slot-0 addressing correct.
    """
    st, log = ctx.state, ctx.log
    if c.entry_id == st.active_chat:
        return Outcome.CONTINUE              # continuation: already in the active slot
    entry = ctx.ledger.get(c.entry_id)
    if entry is None:
        st.loaded_chats.add(c.entry_id)       # new unique request
        # Only vacate if there is a previous conversation occupying the slot. For the very
        # first request (no active_chat) the slot is already vacant, so no erase is needed.
        if st.active_chat:
            if not await _vacate_slot(ctx, log):
                return Outcome.UPSTREAM_DOWN
        return Outcome.CONTINUE

    # Vacate the current occupant of slot 0 so its KV pages are free for the snapshot.
    # NInfer refuses a restore whose KV pages exceed the free pool, and it will not
    # evict the occupant for us. The occupant is already saved to disk, so nothing is
    # lost; it is restored from disk later if the user switches back to it.
    if st.active_chat and st.active_chat != c.entry_id:
        if not await _vacate_slot(ctx, log):
            return Outcome.UPSTREAM_DOWN
    filename = os.path.basename(entry["bin_path"])
    log.info(f"restore: entry {c.entry_id} from {filename}")
    res = await ctx.upstream.slot_restore(c.model_id, filename)
    if res.ok:
        ctx.ledger.mark_restored(c.entry_id)
        st.loaded_chats.add(c.entry_id)
        st.active_digest = res.digest        # the restored session now occupies the slot
        st.last_erase_ok = True             # restore placed the conversation in slot 0
        return Outcome.CONTINUE
    if res.kind is SlotKind.CONN_ERROR:
        log.error(f"restore failed, upstream unreachable: {res.message}")
        st.reset()
        return Outcome.UPSTREAM_DOWN
    if res.kind is SlotKind.TIMEOUT:
        log.error(f"restore of entry {c.entry_id} timed out ({res.message}); continuing with prefill")
        st.loaded_chats.add(c.entry_id)
        return Outcome.CONTINUE
    # HTTP error: the server rejected the restore. Distinguish a capacity problem
    # (snapshot is valid but did not fit the free pool this time) from a bad/missing
    # file. On a capacity problem we keep the entry so it can be retried later and
    # continue with a prefill for now; on a bad/missing file we drop the entry.
    msg = (res.message or "").lower()
    if "does not fit" in msg or "evict other sessions" in msg:
        log.error(f"restore of entry {c.entry_id} did not fit the free KV capacity "
                  f"(HTTP {res.status}: {res.message}); keeping entry and continuing with prefill")
        st.loaded_chats.add(c.entry_id)
        return Outcome.CONTINUE
    log.error(f"restore of entry {c.entry_id} rejected (HTTP {res.status}: {res.message}); "
              "dropping entry and continuing with prefill")
    try:
        os.unlink(entry["bin_path"])
    except OSError:
        pass
    ctx.ledger.remove(c.entry_id)
    st.loaded_chats.add(c.entry_id)
    return Outcome.CONTINUE


async def _vacate_slot(ctx, log):
    """Erase all occupied catalog cells so the KV pages of all resident sessions are
    freed for the snapshot, so the snapshot fits in the free pool.

    The capacity refusal is not (only) about the current occupant. It is about the
    TOTAL KV pages in use across all sessions. With --host-state-slots 8, you have up
    to 8 concurrent sessions, each holding its own KV pages in a catalog cell. When
    you switch conversations, the snapshot must fit in the *free* KV pages. If the
    snapshot is huge (~70% of the total context) and the other cells are still
    occupied, the free pool is too small, and NInfer refuses the restore.

    So before restoring, the proxy evicts ALL occupied cells (not just slot 0),
    freeing the KV pages of every resident session. The snapshot then fits.
    """
    st = ctx.state
    # Query GET /slots to discover which cells are occupied and their session digests.
    # This is strictly better than the old tracked-digest approach: the proxy now
    # always uses the *actual* digest from the live state, so there is no mismatch
    # scenario (the digest is never stale relative to what we're about to erase).
    # If the query fails (upstream unreachable), fall back to the tracked digest
    # so the proxy can still make progress.
    try:
        states = await ctx.upstream.slot_states()
    except (RuntimeError, aiohttp.ClientError) as e:
        log.error(f"GET /slots failed ({e}); falling back to tracked digest")
        # Fall back to the old behavior: erase slot 0 with the tracked digest.
        res = await ctx.upstream.slot_erase(st.active_model, if_digest=(st.active_digest or None))
        if res.ok:
            st.active_digest = ""
            st.last_erase_ok = True
            return True
        if res.kind is SlotKind.CONN_ERROR:
            log.error(f"erase failed, upstream unreachable: {res.message}")
            st.reset()
            st.last_erase_ok = False
            return False
        st.last_erase_ok = False
        return True
    # Identify all occupied cells (retained=True).
    occupied = [s for s in states if s.get("retained")]
    # Evict each occupied cell (call slot_erase on each, passing the session's
    # digest as the if_digest precondition).
    for s in occupied:
        cell_id = s["id"]
        digest = s.get("session_digest") or ""
        res = await ctx.upstream.slot_erase(
            st.active_model,
            if_digest=digest,
            cell_id=cell_id,
        )
        if res.ok:
            log.info(f"erased cell {cell_id} (digest={digest})")
            st.last_erase_ok = True
        else:
            # The erase was rejected. Handle the outcome.
            if res.kind is SlotKind.CONN_ERROR:
                log.error(f"erase of cell {cell_id} failed, upstream unreachable: {res.message}")
                st.reset()
                st.last_erase_ok = False
                return False
            if res.kind is SlotKind.TIMEOUT:
                log.error(f"erase of cell {cell_id} timed out ({res.message}); continuing best-effort")
                st.last_erase_ok = False
                return True
            # HTTP error path
            log.error(f"erase of cell {cell_id} rejected (HTTP {res.status}: {res.message}); continuing best-effort")
            st.last_erase_ok = False
            return True
    # If no occupied cells, the slot is already vacant.
    if not occupied:
        st.active_digest = ""
        st.last_erase_ok = True
        return True
    # If any occupied cell was erased, the slot is now vacant and the snapshot fits.
    st.last_erase_ok = True
    return True
