"""Lazy restore (spec 6.1) plus slot-vacating for new conversations.

NInfer's /slots/{id} addresses the private continuation-catalog cells, not a fixed
active lane. A *new* conversation (no saved state) is published by the engine to the
first *vacant* catalog cell. If slot 0 still holds the previous conversation, the new
one lands in a different cell, and a later blind save to slot 0 would write the wrong
session's bytes to the new entry's file. So before forwarding a new conversation we
erase slot 0 (the previous conversation is already saved to disk, so nothing is lost)
to guarantee the engine publishes the new one into slot 0.
"""
import os
from enum import Enum

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
      in the meantime, and the on-disk snapshot is then the only remaining copy.
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
    # HTTP error: the server rejected this bin (missing/invalid/does not fit)
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
    """Erase the current occupant of slot 0 so a new conversation is published there.

    Uses the tracked active_digest as an atomic precondition (if_digest). Handles the
    defensive edge cases where the occupant is not what we believe:
      - 400 "no retained session"  -> slot already vacant, nothing to do.
      - 409 mismatch               -> occupant changed; erase it unconditionally.
      - 409 busy                   -> an active request holds the slot; best-effort continue.
    Returns True when we can proceed (slot vacant or best-effort), False when the
    upstream is unreachable. Sets st.last_erase_ok (for observability; the save step
    currently proceeds best-effort regardless, since the proxy cannot reliably detect
    which cell a new conversation landed in if the erase failed).
    """
    st = ctx.state
    if_digest = st.active_digest or None
    res = await ctx.upstream.slot_erase(st.active_model, if_digest=if_digest)
    if res.ok:
        st.active_digest = ""
        st.last_erase_ok = True
        log.info(f"erased previous occupant of slot 0 to make room for a new conversation")
        return True
    if res.kind is SlotKind.CONN_ERROR:
        log.error(f"erase failed, upstream unreachable: {res.message}")
        st.reset()
        st.last_erase_ok = False
        return False
    if res.kind is SlotKind.TIMEOUT:
        log.error(f"erase timed out ({res.message}); continuing best-effort")
        st.last_erase_ok = False
        return True
    # HTTP error path
    if res.status == 400:
        # Slot holds no retained session: already vacant. Nothing to do.
        st.active_digest = ""
        st.last_erase_ok = True
        log.info("slot 0 already vacant; no erase needed")
        return True
    if res.status == 409:
        # slot_busy or slot_session_mismatch.
        if "mismatch" in (res.message or "").lower():
            # The occupant is not the session we believe. Erase whatever it is.
            log.info(f"slot 0 digest mismatch ({res.message}); erasing actual occupant")
            res2 = await ctx.upstream.slot_erase(st.active_model)
            if res2.ok:
                st.active_digest = ""
                st.last_erase_ok = True
                return True
            if res2.kind is SlotKind.CONN_ERROR:
                log.error(f"second erase failed, upstream unreachable: {res2.message}")
                st.reset()
                st.last_erase_ok = False
                return False
            log.error(f"second erase failed ({res2.status or ''} {res2.message}); continuing best-effort")
            st.active_digest = ""
            st.last_erase_ok = False
            return True
        # slot_busy: an active request holds the slot. In the single-concurrency proxy
        # this should not happen; log and continue best-effort.
        log.error(f"erase rejected (slot busy: {res.message}); continuing best-effort")
        st.active_digest = ""
        st.last_erase_ok = False
        return True
    # Any other HTTP error: log and continue best-effort.
    log.error(f"erase rejected (HTTP {res.status}: {res.message}); continuing best-effort")
    st.active_digest = ""
    st.last_erase_ok = False
    return True
