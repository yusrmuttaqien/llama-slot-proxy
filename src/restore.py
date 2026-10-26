"""Lazy restore (spec 6.1)."""
import os
from enum import Enum

from src.upstream import SlotKind


class Outcome(Enum):
    CONTINUE = "continue"
    UPSTREAM_DOWN = "upstream_down"


async def maybe_restore(ctx, c):
    """For a TRACKED request: restore the slot from the ledger when appropriate.

    A continuation (the same conversation as the previous request) is already in the
    active slot, so it is never restored. A switch to a different conversation that has
    a saved state is always restored, even if that conversation was seen before: the
    upstream engine may have evicted it in the meantime, and the on-disk snapshot is
    then the only remaining copy. (Spec 6.1, amended for NInfer's dynamic retention —
    the engine, not the proxy, decides what stays resident, so a proxy-side "seen"
    set cannot be trusted to mean "still in the slot".)
    """
    st, log = ctx.state, ctx.log
    if c.entry_id == st.active_chat:
        return Outcome.CONTINUE              # continuation: already in the active slot
    entry = ctx.ledger.get(c.entry_id)
    if entry is None:
        st.loaded_chats.add(c.entry_id)       # new unique request: degrade to prefill
        return Outcome.CONTINUE

    filename = os.path.basename(entry["bin_path"])
    log.info(f"restore: entry {c.entry_id} from {filename}")
    res = await ctx.upstream.slot_restore(c.model_id, filename)
    if res.ok:
        ctx.ledger.mark_restored(c.entry_id)
        st.loaded_chats.add(c.entry_id)
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
