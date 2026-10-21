"""Lazy restore (spec 6.1)."""
import os
from enum import Enum

from src.upstream import SlotKind


class Outcome(Enum):
    CONTINUE = "continue"
    UPSTREAM_DOWN = "upstream_down"


async def maybe_restore(ctx, c):
    """For a TRACKED request: restore the slot from the ledger when appropriate."""
    st, log = ctx.state, ctx.log
    if c.entry_id in st.loaded_chats:
        return Outcome.CONTINUE
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
