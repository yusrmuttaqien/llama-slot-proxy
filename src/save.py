"""Lazy save procedure (spec 6.3.2). Always runs inside the queue."""
import os

from src import evict, slotpath
from src.upstream import SlotKind


def _same_dir(a, b):
    return os.path.normpath(a) == os.path.normpath(b)


def _safe_remove(path, log):
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    except OSError as e:
        log.error(f"could not delete old bin {path}: {e}")


async def save_entry(ctx, entry_id, chat_id, model_id, reason):
    """Save the slot as <entry id>.bin and update the ledger. Never raises; returns success."""
    try:
        return await _save(ctx, entry_id, chat_id, model_id, reason)
    except Exception as e:
        ctx.log.error(f"save of entry {entry_id} failed unexpectedly: {e!r}")
        return False


async def _save(ctx, entry_id, chat_id, model_id, reason):
    log = ctx.log
    log.info(f"save ({reason}): entry {entry_id}, model {model_id}")

    # 1. disk limit
    evict.enforce_limit(ctx.ledger, ctx.config, log, protect_entry_id=entry_id)

    # 2. resolve the save directory
    entry = ctx.ledger.get(entry_id)
    tracked = ctx.state.tracked_paths.get(model_id)
    if tracked is None:
        tracked = await slotpath.ensure_path(ctx, model_id)
    if tracked is not None:
        target_dir = tracked
    elif entry is not None:
        target_dir = os.path.dirname(entry["bin_path"])
    else:
        log.error(f"save of entry {entry_id} skipped: slot-save-path of model {model_id} is unknown")
        return False

    # 3. path comparison (ledger path vs tracked path)
    old_path = None
    if entry is not None and tracked is not None and not _same_dir(os.path.dirname(entry["bin_path"]), tracked):
        old_path = entry["bin_path"]
        log.info(f"slot-save-path changed for model {model_id}; moving bin to {tracked}")

    # 4. save call
    filename = f"{entry_id}.bin"
    res = await ctx.upstream.slot_save(model_id, filename)
    if not res.ok:
        log.error(f"slot save for entry {entry_id} failed: {res.kind.value} {res.status or ''} {res.message}".rstrip())
        if res.kind is SlotKind.CONN_ERROR:
            ctx.state.reset()
        return False

    # 5. ledger (old bin is removed only after the new one is safely written)
    ctx.ledger.upsert_saved(entry_id, chat_id, model_id, os.path.join(target_dir, filename))
    if old_path is not None:
        _safe_remove(old_path, log)
    return True
