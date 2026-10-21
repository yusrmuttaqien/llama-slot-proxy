"""Disk-limit eviction (spec 7.2). Runs before a save, inside the queue."""
import os
from datetime import datetime, timezone

from src.ledger import parse_date

_OLDEST = datetime.min.replace(tzinfo=timezone.utc)


def _sort_key(entry):
    d = entry.get("dates", {})
    saved = parse_date(d.get("saved")) or _OLDEST
    restored = parse_date(d.get("restored")) or _OLDEST
    created = parse_date(d.get("created")) or _OLDEST
    return (max(saved, restored), saved, created)


def enforce_limit(ledger, cfg, log, protect_entry_id=None):
    """Delete oldest entries+bins when over the limit. Returns evicted entry ids."""
    max_bytes = cfg["bin_max_total_gb"] * 1024 ** 3
    total = ledger.total_bin_bytes()
    if total <= max_bytes:
        return []
    target = max_bytes * (1 - cfg["bin_padding_percent"] / 100.0)
    evicted = []
    candidates = sorted((e for e in ledger.entries() if e["entry_id"] != protect_entry_id),
                        key=_sort_key)
    for e in candidates:
        if total <= target:
            break
        try:
            size = os.path.getsize(e["bin_path"])
        except OSError:
            size = 0
        try:
            os.unlink(e["bin_path"])
        except FileNotFoundError:
            pass
        except OSError as err:
            log.error(f"evict: could not delete {e['bin_path']}: {err}")
            continue
        ledger.remove(e["entry_id"])
        total -= size
        evicted.append(e["entry_id"])
        log.info(f"evict: removed entry {e['entry_id']} ({size} bytes)")
    return evicted
