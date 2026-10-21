"""Startup routine (spec 7.1): reconcile ledger and bin files using ledger paths only."""
import os
import re

BIN_RE = re.compile(r"^[0-9a-f]{32}\.bin$")


def _remove_file(path, log):
    try:
        os.unlink(path)
        return True
    except FileNotFoundError:
        return False
    except OSError as e:
        log.error(f"could not delete {path}: {e}")
        return False


def run_startup_checks(ledger, log):
    entries = ledger.entries()
    known_dirs = {os.path.dirname(e["bin_path"]) for e in entries}

    # 1 + 3: entries whose bin is missing or corrupt (zero bytes)
    for e in entries:
        path = e["bin_path"]
        try:
            size = os.path.getsize(path)
        except OSError:
            log.info(f"startup: bin missing for entry {e['entry_id']}; deleting entry")
            ledger.remove(e["entry_id"])
            continue
        if size == 0:
            log.info(f"startup: bin is zero bytes for entry {e['entry_id']}; deleting bin and entry")
            _remove_file(path, log)
            ledger.remove(e["entry_id"])

    # 2: bins that no entry mentions (only files named <32 hex>.bin are ever touched)
    referenced = {os.path.abspath(e["bin_path"]) for e in ledger.entries()}
    for d in sorted(known_dirs):
        try:
            names = os.listdir(d)
        except OSError:
            continue
        for name in names:
            full = os.path.abspath(os.path.join(d, name))
            if BIN_RE.match(name) and full not in referenced and os.path.isfile(full):
                log.info(f"startup: orphan bin {full}; deleting")
                _remove_file(full, log)
