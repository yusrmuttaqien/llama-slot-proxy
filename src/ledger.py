"""Persistent ledger of saved chats (ledger.json, top-level array, atomic writes)."""
import json
import os
import tempfile
from datetime import datetime, timezone

from src.config import BASE_DIR

DEFAULT_LEDGER_PATH = os.path.join(BASE_DIR, "ledger.json")
_REQUIRED = ("entry_id", "chat_id", "model_id", "dates", "bin_path")


def utcnow():
    return datetime.now(timezone.utc)


def parse_date(value):
    """ISO string -> aware datetime, or None."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _valid(entries):
    if not isinstance(entries, list):
        return False
    for e in entries:
        if not isinstance(e, dict) or any(k not in e for k in _REQUIRED):
            return False
        if not isinstance(e["dates"], dict):
            return False
    return True


class Ledger:
    def __init__(self, path, log=None):
        self.path = path
        self.log = log
        self._entries = []

    # ---- persistence -------------------------------------------------
    def load(self):
        reason = None
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if _valid(data):
                self._entries = data
                return
            reason = "invalid structure"
        except FileNotFoundError:
            reason = "missing"
        except (ValueError, OSError) as e:
            reason = f"corrupt ({e})"
        self._entries = []
        if self.log:
            self.log.warn(f"ledger {self.path} {reason}; created empty ledger")
        self._write()

    def _write(self):
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".ledger-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._entries, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    # ---- queries -----------------------------------------------------
    def entries(self):
        return list(self._entries)

    def get(self, entry_id):
        for e in self._entries:
            if e["entry_id"] == entry_id:
                return e
        return None

    def total_bin_bytes(self):
        total = 0
        for e in self._entries:
            try:
                total += os.path.getsize(e["bin_path"])
            except OSError:
                pass
        return total

    # ---- mutations (each persists atomically) ---------------------------
    def upsert_saved(self, entry_id, chat_id, model_id, bin_path):
        now = utcnow().isoformat()
        e = self.get(entry_id)
        if e is None:
            e = {"entry_id": entry_id, "chat_id": chat_id, "model_id": model_id,
                 "dates": {"created": now, "restored": None, "saved": now},
                 "bin_path": bin_path}
            self._entries.append(e)
        else:
            e["dates"]["saved"] = now
            e["bin_path"] = bin_path
        self._write()
        return e

    def mark_restored(self, entry_id):
        e = self.get(entry_id)
        if e is not None:
            e["dates"]["restored"] = utcnow().isoformat()
            self._write()

    def remove(self, entry_id):
        before = len(self._entries)
        self._entries = [e for e in self._entries if e["entry_id"] != entry_id]
        if len(self._entries) != before:
            self._write()
