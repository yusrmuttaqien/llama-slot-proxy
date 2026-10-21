"""Observe a response (without altering it) to extract delta = prompt_n + predicted_n."""
import json

_MAX_BUFFER = 32 * 1024 * 1024


def delta_from_timings(timings):
    if not isinstance(timings, dict):
        return None
    vals = [timings.get(k) for k in ("prompt_n", "predicted_n")]
    vals = [v for v in vals if isinstance(v, (int, float)) and not isinstance(v, bool)]
    return int(sum(vals)) if vals else None


class DeltaTap:
    def __init__(self):
        self._sse = False
        self._buf = bytearray()
        self._overflow = False
        self._timings = None

    def configure(self, content_type):
        self._sse = "text/event-stream" in (content_type or "").lower()

    def feed(self, chunk):
        if self._overflow:
            return
        self._buf += chunk
        if self._sse:
            self._drain_lines(final=False)
        elif len(self._buf) > _MAX_BUFFER:
            self._overflow = True
            self._buf = bytearray()

    def _drain_lines(self, final):
        while True:
            idx = self._buf.find(b"\n")
            if idx < 0:
                break
            line = bytes(self._buf[:idx])
            del self._buf[:idx + 1]
            self._line(line)
        if final and self._buf:
            self._line(bytes(self._buf))
            self._buf = bytearray()
        if len(self._buf) > _MAX_BUFFER:
            self._buf = bytearray()

    def _line(self, line):
        line = line.strip()
        if not line.startswith(b"data:"):
            return
        payload = line[5:].strip()
        if not payload or payload == b"[DONE]":
            return
        try:
            obj = json.loads(payload)
        except ValueError:
            return
        if isinstance(obj, dict) and isinstance(obj.get("timings"), dict):
            self._timings = obj["timings"]

    def delta(self):
        if self._sse:
            self._drain_lines(final=True)
        elif not self._overflow and self._buf:
            try:
                obj = json.loads(bytes(self._buf))
                if isinstance(obj, dict):
                    self._timings = obj.get("timings")
            except ValueError:
                pass
        return delta_from_timings(self._timings)
