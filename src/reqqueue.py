"""FIFO request queue: one in-flight request, optional wait timeout and waiting limit."""
import asyncio
from collections import deque


class QueueFull(Exception):
    pass


class QueueTimeout(Exception):
    pass


class QueueClosed(Exception):
    pass


class RequestQueue:
    def __init__(self, timeout=0.0, limit=0):
        self.timeout = timeout      # seconds waiting only; 0 = indefinite
        self.limit = limit          # waiting requests only; 0 = unlimited
        self._busy = False
        self._waiters = deque()
        self._closed = False
        self._idle = asyncio.Event()
        self._idle.set()

    @property
    def waiting(self):
        return len(self._waiters)

    @property
    def busy(self):
        return self._busy

    async def acquire(self):
        if self._closed:
            raise QueueClosed()
        if not self._busy and not self._waiters:
            self._busy = True
            self._idle.clear()
            return
        if self.limit > 0 and len(self._waiters) >= self.limit:
            raise QueueFull()
        fut = asyncio.get_running_loop().create_future()
        self._waiters.append(fut)
        try:
            if self.timeout > 0:
                await asyncio.wait_for(fut, self.timeout)
            else:
                await fut
        except asyncio.TimeoutError:
            self._discard(fut)
            raise QueueTimeout()
        except asyncio.CancelledError:
            granted = fut.done() and not fut.cancelled() and fut.exception() is None
            self._discard(fut)
            if granted:
                self.release()  # we were handed the slot but are leaving: pass it on
            raise
        except QueueClosed:
            self._discard(fut)
            raise

    def _discard(self, fut):
        try:
            self._waiters.remove(fut)
        except ValueError:
            pass

    def release(self):
        while self._waiters:
            fut = self._waiters.popleft()
            if not fut.done():
                fut.set_result(None)   # slot stays busy, ownership moves on
                return
        self._busy = False
        self._idle.set()

    def close(self):
        """Stop accepting; reject everyone currently waiting."""
        self._closed = True
        while self._waiters:
            fut = self._waiters.popleft()
            if not fut.done():
                fut.set_exception(QueueClosed())

    async def wait_idle(self):
        await self._idle.wait()
