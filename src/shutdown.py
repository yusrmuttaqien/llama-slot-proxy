"""Graceful shutdown (spec 6.5)."""
import asyncio
import os
import signal

from src import save


async def graceful_shutdown(ctx):
    """Stop taking requests, let the in-flight one finish, save the active chat."""
    st, log = ctx.state, ctx.log
    ctx.shutting_down = True
    ctx.queue.close()                  # waiting requests get 503
    log.info("shutdown: waiting for the in-flight request (send the signal again to force exit)")
    await ctx.queue.wait_idle()
    if st.active_chat:
        await save.save_entry(ctx, st.active_chat, st.active_chat_id, st.active_model, "shutdown")


class SignalHandler:
    """First SIGINT/SIGTERM requests graceful shutdown; a second one exits immediately."""

    def __init__(self, loop, event, log, force_exit=None):
        self._event = event
        self._log = log
        self._force = force_exit or (lambda: os._exit(130))
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._on_signal, sig)
            except NotImplementedError:   # e.g. Windows
                signal.signal(sig, lambda s, f: loop.call_soon_threadsafe(self._on_signal, s))

    def _on_signal(self, sig):
        if self._event.is_set():
            self._log.error("second signal received; exiting immediately")
            self._force()
            return
        self._log.info(f"signal {int(sig)} received; shutting down gracefully")
        self._event.set()
