import asyncio
import io
import os

import aiohttp
import pytest
import pytest_asyncio

from src.app import start_server
from src.classify import make_entry_id
from src.config import DEFAULTS
from src.context import build_context
from src.ledger import Ledger
from src.log import Logger
from tests.fake_upstream import FakeUpstream


class Proxy:
    def __init__(self, ctx, runner, url, session, out, err):
        self.ctx, self.runner, self.url, self.session = ctx, runner, url, session
        self.out, self.err = out, err

    @property
    def log_text(self):
        return self.out.getvalue()

    @property
    def err_text(self):
        return self.err.getvalue()

    async def chat(self, chat_id="chatA", model="27B", stream=False, headers=None, settle=True, **extra):
        """POST a chat completion. By default also read the whole body and wait until the proxy
        queue is idle, i.e. until post-response work (saves) has finished."""
        h = {} if chat_id is None else {"x-session-affinity": chat_id}
        h.update(headers or {})
        body = {"model": model, "messages": [{"role": "user", "content": "hi"}], "stream": stream}
        body.update(extra)
        r = await self.session.post(self.url + "/v1/chat/completions", json=body, headers=h)
        if settle:
            await r.read()
            await asyncio.wait_for(self.ctx.queue.wait_idle(), 10)
        return r

    async def stop(self):
        await self.session.close()
        await self.runner.cleanup()


@pytest_asyncio.fixture
async def fake(tmp_path):
    f = FakeUpstream(tmp_path / "slots")
    await f.start()
    yield f
    await f.stop()


@pytest_asyncio.fixture
async def proxy_factory(tmp_path, fake):
    made = []

    async def make(ledger_path=None, upstream_url=None, **overrides):
        cfg = dict(DEFAULTS)
        cfg.update(upstream_url=upstream_url or fake.url, save_timeout=2.0)
        cfg.update(overrides)
        out, err = io.StringIO(), io.StringIO()
        log = Logger(verbose=True, out=out, err=err)
        ledger = Ledger(ledger_path or str(tmp_path / "ledger.json"), log)
        ledger.load()
        ctx = build_context(cfg, log, ledger)
        runner, site = await start_server(ctx, "127.0.0.1", 0)
        port = site._server.sockets[0].getsockname()[1]
        p = Proxy(ctx, runner, f"http://127.0.0.1:{port}", aiohttp.ClientSession(), out, err)
        made.append(p)
        return p

    yield make
    for p in made:
        try:
            await p.stop()
        except Exception:
            pass


@pytest_asyncio.fixture
async def proxy(proxy_factory):
    return await proxy_factory()


def eid(chat="chatA", model="27B"):
    return make_entry_id(chat, model)
