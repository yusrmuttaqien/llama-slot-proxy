"""Upstream (llama-server router) client: session, URL building, slot calls."""
import asyncio
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional
from urllib.parse import urlencode

import aiohttp

SLOT_ID = 0  # single slot, hard-coded by design


class SlotKind(Enum):
    OK = "ok"
    HTTP_ERROR = "http_error"
    TIMEOUT = "timeout"
    CONN_ERROR = "conn_error"


@dataclass
class SlotResult:
    kind: SlotKind
    status: Optional[int] = None
    message: str = ""
    data: Any = None

    @property
    def ok(self):
        return self.kind is SlotKind.OK


class UpstreamClient:
    def __init__(self, base_url, save_timeout):
        self.base_url = base_url.rstrip("/")
        self.save_timeout = save_timeout
        self.session = None

    async def start(self):
        # No total timeout: streams can run for a long time. auto_decompress off keeps
        # proxied bytes transparent.
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None), auto_decompress=False)

    async def close(self):
        if self.session is not None:
            await self.session.close()
            self.session = None

    def url(self, path_qs):
        return self.base_url + path_qs

    async def get_json(self, path_qs):
        """Internal GET bounded by save_timeout. Returns parsed JSON; raises on failure."""
        timeout = aiohttp.ClientTimeout(total=self.save_timeout or None)
        async with self.session.get(self.url(path_qs), timeout=timeout) as r:
            body = await r.read()
            if r.status >= 400:
                raise RuntimeError(f"GET {path_qs} returned HTTP {r.status}")
            return json.loads(body)

    async def _slot_action(self, action, model, filename):
        query = urlencode({"action": action, "model": model})
        url = self.url(f"/slots/{SLOT_ID}?{query}")
        payload = {"model": model, "filename": filename}
        timeout = aiohttp.ClientTimeout(total=self.save_timeout or None)
        try:
            async with self.session.post(url, json=payload, timeout=timeout) as r:
                raw = await r.read()
                if r.status >= 400:
                    return SlotResult(SlotKind.HTTP_ERROR, r.status, _error_message(raw))
                try:
                    data = json.loads(raw)
                except ValueError:
                    data = None
                return SlotResult(SlotKind.OK, r.status, data=data)
        except asyncio.TimeoutError:
            return SlotResult(SlotKind.TIMEOUT, message=f"no response within {self.save_timeout}s")
        except aiohttp.ClientError as e:
            return SlotResult(SlotKind.CONN_ERROR, message=str(e))

    async def slot_save(self, model, filename):
        return await self._slot_action("save", model, filename)

    async def slot_restore(self, model, filename):
        return await self._slot_action("restore", model, filename)


def _error_message(raw):
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            err = data.get("error")
            if isinstance(err, dict) and err.get("message"):
                return str(err["message"])
    except ValueError:
        pass
    return raw[:200].decode("utf-8", "replace")
