"""A fake llama-server (router mode) that records every call it receives."""
import asyncio
import json
import os

from aiohttp import web


class FakeUpstream:
    def __init__(self, slot_dir):
        self.slot_dir = str(slot_dir)
        os.makedirs(self.slot_dir, exist_ok=True)
        self.calls = []
        self.model = "27B"
        self.other_models = ["9B"]
        self.slot_mode = {"save": "ok", "restore": "ok"}   # ok | 400 | 501 | hang
        self.chat_delay = 0.0
        self.chunk_delay = 0.0
        self.stream_chunks = 3
        self.timings = {"prompt_n": 10, "predicted_n": 5}
        self.save_bytes = 100
        self.models_override = None     # full JSON body for /v1/models
        self.models_fail = False
        self.args = None                # override status.args
        self.chat_status = 200
        self.chat_aborted = 0
        self.runner = None
        self.url = None

    # ---- lifecycle ---------------------------------------------------
    async def start(self):
        app = web.Application()
        app.router.add_get("/v1/models", self.models_h)
        app.router.add_get("/models", self.models_h)
        app.router.add_get("/health", self.generic)
        app.router.add_get("/models/sse", self.sse)
        app.router.add_post("/slots/{id}", self.slots)
        app.router.add_post("/v1/chat/completions", self.chat)
        app.router.add_post("/models/unload", self.unload)
        app.router.add_route("*", "/{tail:.*}", self.generic)
        self.runner = web.AppRunner(app, handler_cancellation=True)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()

    # ---- helpers -----------------------------------------------------
    async def _record(self, request):
        raw = await request.read()
        try:
            body = json.loads(raw) if raw else None
        except ValueError:
            body = raw
        call = {"method": request.method, "path": request.path, "query": dict(request.query),
                "body": body, "headers": dict(request.headers),
                "t0": asyncio.get_running_loop().time(), "t1": None}
        self.calls.append(call)
        return call

    def summary(self):
        out = []
        for c in self.calls:
            if c["path"].startswith("/slots/"):
                out.append(f"slot:{c['query'].get('action')}:{c['query'].get('model')}:{c['body'].get('filename')}")
            elif c["path"] == "/v1/chat/completions":
                out.append(f"chat:{c['body'].get('model')}")
            elif c["path"] in ("/v1/models", "/models"):
                out.append("models")
            elif c["path"] == "/models/unload":
                out.append(f"unload:{c['body'].get('model')}")
            else:
                out.append(f"{c['method']} {c['path']}")
        return out

    def bin_path(self, entry_id):
        return os.path.join(self.slot_dir, f"{entry_id}.bin")

    # ---- handlers ----------------------------------------------------
    async def models_h(self, request):
        await self._record(request)
        if self.models_fail:
            return web.json_response({"error": {"message": "boom"}}, status=500)
        if self.models_override is not None:
            return web.json_response(self.models_override)
        args = self.args if self.args is not None else ["--slot-save-path", self.slot_dir, "-np", "1"]
        data = [{"id": self.model, "aliases": [], "object": "model",
                 "status": {"value": "loaded", "args": args}}]
        for other in self.other_models:      # known but not loaded
            data.append({"id": other, "aliases": [], "object": "model",
                         "status": {"value": "unloaded", "args": args}})
        return web.json_response({"object": "list", "data": data})

    async def generic(self, request):
        await self._record(request)
        return web.json_response({"ok": True, "path": request.path})

    async def sse(self, request):
        await self._record(request)
        resp = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await resp.prepare(request)
        await resp.write(b"data: {}\n\n")
        await asyncio.sleep(30)
        return resp

    async def unload(self, request):
        await self._record(request)
        return web.json_response({"success": True})

    async def slots(self, request):
        call = await self._record(request)
        action = request.query.get("action")
        mode = self.slot_mode.get(action, "ok")
        if mode == "hang":
            await asyncio.sleep(30)
        if mode in ("400", "501"):
            return web.json_response(
                {"error": {"code": int(mode), "message": f"fake {action} failure", "type": "invalid_request_error"}},
                status=int(mode))
        filename = call["body"].get("filename")
        path = os.path.join(self.slot_dir, filename)
        if action == "save":
            with open(path, "wb") as f:
                f.write(b"x" * self.save_bytes)
            return web.json_response({"id_slot": 0, "filename": filename, "n_saved": 1, "n_written": self.save_bytes,
                                      "timings": {"save_ms": 1.0}})
        if action == "restore":
            if not os.path.exists(path):
                return web.json_response({"error": {"code": 400, "message": "file not found",
                                                    "type": "invalid_request_error"}}, status=400)
            return web.json_response({"id_slot": 0, "filename": filename, "n_restored": 1, "n_read": 1,
                                      "timings": {"restore_ms": 1.0}})
        return web.json_response({"id_slot": 0, "n_erased": 1})

    async def chat(self, request):
        call = await self._record(request)
        payload = call["body"] if isinstance(call["body"], dict) else {}
        loop = asyncio.get_running_loop()
        try:
            if self.chat_delay:
                await asyncio.sleep(self.chat_delay)
            if self.chat_status != 200:
                return web.json_response({"error": {"message": "fake chat error"}}, status=self.chat_status)
            if payload.get("stream"):
                resp = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
                await resp.prepare(request)
                for i in range(self.stream_chunks):
                    chunk = {"choices": [{"delta": {"content": f"t{i}"}}]}
                    await resp.write(f"data: {json.dumps(chunk)}\n\n".encode())
                    if self.chunk_delay:
                        await asyncio.sleep(self.chunk_delay)
                final = {"choices": [], "usage": {"completion_tokens": 5}, "timings": self.timings}
                await resp.write(f"data: {json.dumps(final)}\n\n".encode())
                await resp.write(b"data: [DONE]\n\n")
                await resp.write_eof()
                return resp
            return web.json_response({"choices": [{"message": {"content": "hi"}}],
                                      "usage": {"completion_tokens": 5}, "timings": self.timings})
        except asyncio.CancelledError:
            self.chat_aborted += 1
            raise
        finally:
            call["t1"] = loop.time()
