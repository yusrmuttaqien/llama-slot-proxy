"""A fake llama-server / NInfer (router mode) that records every call it receives.

Simulates NInfer's private continuation catalog:
  - The engine publishes a new conversation to the first *vacant* catalog cell.
  - Each cell holds one conversation, identified by an entry_id and a session digest.
  - A chat request updates the conversation's digest (new tokens).
  - save/restore/erase operate on a specific cell (the URL path /slots/{id}).
  - GET /slots returns per-cell state.

`catalog_cells` controls how many cells the catalog has. Default 1 keeps the fake
backward-compatible with tests that assume a single slot; set to 8 (or more) to test
the proxy's cell-assignment logic against a realistic NInfer catalog.
"""
import asyncio
import json
import os

from aiohttp import web


class FakeUpstream:
    def __init__(self, slot_dir, catalog_cells=1):
        self.slot_dir = str(slot_dir)
        os.makedirs(self.slot_dir, exist_ok=True)
        self.calls = []
        self.model = "27B"
        self.other_models = ["9B"]
        self.slot_mode = {"save": "ok", "restore": "ok", "erase": "ok"}   # ok | 400 | 409 | 501 | hang
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

        # ---- catalog state (NInfer simulation) ----
        self.catalog_cells = max(1, int(catalog_cells))
        # cell_id -> {"entry_id": str, "digest": str}
        self.catalog = {}
        self._digest_counter = 0
        # Map from entry_id -> set of filenames that have been saved for it (for restore validation)
        self._saved_files = {}

    def _new_digest(self):
        self._digest_counter += 1
        return f"digest-{self._digest_counter}"

    def _find_cell(self, entry_id):
        for cell, state in self.catalog.items():
            if state["entry_id"] == entry_id:
                return cell
        return None

    def _first_vacant_cell(self):
        for cell in range(self.catalog_cells):
            if cell not in self.catalog:
                return cell
        return None

    def _publish(self, entry_id):
        """Simulate the engine publishing a conversation to the catalog."""
        cell = self._find_cell(entry_id)
        if cell is not None:
            # Already resident; a chat updates its digest (new tokens).
            self.catalog[cell]["digest"] = self._new_digest()
            return cell
        cell = self._first_vacant_cell()
        if cell is None:
            # All cells full: evict cell 0 (simplest deterministic policy for tests).
            cell = 0
        self.catalog[cell] = {"entry_id": entry_id, "digest": self._new_digest()}
        return cell

    def _entry_id_of(self, request):
        """Derive the entry_id the proxy would compute for this chat request."""
        from src.classify import make_entry_id, find_chat_id
        body = request.get("body") if isinstance(request, dict) else None
        payload = body if isinstance(body, dict) else {}
        model = payload.get("model")
        headers = request.get("headers", {}) if isinstance(request, dict) else {}
        chat_id = find_chat_id(headers)
        if not model or not chat_id:
            return None
        return make_entry_id(chat_id, model)

    # ---- lifecycle ---------------------------------------------------
    async def start(self):
        app = web.Application()
        app.router.add_get("/v1/models", self.models_h)
        app.router.add_get("/models", self.models_h)
        app.router.add_get("/health", self.generic)
        app.router.add_get("/models/sse", self.sse)
        app.router.add_get("/slots", self.slot_states)
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
            elif c["path"] == "/slots":
                out.append("slots:list")
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

    async def slot_states(self, request):
        """GET /slots -> per-cell state, mirroring NInfer's SlotState."""
        await self._record(request)
        out = []
        for cell in range(self.catalog_cells):
            state = self.catalog.get(cell)
            if state is None:
                out.append({"id": cell, "is_processing": False, "retained": False,
                            "session_digest": None, "n_tokens": 0, "n_saved": 0,
                            "n_written": 0, "filename": None, "timings": {}})
            else:
                out.append({"id": cell, "is_processing": False, "retained": True,
                            "session_digest": state["digest"], "n_tokens": 100, "n_saved": 1,
                            "n_written": self.save_bytes, "filename": f"{state['entry_id']}.bin",
                            "timings": {}})
        return web.json_response(out)

    async def slots(self, request):
        call = await self._record(request)
        action = request.query.get("action")
        mode = self.slot_mode.get(action, "ok")
        if mode == "hang":
            await asyncio.sleep(30)
        if mode in ("400", "409", "501"):
            return web.json_response(
                {"error": {"code": int(mode), "message": f"fake {action} failure", "type": "invalid_request_error"}},
                status=int(mode))

        slot_id = int(request.match_info["id"])
        body = call["body"] if isinstance(call["body"], dict) else {}
        filename = body.get("filename")
        if_digest = body.get("if_digest")

        if action == "save":
            # Save the occupant of the target cell.
            state = self.catalog.get(slot_id)
            if state is None:
                return web.json_response(
                    {"error": {"code": 400, "message": "slot holds no retained session",
                               "type": "invalid_request_error"}}, status=400)
            # if_digest precondition (NInfer checks this on save too)
            if if_digest and state["digest"] != if_digest:
                return web.json_response(
                    {"error": {"code": 409, "message": "slot_session_mismatch",
                               "type": "invalid_request_error"}}, status=409)
            path = os.path.join(self.slot_dir, filename)
            with open(path, "wb") as f:
                f.write(b"x" * self.save_bytes)
            self._saved_files.setdefault(state["entry_id"], set()).add(filename)
            # The digest is stable across a save (no new tokens).
            return web.json_response({"id_slot": slot_id, "filename": filename, "n_saved": 1,
                                      "n_written": self.save_bytes, "session_digest": state["digest"],
                                      "timings": {"save_ms": 1.0}})

        if action == "restore":
            # Load the file into the target cell. NInfer refuses a restore whose
            # snapshot does not fit in the *free* KV capacity, and it will NOT evict
            # the current occupant for you ("evict other sessions first"). We simulate
            # this faithfully:
            #   - If the target cell is occupied by a *different* conversation, the
            #     occupant's pages are not free, so the snapshot does not fit and the
            #     restore is refused.
            #   - If the target cell is occupied by the *same* conversation (e.g. after a
            #     proxy restart, where the engine persisted the session), the restore is a
            #     no-op: the snapshot matches the resident state, so it succeeds without
            #     evicting anything.
            if not filename:
                return web.json_response(
                    {"error": {"code": 400, "message": "filename required",
                               "type": "invalid_request_error"}}, status=400)
            path = os.path.join(self.slot_dir, filename)
            if not os.path.exists(path):
                return web.json_response(
                    {"error": {"code": 400, "message": "file not found",
                               "type": "invalid_request_error"}}, status=400)
            # The entry_id is derived from the filename (the proxy names files <entry_id>.bin).
            entry_id = os.path.basename(filename)
            if entry_id.endswith(".bin"):
                entry_id = entry_id[:-4]
            occupant = self.catalog.get(slot_id)
            if occupant is not None:
                if occupant["entry_id"] == entry_id:
                    # Same conversation already resident: no-op restore (snapshot matches).
                    return web.json_response({"id_slot": slot_id, "filename": filename, "n_restored": 1,
                                              "n_read": 0, "session_digest": occupant["digest"],
                                              "timings": {"restore_ms": 0.5}})
                # Different conversation occupies the cell: capacity refusal.
                return web.json_response(
                    {"error": {"code": 400,
                               "message": "session snapshot does not fit the free KV capacity; evict other sessions first",
                               "type": "invalid_request_error"}}, status=400)
            # Publish the restored conversation into the target cell.
            self.catalog[slot_id] = {"entry_id": entry_id, "digest": self._new_digest()}
            return web.json_response({"id_slot": slot_id, "filename": filename, "n_restored": 1,
                                      "n_read": 1, "session_digest": self.catalog[slot_id]["digest"],
                                      "timings": {"restore_ms": 1.0}})

        if action == "erase":
            state = self.catalog.get(slot_id)
            if state is None:
                return web.json_response(
                    {"error": {"code": 400, "message": "slot holds no retained session",
                               "type": "invalid_request_error"}}, status=400)
            if if_digest and state["digest"] != if_digest:
                return web.json_response(
                    {"error": {"code": 409, "message": "slot_session_mismatch",
                               "type": "invalid_request_error"}}, status=409)
            del self.catalog[slot_id]
            return web.json_response({"id_slot": slot_id, "n_erased": 1})

        return web.json_response({"id_slot": slot_id, "n_erased": 0})

    async def chat(self, request):
        call = await self._record(request)
        payload = call["body"] if isinstance(call["body"], dict) else {}
        loop = asyncio.get_running_loop()
        try:
            if self.chat_delay:
                await asyncio.sleep(self.chat_delay)
            if self.chat_status != 200:
                return web.json_response({"error": {"message": "fake chat error"}}, status=self.chat_status)
            # Simulate the engine publishing/updating the conversation in the catalog.
            entry_id = self._entry_id_of(call)
            if entry_id is not None:
                self._publish(entry_id)
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
