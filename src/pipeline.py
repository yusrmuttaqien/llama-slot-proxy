"""The single request pipeline. The only place that calls feature modules."""
import asyncio

import aiohttp
from aiohttp import web
from multidict import CIMultiDict

from src import restore, save, slotpath
from src.classify import Kind, classify
from src.delta import DeltaTap
from src.ledger import parse_date, utcnow
from src.reqqueue import QueueClosed, QueueFull, QueueTimeout

HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
              "te", "trailers", "transfer-encoding", "upgrade"}


def json_error(status, message, etype="proxy_error"):
    return web.json_response({"error": {"code": status, "message": message, "type": etype}},
                             status=status)


class ForwardResult:
    def __init__(self, response, status, ok=False, disconnected=False, delta=None):
        self.response = response
        self.status = status
        self.ok = ok                  # completed fully with a 2xx status
        self.disconnected = disconnected
        self.delta = delta


async def forward(ctx, request, body, tap=None, reset_on_error=True):
    """Forward one request upstream and stream the response back byte-transparently."""
    log = ctx.log
    skip = HOP_BY_HOP | {"host", "content-length"}
    if tap is not None:
        skip = skip | {"accept-encoding"}   # need readable bytes to observe timings
    headers = CIMultiDict((k, v) for k, v in request.headers.items() if k.lower() not in skip)
    try:
        up = await ctx.upstream.session.request(
            request.method, ctx.upstream.url(request.path_qs),
            data=body or None, headers=headers, allow_redirects=False)
    except aiohttp.ClientError as e:
        log.error(f"upstream unreachable: {e}")
        if reset_on_error:
            ctx.state.reset()
        return ForwardResult(json_error(502, f"upstream llama-server unreachable: {e}", "upstream_error"), 502)

    resp = web.StreamResponse(status=up.status, reason=up.reason)
    for k, v in up.headers.items():
        if k.lower() not in HOP_BY_HOP:
            resp.headers.add(k, v)
    if tap is not None:
        tap.configure(up.headers.get("Content-Type", ""))

    completed = False
    disconnected = False
    try:
        await resp.prepare(request)
        async for chunk in up.content.iter_any():
            if tap is not None:
                tap.feed(chunk)
            await resp.write(chunk)
        await resp.write_eof()
        completed = True
    except (ConnectionResetError, ConnectionError):
        disconnected = True
        log.info("client disconnected mid-response; closing upstream connection")
    except aiohttp.ClientError as e:
        log.error(f"upstream connection lost mid-response: {e}")
        if reset_on_error:
            ctx.state.reset()
        if request.transport is not None:
            request.transport.close()
    finally:
        if completed:
            up.release()
        else:
            up.close()   # also runs on cancellation: llama-server then aborts the generation
    ok = completed and 200 <= up.status < 300
    return ForwardResult(resp, up.status, ok=ok, disconnected=disconnected,
                         delta=tap.delta() if (tap is not None and completed) else None)


async def handle_request(ctx, request):
    body = await request.read()
    c = classify(request.method, request.path, request.headers, body)
    ctx.log.info(f"{request.method} {request.path_qs} -> {c.kind.name}"
                 + (f" entry={c.entry_id}" if c.entry_id else ""))

    if ctx.shutting_down:
        return json_error(503, "proxy is shutting down", "unavailable_error")

    if c.kind is Kind.BYPASS:
        return (await forward(ctx, request, body, reset_on_error=False)).response

    try:
        await ctx.queue.acquire()
    except QueueFull:
        ctx.log.error(f"queue full ({ctx.queue.limit} waiting); rejecting {request.method} {request.path}")
        return json_error(429, "proxy queue is full", "rate_limit_error")
    except QueueTimeout:
        ctx.log.error(f"queue wait exceeded {ctx.queue.timeout}s; rejecting {request.method} {request.path}")
        return json_error(504, "timed out waiting in the proxy queue", "timeout_error")
    except QueueClosed:
        return json_error(503, "proxy is shutting down", "unavailable_error")

    handoff = {"task": None}   # set when a background task takes over releasing the queue
    try:
        return await _process(ctx, request, body, c, handoff)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        ctx.log.error(f"unexpected proxy error: {e!r}")
        return json_error(500, f"internal proxy error: {e}")
    finally:
        if handoff["task"] is None:
            ctx.queue.release()


async def _process(ctx, request, body, c, handoff):
    st = ctx.state
    if not ctx.initialized:
        ctx.initialized = True
        await slotpath.fetch_paths(ctx)      # first hit, lazily, inside the queue

    if c.kind is Kind.UNLOAD:
        return await _handle_unload(ctx, request, body, c)

    if c.kind not in (Kind.NOOP, Kind.TRACKED):
        return (await forward(ctx, request, body)).response

    if c.model_id and c.model_id != st.active_model:
        await _model_change(ctx, c)
    if c.kind is Kind.TRACKED:
        if await restore.maybe_restore(ctx, c) is restore.Outcome.UPSTREAM_DOWN:
            return json_error(502, "upstream llama-server unreachable", "upstream_error")

    st.set_active(c)
    tap = DeltaTap() if c.kind is Kind.TRACKED else None
    result = await forward(ctx, request, body, tap=tap)

    if c.kind is Kind.TRACKED and result.ok:
        # Post-response work keeps the queue until done, and survives the client hanging up.
        async def post():
            try:
                await _save_triggers(ctx, c, result)
            finally:
                ctx.queue.release()
        task = asyncio.ensure_future(post())
        handoff["task"] = task
        ctx.bg_tasks.add(task)
        task.add_done_callback(ctx.bg_tasks.discard)
        await asyncio.shield(task)
    return result.response


async def _save_triggers(ctx, c, result):
    """Spec 6.3.1 triggers 1 (time) and 2 (delta), after a tracked response completed."""
    cfg = ctx.config
    try:
        entry = ctx.ledger.get(c.entry_id)
        saved = parse_date(entry["dates"].get("saved")) if entry else None
        due_time = saved is None or (utcnow() - saved).total_seconds() > cfg["save_time_threshold"]
        due_delta = result.delta is not None and result.delta > cfg["delta_context_threshold"]
        if due_time or due_delta:
            reason = "time" if due_time else f"delta {result.delta}"
            await save.save_entry(ctx, c.entry_id, c.chat_id, c.model_id, reason)
    except Exception as e:
        ctx.log.error(f"post-response save check failed: {e!r}")


async def _model_change(ctx, c):
    """Spec 6.2: save the old model's active chat before the router swaps models, then reset."""
    st = ctx.state
    ctx.log.info(f"model change: {st.active_model or '(none)'} -> {c.model_id}")
    if st.active_chat:
        await save.save_entry(ctx, st.active_chat, st.active_chat_id, st.active_model, "model change")
    st.reset()


async def _handle_unload(ctx, request, body, c):
    """Spec 6.4: save the active chat before forwarding an unload of the active model."""
    st = ctx.state
    is_active = c.model_id == st.active_model
    if is_active and st.active_chat:
        await save.save_entry(ctx, st.active_chat, st.active_chat_id, st.active_model, "unload")
    result = await forward(ctx, request, body)
    if is_active and 200 <= result.status < 300:
        ctx.log.info(f"model {c.model_id} unloaded; resetting state")
        st.reset()
    return result.response
