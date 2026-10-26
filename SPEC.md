# llama-slot-proxy

## 1. Purpose and scope

1. The proxy is a slim layer in front of llama-server.
   1. Clients never call llama-server directly, only the proxy.
   2. The proxy can intercept and watch every request and response as needed.
   3. Every request respects the queue (section 8), except the bypass list (8.5).
   4. Implementation stays minimal: only what these roles need.
   5. The proxy never hits the server on its own at startup. Any first fetch it needs waits for the first incoming request, then runs before that request (inside the queue).
2. Supported setup (everything else is out of scope; the proxy only warns about violations, see 3.4):
   1. llama-server in **router mode**, with **at most one model loaded at a time** **\[new\]** (router flag `--models-max 1`; verify the exact flag name on your build). Non-llama-server upstreams (e.g. NInfer) are supported insofar as they expose the same `POST /slots/{id}?action=save|restore` and `POST /v1/chat/completions` endpoints; their `GET /v1/models` does not expose `status.args`, so the slot-save path comes from config `slot_save_path` (3.3.3) and setup checks 2-5 do not apply to them.
   2. Single slot (`--parallel 1`). The proxy always addresses slot `0` in code, not user configurable.
   3. Only `POST /v1/chat/completions` is processed by the proxy **\[new\]**. Every other endpoint is plain-forwarded (see 5.1).
   4. Proxy and llama-server **share the same filesystem path** **\[new\]**. llama-server writes bins under each model's own `--slot-save-path`; the proxy discovers that path from llama-server (3.3) and reads, lists and deletes bins there. For upstreams without `--slot-save-path` (1.2.1), the shared directory is the one set in config `slot_save_path`.
   5. llama-server must **not** run with `--sleep-idle-seconds` set above 0 (leave it unset or 0, disabled): sleep discards the KV cache without the proxy noticing, so loaded chats (4.1) would go stale and restore would be skipped. **\[new\]**
3. Accepted trade-offs (intentional, listed so they are not mistaken for bugs):
   1. No LRU or message-hash validation. Restore is dumb and degrades naturally when the bin is shorter than, or no longer matches, the request's messages. The next lazy save re-syncs it.
   2. A to B to A within one proxy run relies on llama-server's own RAM prompt cache (`--cache-ram`), not on the proxy restoring.
   3. A chat's unsaved progress is lost when another chat takes the slot and the first never returns. Saving happens only on the triggers in 6.3.
   4. `POST /models/load` is not intercepted (scope).
   5. Non-chat endpoints that use the slot (`/v1/completions`, `/infill`, ...) are plain-forwarded and not tracked, so they can silently replace slot contents.
   6. If the ledger is lost or recreated empty, existing bins become orphans the proxy cannot discover (cleanup only scans directories known from ledger entries).
   7. The proxy cannot see a llama-server restart that happens between requests.

## 2. Glossary

| Term | Meaning |
| --- | --- |
| chat id | Value of the first recognized header present in the request (3.1). Formerly also called conv id / conversation id; use only **chat id**. |
| model id | The `model` field of the request body. |
| entry id | `hashlib.md5` of the exact string `chat id` + `_` + `model id` (hex digest). |
| bin | A saved slot file named `<entry id>.bin`. |
| ledger | Persistent list of saved chats (4.2). |
| delta | Tokens added by one request: `timings.prompt_n + timings.predicted_n` (3.2). |
| noop request | A chat completion without a recognized header and/or without a model id. |

## 3. Information sources

The proxy collects what it needs from these sources and nothing else. Config (section 10) holds only what none of them can provide.

### 3.1 From the request

1. **Model id**: JSON body field `model`.
2. **Chat id**: the value of a recognized header.
   1. Recognized headers live in one list constant in code so adding more is a one-line change. Currently: `x-session-affinity`.
   2. If several recognized headers are present, the first one in list order is used.
3. Entry id is built only when both are present. Otherwise the request is a noop request.

### 3.2 From the response

1. **Delta** **\[new\]**: `timings.prompt_n + timings.predicted_n`, read from the non-streamed response body, or from the last SSE chunk that carries `timings` for streamed responses. This counts new prompt tokens as well as generated tokens, unlike `usage.completion_tokens`.
2. Delta is **per request** (not accumulated since the last save). This is intentional: an accumulated value would trigger a save on every request once past the threshold.
3. Streaming is passed through byte-transparently; the proxy only observes the bytes. Because `timings` is part of the normal response, the old `stream_options.include_usage` injection is no longer needed **\[new\]**. If the live build turns out not to send `timings` in the final stream chunk, reintroduce it (inject `stream_options.include_usage = true` only when `stream` is `true`, merging with any client-set `stream_options`) and use usage instead.

### 3.3 From llama-server

1. **Slot save path per model.** In router mode, call `GET /v1/models` and read the `--slot-save-path` value from each model's `status.args`.
2. Fetched once, as the first internal call (before the first forwarded client request, after the startup routine in 7.1), going through the queue. If it fails or the value is missing (for example the model is unloaded and `args` is absent), the tracked value is `None` and the fetch is retried before the next save.
3. **Config fallback** **\[new\]**: when the fetched `status.args` does not contain `--slot-save-path` (for example the upstream is not llama-server in router mode, such as NInfer), the tracked value falls back to `slot_save_path` from config.json (default empty, no fallback). A discovered value always wins over a configured one; a conflict is warned. A model still `None` after both (no discovery, no config) is handled as no path: saves for it are skipped (6.3.2 step 2) and check 6 (3.4) warns.

### 3.4 Setup validation **\[new\]**

The proxy checks the supported setup (1.2) with warn-only checks that read the same `GET /v1/models` response as 3.3, so they add no upstream call.

1. Checks run whenever that fetch runs: once before the first request, and again on every retry after a failed fetch.
2. A failed check prints a warning to the console (not only in `--verbose`) naming the model and the problem. It never blocks or rejects a request.
3. A model whose `status.args` is absent (for example an unloaded model) skips checks 3 to 5. They run on the next fetch, which is the same retry as the tracked slot-save-path (3.3.2).
4. Argument names are matched in their long and short forms, and the value is read from the following item in `status.args`.

| # | Setup (1.2) | Source | Warn when |
| --- | --- | --- | --- |
| 1 | Router mode | `status` object on the entries of `GET /v1/models` | Entries have no `status` object (not a router). |
| 2 | One loaded model | Count of entries with `status.value` equal to `loaded` | More than one is loaded. |
| 3 | Single slot | `--parallel` or `-np` in `status.args` | Missing, or the value is not `1`. Missing also warns because the server default is not guaranteed to be 1. |
| 4 | Sleep disabled | `--sleep-idle-seconds` in `status.args` | Present with a value above `0`. Absent is fine. |
| 5 | Shared path | `--slot-save-path` in `status.args`, plus a local check | The flag is missing (save and restore would return `501`), or the directory does not exist or is not writable by the proxy (`os.path.isdir` and `os.access` with write permission). |
| 6 | Discoverable path, or config | `status.args` across all entries, plus config `slot_save_path` | No entry exposes a `status` object at all (not llama-server in router mode, e.g. NInfer) and `slot_save_path` is empty; saves for tracked chats would be skipped. |

Checks 3 to 5 depend on what `status.args` actually contains on your build (explicitly passed flags only, or also defaults). Verify against a real response before trusting a missing flag.

## 4. State

### 4.1 Runtime state (in memory, lost on restart)

1. **Active model**: string. Set from every chat completion request that carries a model id.
2. **Active chat**: string, the entry id currently believed to be in the slot. Set from every chat completion request. Set to `""` for a noop request.
3. **Loaded chats**: set of entry ids already seen (or restored) since proxy start or last reset. It means seen or restored, not currently in the slot. Updated by 6.1 and cleared by resets (6.2, 6.4, 9.3). It is kept for observability and for the ledger eviction logic (7.2); the restore decision itself is driven by the *active chat* (6.1), not by this set.
4. **Tracked slot-save-path**: map `{model_id: path}`, populated by 3.3 (with the config fallback of 3.3.3).
5. The old `slot active` boolean is removed. The queue already guarantees one request at a time and nothing reads it. **\[new\]**

### 4.2 Ledger (persistent, `ledger.json`)

Top-level array, one object per entry:

```
[
  {
    "entry_id": "abc123",
    "chat_id": "chat1",
    "model_id": "27B",
    "dates": {"created": "...", "restored": "...", "saved": "..."},
    "bin_path": "/path/abc123.bin"
  }
]
```

1. `bin_path` is the slot-save-path plus `<entry id>.bin`. Its directory part is the *slot-save-path in ledger* used by the save path check (6.3.2).
2. Dates: `created` is when the entry was first added, `saved` is the last successful save, `restored` is the last successful restore (null until the first restore).
3. Default location is beside the main `.py`, overridable with `--ledger-file`. If the file is missing or corrupt at startup, an empty one is created at that location.
4. All writes are atomic **\[new\]**: write a temp file in the same directory, then rename over `ledger.json`.

## 5. Request lifecycle

Every incoming request goes through these steps in order.

1. **Classify.**
   1. `POST /v1/chat/completions` with a model id and a recognized header: a **tracked request** (entry id built).
   2. `POST /v1/chat/completions` without a recognized header and/or model id: a **noop request**.
   3. Anything else, including `POST /models/unload`, which has its own rule in 6.4: **plain forward**. The client is responsible for supplying the model (body for POST, `?model=` for GET); the router returns `400` if it is missing.
2. **Bypass check.** If the request is on the bypass list (8.5), forward it immediately with no queue and no processing.
3. **Enter the queue** (section 8). Everything below runs while holding the queue.
4. **Initial fetch** (only for the first request after startup): populate tracked slot-save-paths (3.3).
5. **Model change check** (6.2), for any chat completion that carries a model id.
6. **Restore check** (6.1), tracked requests only.
7. **Update state** (active chat, active model) and **forward** the request to llama-server.
8. **Stream the response** back to the client unchanged.
9. **After response completes:** evaluate the save triggers (6.3.1) for tracked requests, then release the queue.

Noop and plain-forwarded requests skip steps 5 to 6 except where a trigger applies (a noop request that carries a model id still runs the model change check). No slot pre-processing happens when no trigger fires.

## 6. Roles and triggers

### 6.1 Lazy restore

For a tracked request:

1. Entry id **is** the active chat (a **continuation** — same conversation as the previous request): forward with no restore. The conversation is already in the active slot, so restoring would be a pointless disk round-trip.
2. Entry id is **not** in the ledger: add it to loaded chats (a **new unique request**) and forward with no restore (plain prefill).
3. Otherwise (entry id is in the ledger, and it is a *different* conversation than the active chat): restore the slot, then forward. This happens even if the conversation was seen before, because the upstream engine may have evicted it in the meantime and the on-disk snapshot is then the only remaining copy.
4. Noop request: forward with no restore.

> **Note (NInfer / dynamic retention).** The original rule ("skip restore if the entry is in loaded chats") assumed the proxy controls slot residency, as in llama.cpp. In NInfer the *engine* decides what stays resident (active slot + a fixed pool of host state slots), so a proxy-side "seen" set cannot be trusted to mean "still in the slot". The rule above therefore restores on every switch to a different saved conversation. A redundant restore (switching back to a conversation that is still resident) costs only a small disk read plus a device round-trip, which is far cheaper than the full prefill that a missed restore would trigger.

Restore procedure: call the restore endpoint (12.2) with `filename` set to the file name of the ledger entry's `bin_path`, bounded by `save_timeout`. The proxy cannot inspect the bin; it judges by the HTTP result:

1. **2xx:** set the ledger `restored` date and add the entry id to loaded chats. **\[new\]**
2. **HTTP error (the server returns 400 when the file is missing or invalid, the tokens do not fit the slot context, or the tokens are invalid):** the bin is considered unusable. Delete the entry and its bin, add the entry id to loaded chats so the same request is not retried, log the error, and forward the client request as plain prefill. This is not returned to the client as an error. **\[new\]**
3. **Timeout (`save_timeout`):** log it, keep the entry, add it to loaded chats, and continue the queue (see 9.2).
4. **Connection error:** treated as upstream down (9.3).

### 6.2 Model change

When a chat completion request carries a model id that differs from the active model:

1. Before forwarding, if the active chat has an entry id, **save** it. The save targets the *old* model (old model id in the body and query, old model's tracked path). It must complete before forwarding because the router replaces the loaded model on the new request.
2. Reset loaded chats and active chat (to `""`).
3. Update active model, then continue the lifecycle (restore check, forward).

With an empty active model (first request after start) the active chat is `""`, so nothing is saved.

### 6.3 Lazy save

#### 6.3.1 Triggers

Only these triggers exist. Anything not listed here is intentionally discarded.

1. **By time** (after a tracked request completes): `now - dates.saved` is greater than `save_time_threshold`. A request whose entry has no ledger entry yet counts as never saved, so the first response of a new chat is saved. **\[new\]**
2. **By delta** (after a tracked request completes): the request's delta (3.2) is greater than `delta_context_threshold`.
3. **By model change:** see 6.2.
4. **By unload request:** see 6.4.
5. **By proxy shutdown:** see 6.5.

Triggers 1 and 2 run after the response has been fully sent and before the next queued request starts. They are skipped if the client disconnected mid-response (the slot holds a partial generation); the next completed request will cover it.

#### 6.3.2 Save procedure

All steps run inside the queue, in this order:

1. **Eviction check** (7.2): if total saved bin size is above the limit, evict first.
2. **Resolve the path.** If the model's tracked path is `None`, retry the fetch (3.3). If it is still `None`, use the existing ledger path if the entry has one; with neither, skip the save and log an error.
3. **Path comparison.** If the directory of the ledger entry's `bin_path` differs from the tracked path (and the tracked path is not `None`): save to the new path, and only after that save succeeds delete the old bin, then update `bin_path` in the ledger.
4. **Save** with `filename` = `<entry id>.bin` (12.2), bounded by `save_timeout`.
5. **Update the ledger** (create the entry if new; set `dates.saved`; write atomically).

On an HTTP error or timeout, log it, leave the ledger unchanged, and continue the queue. A failed save never fails the client request that triggered it, because the response is already sent (or the triggering request is an unload or shutdown).

### 6.4 Unload interception

1. Intercept `POST /models/unload` with body `{"model": "..."}`, inside the queue.
2. If the unloaded model equals the active model **and** the active chat has an entry id: save first (6.3.2), then forward the unload.
3. After forwarding (regardless of the model), if the unloaded model equals the active model, reset active chat, loaded chats and active model. **\[new\]**
4. If the unloaded model is not the active model, forward with no save and no reset.

### 6.5 Shutdown

On SIGINT or SIGTERM: **\[new\]**

1. Stop accepting requests. Waiting requests are rejected with `503`.
2. Let the in-flight request finish normally (its own post-response save still runs). A second signal skips waiting and exits immediately.
3. If the active chat has an entry id, save it (6.3.2). The save is bounded by `save_timeout`; a redundant save right after a completed request is accepted for simplicity.
4. Exit.

## 7. Ledger maintenance

The ledger and bin directories are managed using only paths stored in the ledger. No llama-server call is needed for any of this.

### 7.1 Startup routine

Runs once at startup (queue blocking, so no request can restore a missing bin):

1. Entry whose bin file does not exist: delete the entry.
2. Bin not mentioned by any entry: delete the bin. Only files matching `<32 hex characters>.bin` in directories known from ledger entries are considered, so unrelated files in a shared directory are never touched. **\[new\]**
3. Bin that is corrupt: delete the bin and its entry. For now, corrupt means zero bytes.

### 7.2 Disk limit (before each save)

Runs inside the queue, only when a save trigger fires, before the save:

1. Compute total bin size (GiB, 1024^3 bytes) from the files in the ledger.
2. If it is above `bin_max_total_gb`, delete entries (and their bins) until total size is at or below `bin_max_total_gb * (1 - bin_padding_percent / 100)`. **\[new\]**
3. Eviction order, oldest first, is a sort key made of the entry's most recent activity, then tie-breakers:
   1. `max(restored, saved)` (treat a null date as the oldest possible date),
   2. then `saved`,
   3. then `created`.
4. The entry about to be saved (the active chat) is never evicted. **\[new\]**

## 8. Queue

1. Only one request hits llama-server at a time, in arrival order (FIFO). The queue is held for the whole request: pre-processing, forward, streaming the response, and post-response saves.
2. `queue_timeout` (seconds, `0` = indefinite): maximum time a request may **wait** in the queue. It does not cover execution. Exceeding it returns `504`.
3. `queue_limit` (`0` = unlimited): maximum number of requests **waiting**; the in-flight request is not counted. A request arriving over the limit returns `429` immediately.
4. A client that disconnects while waiting is removed from the queue.
5. **Bypass list** **\[new\]**: these are forwarded immediately without queueing or processing, because they do not touch the slot and do not wake a sleeping model:
   1. `GET /health`, `GET /v1/health`
   2. `GET /props`, `GET /metrics`, `GET /slots`
   3. `GET /models`, `GET /v1/models`
   4. `GET /models/sse` (long-lived SSE; if it were queued it would block everything behind it)

   Everything else, including POST token-counting and tokenize endpoints, goes through the queue. The list is a constant in code, like the recognized headers.
6. **Client disconnect mid-stream:** when a write to the client fails, close the upstream connection (llama-server then stops the generation) and release the queue in a `finally` block. For non-streamed requests, run the server with aiohttp handler cancellation enabled (`handler_cancellation=True`) so a disconnect cancels the upstream call too.
7. Internal calls (initial fetch, save, restore, unload) all run inside the queue holder of the request that caused them, so they follow the same rule.
8. The upstream client has **no total timeout** (streams can run long). Only save and restore calls use `save_timeout`.

## 9. Errors and status codes

### 9.1 Returned to the client (and printed to the console)

| Status | When |
| --- | --- |
| 429 | Queue limit exceeded. |
| 504 | Queue wait timeout exceeded. |
| 502 | Upstream unreachable (`aiohttp.ClientError` on connect, or connection lost before any response byte). |
| 503 | Proxy is shutting down. |
| 500 | Unexpected proxy error. |

Upstream HTTP errors (4xx, 5xx from llama-server) on a forwarded client request are returned to the client unchanged.

### 9.2 Not returned to the client (printed to the console, queue continues)

1. Save or restore that returns an HTTP error or hits `save_timeout`. A restore HTTP error additionally drops the entry (6.1.2).
2. Failed fetch of slot-save-path (value stays `None`).
3. Eviction or ledger write problems.

### 9.3 Upstream down

On a connection error: return `502` for the client request and reset active chat, loaded chats and active model, since the slot contents can no longer be trusted. **\[new\]**

## 10. Configuration and CLI

### 10.1 `config.json`

1. Default location beside the main `.py`; override with `--config-file`. If missing or corrupt at startup, auto-created there.
2. It contains a `$docs` key mirroring the real structure 1:1 (no flattening), with a short description per key: function, expected type and unit.
3. Only keys strictly needed by this spec:

| Key | Type | Notes |
| --- | --- | --- |
| `upstream_url` | string | Where to forward (llama-server router). |
| `listen_host` **\[new\]** | string | Bind address, default `127.0.0.1`. |
| `listen_port` | int | Port to listen on. |
| `save_time_threshold` | float (s) | Time trigger (6.3.1). |
| `delta_context_threshold` | int (tokens) | Delta trigger (6.3.1). |
| `save_timeout` | float (s) | Bound for save and restore calls. |
| `bin_max_total_gb` | float (GiB, 1024^3 bytes) | Disk limit (7.2). |
| `bin_padding_percent` | float (%) | Eviction target is `max * (1 - pct/100)`. |
| `queue_timeout` | float (s, 0 = indefinite) | Queue wait only. |
| `queue_limit` | int (0 = unlimited) | Waiting requests only. |

### 10.2 Flags

1. `--config-file`: custom `config.json` path.
2. `--ledger-file`: custom `ledger.json` path.
3. `--generate-config`: one-time run that writes `config.json` (at the default location, or at `--config-file` if given) and exits.
4. `--verbose`: print every proxy activity (section 6) to the console with a simple timestamp, for example `[2025-01-01 12:00:00]`.

## 11. Project structure

1. A single main `.py` is the entry point.
2. Packages are installed in a `venv`, populated from `requirements.txt`.
3. Code is split by concern in files under `/src`.
4. `ledger.json` and `config.json` live beside the main `.py` by default (flags above).

## 12. llama-server reference (router mode, from the endpoint notes)

### 12.1 Routing

1. The router forwards every non-model-management request to the child process that owns the requested model.
2. **POST** endpoints: model read from the `"model"` body field. **GET** endpoints: model read from the `model` query parameter.
3. Missing model returns `400` with message `model name is missing from the request` and type `invalid_request_error`.
4. Autoload: the first request loads the model and blocks until ready. Per-request override `?autoload=true|false`; server-wide `--no-models-autoload`.
5. Model management endpoints handled by the router itself: `GET /models`, `POST /models`, `POST /models/load`, `POST /models/unload` (body `{"model": "..."}`, response `{"success": true}`), `GET /models/sse`, `DELETE /models`.
6. Slot endpoints require `model` in the call (GET `/slots?model=...`; POST body `"model"`).

### 12.2 Slot save, restore, erase (slot id is always `0`)

| Action | Request | Success response |
| --- | --- | --- |
| save | `POST /slots/0?action=save&model=<m>` body `{"model": "<m>", "filename": "<entry id>.bin"}` | `{"id_slot": 0, "filename": ..., "n_saved": ..., "n_written": ..., "timings": {"save_ms": ...}}` |
| restore | `POST /slots/0?action=restore&model=<m>` body `{"model": "<m>", "filename": "<entry id>.bin"}` | `{"id_slot": 0, "filename": ..., "n_restored": ..., "n_read": ..., "timings": {"restore_ms": ...}}` |
| erase (not used by this spec) | `POST /slots/0?action=erase&model=<m>` | `{"id_slot": 0, "n_erased": ...}` |

Notes:

1. `filename` is required in the body and validated as a plain filename (no path traversal). The file is written under the server's `--slot-save-path` for that model, which is why bins are named `<entry id>.bin` and the proxy needs only the directory.
2. Restore errors are `400`: file missing or invalid, tokens do not fit in the slot context, invalid tokens.
3. If the slot is busy, the server defers the save/restore task until the slot is idle and holds the HTTP request open. The proxy does not need to poll `is_processing`; `save_timeout` bounds the wait.
4. Without `--slot-save-path` the server returns `501` (`This server does not support slots action. Start it with --slot-save-path`).
5. API shape in current mainline is `POST /slots/{id}` with `?action=`. The old `/slots/0/save` and `/slots/0/load` paths no longer exist.
6. The save file contains the llama state (KV sequence), context checkpoints and draft-model state; restore restores all of them.

### 12.3 Other endpoints the proxy relies on

1. `GET /v1/models`: in router mode returns the model list. Each entry has `id`, `status` (`value` plus `args` plus `preset`), `context_length` and more. `status.args` carries `--slot-save-path`.
2. `POST /v1/chat/completions`: OAI-compatible; streaming is SSE; the response includes a `timings` object and a standard `usage` object. SSE must be passed through byte-transparently.
3. `GET /health` returns `200` or `503` (loading). `GET /health`, `/props`, `/models` and `/metrics` do not count as tasks and do not wake a sleeping model.
4. `--sleep-idle-seconds`: after inactivity the model and KV cache are unloaded from RAM. This is the constraint in 1.2.5.
