# llama-slot-proxy: Build Plan

Oct 21, 2026 · @Yusril

The proxy is built in 24 small phases. Each one ends with code that the request pipeline, `app.py` or `main.py` actually calls, plus a test that proves it through the real request path, so nothing is left unwired.

## How to run a phase

1. One phase per session. Give the local model only: this phase's text, the spec sections it names (phase map below), and `PROGRESS.md`.
2. Inside a phase the order is: write the tests, write the code, do the wiring, run the whole suite.
3. A phase is done only when every test, old and new, is green. Never weaken an earlier test to make a new phase pass.
4. After each phase the model appends two lines to `PROGRESS.md` (what exists, where it is wired) and you commit.
5. Do only what the phase says. Later phases exist for everything else. If the model drifts, revert and restart with a shorter prompt.

## How wiring is enforced

1. **One pipeline.** All request handling goes through `handle_request` in `src/pipeline.py`. It is the only place that calls feature modules. A feature is not done until `handle_request` (or startup and shutdown in `main.py`) calls it.
2. **Real-path tests.** Every feature test sends an HTTP request to the real proxy app, in front of the fake llama-server from Phase 0, and asserts on the calls the fake recorded (method, path, query, body, order). Code that is written but never called cannot pass.
3. **One context object.** `AppContext` (config, logger, ledger, state, upstream client, queue) is built once in `src/app.py` and passed as an argument. No globals.
4. **Wire line.** Every phase names the exact call site that must call the new code.
5. **Final audit.** Phase 22 fails the build if a module's entry function is never called from the pipeline, or a spec section has no test.

## Layout

```
main.py
requirements.txt
PROGRESS.md
src/
  config.py log.py upstream.py classify.py queue.py ledger.py state.py
  slotpath.py validate.py startup.py restore.py save.py delta.py evict.py
  shutdown.py pipeline.py app.py
tests/
  fake_upstream.py conftest.py test_*.py
```

## Phase map

| Phase | Delivers | Spec |
| --- | --- | --- |
| 0 | Skeleton and fake llama-server | n/a |
| 1 | Config and flags | 10 |
| 2 | Logger and verbose | 10.2 |
| 3 | Plain forwarding and 502 | 5, 9 |
| 4 | Request classification | 2, 3.1, 5.1 |
| 5 | Queue and bypass list | 8 |
| 6 | Client disconnect handling | 8.6 |
| 7 | Ledger and atomic writes | 4.2 |
| 8 | Runtime state | 4.1 |
| 9 | Slot save and restore client | 12.2 |
| 10 | Slot-save-path fetch on first request | 3.3 |
| 11 | Setup validation | 3.4 |
| 12 | Startup routine | 7.1 |
| 13 | Lazy restore | 6.1 |
| 14 | Save procedure | 6.3.2 |
| 15 | Delta from timings | 3.2 |
| 16 | Time and delta save triggers | 6.3.1 |
| 17 | Model change | 6.2 |
| 18 | Unload interception | 6.4 |
| 19 | Disk limit eviction | 7.2 |
| 20 | Shutdown | 6.5 |
| 21 | Error and status audit | 9 |
| 22 | End-to-end scenarios and spec audit | all |
| 23 | Manual check against real llama-server | 12 |

## Part 1: Foundations (phases 0 to 3)

### Phase 0: Skeleton and fake llama-server

**Build:** venv, `requirements.txt` (aiohttp, pytest, pytest-aiohttp), empty `src/`, a `main.py` that prints a version and exits. In `tests/fake_upstream.py` build an aiohttp app that records every call (method, path, query, body, time) and serves: `GET /v1/models` in router shape (`status.value`, `status.args` with `--slot-save-path`, `--parallel`), `POST /slots/0?action=save|restore` (success, switchable to 400, hang or 501; save writes a real file of N bytes into the slot-save-path directory), `POST /v1/chat/completions` (non-stream and SSE with several chunks and a final chunk carrying `timings`, configurable delay), `POST /models/unload`, `GET /health`, `GET /models/sse`. Add fixtures in `conftest.py`.

**Wire:** none yet.

**Tests:** the fake records calls; each switch changes behavior; SSE arrives in several chunks; save creates the file.

### Phase 1: Config

**Build:** `config.py` with the ten keys from the spec (including `listen_host`, default `127.0.0.1`), defaults, `--config-file`, `--generate-config` (writes `config.json` with the `$docs` mirror and exits), auto-create when missing or corrupt.

**Wire:** `main.py` parses flags, loads config, puts it in `AppContext` (`app.py`).

**Tests:** defaults; custom path; generated file validates and `$docs` has every key at the same nesting; corrupt file is recreated; unknown flags rejected.

### Phase 2: Logger

**Build:** `log.py` with `error()` (always prints) and `info()` (only with `--verbose`), each line prefixed `[YYYY-MM-DD HH:MM:SS]`.

**Wire:** `AppContext` owns the logger; `main.py` sets verbose from the flag.

**Tests:** format matches; info is silent without `--verbose`; error always prints.

### Phase 3: Plain forwarding

**Build:** in `app.py` an aiohttp server on `listen_host` and `listen_port` with one catch-all handler that forwards method, path, query, headers and body to `upstream_url`. Stream the response back chunk by chunk. Upstream client has no total timeout. `aiohttp.ClientError` returns `502` with a clear JSON message and logs an error. Upstream 4xx and 5xx pass through unchanged.

**Wire:** `main.py` starts the app; the handler calls `pipeline.handle_request`, which for now only forwards.

**Tests:** GET and POST arrive identical at the fake; SSE chunks reach the client incrementally (timestamps differ); upstream down gives `502`; upstream `400` is passed through.

## Part 2: Request path (phases 4 to 8)

### Phase 4: Classification

**Build:** `classify.py`, pure functions: kind (`BYPASS`, `PLAIN`, `NOOP`, `TRACKED`, `UNLOAD`), model id from the body, recognized-header list as one constant (first match in list order wins, currently `x-session-affinity`), entry id as md5 of `chat_id + '_' + model_id`, and the `stream` flag. A body that is not valid JSON never crashes: it is `PLAIN` or `NOOP`. Only `POST /v1/chat/completions` can be `NOOP` or `TRACKED`.

**Wire:** `pipeline.handle_request` calls `classify` first and logs the kind with `info()`.

**Tests:** table-driven unit tests (missing header, missing model, two recognized headers, other endpoints); integration: verbose log shows the right kind for each request sent through the proxy.

### Phase 5: Queue and bypass

**Build:** `queue.py`: FIFO, one in-flight request. `queue_timeout` covers waiting only (`504`). `queue_limit` counts waiting requests only (`429`). `0` means unlimited for both. A waiting client that disconnects is removed. Release always happens in `finally`. The bypass list is a constant: `GET /health`, `/v1/health`, `/props`, `/metrics`, `/slots`, `/models`, `/v1/models`, `/models/sse`.

**Wire:** `handle_request` acquires the queue for every non-bypass request and holds it until the response is fully streamed.

**Tests:** two concurrent chats reach the fake one after the other (recorded times do not overlap); timeout gives `504`; limit gives `429`; bypass requests pass while a slow chat is running; a disconnected waiter never reaches the fake.

### Phase 6: Client disconnect

**Build:** when a write to the client fails mid-stream, close the upstream response and release the queue. Run the app with handler cancellation enabled so non-stream disconnects cancel the upstream call too.

**Wire:** inside the streaming loop in `handle_request`.

**Tests:** client aborts mid-SSE: the fake sees its connection closed and the next request starts without waiting; same for a non-stream request.

### Phase 7: Ledger

**Build:** `ledger.py`: load (missing or corrupt becomes an empty file), add, get, update, remove, dates (`created`, `saved`, `restored`), a helper for total bin size from the files, `--ledger-file`. Writes are atomic: temp file in the same directory, then rename.

**Wire:** `AppContext` holds the ledger; `main.py` loads it at startup.

**Tests:** round trip; corrupt file recreated; custom path; atomic write (a failed rename leaves the old file intact and no stray temp file).

### Phase 8: Runtime state

**Build:** `state.py`: `active_model`, `active_chat`, `loaded_chats` (set), `tracked_paths` (dict), and `reset()` that clears the first three. No slot-active variable.

**Wire:** `handle_request` updates state right before forwarding a chat completion: active model from the model id when present, active chat from the entry id or `''` for a noop. Other requests leave state alone.

**Tests:** a sequence of tracked, noop and plain requests produces the expected state after each; `reset()` clears what it should.

## Part 3: Slot plumbing (phases 9 to 12)

### Phase 9: Slot client

**Build:** in `upstream.py`: `slot_save(model, filename)` and `slot_restore(model, filename)`. Each sends `POST /slots/0?action=...&model=<m>` with body `{model, filename}`, is bounded by `save_timeout`, and returns one of: ok, HTTP error (with status), timeout, connection error.

**Wire:** intentionally not called yet; Phases 13 and 14 call it.

**Tests:** against the fake: exact path, query and body; each result type, including the fake's 400, hang and 501 modes.

### Phase 10: Slot-save-path fetch

**Build:** `slotpath.py`: call `GET /v1/models`, read `--slot-save-path` from each model's `status.args`, fill `tracked_paths`. Failure or a missing value stores `None`. Provide `ensure_path(model)` that retries when the value is `None`.

**Wire:** `handle_request` runs the fetch once, inside the queue, before forwarding the first non-bypass request after startup.

**Tests:** the fake has zero calls after startup until a client request arrives; on the first request `/v1/models` is called before the forwarded request; the second request does not fetch again; failure stores `None` and the client still gets its response.

### Phase 11: Setup validation

**Build:** `validate.py` with the five warn-only checks from spec 3.4 (router, one loaded model, single slot, sleep disabled, shared path including the local directory check). Warnings use `error()`.

**Wire:** called by the Phase 10 fetch with the parsed response; models without `status.args` skip checks 3 to 5.

**Tests:** one fake variation per check triggers its warning; the clean fake prints none; a warning never blocks the request.

### Phase 12: Startup routine

**Build:** `startup.py`: entry without a bin file is deleted; a bin not in any entry is deleted, but only files named like `[0-9a-f]{32}.bin` in directories known from ledger entries; a zero-byte bin is deleted together with its entry.

**Wire:** `main.py` runs it after loading the ledger and before the server starts accepting requests.

**Tests:** temp directory with each case; an unrelated file in the same directory is untouched; the ledger on disk matches afterwards.

## Part 4: Restore and save (phases 13 to 20)

### Phase 13: Lazy restore

**Build:** `restore.py` for spec 6.1. In ledger and not loaded: restore, then add to loaded and set `restored` date. Not in ledger and not loaded: add to loaded, no restore. Already loaded or noop: nothing. Restore HTTP error: delete entry and bin, add to loaded, log, forward as plain prefill. Restore timeout: keep entry, add to loaded, log. Connection error: `502` plus `state.reset()`.

**Wire:** `handle_request` step before forwarding, for `TRACKED` only.

**Tests:** one test per branch, asserting the fake's call order (restore before chat, no restore calls in the other cases) and ledger contents; client always gets the chat response except on connection error.

### Phase 14: Save procedure

**Build:** `save.py`: `save_active(ctx, entry_id, model)` per spec 6.3.2. Resolve path (`ensure_path`, else the ledger path, else skip and log). If the ledger directory differs from the tracked path, delete the old bin, save to the new path and update `bin_path`. Call `slot_save` with `<entry id>.bin`. On success create or update the entry and its `saved` date. On failure log and leave the ledger unchanged. The eviction step is a no-op placeholder.

**Wire:** not called yet; Phase 16 onward calls it.

**Tests:** called directly against the fake: new entry created; existing entry date updated; changed path moves the bin and `bin_path`; tracked `None` uses the old path; no path anywhere skips; HTTP error and timeout leave the ledger unchanged.

### Phase 15: Delta from timings

**Build:** `delta.py`: read `timings.prompt_n + timings.predicted_n` from a non-stream JSON body, or from the last SSE chunk that carries `timings`. Missing `timings` gives `None`. Observing must never change the bytes sent to the client.

**Wire:** the response loop in `handle_request` passes every chunk through the tap and stores the delta on the request context.

**Tests:** client bytes are identical to the fake's bytes for stream and non-stream; correct delta for each; no `timings` gives `None`.

### Phase 16: Time and delta save triggers

**Build:** after a `TRACKED` response completes: save when the entry has no ledger record, or `now - saved` is above `save_time_threshold`, or delta is above `delta_context_threshold`. Skip when the client disconnected or the response failed.

**Wire:** `handle_request`, after streaming and before releasing the queue, calls `save_active`.

**Tests (use tiny thresholds):** first response of a new chat triggers a save after the chat call; an immediate second request does not; after the time threshold it does; large delta triggers it; the next queued request starts only after the save ends; noop never saves; a failing save does not change the client's response.

### Phase 17: Model change

**Build:** when a chat completion carries a model id different from `active_model`: if the active chat has an entry id, call `save_active` for the old model (old id and old tracked path) before forwarding, then `reset()`, then continue.

**Wire:** `handle_request` step before the restore check; noops that carry a model id run it too.

**Tests:** switching models: the fake sees a save with the old model, then the chat with the new model; first request ever: no save; active chat empty: no save; loaded chats cleared afterwards.

### Phase 18: Unload interception

**Build:** for `POST /models/unload`: inside the queue, if the model equals `active_model` and the active chat has an entry id, save first, then forward; afterwards, if it was the active model, reset state including `active_model`. Other models are forwarded with no save and no reset.

**Wire:** `handle_request` branch for the `UNLOAD` kind.

**Tests:** order of calls at the fake (save, then unload); state cleared; unloading a different model has no save and keeps state.

### Phase 19: Disk limit

**Build:** `evict.py`: if total bin size is above `bin_max_total_gb`, delete entries and bins, oldest first, until total is at or below `max * (1 - bin_padding_percent / 100)`. Sort key: `max(restored, saved)`, then `saved`, then `created`; a null date counts as oldest. Never evict the entry being saved.

**Wire:** replaces the placeholder at step 1 of `save_active`.

**Tests:** under the limit nothing happens; over the limit stops at the padded target; null dates go first; sort ties resolved as specified; the active entry survives; ledger updated.

### Phase 20: Shutdown

**Build:** `shutdown.py`: on SIGINT or SIGTERM stop accepting requests, reject waiting ones with `503`, let the in-flight request finish, save the active chat (bounded by `save_timeout`), exit. A second signal exits immediately.

**Wire:** `main.py` installs the handlers; the queue exposes a close method.

**Tests (proxy as a subprocess in front of the fake):** SIGTERM when idle: a save is recorded, exit code 0; during a stream: the stream completes, then the save, then exit; waiting request gets `503`; second signal exits quickly; no active chat entry id: no save.

## Part 5: Hardening (phases 21 to 23)

### Phase 21: Error and status audit

**Build:** go through spec section 9 and fix gaps only. `429` queue full, `504` queue timeout, `502` upstream unreachable (with state reset), `503` shutting down, `500` unexpected pipeline exception (queue still released). Save and restore failures are logged, never returned.

**Wire:** a top-level try/except in `handle_request`.

**Tests:** one test per row of the table; an injected exception in a feature module still releases the queue and the next request works.

### Phase 22: End-to-end scenarios and spec audit

**Build:** scenario tests, plus a wiring audit. Scenarios: (1) new chat saved, proxy restarted with the same ledger, same chat restored before its chat call; (2) chat A, B, A in one run: no restore for the second A; (3) model switch; (4) unload; (5) upstream down then up; (6) bin deleted by hand, then startup removes the entry. Audit: `SPEC_TRACE.md` maps each spec section to a test name, and `tests/test_audit.py` fails when a mapped test is missing or when an entry function of a feature module (`restore`, `save`, `evict`, `delta`, `validate`, `startup`, `shutdown`) has no call site in `pipeline.py`, `app.py` or `main.py`.

**Wire:** audit runs in the normal test suite.

**Tests:** the scenarios and the audit itself (the audit fails if you comment out one call).

### Phase 23: Manual check against real llama-server

**Build:** nothing. Run the proxy in front of the real router and tick these off, fixing the spec or code where reality differs: `status.args` contents (explicit flags only, or defaults too, and when the model is unloaded); the exact `timings` field names, including in the final SSE chunk; restore of a bad bin returns `400`; the default slot count when `--parallel` is not passed; the `--models-max 1` flag name; save then restore of a real chat shortens its next prefill; `--slot-save-path` is writable from the proxy host.

**Tests:** a written pass or fail note per item in `PROGRESS.md`.
