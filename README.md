# llama-slot-proxy

A slim proxy in front of `llama-server` (router mode, one loaded model, `--parallel 1`) that
lazily saves and restores the KV-cache slot per chat, so switching chats or restarting does not
force a full prompt prefill. It works with any upstream that exposes the same
`POST /slots/{id}?action=save|restore` and `POST /v1/chat/completions` endpoints — llama-server,
and NInfer. See the spec for behavior; `SPEC_TRACE.md` maps spec sections to tests.

## Limitations and trade-offs

Everything below is a constraint you must accept to use the proxy. The proxy only warns about setup issues; it never refuses to run. Full detail in `SPEC.md` §1.2 and §1.3.

**llama-server setup (required):**
- Router mode, one model loaded at a time (`--models-max 1`; verify the flag name on your build)
- Single slot: `--parallel 1` (or `-np 1`) — the proxy hard-codes slot `0`
- `--slot-save-path` set and readable/writable by the proxy host; the proxy and llama-server share that filesystem
- If the upstream does not expose `--slot-save-path` in `/v1/models` `status.args` (e.g. NInfer), set `slot_save_path` in `config.json` instead — the directory the upstream writes slot saves into, still shared with the proxy host
- **NInfer:** the engine assigns each conversation to the first *vacant* catalog cell (not always cell 0). The proxy handles this by erasing slot 0 before publishing a new conversation (6.1.1), so the new one lands in cell 0 and the proxy's fixed slot-0 addressing stays correct. Requires `--host-state-slots` ≥ 1 (the catalog size).
- `--sleep-idle-seconds` unset or 0 — sleep discards the KV cache behind the proxy's back

**How the proxy works (by design):**
- Only `POST /v1/chat/completions` is tracked; every other endpoint is plain-forwarded
- A request is only saved/restored if it carries the `x-session-affinity` header and a `model` in the body — otherwise it's a noop
- Single-flight FIFO queue: only one request reaches llama-server at a time, so concurrent work serializes (cheap GETs are exempt)
- Saves are lazy: a chat is persisted only on the time/delta/model-change/unload/shutdown triggers, not immediately
- Dumb restore, no validation: if a bin no longer matches the request it degrades, and the next save re-syncs it
- **New conversations vacate slot 0:** before forwarding a brand-new chat, the proxy erases the previous occupant of slot 0 (using the tracked `session_digest` as an atomic `if_digest` precondition) so the engine publishes the new conversation into slot 0. The previous conversation is already saved to disk, so nothing is lost. If the erase fails (timeout, busy), the proxy continues best-effort; in the rare case the new conversation lands in a non-zero cell, the save may write the wrong session's bytes (a known limitation, but the erase almost always succeeds).
- A→B→A within one run relies on llama-server's RAM cache (`--cache-ram`), not on the proxy
- Unsaved progress is lost if another chat takes the slot and the first never returns
- Non-chat slot endpoints (`/v1/completions`, `/infill`, ...) can silently replace slot contents
- `POST /models/load` is not intercepted
- A lost or recreated-empty ledger leaves orphan bins the proxy can't discover
- The proxy can't see a llama-server restart that happens between requests

## Run

```
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
./venv/bin/python main.py --generate-config      # writes config.json (edit upstream_url etc.)
./venv/bin/python main.py --verbose              # point your clients at listen_host:listen_port
```

Clients identify a chat with the header `x-session-affinity: <chat id>` and a `model` in the body of
`POST /v1/chat/completions`. Anything else is forwarded untouched (still queued, except cheap GETs).

Flags: `--config-file PATH`, `--ledger-file PATH`, `--generate-config`, `--verbose`.
Files `config.json` and `ledger.json` live beside `main.py` by default.

## Tests

```
./venv/bin/python -m pytest
```

Tests run the real proxy in front of a fake llama-server (`tests/fake_upstream.py`) and assert on
the calls it records. `tests/test_audit.py` fails if a feature module is not wired into the pipeline.

## Layout

`main.py` entry point; `src/pipeline.py` is the single request pipeline that calls every feature
module (`classify`, `reqqueue`, `restore`, `save`, `evict`, `delta`, `slotpath`, `validate`,
`startup`, `shutdown`, `ledger`, `state`, `upstream`, `config`, `log`).
