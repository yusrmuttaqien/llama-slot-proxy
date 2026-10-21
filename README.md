# llama-slot-proxy

A slim proxy in front of `llama-server` (router mode, one loaded model, `--parallel 1`) that
lazily saves and restores the KV-cache slot per chat, so switching chats or restarting does not
force a full prompt prefill. See the spec for behavior; `SPEC_TRACE.md` maps spec sections to tests.

## Limitations and trade-offs

Everything below is a constraint you must accept to use the proxy. The proxy only warns about setup issues; it never refuses to run. Full detail in `SPEC.md` §1.2 and §1.3.

**llama-server setup (required):**
- Router mode, one model loaded at a time (`--models-max 1`; verify the flag name on your build)
- Single slot: `--parallel 1` (or `-np 1`) — the proxy hard-codes slot `0`
- `--slot-save-path` set and readable/writable by the proxy host; the proxy and llama-server share that filesystem
- `--sleep-idle-seconds` unset or 0 — sleep discards the KV cache behind the proxy's back

**How the proxy works (by design):**
- Only `POST /v1/chat/completions` is tracked; every other endpoint is plain-forwarded
- A request is only saved/restored if it carries the `x-session-affinity` header and a `model` in the body — otherwise it's a noop
- Single-flight FIFO queue: only one request reaches llama-server at a time, so concurrent work serializes (cheap GETs are exempt)
- Saves are lazy: a chat is persisted only on the time/delta/model-change/unload/shutdown triggers, not immediately
- Dumb restore, no validation: if a bin no longer matches the request it degrades, and the next save re-syncs it
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
