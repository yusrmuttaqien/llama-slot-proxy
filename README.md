# llama-slot-proxy

A slim proxy in front of `llama-server` (router mode, one loaded model, `--parallel 1`) that
lazily saves and restores the KV-cache slot per chat, so switching chats or restarting does not
force a full prompt prefill. See the spec for behavior; `SPEC_TRACE.md` maps spec sections to tests.

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

## llama-server setup this proxy expects

- router mode, one model loaded at a time (`--models-max 1`; verify the flag name on your build)
- `--parallel 1` (or `-np 1`), `--slot-save-path` set and readable/writable by the proxy host
- `--sleep-idle-seconds` unset or 0
- the proxy only warns (console) when these look wrong; it never refuses to run

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

## Notes on choices the spec left open

- Status codes: 429 queue full, 504 queue wait timeout, 502 upstream unreachable, 503 shutting down, 500 internal error.
- A chat with no ledger entry counts as never saved, so its first completed response is saved.
- When a save moves a bin to a new slot-save-path, the old bin is deleted after the new save succeeds.
- `bin_max_total_gb` is GiB (1024^3 bytes).
