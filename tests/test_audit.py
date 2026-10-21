"""Wiring audit: every spec section has tests, and every feature module is actually called."""
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


def _all_test_names():
    names = set()
    for fn in os.listdir(os.path.join(ROOT, "tests")):
        if fn.startswith("test_") and fn.endswith(".py"):
            names |= set(re.findall(r"^(?:async )?def (test_\w+)\(", _read(f"tests/{fn}"), re.M))
    return names


def test_every_traced_test_exists():
    names = _all_test_names()
    rows = [l for l in _read("SPEC_TRACE.md").splitlines() if l.startswith("| ") and "test_" in l]
    assert len(rows) >= 15
    missing = []
    for row in rows:
        for name in re.findall(r"test_\w+", row):
            if name not in names:
                missing.append(name)
    assert missing == []


# (call text, files that must contain it)
CALL_SITES = [
    ("classify(", ["src/pipeline.py"]),
    ("DeltaTap()", ["src/pipeline.py"]),
    ("restore.maybe_restore(", ["src/pipeline.py"]),
    ("save.save_entry(", ["src/pipeline.py", "src/shutdown.py"]),
    ("slotpath.fetch_paths(", ["src/pipeline.py"]),
    ("slotpath.ensure_path(", ["src/save.py"]),
    ("evict.enforce_limit(", ["src/save.py"]),
    ("validate.run(", ["src/slotpath.py"]),
    ("run_startup_checks(", ["main.py"]),
    ("graceful_shutdown(", ["main.py"]),
    ("SignalHandler(", ["main.py"]),
    ("start_server(", ["main.py"]),
    ("ctx.queue.acquire(", ["src/pipeline.py"]),
    ("handle_request(", ["src/app.py"]),
    ("ledger.load(", ["main.py"]),
    ("load_config(", ["main.py"]),
]


def test_every_feature_has_a_call_site():
    missing = [(call, f) for call, files in CALL_SITES for f in files if call not in _read(f)]
    assert missing == []


def test_state_updates_and_trigger_calls_live_in_the_pipeline():
    src = _read("src/pipeline.py")
    for needle in ("st.set_active(", "_model_change(", "_handle_unload(", "_save_triggers(", "st.reset()"):
        assert needle in src, needle
