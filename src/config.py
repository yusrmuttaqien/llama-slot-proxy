"""config.json handling: defaults, self-documenting $docs, validation."""
import json
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

DEFAULTS = {
    "upstream_url": "http://127.0.0.1:8080",
    "listen_host": "127.0.0.1",
    "listen_port": 8081,
    "save_time_threshold": 300.0,
    "delta_context_threshold": 2000,
    "save_timeout": 120.0,
    "bin_max_total_gb": 50.0,
    "bin_padding_percent": 10.0,
    "queue_timeout": 0.0,
    "queue_limit": 0,
}

DOCS = {
    "upstream_url": "string (URL). Base URL of the llama-server router the proxy forwards to, e.g. http://127.0.0.1:8080.",
    "listen_host": "string (address). Interface the proxy binds to. Default 127.0.0.1 (local only).",
    "listen_port": "int (port, 0-65535). TCP port the proxy listens on.",
    "save_time_threshold": "float (seconds). Save a chat after a request when its last save is older than this.",
    "delta_context_threshold": "int (tokens). Save a chat after a request when that request added more than this many tokens (timings.prompt_n + timings.predicted_n).",
    "save_timeout": "float (seconds). Max time to wait for one slot save or restore call before logging an error and moving on.",
    "bin_max_total_gb": "float (GiB, 1024^3 bytes). When total saved bin size exceeds this, the oldest bins are deleted before the next save.",
    "bin_padding_percent": "float (percent, 0-100). Eviction continues until total size is below bin_max_total_gb * (1 - percent/100).",
    "queue_timeout": "float (seconds, 0 = indefinite). Max time a request may wait in the proxy queue (not counting execution).",
    "queue_limit": "int (requests, 0 = unlimited). Max requests waiting in the queue; the in-flight request is not counted.",
}

_INT_KEYS = {"listen_port", "delta_context_threshold", "queue_limit"}
_FLOAT_KEYS = {
    "save_time_threshold", "save_timeout", "bin_max_total_gb",
    "bin_padding_percent", "queue_timeout",
}
_STR_KEYS = {"upstream_url", "listen_host"}


class ConfigError(Exception):
    pass


def default_document():
    doc = {"$docs": dict(DOCS)}
    doc.update(DEFAULTS)
    return doc


def write_config(path, doc=None):
    doc = doc if doc is not None else default_document()
    d = os.path.dirname(os.path.abspath(path))
    os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")


def generate_config(path):
    """--generate-config: write a fresh config.json. Refuses to overwrite."""
    if os.path.exists(path):
        raise ConfigError(f"{path} already exists; not overwriting")
    write_config(path)
    return path


def _validate(raw):
    cfg = dict(DEFAULTS)
    for key, value in raw.items():
        if key == "$docs" or key not in DEFAULTS:
            continue
        if key in _STR_KEYS:
            if not isinstance(value, str) or not value:
                raise ConfigError(f"config '{key}' must be a non-empty string")
        elif key in _INT_KEYS:
            if isinstance(value, bool) or not isinstance(value, int):
                raise ConfigError(f"config '{key}' must be an integer")
        else:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ConfigError(f"config '{key}' must be a number")
            value = float(value)
        cfg[key] = value
    if not cfg["upstream_url"].startswith(("http://", "https://")):
        raise ConfigError("config 'upstream_url' must start with http:// or https://")
    if not 0 <= cfg["listen_port"] <= 65535:
        raise ConfigError("config 'listen_port' must be 0-65535")
    for key in ("save_time_threshold", "delta_context_threshold", "save_timeout",
                "bin_max_total_gb", "queue_timeout", "queue_limit"):
        if cfg[key] < 0:
            raise ConfigError(f"config '{key}' must be >= 0")
    if not 0 <= cfg["bin_padding_percent"] <= 100:
        raise ConfigError("config 'bin_padding_percent' must be 0-100")
    return cfg


def load_config(path, log=None):
    """Load config; if missing or corrupt, create a default file there."""
    raw = None
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            raise ValueError("top level is not an object")
    except FileNotFoundError:
        raw = None
        reason = "missing"
    except (ValueError, OSError) as e:
        raw = None
        reason = f"corrupt ({e})"
    if raw is None:
        write_config(path)
        msg = f"config file {path} {reason}; created default"
        if log:
            log.warn(msg)
        raw = default_document()
    return _validate(raw)
