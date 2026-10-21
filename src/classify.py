"""Pure request classification (spec section 5.1, 3.1)."""
import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

# Recognized chat-identification headers; first one present (in this order) wins.
# Add more names here.
RECOGNIZED_HEADERS = ["x-session-affinity"]

# Cheap GET endpoints that never touch the slot: forwarded without queueing.
BYPASS_ENDPOINTS = {
    ("GET", "/health"),
    ("GET", "/v1/health"),
    ("GET", "/props"),
    ("GET", "/metrics"),
    ("GET", "/slots"),
    ("GET", "/models"),
    ("GET", "/v1/models"),
    ("GET", "/models/sse"),
}

CHAT_COMPLETIONS_PATH = "/v1/chat/completions"
UNLOAD_PATH = "/models/unload"


class Kind(Enum):
    BYPASS = "bypass"
    PLAIN = "plain"
    NOOP = "noop"
    TRACKED = "tracked"
    UNLOAD = "unload"


@dataclass
class Classified:
    kind: Kind
    model_id: Optional[str] = None
    chat_id: Optional[str] = None
    entry_id: Optional[str] = None
    stream: bool = False


def make_entry_id(chat_id, model_id):
    return hashlib.md5(f"{chat_id}_{model_id}".encode("utf-8")).hexdigest()


def find_chat_id(headers):
    lower = {k.lower(): v for k, v in headers.items()}
    for name in RECOGNIZED_HEADERS:
        value = lower.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _parse_body(body):
    if not body:
        return None
    try:
        data = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _model_of(data):
    if data is None:
        return None
    model = data.get("model")
    return model if isinstance(model, str) and model else None


def classify(method, path, headers, body):
    method = method.upper()
    if (method, path) in BYPASS_ENDPOINTS:
        return Classified(Kind.BYPASS)
    if method == "POST" and path == UNLOAD_PATH:
        model = _model_of(_parse_body(body))
        if model:
            return Classified(Kind.UNLOAD, model_id=model)
        return Classified(Kind.PLAIN)
    if method == "POST" and path == CHAT_COMPLETIONS_PATH:
        data = _parse_body(body)
        model = _model_of(data)
        chat_id = find_chat_id(headers)
        stream = bool(data.get("stream") is True) if data else False
        if model and chat_id:
            return Classified(Kind.TRACKED, model_id=model, chat_id=chat_id,
                              entry_id=make_entry_id(chat_id, model), stream=stream)
        return Classified(Kind.NOOP, model_id=model, chat_id=chat_id, stream=stream)
    return Classified(Kind.PLAIN)
