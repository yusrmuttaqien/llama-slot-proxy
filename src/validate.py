"""Warn-only setup validation (spec 3.4). Runs on the /v1/models response."""
import os


def check(models_response):
    """Return a list of warning strings."""
    from src.slotpath import get_arg
    warnings = []
    models = models_response.get("data") if isinstance(models_response, dict) else None
    if not isinstance(models, list) or not models:
        return ["setup check: /v1/models returned no models"]
    entries = [m for m in models if isinstance(m, dict)]
    with_status = [m for m in entries if isinstance(m.get("status"), dict)]
    if not with_status:
        warnings.append("setup check 1: models have no 'status' object; llama-server is "
                        "probably not in router mode")
    loaded = [m.get("id") for m in with_status if m["status"].get("value") == "loaded"]
    if len(loaded) > 1:
        warnings.append(f"setup check 2: {len(loaded)} models are loaded ({', '.join(map(str, loaded))}); "
                        "only one loaded model is supported (router --models-max 1)")
    for m in with_status:
        name = m.get("id")
        args = m["status"].get("args")
        if not isinstance(args, list):
            continue  # e.g. unloaded model: checks 3-5 wait for a later fetch
        par = get_arg(args, "--parallel", "-np")
        if par != "1":
            warnings.append(f"setup check 3 ({name}): --parallel/-np is "
                            f"{'missing' if par is None else repr(par)}; single slot (1) is required")
        sleep = get_arg(args, "--sleep-idle-seconds")
        if sleep is not None:
            try:
                bad = float(sleep) > 0
            except ValueError:
                bad = True
            if bad:
                warnings.append(f"setup check 4 ({name}): --sleep-idle-seconds is {sleep!r}; sleep discards "
                                "the KV cache behind the proxy's back, it must be unset or 0")
        path = get_arg(args, "--slot-save-path")
        if not path:
            warnings.append(f"setup check 5 ({name}): --slot-save-path is missing; slot save/restore "
                            "would return 501")
        elif not os.path.isdir(path):
            warnings.append(f"setup check 5 ({name}): slot-save-path {path} does not exist on this host")
        elif not os.access(path, os.W_OK):
            warnings.append(f"setup check 5 ({name}): slot-save-path {path} is not writable by the proxy")
    return warnings


# Check 6: an upstream that exposes no status.args at all (not llama-server in router mode,
# e.g. NInfer) can only be handled through config slot_save_path.

def check6(models_response, config=None):
    """Return warning strings for check 6 (upstream without status.args, no config path)."""
    models = models_response.get("data") if isinstance(models_response, dict) else None
    if not isinstance(models, list) or not models:
        return []
    entries = [m for m in models if isinstance(m, dict)]
    if any(isinstance(m.get("status"), dict) for m in entries):
        return []
    has_config = isinstance(config, dict) and bool(config.get("slot_save_path"))
    if has_config:
        return []
    return ["setup check 6: /v1/models exposes no status object (the upstream is not a llama-server "
            "in router mode, e.g. NInfer) and config slot_save_path is unset; saves for tracked "
            "chats will be skipped. Set slot_save_path in config.json to the directory the upstream "
            "writes slot saves into."]


def run(ctx, models_response):
    for w in check(models_response):
        ctx.log.warn(w)
    for w in check6(models_response, ctx.config):
        ctx.log.warn(w)
