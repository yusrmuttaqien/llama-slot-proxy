"""Per-model slot-save-path discovery from GET /v1/models (status.args), plus
config.json `slot_save_path` as a fallback for upstreams that do not expose
--slot-save-path (e.g. NInfer). A discovered path always wins over the
configured one; a conflict is warned."""
from src import validate


def get_arg(args, *names):
    """Value of the first matching flag in args list; '' if flag has no value; None if absent."""
    if not isinstance(args, list):
        return None
    items = [str(a) for a in args]
    for i, item in enumerate(items):
        for name in names:
            if item == name:
                if i + 1 < len(items) and not items[i + 1].startswith("-"):
                    return items[i + 1]
                if i + 1 < len(items) and _is_number(items[i + 1]):
                    return items[i + 1]
                return ""
            if item.startswith(name + "="):
                return item[len(name) + 1:]
    return None


def _is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def config_path(ctx):
    """slot_save_path from config.json (stripped); None if unset or empty."""
    value = ctx.config.get("slot_save_path")
    if not isinstance(value, str):
        return None
    return value.strip() or None


def apply_config_fallback(ctx):
    """Fill tracked paths that discovery could not resolve from config.
    Discovered paths always win; a conflict is warned."""
    path = config_path(ctx)
    if not path:
        return
    for name, detected in list(ctx.state.tracked_paths.items()):
        if detected is None:
            ctx.state.tracked_paths[name] = path
            ctx.log.info(f"slot-save-path for model {name}: {path} (from config; "
                         f"upstream does not expose --slot-save-path)")
        elif detected != path:
            ctx.log.warn(f"config slot_save_path {path!r} differs from discovered "
                         f"--slot-save-path {detected!r} for {name}; keeping the discovered path")


async def fetch_paths(ctx):
    """Fetch /v1/models once, fill tracked paths, run setup validation. Never raises."""
    try:
        data = await ctx.upstream.get_json("/v1/models")
    except Exception as e:  # network error, HTTP error, malformed JSON
        ctx.log.error(f"could not fetch slot-save-path from /v1/models: {e}")
        apply_config_fallback(ctx)
        return False
    models = data.get("data") if isinstance(data, dict) else None
    if not isinstance(models, list):
        ctx.log.error("unexpected /v1/models response (no data array)")
        apply_config_fallback(ctx)
        return False
    for m in models:
        if not isinstance(m, dict) or not m.get("id"):
            continue
        status = m.get("status") if isinstance(m.get("status"), dict) else {}
        path = get_arg(status.get("args"), "--slot-save-path") or None
        names = [m["id"]] + [a for a in (m.get("aliases") or []) if isinstance(a, str)]
        for name in names:
            ctx.state.tracked_paths[name] = path
        ctx.log.info(f"slot-save-path for model {m['id']}: {path}")
    apply_config_fallback(ctx)
    validate.run(ctx, data)
    return True


async def ensure_path(ctx, model_id):
    """Tracked path for a model; retry fetch when unknown (None), then fall back to config."""
    if ctx.state.tracked_paths.get(model_id) is None:
        await fetch_paths(ctx)
    if ctx.state.tracked_paths.get(model_id) is None:
        path = config_path(ctx)
        if path is not None:
            ctx.state.tracked_paths[model_id] = path
            ctx.log.info(f"slot-save-path for {model_id}: {path} (from config)")
    return ctx.state.tracked_paths.get(model_id)
