"""Per-model slot-save-path discovery from GET /v1/models (status.args)."""
from src import validate


def get_arg(args, *names):
    """Value of the first matching flag in an args list; '' if flag has no value; None if absent."""
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


async def fetch_paths(ctx):
    """Fetch /v1/models once, fill tracked paths, run setup validation. Never raises."""
    try:
        data = await ctx.upstream.get_json("/v1/models")
    except Exception as e:  # network error, HTTP error, bad JSON
        ctx.log.error(f"could not fetch slot-save-path from /v1/models: {e}")
        return False
    models = data.get("data") if isinstance(data, dict) else None
    if not isinstance(models, list):
        ctx.log.error("unexpected /v1/models response (no data array)")
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
    validate.run(ctx, data)
    return True


async def ensure_path(ctx, model_id):
    """Tracked path for a model; retries the fetch when it is unknown (None)."""
    if ctx.state.tracked_paths.get(model_id) is None:
        await fetch_paths(ctx)
    return ctx.state.tracked_paths.get(model_id)
