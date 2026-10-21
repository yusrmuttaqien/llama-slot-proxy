"""aiohttp application factory."""
from aiohttp import web

from src.pipeline import handle_request


def create_app(ctx):
    # client_max_size=0: chat requests with long contexts or images can be large.
    app = web.Application(client_max_size=0)

    async def on_startup(_app):
        await ctx.upstream.start()

    async def on_cleanup(_app):
        await ctx.upstream.close()

    async def handler(request):
        return await handle_request(ctx, request)

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    app.router.add_route("*", "/{tail:.*}", handler)
    return app


async def start_server(ctx, host=None, port=None):
    """Create the runner/site. handler_cancellation=True so a client disconnect cancels the handler."""
    app = create_app(ctx)
    runner = web.AppRunner(app, handler_cancellation=True)
    await runner.setup()
    site = web.TCPSite(runner, host or ctx.config["listen_host"],
                       ctx.config["listen_port"] if port is None else port)
    await site.start()
    return runner, site
