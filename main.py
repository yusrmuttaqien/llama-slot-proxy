#!/usr/bin/env python3
"""llama-slot-proxy: slot save/restore proxy in front of llama-server (router mode)."""
import argparse
import asyncio
import sys

from src.app import start_server
from src.config import ConfigError, DEFAULT_CONFIG_PATH, generate_config, load_config
from src.context import build_context
from src.ledger import DEFAULT_LEDGER_PATH, Ledger
from src.log import Logger
from src.shutdown import SignalHandler, graceful_shutdown
from src.startup import run_startup_checks


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="llama-server slot save/restore proxy")
    p.add_argument("--config-file", default=None, help="path to config.json (default: beside main.py)")
    p.add_argument("--ledger-file", default=None, help="path to ledger.json (default: beside main.py)")
    p.add_argument("--generate-config", action="store_true",
                   help="write a default config.json (at --config-file if given) and exit")
    p.add_argument("--verbose", action="store_true", help="log every proxy activity")
    return p.parse_args(argv)


async def run(args):
    log = Logger(verbose=args.verbose)
    config = load_config(args.config_file or DEFAULT_CONFIG_PATH, log)
    ledger = Ledger(args.ledger_file or DEFAULT_LEDGER_PATH, log)
    ledger.load()
    run_startup_checks(ledger, log)          # before the first request can be accepted

    ctx = build_context(config, log, ledger)
    runner, site = await start_server(ctx)
    log.info(f"listening on http://{config['listen_host']}:{config['listen_port']} "
             f"-> {config['upstream_url']}")
    print(f"llama-slot-proxy listening on {config['listen_host']}:{config['listen_port']} "
          f"-> {config['upstream_url']}", flush=True)

    stop = asyncio.Event()
    SignalHandler(asyncio.get_running_loop(), stop, log)
    await stop.wait()
    await graceful_shutdown(ctx)
    await runner.cleanup()
    log.info("bye")


def main(argv=None):
    args = parse_args(argv)
    if args.generate_config:
        path = args.config_file or DEFAULT_CONFIG_PATH
        try:
            generate_config(path)
        except ConfigError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        print(f"wrote {path}")
        return 0
    try:
        asyncio.run(run(args))
    except ConfigError as e:
        print(f"config error: {e}", file=sys.stderr)
        return 2
    except OSError as e:
        print(f"startup error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
