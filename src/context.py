"""AppContext: everything the pipeline needs, built once and passed around (no globals)."""
from dataclasses import dataclass, field

from src.ledger import Ledger
from src.log import Logger
from src.reqqueue import RequestQueue
from src.state import State
from src.upstream import UpstreamClient


@dataclass
class AppContext:
    config: dict
    log: Logger
    ledger: Ledger
    state: State
    upstream: UpstreamClient
    queue: RequestQueue
    initialized: bool = False        # first-request fetch done?
    shutting_down: bool = False
    bg_tasks: set = field(default_factory=set)


def build_context(config, log, ledger):
    return AppContext(
        config=config,
        log=log,
        ledger=ledger,
        state=State(),
        upstream=UpstreamClient(config["upstream_url"], config["save_timeout"]),
        queue=RequestQueue(config["queue_timeout"], config["queue_limit"]),
    )
