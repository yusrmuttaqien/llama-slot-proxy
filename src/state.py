"""Runtime (in-memory) state. Lost on restart by design."""


class State:
    def __init__(self):
        self.active_model = ""
        self.active_chat = ""        # entry id believed to be in the slot, "" = none/noop
        self.active_chat_id = ""     # chat id of active_chat (needed to create a ledger entry)
        self.loaded_chats = set()    # entry ids seen or restored since start/reset
        self.tracked_paths = {}      # model id -> slot-save-path (None = unknown)

    def set_active(self, classified):
        if classified.model_id:
            self.active_model = classified.model_id
        self.active_chat = classified.entry_id or ""
        self.active_chat_id = classified.chat_id if classified.entry_id else ""

    def reset(self):
        """Forget what we believe is in the slot (model change, unload, upstream down)."""
        self.active_model = ""
        self.active_chat = ""
        self.active_chat_id = ""
        self.loaded_chats = set()
