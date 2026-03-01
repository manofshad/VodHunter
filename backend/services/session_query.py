class SessionQueryService:
    def __init__(self, store):
        self.store = store

    def list_live_sessions(self, limit: int, offset: int):
        return self.store.list_live_sessions(limit=limit, offset=offset)
