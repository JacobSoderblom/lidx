class EventStore:
    """A domain class whose `append` method shares a bare name with
    `list.append` — the exact collision pattern from issue #45's
    measurement (InMemoryEventStore.append had 564 false callers, all
    plain `list.append` through unrelated local variables)."""

    def __init__(self):
        self._events = []

    def append(self, event):
        self._events.append(event)
        return self

    def flush(self):
        self.append(None)
        return self._events
