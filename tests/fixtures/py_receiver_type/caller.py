from store import EventStore


def build_events(event_store: EventStore):
    """A genuine call through an annotated parameter — must resolve to
    EventStore.append, not stay NULL and not bind to some other `append`."""
    event_store.append(1)
    return event_store


def collect():
    """`cells` is a plain list local. Calling `.append` on it must NOT bind
    to EventStore.append (the false-positive pattern this fix removes)."""
    cells = []
    cells.append(1)
    return cells
