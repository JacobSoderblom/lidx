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


def process_all(funcs):
    """`acc` is a *lambda* parameter, not a local of `process_all` itself.
    A lambda never gets its own scope in the extractor (unlike `def`), so
    a call through one of its own parameters must still not bind to
    EventStore.append — without folding lambda params into the enclosing
    scope, `acc` would look like an untracked name and fall through to the
    bare-name tier exactly like `pkg.module.Class.method()` is meant to."""
    result = []
    walk_all(funcs, lambda n, acc=result: acc.append(n))
    return result


class Registry:
    """A class-level list attribute accessed through the class name, not
    `self` — the exact shape of the false positive found in dpb's
    `_FakeCredential.instances.append(self)`: a chained attribute off a
    bare class reference. Must NOT bind to EventStore.append."""

    instances = []

    def __init__(self):
        Registry.instances.append(self)
