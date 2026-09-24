class Widget:
    """The only `process` in this fixture -- a method-kind symbol. Before
    #75, a receiver-less call to the same name would fuzzy-bind to this via
    the bare-name fallback; #75 bars a bare call from binding to any
    method-kind candidate."""

    def process(self):
        return "widget"


def bare_caller():
    """Bare call, no receiver: process is not declared or imported into
    this module. Must stay UNRESOLVED."""
    return process()
