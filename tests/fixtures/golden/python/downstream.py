from caller import entry


def use_entry() -> str:
    """Calls into `caller.py`'s `entry`, giving the incremental golden
    tests in tests/golden_python.rs a genuine cross-file incoming edge:
    it must still resolve to `caller.entry` after an unrelated content
    edit to `caller.py`, and go unresolved (not dangling, not silently
    rebound to something else) after `caller.entry` is renamed or
    `caller.py` is deleted -- even though `downstream.py` itself is never
    resynced in any of those scenarios."""
    return entry()
