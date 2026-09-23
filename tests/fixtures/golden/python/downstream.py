from caller import entry


def use_entry() -> str:
    """Calls into `caller.py`'s `entry`, giving the incremental golden test
    an incoming edge to exercise: editing `caller.py` alone (content
    changes, no call sites move) re-parses its symbols with new ids, which
    would leave this edge dangling without `repair_dangling_symbol_ids` +
    `resolve_null_target_edges` running after the sync -- see
    tests/golden_python.rs's incremental test."""
    return entry()
