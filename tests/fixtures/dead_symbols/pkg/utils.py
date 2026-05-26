def helper_used():
    """Called by live_function in app.py — not dead."""
    return "used"


def helper_unused():
    """Never referenced — should appear in dead_symbols."""
    return "unused"
