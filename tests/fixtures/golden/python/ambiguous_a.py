def run() -> None:
    """Same name as `ambiguous_b.run` — a bare `run()` call site with no
    matching import in either direction is genuinely ambiguous between the
    two."""
    return None
