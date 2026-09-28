def local_util() -> str:
    """Decoy: same name as `caller.local_util`, in a different module, so
    `caller.entry`'s plain same-module call must resolve via its own
    module's exact qualname -- not merely because the name happens to be
    unique repo-wide."""
    return "other"


def format_greeting(name: str) -> str:
    """Decoy: same name as `helper.format_greeting`, so
    `call_imported_helper`'s `from helper import format_greeting` binding
    must resolve to `helper`'s definition specifically, not just the only
    `format_greeting` in the repo."""
    return f"Other, {name}"
