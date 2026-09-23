import requests

from greeter import Greeter
from animals import Dog
from helper import format_greeting


def local_util() -> str:
    return "local"


def entry() -> str:
    """Plain call: a bare same-module function call."""
    return local_util()


def call_imported_helper(name: str) -> str:
    """`from x import y` call: `format_greeting` is bound by name only,
    not through an attribute access on `helper`."""
    return format_greeting(name)


def call_receiver_typed(g: Greeter) -> str:
    """Receiver-typed call: `g`'s annotation pins its type, so `.greet`
    must resolve to `Greeter.greet` and nothing else."""
    return g.greet("world")


def call_inherited(d: Dog) -> str:
    """Inherited method call: `Dog` has no `speak` of its own — must
    dispatch to `Animal.speak` via `Dog`'s recorded EXTENDS edge."""
    return d.speak()


def call_ambiguous() -> None:
    """Ambiguous bare name: `run` exists in two unrelated modules and is
    imported from neither here — must stay unresolved, not guess."""
    run()


def call_external() -> None:
    """Call into an external library: `requests` is never indexed (it
    is not part of this fixture repo), so this must stay unresolved."""
    requests.get("https://example.com")
