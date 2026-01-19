"""Core module doc."""

from . import utils
from .utils import Helper as H

class Base:
    pass

class Greeter(Base):
    """Greeter doc."""
    def greet(self, name: str) -> str:
        "Greets someone."
        return f"Hi {name}"

def make_greeter():
    return Greeter()
