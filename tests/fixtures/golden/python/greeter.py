class Greeter:
    """Method call on self: `greet` calls `format` through `self.`."""

    def greet(self, name: str) -> str:
        return self.format(name)

    def format(self, name: str) -> str:
        return f"Hello, {name}"


class LoudGreeter:
    """Decoy: a second, unrelated `greet` (and `format`), so
    `call_receiver_typed`'s `g: Greeter` annotation must pin
    `Greeter.greet` specifically via receiver-type resolution -- not just
    land on the only `greet` in the repo."""

    def greet(self, name: str) -> str:
        return f"HELLO {name}"

    def format(self, name: str) -> str:
        return f"LOUD, {name}"
