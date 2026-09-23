class Greeter:
    """Method call on self: `greet` calls `format` through `self.`."""

    def greet(self, name: str) -> str:
        return self.format(name)

    def format(self, name: str) -> str:
        return f"Hello, {name}"
