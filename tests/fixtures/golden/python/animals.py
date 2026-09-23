class Animal:
    """Base class declaring the method `Dog` inherits without overriding."""

    def speak(self) -> str:
        return "..."


class Dog(Animal):
    """No `speak` override — a call through a `Dog`-typed receiver must
    dispatch to `Animal.speak` via the inheritance tier."""

    def bark(self) -> str:
        return "Woof"
