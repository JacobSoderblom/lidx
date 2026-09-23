class Animal:
    """Base class declaring the method `Dog` inherits without overriding."""

    def speak(self) -> str:
        return "..."


class Dog(Animal):
    """No `speak` override — a call through a `Dog`-typed receiver must
    dispatch to `Animal.speak` via the inheritance tier."""

    def bark(self) -> str:
        return "Woof"


class Cat:
    """Decoy: an unrelated class with its own `speak`, so
    `call_inherited`'s `d: Dog` annotation must dispatch via `Dog`'s
    recorded EXTENDS edge to `Animal.speak` specifically -- not just land
    on the only other `speak` in the repo."""

    def speak(self) -> str:
        return "Meow"
