class Col:
    def to_def(self): ...
    def with_nullable(self, v): ...


class Table:
    def __init__(self):
        self._columns: dict[str, Col] = {}
        self._names: list[str] = []

    def defs(self):
        return tuple(c.to_def() for c in self._columns.values())

    def by_name(self):
        return {n: c.to_def() for n, c in self._columns.items()}

    def name_calls(self):
        return [n.to_def() for n in self._names]


def nullable(columns: list[Col]):
    return [c.with_nullable(True) for c in columns]


def each(columns: list[Col]):
    for c in columns:
        c.to_def()


def numbered(columns: list[Col]):
    for i, c in enumerate(columns):
        c.with_nullable(i)


def reassigned(items: list[Col]):
    columns: list[Col] = list(items)
    columns = [c if c.to_def() else c.with_nullable(True) for c in columns]
    return columns


def untyped(columns):
    return [c.to_def() for c in columns]


def not_a_class(numbers: list[int]):
    for n in numbers:
        n.to_def()
