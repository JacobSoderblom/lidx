from typing import Self


class Col:
    def to_def(self): ...
    def with_nullable(self, v): ...
    def copy(self) -> "Col": ...
    def same(self) -> Self: ...


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
    columns = sorted(columns, key=len)
    columns = [c for c in columns if c.to_def()]
    return [c.with_nullable(True) for c in columns]


def reassigned_to_other(xs: list[Col], other):
    xs = other
    return [x.to_def() for x in xs]


def reassigned_to_new_list(xs: list[Col]):
    xs = [Table()]
    return [x.with_nullable(True) for x in xs]


def reassigned_by_mapping(xs: list[Col]):
    xs = [x.copy() for x in xs]
    return [x.to_def() for x in xs]


def declared_class_survives_reassignment(item: Col, other):
    item = other
    item.to_def()


def undeclared_class_collapses():
    thing = Col()
    thing = Table()
    thing.to_def()


def make(c) -> Col: ...


def comprehension_body_type(columns: list[Col]):
    kept = [c if c.to_def() else c.copy() for c in columns]
    for k in kept:
        k.with_nullable(1)
    chained = tuple(c.same() for c in columns)
    for ch in chained:
        ch.with_nullable(2)
    built = [make(c) for c in columns]
    for b in built:
        b.copy()


def comprehension_body_unknown(columns: list[Col]):
    mixed = [c if c.to_def() else Table() for c in columns]
    for m in mixed:
        m.with_nullable(3)


def nested_def_has_its_own_scope(text):
    def gen():
        for to_def in text:
            to_def.to_def()

    return gen


def clause_targets(items: list[Col]):
    return [y.to_def() for y in items for y in y.zz]


def untyped(columns):
    return [c.to_def() for c in columns]


def not_a_class(numbers: list[int]):
    for n in numbers:
        n.to_def()


class _Builder:
    def build(self): ...


def private_class_in_nested_def():
    def wrap(cls):
        builder = _Builder()
        return builder.build()

    return wrap
