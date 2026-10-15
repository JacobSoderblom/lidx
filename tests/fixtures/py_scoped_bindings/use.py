from pkg.dp import table
from pkg.ds import table as ds_table


def build(rows):
    first = table("a")
    second = ds_table("b")
    return [f"{table}:{op}" for table, op in rows], first, second


def with_callback(handlers):
    return [ds_table(h) for h in handlers], sorted(handlers, key=lambda ds_table: ds_table)


def shadowed(callbacks):
    return [table(1) for table in callbacks]
