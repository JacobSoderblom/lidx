//! A Python function (or class) used as a value -- passed, stored, returned or
//! assigned -- is a `REFERENCES` edge from the enclosing symbol, resolved like
//! a call to the same text. Locals, plain data and names already called are not.

mod common;

use common::{edge_targets, ref_targets, unresolved_accounting};

const FILES: &[(&str, &str)] = &[
    (
        "app/util.py",
        "def helper(x):\n    return x\n\n\ndef other(x):\n    return x\n\n\nLIMIT = 10\n",
    ),
    (
        "app/main.py",
        r#"import app.util as util
from app.util import helper, other, LIMIT


def handler(x):
    return x


def second(x):
    return x


def register(cb, key=None):
    return cb


def positional():
    register(handler)


def keyword():
    register(None, key=handler)


def in_list():
    return [handler, second]


def in_tuple():
    x = (handler, second)
    return x


def in_set():
    s = {handler}
    return s


def in_dict():
    d = {'a': handler, 'b': second}
    return d


def returned():
    return handler


def returned_pair():
    return handler, second


def assigned():
    cb = handler
    return cb


def imported():
    register(helper)
    register(util.other)


class Svc:
    def run(self):
        return 1

    def go(self):
        register(self.run)
        return {'k': 1}

    def table(self):
        return {'k': self.run}


def class_value():
    register(Svc)


TABLE = {'h': handler}
register(second)


def negatives(handler_arg, other_local):
    local = 3
    register(local)
    register(handler_arg)
    register(LIMIT)
    register(other)
    handler_arg = other_local
    return local


def called():
    handler(1)
    register(handler)
    return handler


def shadowed():
    handler = 5
    register(handler)
"#,
    ),
];

#[test]
fn argument_shapes() {
    assert_eq!(
        ref_targets(FILES, "app.main.positional"),
        ["app.main.handler"]
    );
    assert_eq!(ref_targets(FILES, "app.main.keyword"), ["app.main.handler"]);
}

#[test]
fn collection_return_and_assignment_shapes() {
    for (caller, want) in [
        (
            "app.main.in_list",
            vec!["app.main.handler", "app.main.second"],
        ),
        (
            "app.main.in_tuple",
            vec!["app.main.handler", "app.main.second"],
        ),
        ("app.main.in_set", vec!["app.main.handler"]),
        (
            "app.main.in_dict",
            vec!["app.main.handler", "app.main.second"],
        ),
        ("app.main.returned", vec!["app.main.handler"]),
        (
            "app.main.returned_pair",
            vec!["app.main.handler", "app.main.second"],
        ),
        ("app.main.assigned", vec!["app.main.handler"]),
    ] {
        assert_eq!(ref_targets(FILES, caller), want, "{caller}");
    }
}

#[test]
fn attribute_forms() {
    assert_eq!(ref_targets(FILES, "app.main.Svc.go"), ["app.main.Svc.run"]);
    assert_eq!(
        ref_targets(FILES, "app.main.Svc.table"),
        ["app.main.Svc.run"]
    );
    let t = ref_targets(FILES, "app.main.imported");
    assert_eq!(t, ["app.util.helper", "app.util.other"], "{t:?}");
}

#[test]
fn class_values_and_module_level_sources() {
    assert_eq!(ref_targets(FILES, "app.main.class_value"), ["app.main.Svc"]);
    let t = ref_targets(FILES, "app.main");
    assert_eq!(t, ["app.main.handler", "app.main.second"], "{t:?}");
}

#[test]
fn locals_params_data_and_calls_are_not_references() {
    // `local`, `handler_arg`, `LIMIT` (a constant) never bind; `other` is a
    // function from a different module imported by name and does bind.
    let t = ref_targets(FILES, "app.main.negatives");
    assert_eq!(t, ["app.util.other"], "{t:?}");
    // `handler(1)` is a CALLS edge, so its pair gets no REFERENCES edge.
    assert!(ref_targets(FILES, "app.main.called").is_empty());
    assert_eq!(
        edge_targets(FILES, "app.main.called", "CALLS")
            .into_iter()
            .filter(|t| t == "app.main.handler")
            .count(),
        1
    );
    // A local rebinding of the name shadows the module function.
    assert!(ref_targets(FILES, "app.main.shadowed").is_empty());
}

#[test]
fn unresolved_value_references_are_not_counted() {
    unresolved_accounting(FILES).assert_value_references_not_counted();
}

const SHADOWS: &[(&str, &str)] = &[(
    "app/shadow.py",
    r#"def handler(x):
    return x


def h(x):
    return x


def register(cb):
    return cb


def comprehension(handlers):
    return [register(h) for h in handlers]


def dict_comprehension(pairs):
    return {k: register(v) for k, v in pairs}


def walrus(xs):
    if (handler := xs[0]):
        register(handler)


MODULE_LEVEL = [register(h) for h in range(3)]
MODULE_LAMBDA = lambda handler: register(handler)
"#,
)];

#[test]
fn comprehension_variables_and_walrus_names_are_locals() {
    for caller in ["comprehension", "dict_comprehension", "walrus"] {
        let t = ref_targets(SHADOWS, &format!("app.shadow.{caller}"));
        assert!(t.is_empty(), "{caller}: {t:?}");
    }
    // Module-level expressions have no function scope to fold bindings into.
    let t = ref_targets(SHADOWS, "app.shadow");
    assert!(t.is_empty(), "{t:?}");
}
