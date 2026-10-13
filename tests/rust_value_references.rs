//! A Rust function used as a value -- an identifier or path passed as a call
//! argument (also wrapped: `Some(f)`) or given as a struct field's value --
//! is a `REFERENCES` edge from the enclosing symbol. Locals, plain data,
//! callees and unresolvable names are not.

mod common;

use common::{edge_targets, edge_targets_with_resolution, ref_targets, unresolved_accounting};

const FILES: &[(&str, &str)] = &[
    (
        "src/util.rs",
        r#"pub fn helper(x: i32) -> i32 {
    x
}

pub fn other(x: i32) -> i32 {
    x
}

pub const LIMIT: i32 = 10;

pub struct Widget;

impl Widget {
    pub fn build(x: i32) -> i32 {
        x
    }
}
"#,
    ),
    (
        "src/lib.rs",
        r#"mod util;

use crate::util;
use crate::util::{helper, Widget};

fn handler(x: i32) -> i32 {
    x
}

fn second(x: i32) -> i32 {
    x
}

fn apply(f: fn(i32) -> i32, v: i32) -> i32 {
    f(v)
}

pub struct Config {
    pub on_done: fn(i32) -> i32,
    pub fallback: Option<fn(i32) -> i32>,
}

fn argument() -> i32 {
    apply(handler, 1)
}

fn wrapped() -> Option<fn(i32) -> i32> {
    Some(handler)
}

fn imported_name() -> i32 {
    apply(helper, 1)
}

fn path_forms() -> i32 {
    apply(util::other, 1) + apply(Widget::build, 2)
}

fn self_path() -> i32 {
    apply(self::second, 1)
}

fn field_value() -> Config {
    Config { on_done: handler, fallback: Some(second) }
}

fn field_shorthand() -> Config {
    let fallback = None;
    let on_done = handler;
    Config { on_done, fallback }
}

fn shorthand_item() -> Config {
    let fallback = None;
    Config { on_done: handler, fallback }
}

fn method_arg(items: Vec<i32>) -> Vec<i32> {
    items.into_iter().map(handler).collect()
}

struct Svc;

impl Svc {
    fn run(x: i32) -> i32 {
        x
    }

    fn go(&self) -> i32 {
        apply(Self::run, 1) + apply(handler, 2)
    }
}

fn negatives(local: i32, handler_arg: fn(i32) -> i32) -> i32 {
    let second = 5;
    apply(handler_arg, local) + apply(handler_arg, second) + util::LIMIT + apply(util::helper, util::LIMIT)
}

fn shadowed() -> i32 {
    let handler = |x: i32| x + 1;
    apply(handler, 1)
}

fn called() -> i32 {
    handler(1) + apply(handler, 1)
}
"#,
    ),
];

#[test]
fn identifier_and_wrapped_arguments() {
    assert_eq!(ref_targets(FILES, "crate::argument"), ["crate::handler"]);
    assert_eq!(ref_targets(FILES, "crate::wrapped"), ["crate::handler"]);
    assert_eq!(ref_targets(FILES, "crate::method_arg"), ["crate::handler"]);
}

#[test]
fn imported_and_path_arguments() {
    assert_eq!(
        ref_targets(FILES, "crate::imported_name"),
        ["crate::util::helper"]
    );
    assert_eq!(
        ref_targets(FILES, "crate::path_forms"),
        ["crate::util::Widget::build", "crate::util::other"]
    );
    assert_eq!(ref_targets(FILES, "crate::self_path"), ["crate::second"]);
    assert_eq!(
        ref_targets(FILES, "crate::Svc::go"),
        ["crate::Svc::run", "crate::handler"]
    );
}

#[test]
fn struct_field_values() {
    assert_eq!(
        ref_targets(FILES, "crate::field_value"),
        ["crate::handler", "crate::second"]
    );
    // `on_done` is a local holding the fn (`let on_done = handler` is the
    // use); `fallback` is a local too.
    assert_eq!(
        ref_targets(FILES, "crate::field_shorthand"),
        ["crate::handler"]
    );
    assert_eq!(
        ref_targets(FILES, "crate::shorthand_item"),
        ["crate::handler"]
    );
}

#[test]
fn locals_params_data_and_calls_are_not_references() {
    // `second` is a local here (shadowing the fn), `handler_arg`/`local` are
    // parameters, `util::LIMIT` is a constant; only `util::helper` is a fn.
    assert_eq!(
        ref_targets(FILES, "crate::negatives"),
        ["crate::util::helper"]
    );
    assert!(ref_targets(FILES, "crate::shadowed").is_empty());
    // `handler(1)` is a call: the pair keeps its CALLS edge only.
    assert!(ref_targets(FILES, "crate::called").is_empty());
    assert!(edge_targets(FILES, "crate::called", "CALLS").contains(&"crate::handler".to_string()));
}

#[test]
fn unresolved_value_references_are_not_counted() {
    unresolved_accounting(FILES).assert_value_references_not_counted();
}

const MODULE_SCOPE: &[(&str, &str)] = &[(
    "src/lib.rs",
    r#"fn handler(x: i32) -> i32 {
    x
}

fn apply(f: fn(i32) -> i32, v: i32) -> i32 {
    f(v)
}

struct Profile {
    hook: Option<fn(i32) -> i32>,
    fixed: fn(i32) -> i32,
}

static PROFILE: Profile = Profile {
    hook: Some(handler),
    fixed: handler,
};

const TABLE: [fn(i32) -> i32; 1] = [handler];

fn nested_fn() -> i32 {
    fn handler(x: i32) -> i32 {
        x + 1
    }
    apply(handler, 1)
}

mod tests {
    use super::*;

    fn through_glob() -> i32 {
        handler(1) + apply(handler, 2)
    }
}
"#,
)];

#[test]
fn module_level_initializers_reference_from_the_module() {
    let t = ref_targets(MODULE_SCOPE, "crate");
    assert_eq!(
        t,
        ["crate::handler", "crate::handler", "crate::handler"],
        "{t:?}"
    );
}

#[test]
fn a_nested_fn_item_is_a_local_name() {
    assert!(ref_targets(MODULE_SCOPE, "crate::nested_fn").is_empty());
}

#[test]
fn glob_imports_resolve_references_and_calls_alike() {
    assert!(ref_targets(MODULE_SCOPE, "crate::tests::through_glob").is_empty());
    // `handler(1)` is a call through `use super::*`, so the pair keeps only it.
    assert_eq!(
        edge_targets_with_resolution(MODULE_SCOPE, "crate::tests::through_glob", "CALLS"),
        [
            ("crate::apply".to_string(), "import".to_string()),
            ("crate::handler".to_string(), "import".to_string())
        ]
    );
}

const GLOB_REF: &[(&str, &str)] = &[(
    "src/lib.rs",
    r#"fn handler(x: i32) -> i32 {
    x
}

fn apply(f: fn(i32) -> i32, v: i32) -> i32 {
    f(v)
}

mod tests {
    use super::*;

    fn only_a_reference() -> i32 {
        apply(handler, 2)
    }
}
"#,
)];

#[test]
fn a_value_reference_through_a_glob_import_binds() {
    assert_eq!(
        edge_targets_with_resolution(GLOB_REF, "crate::tests::only_a_reference", "REFERENCES"),
        [("crate::handler".to_string(), "import".to_string())]
    );
}
