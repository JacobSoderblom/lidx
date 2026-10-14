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

/// A `static` or `const` initialiser is walked like a body (#497): the
/// references in it belong to the item, as rust-analyzer keys them, not to
/// the module.
#[test]
fn module_level_initializers_reference_from_their_item() {
    assert!(ref_targets(MODULE_SCOPE, "crate").is_empty());
    let t = ref_targets(MODULE_SCOPE, "crate::PROFILE");
    assert_eq!(t, ["crate::handler", "crate::handler"], "{t:?}");
    let t = ref_targets(MODULE_SCOPE, "crate::TABLE");
    assert_eq!(t, ["crate::handler"], "{t:?}");
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

const SHAPES: &[(&str, &str)] = &[
    (
        "Cargo.toml",
        "[package]\nname = \"shapes\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    ),
    (
        "src/lib.rs",
        r#"use std::io;

mod cfg;
mod sink;

use crate::cfg::{get_cfg, parse_or_exit};
use crate::sink::{Sink, SinkError};

pub fn bool(x: &str) -> bool {
    !x.is_empty()
}

fn load() -> Result<Vec<i32>, String> {
    Ok(vec![])
}

pub fn fallback() -> usize {
    load().unwrap_or_else(get_cfg).len()
}

pub fn turbofish(m: Option<&str>) -> Option<usize> {
    m.map(parse_or_exit::<usize>)
}

pub fn called_turbofish() -> usize {
    parse_or_exit::<usize>("3")
}

pub fn primitive_named_fn(m: Option<&str>) -> Option<bool> {
    m.map(bool)
}

pub struct Selection;

impl Selection {
    pub fn select(&self) -> i32 {
        fn get_field(i: &i32) -> i32 {
            *i
        }
        let get_field: fn(&i32) -> i32 = get_field;
        [1].iter().map(get_field).sum()
    }

    pub fn direct(&self) -> i32 {
        fn twice(i: &i32) -> i32 {
            *i * 2
        }
        [1].iter().map(twice).sum()
    }

    pub fn rebound(&self) -> i32 {
        fn once(i: &i32) -> i32 {
            *i
        }
        let once = |i: &i32| *i + 1;
        [1].iter().map(once).sum()
    }
}

pub fn generic<S: Sink>() -> Result<(), S::Error> {
    Some(1).ok_or("x").map_err(S::Error::error_message)
}

pub fn generic_std_item<T: Default>() -> Option<T> {
    Some(()).map(T::default)
}

pub fn written_path() -> Result<(), io::Error> {
    Err("x").map_err(io::Error::error_message)
}

pub struct Info {
    short: String,
    long: String,
}

fn short() -> &'static str {
    "s"
}

static INFO: std::sync::LazyLock<Info> = std::sync::LazyLock::new(|| {
    let short = String::new();
    let long = String::new();
    Info { short, long }
});

pub fn read() -> (&'static str, &'static str) {
    (&INFO.short, short())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn through_glob() -> usize {
        load().unwrap_or_else(get_cfg).len()
    }
}
"#,
    ),
    (
        "src/cfg.rs",
        r#"pub fn get_cfg(_e: String) -> Vec<i32> {
    vec![]
}

pub fn parse_or_exit<T: std::str::FromStr>(s: &str) -> T {
    s.parse().ok().unwrap()
}
"#,
    ),
    (
        "src/sink.rs",
        r#"use std::io;

pub trait SinkError: Sized {
    fn error_message<T: std::fmt::Display>(message: T) -> Self;
}

pub trait Sink {
    type Error: SinkError;
}

impl SinkError for io::Error {
    fn error_message<T: std::fmt::Display>(message: T) -> io::Error {
        io::Error::new(io::ErrorKind::Other, message.to_string())
    }
}
"#,
    ),
];

#[test]
fn a_turbofish_names_the_function_it_instantiates() {
    assert_eq!(
        ref_targets(SHAPES, "crate::turbofish"),
        ["crate::cfg::parse_or_exit"]
    );
    // `parse_or_exit::<usize>(..)` is a call.
    assert!(ref_targets(SHAPES, "crate::called_turbofish").is_empty());
}

#[test]
fn a_primitive_named_function_is_a_value_reference() {
    assert_eq!(
        ref_targets(SHAPES, "crate::primitive_named_fn"),
        ["crate::bool"]
    );
}

#[test]
fn a_nested_fn_is_referenced_by_its_impl_symbol() {
    // `let f: fn(..) = f;` reads the item before the new binding exists.
    assert_eq!(
        ref_targets(SHAPES, "crate::Selection::select"),
        ["crate::Selection::get_field"]
    );
    assert_eq!(
        ref_targets(SHAPES, "crate::Selection::direct"),
        ["crate::Selection::twice"]
    );
    // A closure of the same name rebinds it.
    assert!(ref_targets(SHAPES, "crate::Selection::rebound").is_empty());
}

#[test]
fn a_generic_parameter_path_names_the_trait_item_it_is_bound_by() {
    assert_eq!(
        edge_targets_with_resolution(SHAPES, "crate::generic", "REFERENCES"),
        [(
            "crate::sink::SinkError::error_message".to_string(),
            "receiver_type".to_string()
        )]
    );
    // `T::default` is std's `Default::default`.
    assert!(ref_targets(SHAPES, "crate::generic_std_item").is_empty());
}

#[test]
fn a_path_through_use_std_binds_to_the_impl_written_with_that_path() {
    assert_eq!(
        ref_targets(SHAPES, "crate::written_path"),
        ["io::Error::error_message"]
    );
}

#[test]
fn a_static_initializers_locals_are_not_functions() {
    // `Info { short, long }` shorthand-initialises from the closure's locals.
    assert!(ref_targets(SHAPES, "crate").is_empty());
}

#[test]
fn a_name_a_parent_module_imports_is_bound_through_the_glob() {
    assert_eq!(
        ref_targets(SHAPES, "crate::tests::through_glob"),
        ["crate::cfg::get_cfg"]
    );
    assert_eq!(
        ref_targets(SHAPES, "crate::fallback"),
        ["crate::cfg::get_cfg"]
    );
}

const BIN_CRATE: &[(&str, &str)] = &[
    (
        "Cargo.toml",
        "[package]\nname = \"tool\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    ),
    (
        "src/bin/tool/main.rs",
        "mod app;\nmod config;\n\nfn main() {\n    app::run();\n}\n",
    ),
    (
        "src/bin/tool/config.rs",
        "pub fn third(x: i32) -> i32 {\n    x\n}\n",
    ),
    (
        "src/bin/tool/app.rs",
        r#"use crate::config::third;

pub fn run() -> Option<i32> {
    Some(2).map(third)
}

pub fn qualified() -> Option<i32> {
    Some(2).map(crate::config::third)
}
"#,
    ),
];

#[test]
fn crate_in_a_bin_target_is_that_targets_root() {
    assert_eq!(
        ref_targets(BIN_CRATE, "crate::bin::tool::app::run"),
        ["crate::bin::tool::config::third"]
    );
    assert_eq!(
        ref_targets(BIN_CRATE, "crate::bin::tool::app::qualified"),
        ["crate::bin::tool::config::third"]
    );
}

const FROM_IMPLS: &[(&str, &str)] = &[
    (
        "Cargo.toml",
        "[package]\nname = \"conv\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    ),
    (
        "src/lib.rs",
        r#"mod model;
mod load;

pub type AppResult<T> = Result<T, ConfigError>;

pub struct ConfigError;
pub struct Other;

pub struct Symbol;
pub struct Compact;

impl From<Symbol> for Compact {
    fn from(_s: Symbol) -> Self {
        Compact
    }
}

impl From<&Symbol> for Compact {
    fn from(_s: &Symbol) -> Self {
        Compact
    }
}

pub struct Failure;

impl From<ConfigError> for Failure {
    fn from(_e: ConfigError) -> Self {
        Failure
    }
}

impl From<Other> for Failure {
    fn from(_e: Other) -> Self {
        Failure
    }
}

pub fn make() -> Vec<Symbol> {
    Vec::new()
}

pub fn parse(_a: u8) -> AppResult<u8> {
    Ok(1)
}

pub fn owned() -> Vec<Compact> {
    let items = make();
    items.into_iter().map(Compact::from).collect()
}

pub fn borrowed(items: &[Symbol]) -> Vec<Compact> {
    items.iter().map(Compact::from).collect()
}

pub fn borrowed_param(items: &Vec<Symbol>) -> Vec<Compact> {
    items.into_iter().map(Compact::from).collect()
}

pub fn through_a_call() -> Vec<Compact> {
    make().into_iter().map(Compact::from).collect()
}

pub fn error_of_an_alias() -> Result<u8, Failure> {
    parse(1).map_err(Failure::from)
}

pub fn unknown_receiver(x: Option<Symbol>) -> Option<Compact> {
    x.map(Compact::from)
}

pub struct Value;
pub type TextResult = Result<String, String>;

impl From<String> for Value {
    fn from(_s: String) -> Self {
        Value
    }
}

impl From<bool> for Value {
    fn from(_b: bool) -> Self {
        Value
    }
}

pub enum Function {
    Nullary(fn(u8) -> TextResult),
}

pub fn call(function: Function) -> Option<Result<Value, String>> {
    match function {
        Function::Nullary(f) => Some(f(1).map(Value::from)),
    }
}

pub struct Builder;

impl Builder {
    pub fn build(_a: u8) -> AppResult<Builder> {
        Ok(Builder)
    }
}

pub fn from_a_constructor() -> Result<Builder, Failure> {
    Builder::build(1).map_err(Failure::from)
}

pub fn not_known(items: Vec<Symbol>) -> Vec<Compact> {
    items.into_iter().filter_map(|s| Some(s)).map(Compact::from).collect()
}
"#,
    ),
];

#[test]
fn a_from_impl_is_picked_by_the_type_the_combinator_hands_it() {
    let from = |target: &str| {
        let t = ref_targets(FROM_IMPLS, target);
        t.iter().map(|q| q.to_string()).collect::<Vec<_>>()
    };
    let (_tmp, root, db_path) = common::index_repo("lidx-from-impls-", FROM_IMPLS);
    let indexer = lidx::indexer::Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    // (caller, parameter type of the impl method bound)
    let bound = |caller: &str| -> Vec<String> {
        conn.prepare(
            "SELECT t.signature FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.kind = 'REFERENCES' AND s.qualname = ?",
        )
        .unwrap()
        .query_map(rusqlite::params![gv, caller], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
    };
    let _ = from;
    let has = |caller: &str, param: &str| {
        let got = bound(caller);
        assert_eq!(got.len(), 1, "{caller}: {got:?}");
        assert!(got[0].contains(param), "{caller}: {got:?}");
    };
    has("crate::owned", "(_s: Symbol)");
    has("crate::borrowed", "(_s: &Symbol)");
    has("crate::borrowed_param", "(_s: &Symbol)");
    has("crate::through_a_call", "(_s: Symbol)");
    has("crate::error_of_an_alias", "(_e: ConfigError)");
    // A call through a function pointer held by an enum variant: the alias
    // of its result type gives the `Ok` type, which picks the impl.
    let both = bound("crate::call");
    assert_eq!(both.len(), 1, "{both:?}");
    assert!(both[0].contains("(_s: String)"), "{both:?}");
    has("crate::from_a_constructor", "(_e: ConfigError)");
    // A receiver whose item type is not known stays unbound.
    has("crate::unknown_receiver", "(_s: Symbol)");
    assert!(bound("crate::not_known").is_empty());
}
