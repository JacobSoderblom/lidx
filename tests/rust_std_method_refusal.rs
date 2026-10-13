//! Rust calls the repo cannot own (issue #425): a std method name on a
//! receiver of unknown type stays unresolved instead of binding to the repo's
//! one same-named method; a bare `f()` names the module's free function, not
//! the enclosing impl's method; `debug_assert!(..)` arguments are scanned; a
//! chained call's evidence starts at its method-name line.

mod common;

use std::sync::OnceLock;

use lidx::indexer::Indexer;

const LIB_RS: &str = r#"
mod decls;
use crate::decls::{make_e, make_option};
pub struct E;
impl Clone for E {
    fn clone(&self) -> E { E }
}
impl E {
    pub fn map(&self, _f: u32) -> u32 { 0 }
    pub fn dup(&self) -> E { self.clone() }
    pub fn len(&self) -> usize { 0 }
}
pub fn untyped(x: Foo, f: u32) {
    x.clone();
    x.map(f);
}
pub fn typed(e: E) {
    e.clone();
}
pub fn deferred_std() {
    let o = make_option();
    o.map(1);
    let n = make_e();
    n.len();
}
pub struct A;
impl A {
    fn helper(&self) {}
    fn caller(&self) {
        helper(1);
    }
    fn nested_caller(&self) {
        fn local() {}
        local();
    }
}
pub fn helper(_x: i32) {}
pub fn foo() -> bool { true }
pub fn macros() {
    debug_assert!(foo());
}
pub struct T;
impl T {
    pub fn count_things(&self) -> T { T }
    pub fn done(&self) {}
}
pub fn chained(t: T) {
    t
        .count_things()
        .done();
}
"#;

const DECLS_RS: &str = "use crate::E;\n\
pub fn make_option() -> Option<u32> { None }\n\
pub fn make_e() -> E { E }\n";

const FILES: [(&str, &str); 3] = [
    ("Cargo.toml", "[package]\nname = \"x\"\n"),
    ("src/lib.rs", LIB_RS),
    ("src/decls.rs", DECLS_RS),
];

/// Every resolved CALLS edge: (caller qualname, target qualname, evidence start line).
type Edges = Vec<(String, String, Option<i64>)>;

/// The fixture indexed once; the guard keeps the temp tree alive.
fn edges() -> &'static Edges {
    static EDGES: OnceLock<(tempfile::TempDir, Edges)> = OnceLock::new();
    &EDGES
        .get_or_init(|| {
            let (tmp, root, db_path) = common::index_repo("lidx-std-refusal-", &FILES);
            let indexer = Indexer::new(root, db_path).unwrap();
            let gv = indexer.db().current_graph_version().unwrap();
            let conn = indexer.db().read_conn().unwrap();
            let mut stmt = conn
                .prepare(
                    "SELECT s.qualname, t.qualname, e.evidence_start_line
                     FROM edges e
                     JOIN symbols s ON s.id = e.source_symbol_id
                     JOIN symbols t ON t.id = e.target_symbol_id
                     WHERE e.graph_version = ? AND e.kind = 'CALLS'",
                )
                .unwrap();
            let rows = stmt
                .query_map(rusqlite::params![gv], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            (tmp, rows)
        })
        .1
}

fn targets(caller: &str) -> Vec<&'static str> {
    edges()
        .iter()
        .filter(|(c, _, _)| c == caller)
        .map(|(_, t, _)| t.as_str())
        .collect()
}

#[test]
fn std_names_on_untyped_receivers_stay_unresolved() {
    assert!(targets("crate::untyped").is_empty());
}

#[test]
fn self_and_typed_receivers_still_bind() {
    assert_eq!(targets("crate::E::dup"), ["crate::E::clone"]);
    assert_eq!(targets("crate::typed"), ["crate::E::clone"]);
}

#[test]
fn deferred_receivers_that_do_not_finish_refuse_std_names_too() {
    // `make_option()` is declared in another file and returns `Option<u32>`,
    // not a repo type: `o.map(..)` must not fall to `E::map`. `make_e()`
    // returns a repo type, so `n.len()` still binds.
    let methods: Vec<_> = targets("crate::deferred_std")
        .into_iter()
        .filter(|t| t.starts_with("crate::E::"))
        .collect();
    assert_eq!(methods, ["crate::E::len"]);
}

#[test]
fn bare_call_binds_the_module_function_not_the_impl_method() {
    assert_eq!(targets("crate::A::caller"), ["crate::helper"]);
}

#[test]
fn nested_fn_called_from_an_impl_method_still_binds() {
    // A nested `fn` is symbolised under the enclosing impl.
    assert_eq!(targets("crate::A::nested_caller"), ["crate::A::local"]);
}

#[test]
fn debug_assert_arguments_yield_calls() {
    assert_eq!(targets("crate::macros"), ["crate::foo"]);
}

#[test]
fn chained_call_evidence_starts_at_the_method_name_line() {
    let src_line = |needle: &str| {
        LIB_RS
            .lines()
            .position(|l| l.contains(needle))
            .map(|i| i as i64 + 1)
    };
    let line_of = |target: &str| {
        edges()
            .iter()
            .find(|(c, t, _)| c == "crate::chained" && t == target)
            .unwrap_or_else(|| panic!("no edge to {target}"))
            .2
    };
    assert_eq!(line_of("crate::T::done"), src_line(".done()"));
    assert_eq!(
        line_of("crate::T::count_things"),
        src_line(".count_things()")
    );
}

#[test]
fn bare_call_of_a_bound_local_never_binds_an_item() {
    let files = [
        ("Cargo.toml", "[package]\nname = \"x\"\n"),
        (
            "src/lib.rs",
            "pub fn path() {}\n\
             pub fn by_param(path: fn()) { path(); }\n\
             pub fn by_let() { let path = || (); path(); }\n\
             pub fn by_pattern(o: Option<fn()>) { if let Some(path) = o { path(); } }\n\
             pub fn by_closure() { let f = |path: fn()| path(); f(path); }\n\
             pub fn plain() { path(); }\n",
        ),
    ];
    for caller in ["crate::by_param", "crate::by_let", "crate::by_pattern"] {
        let t = common::call_targets(&files, caller);
        assert!(t.is_empty(), "{caller}: {t:?}");
    }
    assert_eq!(
        common::call_targets(&files, "crate::plain"),
        ["crate::path"]
    );
}
