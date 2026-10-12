//! Rust calls the repo cannot own (issue #425): a std method name on a
//! receiver of unknown type stays unresolved instead of binding to the repo's
//! one same-named method; a bare `f()` names the module's free function, not
//! the enclosing impl's method; `debug_assert!(..)` arguments are scanned; a
//! chained call's evidence starts at its method-name line.

mod common;

use lidx::indexer::Indexer;

const LIB_RS: &str = r#"
pub struct E;
impl Clone for E {
    fn clone(&self) -> E { E }
}
impl E {
    pub fn map(&self, _f: u32) -> u32 { 0 }
    pub fn dup(&self) -> E { self.clone() }
}
pub fn untyped(x: Foo, f: u32) {
    x.clone();
    x.map(f);
}
pub fn typed(e: E) {
    e.clone();
}
pub struct A;
impl A {
    fn helper(&self) {}
    fn caller(&self) {
        helper(1);
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
pub fn chained(v: Vec<u32>) {
    v.iter()
        .count_things()
        .done();
}
"#;

const FILES: [(&str, &str); 2] = [
    ("Cargo.toml", "[package]\nname = \"x\"\n"),
    ("src/lib.rs", LIB_RS),
];

/// (target qualname or raw target text, evidence start line) per CALLS edge
/// out of `caller`.
fn calls_from(caller: &str) -> Vec<(String, Option<i64>)> {
    let (_tmp, root, db_path) = common::index_repo("lidx-std-refusal-", &FILES);
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(t.qualname, e.target_qualname, ''), e.evidence_start_line
             FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS' AND s.qualname = ?
               AND (e.target_symbol_id IS NOT NULL OR e.target_qualname IS NOT NULL)",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv, caller], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

fn resolved(caller: &str) -> Vec<String> {
    let (_tmp, root, db_path) = common::index_repo("lidx-std-refusal-r-", &FILES);
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS' AND s.qualname = ?",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv, caller], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn std_names_on_untyped_receivers_stay_unresolved() {
    assert!(
        resolved("crate::untyped").is_empty(),
        "{:?}",
        resolved("crate::untyped")
    );
}

#[test]
fn self_and_typed_receivers_still_bind() {
    assert_eq!(resolved("crate::E::dup"), vec!["crate::E::clone"]);
    assert_eq!(resolved("crate::typed"), vec!["crate::E::clone"]);
}

#[test]
fn bare_call_binds_the_module_function_not_the_impl_method() {
    assert_eq!(resolved("crate::A::caller"), vec!["crate::helper"]);
}

#[test]
fn debug_assert_arguments_yield_calls() {
    assert_eq!(resolved("crate::macros"), vec!["crate::foo"]);
}

#[test]
fn chained_call_evidence_starts_at_the_method_name_line() {
    let edges = calls_from("crate::chained");
    let src_line = |needle: &str| {
        LIB_RS
            .lines()
            .position(|l| l.contains(needle))
            .map(|i| i as i64 + 1)
    };
    let done = edges
        .iter()
        .find(|(t, _)| t == "crate::T::done")
        .expect("done edge");
    assert_eq!(done.1, src_line(".done()"));
    let ct = edges
        .iter()
        .find(|(t, _)| t == "crate::T::count_things")
        .expect("count_things edge");
    assert_eq!(ct.1, src_line(".count_things()"));
}
