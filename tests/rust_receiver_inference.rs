//! Receiver-type inference for locally-typed receivers: a local initialised
//! from a free-fn call takes the fn's declared return type, chains through
//! `Self`/`&mut Self` returning methods keep their type, `.clone()` keeps the
//! type, and fn references inside macro arguments are recorded as calls.

mod common;

const TOML: &str = "[package]\nname = \"app\"\nversion = \"0.1.0\"\n";
const LIB: &str = "pub mod m;\npub mod dup;\npub mod b;\npub mod s;\npub mod uses;\n";
const M: &str = "\
pub trait Matcher {\n\
    fn find(&self) -> u8;\n\
    fn find_iter(&self, x: u8) -> u8 {\n\
        self.find() + x\n\
    }\n\
}\n\
pub struct RegexMatcher;\n\
impl Matcher for RegexMatcher {\n\
    fn find(&self) -> u8 { 1 }\n\
}\n\
impl RegexMatcher {\n\
    pub fn only(&self) -> u8 { 5 }\n\
}\n\
pub struct Other;\n\
impl Matcher for Other {\n\
    fn find(&self) -> u8 { 2 }\n\
    fn find_iter(&self, x: u8) -> u8 { x }\n\
}\n\
pub struct Third;\n\
impl Third {\n\
    pub fn find_iter(&self, x: u8) -> u8 { x }\n\
}\n";
const DUP: &str = "\
use crate::m::Matcher;\n\
pub struct RegexMatcher;\n\
impl Matcher for RegexMatcher {\n\
    fn find(&self) -> u8 { 3 }\n\
}\n\
impl RegexMatcher {\n\
    pub fn only(&self) -> u8 { 4 }\n\
}\n";
const B: &str = "\
pub struct Walk;\n\
pub struct WalkBuilder;\n\
impl WalkBuilder {\n\
    pub fn new() -> Self { WalkBuilder }\n\
    pub fn add_custom(&mut self, _n: &str) -> &mut Self { self }\n\
    pub fn build(&self) -> Walk { Walk }\n\
}\n\
pub struct OtherBuilder;\n\
impl OtherBuilder {\n\
    pub fn new() -> Self { OtherBuilder }\n\
    pub fn add_custom(&mut self, _n: &str) -> &mut Self { self }\n\
    pub fn build(&self) -> Walk { Walk }\n\
}\n\
pub fn chain() {\n\
    let _w = WalkBuilder::new().add_custom(\"a\").build();\n\
}\n\
pub fn chain_twice() {\n\
    WalkBuilder::new()\n\
        .add_custom(\"a\")\n\
        .add_custom(\"b\");\n\
}\n\
pub fn chain_unknown(x: u8) {\n\
    external(x).add_custom(\"a\");\n\
}\n\
pub fn builder_fn() -> WalkBuilder { WalkBuilder }\n\
pub fn via_fn() {\n\
    builder_fn().add_custom(\"a\");\n\
}\n";
const S: &str = "\
#[derive(Clone)]\n\
pub struct Worker;\n\
impl Worker {\n\
    pub fn new() -> Self { Worker }\n\
    pub fn search(&mut self) -> u8 { 1 }\n\
}\n\
pub struct Alt;\n\
impl Alt {\n\
    pub fn search(&mut self) -> u8 { 2 }\n\
}\n\
pub struct Args;\n\
impl Args {\n\
    pub fn worker(&self) -> Result<Worker, u8> { Ok(Worker) }\n\
}\n\
pub fn local_clone() {\n\
    let w = Worker::new();\n\
    let mut c = w.clone();\n\
    c.search();\n\
}\n\
pub fn ref_clone(w: &Worker) {\n\
    let mut c = w.clone();\n\
    c.search();\n\
}\n";
const USES: &str = "\
use crate::m::{Matcher, RegexMatcher, Third};\n\
use crate::s::Args;\n\
use crate::b::WalkBuilder;\n\
fn matcher(_p: &str) -> RegexMatcher { RegexMatcher }\n\
fn mk() -> Result<RegexMatcher, u8> { Ok(RegexMatcher) }\n\
fn opt() -> Option<Third> { None }\n\
pub fn p3a() {\n\
    let matcher = matcher(\"x\");\n\
    matcher.find_iter(1);\n\
}\n\
pub fn p3a_same_name_types() {\n\
    let m = matcher(\"x\");\n\
    m.only();\n\
}\n\
pub fn p3a_try() -> Result<u8, u8> {\n\
    let m = mk()?;\n\
    Ok(m.find_iter(1))\n\
}\n\
pub fn p3a_opt() -> Option<u8> {\n\
    let t = opt()?;\n\
    Some(t.find_iter(1))\n\
}\n\
pub fn p3a_unknown() {\n\
    let v = external();\n\
    v.find_iter(1);\n\
}\n\
pub fn p3a_closure_shadow() {\n\
    let matcher = |_p: &str| Third;\n\
    let t = matcher(\"x\");\n\
    t.find_iter(1);\n\
}\n\
pub fn p3a_later_shadow() {\n\
    let a = matcher(\"x\");\n\
    a.find_iter(1);\n\
    let matcher = 1;\n\
    let _ = matcher;\n\
}\n\
pub fn p3c_closure(args: &Args) {\n\
    let searcher = args.worker().unwrap();\n\
    let f = move || {\n\
        let mut searcher = searcher.clone();\n\
        searcher.search();\n\
    };\n\
    f();\n\
}\n\
pub fn chain_across_files() {\n\
    WalkBuilder::new()\n\
        .add_custom(\"a\")\n\
        .add_custom(\"b\")\n\
        .build();\n\
}\n\
pub fn p3c_unknown(x: u8) {\n\
    let mut s = external(x).clone();\n\
    s.search();\n\
}\n";

fn files() -> Vec<(&'static str, &'static str)> {
    vec![
        ("Cargo.toml", TOML),
        ("src/lib.rs", LIB),
        ("src/m.rs", M),
        ("src/dup.rs", DUP),
        ("src/b.rs", B),
        ("src/s.rs", S),
        ("src/uses.rs", USES),
    ]
}

fn targets(source: &str) -> Vec<Option<String>> {
    let (_tmp, snap) = common::index_files(&files());
    snap.into_iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == source)
        .map(|e| e.target_qualname)
        .collect()
}

fn count(source: &str, target: &str) -> usize {
    targets(source)
        .iter()
        .filter(|t| t.as_deref() == Some(target))
        .count()
}

const TRAIT_FI: &str = "crate::m::Matcher::find_iter";

#[test]
fn local_from_free_fn_call_gets_declared_return_type() {
    // `RegexMatcher` does not override `find_iter`: the trait default.
    assert_eq!(
        count("crate::uses::p3a", TRAIT_FI),
        1,
        "{:?}",
        targets("crate::uses::p3a")
    );
}

#[test]
fn question_mark_unwraps_result_return() {
    assert_eq!(
        count("crate::uses::p3a_try", TRAIT_FI),
        1,
        "{:?}",
        targets("crate::uses::p3a_try")
    );
}

#[test]
fn question_mark_unwraps_option_return() {
    assert_eq!(
        count("crate::uses::p3a_opt", "crate::m::Third::find_iter"),
        1,
        "{:?}",
        targets("crate::uses::p3a_opt")
    );
}

#[test]
fn unknown_fn_return_stays_unresolved() {
    for (src, forbidden) in [
        (
            "crate::uses::p3a_unknown",
            vec![
                TRAIT_FI,
                "crate::m::Other::find_iter",
                "crate::m::Third::find_iter",
            ],
        ),
        // The closure shadows the fn: its type is not `RegexMatcher`.
        (
            "crate::uses::p3a_closure_shadow",
            vec![TRAIT_FI, "crate::m::Other::find_iter"],
        ),
    ] {
        for t in targets(src).into_iter().flatten() {
            assert!(!forbidden.contains(&t.as_str()), "{src}: {t}");
        }
    }
}

#[test]
fn later_shadow_does_not_hide_earlier_fn_call() {
    assert_eq!(
        count("crate::uses::p3a_later_shadow", TRAIT_FI),
        1,
        "{:?}",
        targets("crate::uses::p3a_later_shadow")
    );
}

#[test]
fn chain_from_constructor_binds_each_link() {
    let c = "crate::b::WalkBuilder::add_custom";
    assert_eq!(
        count("crate::b::chain", c),
        1,
        "{:?}",
        targets("crate::b::chain")
    );
    assert_eq!(
        count("crate::b::chain", "crate::b::WalkBuilder::build"),
        1,
        "{:?}",
        targets("crate::b::chain")
    );
    assert_eq!(
        count("crate::b::chain", "crate::b::OtherBuilder::add_custom"),
        0
    );
}

#[test]
fn chained_same_method_twice_yields_two_distinct_edges() {
    let (_tmp, _root, db_path) = common::index_repo("lidx-chain-", &files());
    let conn = rusqlite::Connection::open(db_path).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT e.evidence_start_line, e.evidence_end_line FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND s.qualname = 'crate::b::chain_twice'
               AND t.qualname = 'crate::b::WalkBuilder::add_custom'
             ORDER BY e.evidence_end_line",
        )
        .unwrap();
    let spans: Vec<(i64, i64)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    // The outer call spans the whole chain, the inner call stops one line
    // earlier: two edges with different spans.
    assert_eq!(spans.len(), 2, "{spans:?}");
    assert_ne!(spans[0], spans[1], "{spans:?}");
}

#[test]
fn chain_from_free_fn_call_binds() {
    assert_eq!(
        count("crate::b::via_fn", "crate::b::WalkBuilder::add_custom"),
        1,
        "{:?}",
        targets("crate::b::via_fn")
    );
}

#[test]
fn chain_from_unknown_stays_unresolved() {
    for t in targets("crate::b::chain_unknown").into_iter().flatten() {
        assert!(!t.ends_with("::add_custom"), "{t}");
    }
}

#[test]
fn clone_of_unknown_stays_unresolved() {
    for t in targets("crate::uses::p3c_unknown").into_iter().flatten() {
        assert!(!t.ends_with("::search"), "{t}");
    }
}

#[test]
fn chain_across_files_binds_each_link_through_declared_returns() {
    let src = "crate::uses::chain_across_files";
    assert_eq!(
        count(src, "crate::b::WalkBuilder::add_custom"),
        1,
        "{:?}",
        targets(src)
    );
    assert_eq!(
        count(src, "crate::b::WalkBuilder::build"),
        1,
        "{:?}",
        targets(src)
    );
    assert_eq!(count(src, "crate::b::OtherBuilder::add_custom"), 0);
    assert_eq!(count(src, "crate::b::OtherBuilder::build"), 0);
}

#[test]
fn imported_type_is_told_apart_from_same_named_types() {
    let src = "crate::uses::p3a_same_name_types";
    assert_eq!(
        count(src, "crate::m::RegexMatcher::only"),
        1,
        "{:?}",
        targets(src)
    );
    assert_eq!(
        count(src, "crate::dup::RegexMatcher::only"),
        0,
        "{:?}",
        targets(src)
    );
    // A trait default reached through the imported one of two same-named types.
    assert_eq!(count("crate::uses::p3a", TRAIT_FI), 1);
}
