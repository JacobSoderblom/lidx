//! A workspace facade crate re-exporting member crates under new names
//! (`pub extern crate grep_cli as cli;`) -- a call written through the
//! facade (`grep::cli::f()`) reaches the member crate's item, including one
//! that member re-exports from a private module, and a method called on a
//! field typed through the facade (`grep::searcher::Searcher`) binds to that
//! type's method, not to a same-named method of a sibling type.

mod common;

const FACADE_TOML: &str = "[package]\nname = \"grep\"\nversion = \"0.1.0\"\n";
const FACADE_LIB: &str = "pub extern crate grep_cli as cli;\n\
pub extern crate grep_searcher as searcher;\n";

const CLI_TOML: &str = "[package]\nname = \"grep-cli\"\nversion = \"0.1.0\"\n";
const CLI_LIB: &str = "mod human;\npub use crate::human::{parse_size, ParseError};\n";
const CLI_HUMAN: &str = "pub struct ParseError;\npub fn parse_size(n: u64) -> u64 {\n    n\n}\n";

const SEARCHER_TOML: &str = "[package]\nname = \"grep-searcher\"\nversion = \"0.1.0\"\n";
const SEARCHER_LIB: &str = "mod searcher;\npub use crate::searcher::{Searcher, SearcherBuilder};\n";
const SEARCHER_MOD: &str = "pub struct Searcher;\npub struct SearcherBuilder;\n\
impl Searcher {\n    pub fn set_binary_detection(&mut self, d: u8) {}\n}\n\
impl SearcherBuilder {\n    pub fn set_binary_detection(&mut self, d: u8) {}\n}\n";

const APP_TOML: &str = "[package]\nname = \"app\"\nversion = \"0.1.0\"\n";
const APP_MAIN: &str = "mod search;\nfn main() {\n    let n = grep::cli::parse_size(1);\n}\n";
const APP_SEARCH: &str = "pub struct Worker {\n    searcher: grep::searcher::Searcher,\n}\n\
impl Worker {\n    pub fn search(&mut self) {\n        self.searcher.set_binary_detection(1);\n    }\n}\n";

fn files() -> Vec<(&'static str, &'static str)> {
    vec![
        ("crates/grep/Cargo.toml", FACADE_TOML),
        ("crates/grep/src/lib.rs", FACADE_LIB),
        ("crates/cli/Cargo.toml", CLI_TOML),
        ("crates/cli/src/lib.rs", CLI_LIB),
        ("crates/cli/src/human.rs", CLI_HUMAN),
        ("crates/searcher/Cargo.toml", SEARCHER_TOML),
        ("crates/searcher/src/lib.rs", SEARCHER_LIB),
        ("crates/searcher/src/searcher/mod.rs", SEARCHER_MOD),
        ("src/main.rs", APP_MAIN),
        ("src/search.rs", APP_SEARCH),
        ("Cargo.toml", APP_TOML),
    ]
}

fn targets(source: &str) -> Vec<Option<String>> {
    let (_tmp, snap) = common::index_files(&files());
    snap.into_iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == source)
        .map(|e| e.target_qualname)
        .collect()
}

#[test]
fn call_through_facade_extern_crate_alias_reaches_member_item() {
    assert_eq!(
        targets("crate::main"),
        vec![Some("crate::human::parse_size".to_string())]
    );
}

#[test]
fn method_on_field_typed_through_facade_binds_to_that_types_method() {
    assert_eq!(
        targets("crate::search::Worker::search"),
        vec![Some(
            "crate::searcher::Searcher::set_binary_detection".to_string()
        )]
    );
}

#[test]
fn unknown_facade_member_stays_unresolved() {
    let mut files = files();
    files[8] = (
        "src/main.rs",
        "fn main() {\n    let n = grep::cli::no_such_fn(1);\n}\n",
    );
    let (_tmp, snap) = common::index_files(&files);
    let t: Vec<_> = snap
        .into_iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == "crate::main")
        .map(|e| (e.target_qualname, e.resolution_kind))
        .collect();
    assert!(
        t.iter()
            .all(|(target, _)| target.as_deref().is_none_or(|t| !t.ends_with("parse_size"))),
        "{t:?}"
    );
}

#[test]
fn use_crate_as_alias_in_facade_is_followed_like_extern_crate() {
    let mut files = files();
    files[1] = (
        "crates/grep/src/lib.rs",
        "pub use grep_cli as cli;\npub use grep_searcher as searcher;\n",
    );
    let (_tmp, snap) = common::index_files(&files);
    let t: Vec<_> = snap
        .into_iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == "crate::main")
        .map(|e| e.target_qualname)
        .collect();
    assert_eq!(t, vec![Some("crate::human::parse_size".to_string())]);
}

#[test]
fn field_of_generic_type_does_not_type_the_receiver() {
    let files = [
        ("Cargo.toml", APP_TOML),
        (
            "src/main.rs",
            "struct A;\nimpl A {\n    fn go(&self) {}\n}\nstruct B;\nimpl B {\n    fn go(&self) {}\n}\n\
struct W<T> {\n    inner: T,\n}\nimpl<T> W<T> {\n    fn run(&self) {\n        self.inner.go();\n    }\n}\n",
        ),
    ];
    let (_tmp, snap) = common::index_files(&files);
    let t: Vec<_> = snap
        .into_iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == "crate::W::run")
        .map(|e| e.target_qualname)
        .collect();
    assert!(
        t.iter()
            .all(|t| !t.as_deref().is_some_and(|t| t.ends_with("::go"))),
        "{t:?}"
    );
}
