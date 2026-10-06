//! A Rust bare call `foo(args)` (no receiver, no path) can only name a free
//! function in scope or a local binding, never a method or associated fn of
//! the enclosing `impl`, even when one shares the name.

mod common;

const SEARCH_RS: &str = "pub struct Worker;\n\
impl Worker {\n\
    pub fn search_path(&self, p: &str) -> usize {\n        search_path(p)\n    }\n\
    pub fn search_reader(&self, p: &str) -> usize {\n        search_path(p) + search_reader(p)\n    }\n\
    pub fn assoc(p: &str) -> usize {\n        Self::search_path_assoc(p)\n    }\n\
    fn search_path_assoc(p: &str) -> usize {\n        p.len()\n    }\n}\n\
fn search_path(p: &str) -> usize {\n    p.len()\n}\n\
fn search_reader(p: &str) -> usize {\n    p.len()\n}\n";

const UTIL_RS: &str = "pub fn trim_prefix(a: usize) -> usize {\n    a\n}\n";
const STANDARD_RS: &str = "use crate::util::trim_prefix;\n\
pub struct Impl;\n\
impl Impl {\n\
    fn trim_prefix(&self, a: usize) -> usize {\n        a\n    }\n\
    pub fn run(&self) -> usize {\n        trim_prefix(1)\n    }\n}\n";
const LIB_RS: &str = "mod search;\nmod util;\nmod standard;\n";

fn targets(files: &[(&str, &str)], source: &str) -> Vec<Option<String>> {
    let (_tmp, snap) = common::index_files(files);
    snap.into_iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == source)
        .map(|e| e.target_qualname)
        .collect()
}

#[test]
fn bare_call_in_impl_resolves_to_same_module_free_fn_not_method() {
    let files = [("src/lib.rs", LIB_RS), ("src/search.rs", SEARCH_RS)];
    assert_eq!(
        targets(&files, "crate::search::Worker::search_path"),
        vec![Some("crate::search::search_path".to_string())]
    );
    let t = targets(&files, "crate::search::Worker::search_reader");
    assert!(
        t.iter()
            .all(|t| t.as_deref() != Some("crate::search::Worker::search_path")),
        "{t:?}"
    );
    assert!(t.contains(&Some("crate::search::search_path".to_string())));
    assert!(t.contains(&Some("crate::search::search_reader".to_string())));
}

#[test]
fn bare_call_in_impl_resolves_through_use_import_not_method() {
    let files = [
        ("src/lib.rs", LIB_RS),
        ("src/util.rs", UTIL_RS),
        ("src/standard.rs", STANDARD_RS),
        ("src/search.rs", SEARCH_RS),
    ];
    assert_eq!(
        targets(&files, "crate::standard::Impl::run"),
        vec![Some("crate::util::trim_prefix".to_string())]
    );
}

#[test]
fn bare_call_with_only_a_method_of_that_name_stays_unresolved() {
    let files = [
        ("src/lib.rs", "mod m;\n"),
        (
            "src/m.rs",
            "pub struct S;\nimpl S {\n    fn helper(&self) {}\n    pub fn run(&self) {\n        helper();\n    }\n}\n",
        ),
    ];
    assert_eq!(targets(&files, "crate::m::S::run"), vec![None]);
}

#[test]
fn qualified_and_self_calls_still_resolve_to_methods() {
    let files = [("src/lib.rs", LIB_RS), ("src/search.rs", SEARCH_RS)];
    assert_eq!(
        targets(&files, "crate::search::Worker::assoc"),
        vec![Some("crate::search::Worker::search_path_assoc".to_string())]
    );
}
