use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::rust::{RustExtractor, module_name_from_rel_path};

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("src/lib.rs"), "crate");
    assert_eq!(module_name_from_rel_path("src/main.rs"), "crate");
    assert_eq!(module_name_from_rel_path("src/foo/mod.rs"), "crate::foo");
    assert_eq!(
        module_name_from_rel_path("src/foo/bar.rs"),
        "crate::foo::bar"
    );
    assert_eq!(
        module_name_from_rel_path("tests/foo.rs"),
        "crate::tests::foo"
    );
}

#[test]
fn extract_symbols_and_edges() {
    let source = r#"
use crate::foo::{Bar, Baz as Qux};

struct Foo;

enum Kind { A, B }

trait Greeter {
    fn hello(&self);
}

impl Greeter for Foo {
    fn hello(&self) {}
}

impl Foo {
    fn method(&self) {}
}

fn helper() {}
fn util() { helper(); }
const MAX: usize = 10;
"#;
    let mut extractor = RustExtractor::new().unwrap();
    let extracted = extractor.extract(source, "crate::pkg::mod").unwrap();

    let names: Vec<_> = extracted
        .symbols
        .iter()
        .map(|s| (s.kind.as_str(), s.qualname.as_str()))
        .collect();

    assert!(names.contains(&("module", "crate::pkg::mod")));
    assert!(names.contains(&("struct", "crate::pkg::mod::Foo")));
    assert!(names.contains(&("enum", "crate::pkg::mod::Kind")));
    assert!(names.contains(&("trait", "crate::pkg::mod::Greeter")));
    assert!(names.contains(&("function", "crate::pkg::mod::helper")));
    assert!(names.contains(&("function", "crate::pkg::mod::util")));
    assert!(names.contains(&("const", "crate::pkg::mod::MAX")));
    assert!(names.contains(&("method", "crate::pkg::mod::Foo::method")));
    assert!(names.contains(&("method", "crate::pkg::mod::Foo::hello")));
    assert!(names.contains(&("method", "crate::pkg::mod::Greeter::hello")));

    let edge_kinds: Vec<_> = extracted.edges.iter().map(|e| e.kind.as_str()).collect();
    assert!(edge_kinds.contains(&"CONTAINS"));
    assert!(edge_kinds.contains(&"IMPORTS"));
    assert!(edge_kinds.contains(&"IMPLEMENTS"));
    assert!(edge_kinds.contains(&"CALLS"));

    let call_edges: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CALLS")
        .collect();
    assert!(
        call_edges
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("crate::pkg::mod::helper"))
    );
}

#[test]
fn extract_external_mod_edges() {
    let source = r#"
mod foo;

mod inline {
    mod bar;
}
"#;
    let mut extractor = RustExtractor::new().unwrap();
    let extracted = extractor.extract(source, "crate::pkg").unwrap();

    let module_edges: Vec<_> = extracted
        .edges
        .iter()
        .filter(|edge| edge.kind == "MODULE_FILE")
        .collect();

    assert!(
        module_edges
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("crate::pkg::foo"))
    );
    assert!(
        module_edges
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("crate::pkg::inline::bar"))
    );
    assert!(
        !module_edges
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("crate::pkg::inline"))
    );
}
