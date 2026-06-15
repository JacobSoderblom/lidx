use lidx::indexer::Indexer;
use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::rust::{RustExtractor, module_name_from_rel_path};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Create an isolated temp dir, write the given Rust source as `src/lib.rs`,
/// index it, and return the (repo_root, db_path) for DB queries.
fn index_rust_source(label: &str, source: &str) -> (PathBuf, PathBuf) {
    let mut repo_root = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    repo_root.push(format!("lidx-rust-extract-{label}-{nanos}-{counter}"));
    std::fs::create_dir_all(repo_root.join("src")).unwrap();
    std::fs::write(repo_root.join("src").join("lib.rs"), source).unwrap();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    (repo_root, db_path)
}

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

#[test]
fn dotted_method_call_emits_bare_method_name() {
    // A method call on a receiver (`db.insert(...)`) cannot be resolved to a real
    // qualname without receiver-type analysis. Rather than dropping the target on the
    // floor, the extractor emits the bare method name so the bare-method-name recovery
    // machinery can still surface the caller for upstream/impact analysis.
    let source = r#"
fn save(db: &Database) {
    db.insert("key", "value");
}
"#;
    let mut extractor = RustExtractor::new().unwrap();
    let extracted = extractor.extract(source, "crate").unwrap();

    let call_edges: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CALLS")
        .collect();

    assert!(
        call_edges
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("insert")),
        "dotted call db.insert() should emit target_qualname=\"insert\", got: {:?}",
        call_edges
            .iter()
            .map(|e| (e.target_qualname.as_deref(), e.detail.as_deref()))
            .collect::<Vec<_>>()
    );
}

#[test]
fn fully_resolved_calls_are_unchanged() {
    // Free-function and self.method calls already resolve to real qualnames; the
    // bare-method-name fallback must not perturb them.
    let source = r#"
fn helper() {}

fn caller() {
    helper();
}

struct Foo;

impl Foo {
    fn run(&self) {
        self.step();
    }
    fn step(&self) {}
}
"#;
    let mut extractor = RustExtractor::new().unwrap();
    let extracted = extractor.extract(source, "crate").unwrap();

    let call_edges: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CALLS")
        .collect();

    // Free-function call resolves to the module-qualified name.
    assert!(
        call_edges
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("crate::helper")),
        "free function call should resolve to crate::helper, got: {:?}",
        call_edges
            .iter()
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>()
    );
    // self.method() resolves to the container-qualified name, NOT the bare method.
    assert!(
        call_edges
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("crate::Foo::step")),
        "self.step() should resolve to crate::Foo::step, got: {:?}",
        call_edges
            .iter()
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>()
    );
    assert!(
        !call_edges
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("step")),
        "self.step() must not degrade to the bare method name, got: {:?}",
        call_edges
            .iter()
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>()
    );
}

#[test]
fn turbofish_method_call_emits_clean_method_name() {
    // A turbofished method call (`x.parse::<T>()`) parses as a generic_function whose
    // raw text carries the turbofish. The bare-method-name fallback must resolve the
    // actual method name from the inner callee, never a fragment of the type arguments
    // (e.g. "<u32>" or "IpAddr>") — those would pollute target_qualname with garbage.
    let source = r#"
fn run(x: &Parser) {
    x.parse::<std::net::IpAddr>();
    x.foo::<u32>();
}
"#;
    let mut extractor = RustExtractor::new().unwrap();
    let extracted = extractor.extract(source, "crate").unwrap();

    let targets: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CALLS")
        .map(|e| e.target_qualname.as_deref())
        .collect();

    assert!(
        targets.contains(&Some("parse")),
        "x.parse::<T>() should emit the bare method name \"parse\", got: {targets:?}"
    );
    assert!(
        targets.contains(&Some("foo")),
        "x.foo::<u32>() should emit the bare method name \"foo\", got: {targets:?}"
    );
    // No edge may carry a turbofish fragment as its target.
    assert!(
        !targets.iter().any(|t| {
            t.is_some_and(|name| name.contains('<') || name.contains('>') || name.contains(','))
        }),
        "no CALLS target may contain a turbofish fragment, got: {targets:?}"
    );
}

#[test]
fn turbofish_free_function_call_is_not_bare_named() {
    // A turbofished *free*-function call (`foo::<u8>()`) has no receiver, so the
    // bare-method-name fallback must not fire — it stays unresolved with the raw text
    // in `detail`, exactly as the non-turbofished free call would.
    let source = r#"
fn run() {
    foo::<u8>();
}
"#;
    let mut extractor = RustExtractor::new().unwrap();
    let extracted = extractor.extract(source, "crate").unwrap();

    let call_edges: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CALLS")
        .collect();

    assert!(
        !call_edges
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("foo")
                || edge
                    .target_qualname
                    .as_deref()
                    .is_some_and(|name| name.contains('<'))),
        "free turbofish foo::<u8>() must not emit a bare-name target, got: {:?}",
        call_edges
            .iter()
            .map(|e| (e.target_qualname.as_deref(), e.detail.as_deref()))
            .collect::<Vec<_>>()
    );
}

#[test]
fn extract_env_var_config_read() {
    let source = r#"
use std::env;

fn main() {
    let db_url = env::var("DATABASE_URL").unwrap();
    let api_key = std::env::var("API_KEY").expect("missing");
}
"#;
    let module = module_name_from_rel_path("src/main.rs");
    let mut extractor = RustExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("env://DATABASE_URL") }),
        "expected CONFIG_READ for env://DATABASE_URL, found: {:?}",
        config_reads
            .iter()
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>()
    );
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("env://API_KEY") }),
        "expected CONFIG_READ for env://API_KEY"
    );
}

#[test]
fn dotted_method_call_is_recovered_by_qualname_pattern() {
    // End-to-end: a dotted Rust call (`db.insert(...)`) now ships a CALLS edge whose
    // target_qualname is the bare method name "insert". The bare-method-name recovery
    // path (incoming_edges_by_qualname_pattern) must then find the caller — restoring
    // upstream/impact reach for Rust method calls.
    let source = r#"
pub fn save(db: &Database) {
    db.insert("key", "value");
}
"#;
    let (repo_root, db_path) = index_rust_source("recover-insert", source);
    let indexer = Indexer::new(repo_root.clone(), db_path).unwrap();
    let db = indexer.db();
    let graph_version = db.current_graph_version().unwrap();

    let recovered = db
        .incoming_edges_by_qualname_pattern("insert", "CALLS", None, graph_version)
        .unwrap();

    assert!(
        recovered
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("insert")),
        "incoming_edges_by_qualname_pattern(\"insert\") should recover the CALLS edge, got: {:?}",
        recovered
            .iter()
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>()
    );

    // The recovered edge must carry a resolved source symbol (the caller `save`),
    // otherwise upstream traversal cannot reach it.
    let edge = recovered
        .iter()
        .find(|edge| edge.target_qualname.as_deref() == Some("insert"))
        .unwrap();
    let caller = db
        .get_symbol_by_id(edge.source_symbol_id.unwrap())
        .unwrap()
        .expect("recovered edge should resolve to the caller symbol");
    assert_eq!(caller.qualname, "crate::save");

    let _ = std::fs::remove_dir_all(&repo_root);
}
