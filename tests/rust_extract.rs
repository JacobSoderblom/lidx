mod common;

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
fn dotted_method_call_resolves_to_method_symbol() {
    // End-to-end: a dotted Rust call (`db.insert(...)`) ships a CALLS edge
    // whose target_qualname is the bare method name "insert"; the resolver
    // must bind it to the method symbol so upstream/impact analysis reaches
    // the caller.
    let source = r#"
pub struct Database;
impl Database {
    pub fn insert(&self, _k: &str, _v: &str) {}
}
pub fn save(db: &Database) {
    db.insert("key", "value");
}
"#;
    let (repo_root, db_path) = index_rust_source("recover-insert", source);
    let indexer = Indexer::new(repo_root.clone(), db_path).unwrap();
    let db = indexer.db();
    let graph_version = db.current_graph_version().unwrap();

    let insert_id = db
        .lookup_symbol_id("crate::Database::insert", graph_version)
        .unwrap()
        .expect("insert method symbol");
    let save_id = db
        .lookup_symbol_id("crate::save", graph_version)
        .unwrap()
        .expect("save symbol");
    let edges = db.edges_for_symbol(insert_id, None, graph_version).unwrap();
    assert!(
        edges.iter().any(|e| e.kind == "CALLS"
            && e.target_symbol_id == Some(insert_id)
            && e.source_symbol_id == Some(save_id)),
        "expected save -> Database::insert CALLS edge, got: {edges:?}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn multiline_path_call_resolves_like_single_line() {
    // A call path split across lines (formatting style, not semantics)
    // must resolve to the same qualname as the single-line form. Rust
    // only treats `::`-paths as resolvable call targets (a `.`-joined
    // path is left `None`, unchanged by this test), so the chain break
    // sits at the `::` separator here.
    let source = "
struct Foo;
impl Foo {
    fn make() -> Foo { Foo }
}
fn caller() {
    Foo
        ::make();
}
";
    let mut extractor = RustExtractor::new().unwrap();
    let extracted = extractor.extract(source, "crate::pkg::mod").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.detail.is_none())
        .expect("Foo::make() call edge");
    assert_eq!(call.target_qualname.as_deref(), Some("Foo::make"));
}

/// Create an isolated temp dir with a crate rooted at
/// `<repo_root>/<crate_dir>` (a `Cargo.toml` written there) and each
/// `(rel_path, content)` pair written under that crate root, then index the
/// whole `repo_root`. Mirrors a polyglot repo where a Rust crate lives
/// several directories deep (e.g. `node/dpb-app/src-tauri`), not at the repo
/// root (issue #129).
fn index_nested_crate(
    label: &str,
    crate_dir: &str,
    files: &[(&str, &str)],
) -> (tempfile::TempDir, PathBuf, PathBuf) {
    let prefix = format!("lidx-rust-nested-crate-{label}-");
    let tmp = tempfile::Builder::new().prefix(&prefix).tempdir().unwrap();
    let repo_root = tmp.path().to_path_buf();
    let crate_root = repo_root.join(crate_dir);
    std::fs::create_dir_all(&crate_root).unwrap();
    std::fs::write(
        crate_root.join("Cargo.toml"),
        "[package]\nname = \"src-tauri\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    common::write_files(&crate_root, files);
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    (tmp, repo_root, db_path)
}

#[test]
fn module_name_from_path_uses_nearest_cargo_toml_ancestor() {
    // A `Cargo.toml` several directories deep marks its own dir as a crate
    // root; module paths for files under it must be relative to ITS `src/`,
    // not to the repo root -- not `crate::node::dpb-app::src-tauri::src::...`
    // (issue #129).
    let _tmp = tempfile::Builder::new()
        .prefix("lidx-rust-crate-root-")
        .tempdir()
        .unwrap();
    let repo_root = _tmp.path().to_path_buf();
    let crate_root = repo_root.join("node/dpb-app/src-tauri");
    std::fs::create_dir_all(crate_root.join("src/commands/deploy")).unwrap();
    std::fs::write(
        crate_root.join("Cargo.toml"),
        "[package]\nname = \"src-tauri\"\n",
    )
    .unwrap();

    let extractor = RustExtractor::new()
        .unwrap()
        .with_repo_root(repo_root.clone());
    assert_eq!(
        extractor.module_name_from_rel_path("node/dpb-app/src-tauri/src/commands/deploy/sync.rs"),
        "crate::commands::deploy::sync"
    );
    assert_eq!(
        extractor.module_name_from_rel_path("node/dpb-app/src-tauri/src/lib.rs"),
        "crate"
    );
    assert_eq!(
        extractor.module_name_from_rel_path("node/dpb-app/src-tauri/src/grpc.rs"),
        "crate::grpc"
    );
}

#[test]
fn nested_crate_import_resolves_across_files() {
    // End-to-end (issue #129): a crate nested under `node/dpb-app/src-tauri`
    // has a file `src/commands/deploy/sync.rs` with `use
    // crate::grpc::create_channel;` calling that function. Before the fix,
    // both files' qualnames embedded the full repo path
    // (`crate::node::dpb-app::src-tauri::src::...`), so the `use` target
    // never matched `crate::grpc::create_channel` and the CALLS edge fell
    // back to the unresolved bare name.
    let files = [
        ("src/grpc.rs", "pub fn create_channel() {}\n"),
        (
            "src/commands/deploy/sync.rs",
            "use crate::grpc::create_channel;\n\npub fn call_sync_grpc() {\n    create_channel();\n}\n",
        ),
    ];
    let (_tmp, repo_root, db_path) =
        index_nested_crate("import-binding", "node/dpb-app/src-tauri", &files);
    let indexer = Indexer::new(repo_root.clone(), db_path).unwrap();
    let db = indexer.db();
    let graph_version = db.current_graph_version().unwrap();

    let channel_id = db
        .lookup_symbol_id("crate::grpc::create_channel", graph_version)
        .unwrap()
        .expect("crate::grpc::create_channel symbol (crate-relative qualname)");
    let call_id = db
        .lookup_symbol_id(
            "crate::commands::deploy::sync::call_sync_grpc",
            graph_version,
        )
        .unwrap()
        .expect("crate::commands::deploy::sync::call_sync_grpc symbol (crate-relative qualname)");

    let edges = db
        .edges_for_symbol(channel_id, None, graph_version)
        .unwrap();
    assert!(
        edges.iter().any(|e| e.kind == "CALLS"
            && e.target_symbol_id == Some(channel_id)
            && e.source_symbol_id == Some(call_id)),
        "expected call_sync_grpc -> grpc::create_channel CALLS edge via \
         `use crate::grpc::create_channel`, got: {edges:?}"
    );
}
