//! Issue #203: Python module-level (and class-level) assignments were never
//! emitted as symbols, so `from util import LIMIT` was permanently
//! unresolved.

mod common;

use lidx::indexer::Indexer;
use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::python::PythonExtractor;
use lidx::rpc;

fn extract(source: &str) -> Vec<(String, String)> {
    let mut extractor = PythonExtractor::new().unwrap();
    let out = extractor.extract(source, "m").unwrap();
    out.symbols
        .iter()
        .filter(|s| s.kind == "const" || s.kind == "variable")
        .map(|s| (s.kind.clone(), s.qualname.clone()))
        .collect()
}

fn qualnames(source: &str) -> Vec<String> {
    extract(source).into_iter().map(|(_, q)| q).collect()
}

#[test]
fn module_assignment_emits_symbol_with_span_and_contains_edge() {
    let source = "LIMIT = 5\nname = 'x'\n";
    let mut extractor = PythonExtractor::new().unwrap();
    let out = extractor.extract(source, "util").unwrap();
    let limit = out
        .symbols
        .iter()
        .find(|s| s.qualname == "util.LIMIT")
        .expect("util.LIMIT symbol");
    assert_eq!(limit.kind, "const");
    assert_eq!(limit.name, "LIMIT");
    assert_eq!(
        &source[limit.start_byte as usize..limit.end_byte as usize],
        "LIMIT = 5"
    );
    let name = out
        .symbols
        .iter()
        .find(|s| s.qualname == "util.name")
        .unwrap();
    assert_eq!(name.kind, "variable");
    assert!(out.edges.iter().any(|e| e.kind == "CONTAINS"
        && e.source_qualname.as_deref() == Some("util")
        && e.target_qualname.as_deref() == Some("util.LIMIT")));
}

#[test]
fn annotated_and_bare_annotation_yield_symbols() {
    let q = qualnames("TIMEOUT: int = 30\nRETRIES: int\n");
    assert_eq!(q, vec!["m.TIMEOUT", "m.RETRIES"]);
}

#[test]
fn function_local_assignment_yields_no_symbol() {
    let q = qualnames(
        "def f():\n    local = 1\n    OTHER: int = 2\n    a, b = 1, 2\n\nclass C:\n    def m(self):\n        x = 1\n        self.y = 2\n",
    );
    assert!(q.is_empty(), "{q:?}");
}

#[test]
fn tuple_unpacking_and_chained_assignment_yield_one_symbol_per_name() {
    let q = qualnames("X, Y = 1, 2\nA = B = 1\n(P, (Q, *R)) = f()\n");
    assert_eq!(q, vec!["m.X", "m.Y", "m.A", "m.B", "m.P", "m.Q", "m.R"]);
}

#[test]
fn attribute_and_subscript_targets_yield_no_symbol() {
    let q = qualnames("import os\nos.environ['A'] = '1'\nobj.attr = 2\n");
    assert!(q.is_empty(), "{q:?}");
}

#[test]
fn augmented_assignment_yields_no_symbol() {
    let q = qualnames("X = 1\nX += 1\nY -= 2\n");
    assert_eq!(q, vec!["m.X"]);
}

#[test]
fn reassigned_name_yields_one_symbol() {
    let q = qualnames("X = 1\nX = 2\nif True:\n    X = 3\n");
    assert_eq!(q, vec!["m.X"]);
}

/// Decision: class-level assignments ARE symbols, qualified by the class
/// (`m.C.attr`), with a CONTAINS edge from the class.
#[test]
fn class_level_assignments_are_symbols_qualified_by_class() {
    let source = "class C:\n    attr = 1\n    TAG: str = 'a'\n    bare: int\n\n    def m(self):\n        self.z = 1\n";
    let mut extractor = PythonExtractor::new().unwrap();
    let out = extractor.extract(source, "m").unwrap();
    let q: Vec<_> = out
        .symbols
        .iter()
        .filter(|s| s.kind == "const" || s.kind == "variable")
        .map(|s| s.qualname.as_str())
        .collect();
    assert_eq!(q, vec!["m.C.attr", "m.C.TAG", "m.C.bare"]);
    assert!(out.edges.iter().any(|e| e.kind == "CONTAINS"
        && e.source_qualname.as_deref() == Some("m.C")
        && e.target_qualname.as_deref() == Some("m.C.attr")));
}

fn indexed(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-py-consts-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

const UTIL: &str = "LIMIT = 5\n\n\ndef helper():\n    return LIMIT\n";
const TEST_UTIL: &str =
    "from util import helper, LIMIT\n\n\ndef test_it():\n    assert helper() == LIMIT\n";

#[test]
fn import_of_module_constant_resolves_and_read_symbol_returns_source() {
    let (_tmp, mut indexer) = indexed(&[("util.py", UTIL), ("test_util.py", TEST_UTIL)]);

    let gv = indexer.db().current_graph_version().unwrap();
    let rows = indexer.db().unresolved_reference_summary(gv).unwrap();
    assert!(
        rows.iter().all(|r| r.count == 0 || r.language != "python"),
        "no unresolved python references expected: {rows:?}"
    );

    let edges = lidx_edges(&indexer, gv);
    assert!(
        edges.iter().any(|(src, kind, tgt)| src == "test_util"
            && kind == "IMPORTS"
            && tgt.as_deref() == Some("util.LIMIT")),
        "IMPORTS edge must resolve to util.LIMIT: {edges:?}"
    );

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "util.LIMIT"}),
    )
    .unwrap();
    assert!(result.to_string().contains("LIMIT = 5"), "{result}");
}

#[test]
fn all_export_still_suppresses_unused_import_and_is_a_symbol() {
    let (_tmp, mut indexer) = indexed(&[
        ("pkg/thing.py", "def thing():\n    return 1\n"),
        (
            "pkg/__init__.py",
            "from pkg.thing import thing\nimport sys\n\n__all__ = [\"thing\"]\n",
        ),
    ]);
    let result = rpc::handle_method(&mut indexer, "dead_symbols", serde_json::json!({})).unwrap();
    let unused: Vec<String> = result["unused_imports"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["target_qualname"].as_str().map(String::from))
        .collect();
    assert!(!unused.iter().any(|q| q == "pkg.thing.thing"), "{unused:?}");
    assert!(unused.iter().any(|q| q == "sys"), "{unused:?}");
}

fn lidx_edges(indexer: &Indexer, gv: i64) -> Vec<(String, String, Option<String>)> {
    let snapshot = common::golden::snapshot_edges(indexer.db(), gv).unwrap();
    snapshot
        .into_iter()
        .map(|e| (e.source_qualname, e.kind, e.target_qualname))
        .collect()
}
