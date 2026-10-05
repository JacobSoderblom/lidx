//! Issue #336: Python imports inside function/method bodies (lazy or
//! cycle-breaking imports) emitted no `IMPORTS`/`IMPORTS_FILE` edge because
//! the extractor's import handler was gated on module level (`fn_depth == 0`).

mod common;

use std::collections::BTreeSet;

use common::golden;
use lidx::indexer::Indexer;
use lidx::rpc;

/// `(kind, target_qualname, evidence_start_line, bound_name)` of the IMPORTS /
/// IMPORTS_FILE edges whose file path ends with `file`, sorted.
fn import_edges(indexer: &Indexer, file: &str) -> Vec<(String, String, i64, Option<String>)> {
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT e.kind, COALESCE(e.target_qualname, ''), COALESCE(e.evidence_start_line, -1), e.detail
             FROM edges e JOIN files f ON f.id = e.file_id
             WHERE f.path = ?1 AND e.graph_version = ?2 AND e.kind IN ('IMPORTS', 'IMPORTS_FILE')",
        )
        .unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let mut rows: Vec<_> = stmt
        .query_map(rusqlite::params![file, gv], |r| {
            let detail: Option<String> = r.get(3)?;
            let bound = detail
                .and_then(|d| serde_json::from_str::<serde_json::Value>(&d).ok())
                .and_then(|v| v["bound_name"].as_str().map(String::from));
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, bound))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    rows.sort();
    rows
}

fn indexed(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let (tmp, root, db_path) = common::index_repo("lidx-lazy-imports-", files);
    let mut indexer = Indexer::new(root, db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

const HELPERS: &str = "def serialize(x):\n    return x\n";
const LAZY: &str = "def run(x):\n    from pkg.helpers import serialize\n    return serialize(x)\n";

/// The `pkg/__init__.py` + `pkg/helpers.py` + `pkg/lazy.py` fixture shared by
/// several tests (lazy.py content overridable).
fn lazy_files(lazy: &str) -> Vec<(&str, &str)> {
    vec![
        ("pkg/__init__.py", ""),
        ("pkg/helpers.py", HELPERS),
        ("pkg/lazy.py", lazy),
    ]
}

#[test]
fn function_scoped_import_emits_imports_and_imports_file() {
    let (_tmp, indexer) = indexed(&lazy_files(LAZY));
    let edges = import_edges(&indexer, "pkg/lazy.py");
    let imports: Vec<_> = edges.iter().filter(|e| e.0 == "IMPORTS").collect();
    assert_eq!(
        imports,
        vec![&(
            "IMPORTS".to_string(),
            "pkg.helpers.serialize".to_string(),
            2,
            Some("serialize".to_string())
        )],
        "{edges:?}"
    );
    assert!(
        edges
            .iter()
            .any(|e| e.0 == "IMPORTS_FILE" && e.1 == "pkg.helpers.serialize" && e.2 == 2),
        "IMPORTS_FILE to pkg.helpers.serialize expected: {edges:?}"
    );
}

#[test]
fn lazy_import_shapes_all_emit_edges() {
    let src = "\
from typing import TYPE_CHECKING


def alias():
    import a.b as c
    return c


def rel():
    from . import sibling
    return sibling


def guarded():
    if TYPE_CHECKING:
        from pkg.types import T
    return 1


class K:
    import json

    def method(self):
        import os
        return os


def first():
    import shared
    return shared


def second():
    import shared
    return shared
";
    let (_tmp, indexer) = indexed(&[
        ("pkg/__init__.py", ""),
        ("pkg/sibling.py", ""),
        // Targets must exist in the repo: unresolved IMPORTS edges are moved
        // to the unresolved-reference store, not kept as edges.
        ("a/__init__.py", ""),
        ("a/b.py", ""),
        ("pkg/types.py", "class T:\n    pass\n"),
        ("json.py", ""),
        ("os.py", ""),
        ("shared.py", ""),
        ("pkg/modx.py", src),
    ]);
    let edges = import_edges(&indexer, "pkg/modx.py");
    // (shape, edge kind, target, evidence line). The relative import only
    // persists its resolved IMPORTS_FILE edge.
    let expected = [
        ("aliased import in function", "IMPORTS", "a.b", 5),
        (
            "relative import in function",
            "IMPORTS_FILE",
            "pkg.sibling",
            10,
        ),
        ("TYPE_CHECKING-guarded import", "IMPORTS", "pkg.types.T", 16),
        ("class-body import", "IMPORTS", "json", 21),
        ("method import", "IMPORTS", "os", 24),
        ("first duplicate import", "IMPORTS", "shared", 29),
        ("second duplicate import", "IMPORTS", "shared", 34),
    ];
    for (shape, kind, target, line) in expected {
        assert!(
            edges
                .iter()
                .any(|e| e.0 == kind && e.1 == target && e.2 == line),
            "{shape}: expected {kind} -> {target} at line {line} in {edges:?}"
        );
    }
}

#[test]
fn unused_imports_is_file_scoped_for_function_imports() {
    let (_tmp, mut indexer) = indexed(&[(
        "app.py",
        // File-level semantics: a bound name appearing anywhere outside import
        // statements counts as used, regardless of which function it is in.
        "def a():\n    import json\n    import sys\n    return json.dumps({})\n",
    )]);
    let result = rpc::handle_method(&mut indexer, "dead_symbols", serde_json::json!({})).unwrap();
    let unused: Vec<&str> = result["unused_imports"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["target_qualname"].as_str())
        .collect();
    assert!(!unused.contains(&"json"), "{unused:?}");
    assert!(unused.contains(&"sys"), "{unused:?}");
}

type UnresolvedRow = (String, String, Option<String>, String);

fn unresolved_snapshot(indexer: &Indexer) -> BTreeSet<UnresolvedRow> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(s.qualname, ''), ur.edge_kind, ur.reference_name, ur.reason
             FROM unresolved_references ur
             LEFT JOIN symbols s ON s.id = ur.source_symbol_id
             WHERE ur.graph_version = ?",
        )
        .unwrap();
    stmt.query_map([gv], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

#[test]
fn lazy_import_incremental_matches_fresh() {
    // The extra unresolved lazy import makes the unresolved-reference
    // comparison non-trivial.
    let lazy = format!("{LAZY}\n\ndef other():\n    import not_in_repo\n");
    let (_tmp, root, db_path) = common::index_repo("lidx-lazy-incr-", &lazy_files(&lazy));
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    let edited = format!("{lazy}\n# touched\n");
    common::write_files(&root, &[("pkg/lazy.py", &edited)]);
    indexer
        .sync_rel_paths(&["pkg/lazy.py".to_string()])
        .unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let unresolved = unresolved_snapshot(&indexer);

    let (_t, fresh_root, fresh_db) = common::index_repo("lidx-lazy-fresh-", &lazy_files(&edited));
    let mut fresh = Indexer::new(fresh_root, fresh_db).unwrap();
    fresh.reindex().unwrap();
    let fresh_gv = fresh.db().current_graph_version().unwrap();
    let fresh_edges = golden::snapshot_edges(fresh.db(), fresh_gv).unwrap();
    common::assert_matches_fresh(&snapshot, &fresh_edges);
    assert!(
        !unresolved.is_empty(),
        "fixture should leave an unresolved import"
    );
    assert_eq!(unresolved, unresolved_snapshot(&fresh));
    assert!(snapshot.iter().any(|e| e.kind == "IMPORTS_FILE"));
}
