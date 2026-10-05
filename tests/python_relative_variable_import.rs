//! Issue #333: `from .plan import NAME` where `NAME` is a module-level
//! assignment (kind `variable`/`const`) stayed unresolved; the absolute form
//! resolved.

mod common;

use lidx::indexer::Indexer;

const PLAN: &str = "from typing import Literal\n\nEvolveAction = Literal[\"a\", \"b\"]\nLIMIT = 5\n\n\nclass Spec:\n    pass\n";

fn indexed() -> (tempfile::TempDir, std::path::PathBuf, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-py-relvar-")
        .tempdir()
        .unwrap();
    common::write_files(
        tmp.path(),
        &[
            ("pkg/__init__.py", ""),
            (
                "pkg/sub/__init__.py",
                "from .plan import EvolveAction, LIMIT, Spec\n",
            ),
            ("pkg/sub/plan.py", PLAN),
        ],
    );
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    (tmp, db_path, indexer)
}

#[test]
fn relative_import_of_module_variable_resolves() {
    let (_tmp, _db_path, indexer) = indexed();
    let gv = indexer.db().current_graph_version().unwrap();
    let edges = common::golden::snapshot_edges(indexer.db(), gv).unwrap();
    for target in [
        "pkg.sub.plan.EvolveAction",
        "pkg.sub.plan.LIMIT",
        "pkg.sub.plan.Spec",
    ] {
        assert!(
            edges.iter().any(|e| e.kind == "IMPORTS"
                && e.source_qualname == "pkg.sub"
                && e.target_qualname.as_deref() == Some(target)),
            "IMPORTS must resolve to {target}: {edges:?}"
        );
    }
}

#[test]
fn relative_variable_import_leaves_no_unresolved_row() {
    let (_tmp, db_path, _indexer) = indexed();
    let conn = rusqlite::Connection::open(db_path).unwrap();
    let mut stmt = conn
        .prepare("SELECT reference_name FROM unresolved_references WHERE edge_kind = 'IMPORTS'")
        .unwrap();
    let names: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        !names.iter().any(|n| n.starts_with(".plan.")),
        "relative imports from .plan must resolve: {names:?}"
    );
}
