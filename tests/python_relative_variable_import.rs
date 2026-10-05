//! Issue #333: a Python relative import (`from .plan import NAME`) resolves
//! by making the specifier absolute against the importing module's package,
//! then an exact-qualname match -- whatever the target's kind (a module-level
//! variable included).

mod common;

use common::golden;
use lidx::indexer::Indexer;

const PLAN: &str = "from typing import Literal\n\nEvolveAction = Literal[\"a\", \"b\"]\nLIMIT = 5\n\n\nclass Spec:\n    pass\n";

/// Indexes `files`; returns the tree guard (the db lives under it) and the indexer.
fn indexed(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let (tmp, root, db_path) = common::index_repo("lidx-py-relvar-", files);
    (tmp, Indexer::new(root, db_path).unwrap())
}

/// `(source, resolved target)` of every IMPORTS edge that has one.
fn imports(indexer: &Indexer) -> Vec<(String, String)> {
    let gv = indexer.db().current_graph_version().unwrap();
    golden::snapshot_edges(indexer.db(), gv)
        .unwrap()
        .into_iter()
        .filter(|e| e.kind == "IMPORTS")
        .filter_map(|e| Some((e.source_qualname, e.target_qualname?)))
        .collect()
}

fn assert_binds(indexer: &Indexer, source: &str, target: &str) {
    let all = imports(indexer);
    assert!(
        all.iter().any(|(s, t)| s == source && t == target),
        "{source} must import {target}: {all:?}"
    );
}

fn unresolved_imports(indexer: &Indexer) -> Vec<String> {
    let conn = indexer.db().read_conn().unwrap();
    let gv: i64 = conn
        .query_row("SELECT MAX(graph_version) FROM symbols", [], |r| r.get(0))
        .unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT reference_name FROM unresolved_references
             WHERE edge_kind = 'IMPORTS' AND graph_version = ?",
        )
        .unwrap();
    stmt.query_map([gv], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn relative_import_of_module_variable_resolves() {
    let (_tmp, indexer) = indexed(&[
        ("pkg/__init__.py", ""),
        (
            "pkg/sub/__init__.py",
            "from .plan import EvolveAction, LIMIT, Spec\n",
        ),
        ("pkg/sub/plan.py", PLAN),
    ]);
    for target in [
        "pkg.sub.plan.EvolveAction",
        "pkg.sub.plan.LIMIT",
        "pkg.sub.plan.Spec",
    ] {
        assert_binds(&indexer, "pkg.sub", target);
    }
    let names = unresolved_imports(&indexer);
    assert!(
        !names.iter().any(|n| n.starts_with(".plan.")),
        "relative imports from .plan must leave no unresolved row: {names:?}"
    );
}

#[test]
fn same_named_variable_in_two_packages_binds_each_to_its_own() {
    let (_tmp, indexer) = indexed(&[
        ("pkg/__init__.py", ""),
        ("pkg/a/__init__.py", "from .plan import EvolveAction\n"),
        ("pkg/a/plan.py", PLAN),
        ("pkg/b/__init__.py", "from .plan import EvolveAction\n"),
        ("pkg/b/plan.py", PLAN),
    ]);
    assert_binds(&indexer, "pkg.a", "pkg.a.plan.EvolveAction");
    assert_binds(&indexer, "pkg.b", "pkg.b.plan.EvolveAction");
}

#[test]
fn from_dot_import_names_the_sibling_module_not_a_stray_variable() {
    let (_tmp, indexer) = indexed(&[
        ("pkg/__init__.py", ""),
        ("pkg/sub/__init__.py", ""),
        ("pkg/sub/main.py", "from . import x\n"),
        ("pkg/sub/x.py", "def f():\n    pass\n"),
        ("other.py", "x = 1\n"),
    ]);
    let all = imports(&indexer);
    assert!(
        all.iter()
            .any(|(s, t)| s == "pkg.sub.main" && t == "pkg.sub.x"),
        "`from . import x` must bind the sibling module: {all:?}"
    );
    assert!(
        !all.iter().any(|(_, t)| t == "other.x"),
        "must not bind the stray variable: {all:?}"
    );
}

#[test]
fn parent_package_relative_import_resolves() {
    let (_tmp, indexer) = indexed(&[
        ("pkg/__init__.py", ""),
        ("pkg/shared.py", "V = 1\n"),
        ("pkg/sub/__init__.py", ""),
        ("pkg/sub/mod.py", "from ..shared import V\n"),
    ]);
    assert_binds(&indexer, "pkg.sub.mod", "pkg.shared.V");
}

#[test]
fn multi_segment_relative_import_keeps_all_segments() {
    let (_tmp, indexer) = indexed(&[
        ("pkg/__init__.py", ""),
        ("pkg/a/__init__.py", ""),
        ("pkg/a/b.py", "V = 1\n"),
        ("pkg/mod.py", "from .a.b import V\n"),
    ]);
    assert_binds(&indexer, "pkg.mod", "pkg.a.b.V");
}

#[test]
fn incremental_sync_matches_fresh_index_for_relative_variable_imports() {
    let init = "from .plan import EvolveAction\n";
    let (_tmp, root, db_path) = common::index_repo(
        "lidx-py-relvar-inc-",
        &[("pkg/__init__.py", ""), ("pkg/sub/__init__.py", init)],
    );
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();

    // Target file appears.
    common::write_files(&root, &[("pkg/sub/plan.py", PLAN)]);
    indexer
        .sync_rel_paths(&["pkg/sub/plan.py".to_string()])
        .unwrap();
    assert_binds(&indexer, "pkg.sub", "pkg.sub.plan.EvolveAction");
    let gv = indexer.db().current_graph_version().unwrap();
    let snap = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_t, fresh) = common::index_files(&[
        ("pkg/__init__.py", ""),
        ("pkg/sub/__init__.py", init),
        ("pkg/sub/plan.py", PLAN),
    ]);
    common::assert_matches_fresh(&snap, &fresh);

    // Target is renamed away: the import goes back to unresolved.
    let renamed = PLAN.replace("EvolveAction", "Renamed");
    common::write_files(&root, &[("pkg/sub/plan.py", &renamed)]);
    indexer
        .sync_rel_paths(&["pkg/sub/plan.py".to_string()])
        .unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snap = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_t, fresh) = common::index_files(&[
        ("pkg/__init__.py", ""),
        ("pkg/sub/__init__.py", init),
        ("pkg/sub/plan.py", &renamed),
    ]);
    common::assert_matches_fresh(&snap, &fresh);
}
