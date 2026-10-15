//! A name imported from a package resolves through what the package's
//! `__init__` binds. After an incremental sync that changes the `__init__`,
//! the importers' edges must equal a fresh reindex of the same tree.

mod common;

use common::golden;
use lidx::indexer::Indexer;

const TABLE: &str = "def table(x):\n    return 'table'\n";
const OTHER: &str = "def table(x):\n    return 'other'\n";
const USE: &str = "from pkg import table as t\n\n\ndef run():\n    return t(1)\n";

fn files(init: &str) -> Vec<(&'static str, &str)> {
    vec![
        ("pkg/__init__.py", init),
        ("pkg/table.py", TABLE),
        ("pkg/other.py", OTHER),
        ("use.py", USE),
    ]
}

/// Index `before`, rewrite the `__init__` to `after`, sync it, and check the
/// edges against a fresh index. Returns the target of `use.run`'s call.
fn sync_init(before: &str, after: &str) -> Option<String> {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-py-reexport-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, &files(before));
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();

    common::write_files(&root, &[("pkg/__init__.py", after)]);
    indexer
        .sync_rel_paths(&["pkg/__init__.py".to_string()])
        .unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&files(after));
    common::assert_matches_fresh(&snapshot, &fresh);

    snapshot
        .iter()
        .find(|e| e.kind == "CALLS" && e.source_qualname == "use.run")
        .and_then(|e| e.target_qualname.clone())
}

#[test]
fn a_re_export_changed_from_one_module_to_another_follows() {
    let target = sync_init("from .table import table\n", "from .other import table\n");
    assert_eq!(target.as_deref(), Some("pkg.other.table"));
}

#[test]
fn a_removed_re_export_unbinds_the_call() {
    let target = sync_init("from .table import table\n", "");
    assert_eq!(target, None);
}

#[test]
fn an_added_re_export_binds_the_call() {
    let target = sync_init("", "from .other import table\n");
    assert_eq!(target.as_deref(), Some("pkg.other.table"));
}

#[test]
fn a_star_import_added_beside_an_explicit_binding_changes_nothing() {
    let target = sync_init(
        "from .table import table\n",
        "from .table import table\nfrom .other import *\n",
    );
    assert_eq!(target.as_deref(), Some("pkg.table.table"));
}

#[test]
fn a_re_export_through_a_second_package_follows_the_inner_init() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-py-reexport-chain-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    let chain = |inner: &'static str| -> Vec<(&'static str, &'static str)> {
        vec![
            ("pkg/__init__.py", "from .sub import table\n"),
            ("pkg/sub/__init__.py", inner),
            ("pkg/sub/table.py", TABLE),
            ("pkg/sub/other.py", OTHER),
            ("use.py", USE),
        ]
    };
    common::write_files(&root, &chain("from .table import table\n"));
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();

    let after = "from .other import table\n";
    common::write_files(&root, &[("pkg/sub/__init__.py", after)]);
    indexer
        .sync_rel_paths(&["pkg/sub/__init__.py".to_string()])
        .unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&chain(after));
    common::assert_matches_fresh(&snapshot, &fresh);
    assert!(snapshot.iter().any(|e| e.kind == "CALLS"
        && e.source_qualname == "use.run"
        && e.target_qualname.as_deref() == Some("pkg.sub.other.table")));
}

#[test]
fn a_reindex_after_the_init_changed_matches_fresh_too() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-py-reexport-reindex-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, &files("from .table import table\n"));
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();

    let after = "from .other import table\n";
    common::write_files(&root, &[("pkg/__init__.py", after)]);
    indexer.reindex().unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&files(after));
    common::assert_matches_fresh(&snapshot, &fresh);
    assert!(snapshot.iter().any(|e| e.kind == "CALLS"
        && e.source_qualname == "use.run"
        && e.target_qualname.as_deref() == Some("pkg.other.table")));
}
