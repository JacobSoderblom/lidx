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

/// Edit `edited` (one file of a star-import package) and check the sync
/// against a fresh index; returns the target of `use.run`.
fn sync_star_source(before: &str, after: &str) -> Option<String> {
    const INIT: &str = "from .other import *\n";
    const USE_STAR: &str = "from pl import table as t\n\n\ndef run():\n    return t(1)\n";
    let make = |other: &'static str| -> Vec<(&'static str, &'static str)> {
        vec![
            ("pl/__init__.py", INIT),
            ("pl/other.py", other),
            ("use.py", USE_STAR),
        ]
    };
    let before: &'static str = Box::leak(before.to_string().into_boxed_str());
    let after: &'static str = Box::leak(after.to_string().into_boxed_str());
    let tmp = tempfile::Builder::new()
        .prefix("lidx-py-star-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, &make(before));
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();

    common::write_files(&root, &[("pl/other.py", after)]);
    indexer
        .sync_rel_paths(&["pl/other.py".to_string()])
        .unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&make(after));
    common::assert_matches_fresh(&snapshot, &fresh);
    snapshot
        .iter()
        .find(|e| e.kind == "CALLS" && e.source_qualname == "use.run")
        .and_then(|e| e.target_qualname.clone())
}

#[test]
fn an_all_added_to_a_star_source_unbinds_the_call() {
    let target = sync_star_source(
        "def table(x): ...\n\n\ndef make(x): ...\n",
        "__all__ = [\"make\"]\n\n\ndef table(x): ...\n\n\ndef make(x): ...\n",
    );
    assert_eq!(target, None);
}

#[test]
fn a_name_added_to_a_star_source_binds_the_call() {
    let target = sync_star_source(
        "def make(x): ...\n",
        "def make(x): ...\n\n\ndef table(x): ...\n",
    );
    assert_eq!(target.as_deref(), Some("pl.other.table"));
}

#[test]
fn a_star_source_name_made_private_unbinds_the_call() {
    let target = sync_star_source("def table(x): ...\n", "def _table(x): ...\n");
    assert_eq!(target, None);
}

type Files = Vec<(&'static str, &'static str)>;

/// Index `base`, rewrite `edits`, sync those paths, and check the edges
/// against a fresh index of the edited tree. Returns how many files the sync
/// extracted again and the target of each call in `use.run`.
fn sync_edit(base: &Files, edits: &Files) -> (usize, Vec<String>) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-py-surface-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, base);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();

    common::write_files(&root, edits);
    let paths: Vec<String> = edits.iter().map(|(p, _)| p.to_string()).collect();
    let stats = indexer.sync_rel_paths(&paths).unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let mut after = base.clone();
    for (path, content) in edits {
        match after.iter_mut().find(|(p, _)| p == path) {
            Some(entry) => entry.1 = content,
            None => after.push((path, content)),
        }
    }
    let (_fresh_tmp, fresh) = common::index_files(&after);
    common::assert_matches_fresh(&snapshot, &fresh);
    let targets = snapshot
        .iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == "use.run")
        .filter_map(|e| e.target_qualname.clone())
        .collect();
    (stats.indexed, targets)
}

const NOISE: &str = "from json import dumps\n\n\ndef noisy():\n    return dumps(1)\n";
const USE_TWO: &str =
    "from pkg import table as t, extra as ex\n\n\ndef run():\n    t(1)\n    ex(2)\n";
const EXTRA: &str = "def extra(x):\n    return 'extra'\n";

fn one_level() -> Files {
    vec![
        ("pkg/__init__.py", "from .table import table\n"),
        ("pkg/table.py", TABLE),
        ("pkg/other.py", EXTRA),
        ("use.py", USE_TWO),
        ("noise.py", NOISE),
    ]
}

#[test]
fn a_body_only_edit_to_a_re_exported_module_extracts_nothing_else() {
    let (indexed, targets) = sync_edit(
        &one_level(),
        &vec![("pkg/table.py", "def table(x):\n    return 'changed body'\n")],
    );
    assert_eq!(indexed, 1);
    assert_eq!(targets, vec!["pkg.table.table".to_string()]);
}

#[test]
fn a_leaf_module_edit_extracts_nothing_else() {
    let (indexed, _) = sync_edit(&one_level(), &vec![("noise.py", "x = 1\n")]);
    assert_eq!(indexed, 1);
}

#[test]
fn a_def_added_to_a_re_exported_module_that_no_import_names_extracts_nothing_else() {
    let (indexed, targets) = sync_edit(
        &one_level(),
        &vec![(
            "pkg/table.py",
            "def table(x):\n    return 'table'\n\n\ndef more(x): ...\n",
        )],
    );
    // `more` is not a name the package imports, so no importer can change
    assert_eq!(indexed, 1);
    assert_eq!(targets, vec!["pkg.table.table".to_string()]);
}

#[test]
fn a_re_exported_def_removed_unbinds_its_importers() {
    let (_, targets) = sync_edit(&one_level(), &vec![("pkg/table.py", "x = 1\n")]);
    assert!(targets.is_empty(), "{targets:?}");
}

#[test]
fn a_re_exported_def_renamed_unbinds_its_importers() {
    let (indexed, targets) = sync_edit(
        &one_level(),
        &vec![("pkg/table.py", "def table_two(x):\n    return 'table'\n")],
    );
    assert_eq!(indexed, 2);
    assert!(targets.is_empty(), "{targets:?}");
}

#[test]
fn a_name_added_to_an_init_import_binds_its_importers() {
    let (indexed, targets) = sync_edit(
        &one_level(),
        &vec![(
            "pkg/__init__.py",
            "from .table import table\nfrom .other import extra\n",
        )],
    );
    assert_eq!(indexed, 2);
    assert!(
        targets.contains(&"pkg.other.extra".to_string()),
        "{targets:?}"
    );
}

#[test]
fn a_body_only_edit_to_an_init_extracts_nothing_else() {
    let (indexed, _) = sync_edit(
        &one_level(),
        &vec![(
            "pkg/__init__.py",
            "from .table import table\n\n\ndef helper():\n    return 1\n",
        )],
    );
    // a new def nothing imports is a surface change no importer depends on
    assert_eq!(indexed, 1);
    let base: Files = vec![
        (
            "pkg/__init__.py",
            "from .table import table\n\n\ndef helper():\n    return 1\n",
        ),
        ("pkg/table.py", TABLE),
        ("pkg/other.py", EXTRA),
        ("use.py", USE_TWO),
        ("noise.py", NOISE),
    ];
    let (indexed, _) = sync_edit(
        &base,
        &vec![(
            "pkg/__init__.py",
            "from .table import table\n\n\ndef helper():\n    return 2\n",
        )],
    );
    assert_eq!(indexed, 1);
}

fn two_level() -> Files {
    vec![
        ("pkg/__init__.py", "from .sub import table\n"),
        ("pkg/sub/__init__.py", "from .mod import table\n"),
        ("pkg/sub/mod.py", TABLE),
        ("pkg/sub/other.py", OTHER),
        ("use.py", USE),
        ("noise.py", NOISE),
    ]
}

#[test]
fn a_two_level_re_export_follows_a_change_in_the_innermost_module() {
    let (indexed, targets) = sync_edit(
        &two_level(),
        &vec![("pkg/sub/mod.py", "def table_two(x): ...\n")],
    );
    assert_eq!(indexed, 2);
    assert!(targets.is_empty(), "{targets:?}");
    let (indexed, targets) = sync_edit(
        &two_level(),
        &vec![("pkg/sub/mod.py", "def table(x):\n    return 'body'\n")],
    );
    assert_eq!(indexed, 1);
    assert_eq!(targets, vec!["pkg.sub.mod.table".to_string()]);
}

#[test]
fn a_two_level_re_export_follows_a_change_in_the_inner_init() {
    let (indexed, targets) = sync_edit(
        &two_level(),
        &vec![("pkg/sub/__init__.py", "from .other import table\n")],
    );
    assert_eq!(indexed, 2);
    assert_eq!(targets, vec!["pkg.sub.other.table".to_string()]);
}

fn edge_rows(indexer: &Indexer, kind: &str, source: &str) -> (i64, i64) {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let count = |only_resolved: bool| -> i64 {
        conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
                 WHERE e.kind = ?1 AND s.qualname = ?2 AND e.graph_version = ?3 {}",
                if only_resolved {
                    "AND e.target_symbol_id IS NOT NULL"
                } else {
                    ""
                }
            ),
            rusqlite::params![kind, source, gv],
            |row| row.get(0),
        )
        .unwrap()
    };
    (count(true), count(false))
}

/// Rewrite `pkg/a.py` and sync it: the importer's identical uses on one line
/// keep their multiplicity, as in a fresh index.
fn sync_kind_change(before: &str, after: &str, use_src: &'static str, kind: &str, reindex: bool) {
    let base = |a: &'static str| -> Files {
        vec![
            ("pkg/__init__.py", "from .a import table\n"),
            ("pkg/a.py", a),
            ("use.py", use_src),
        ]
    };
    let before: &'static str = Box::leak(before.to_string().into_boxed_str());
    let after: &'static str = Box::leak(after.to_string().into_boxed_str());
    let tmp = tempfile::Builder::new()
        .prefix("lidx-py-kind-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, &base(before));
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();

    common::write_files(&root, &[("pkg/a.py", after)]);
    if reindex {
        indexer.reindex().unwrap();
    } else {
        indexer.sync_rel_paths(&["pkg/a.py".to_string()]).unwrap();
    }
    let incremental = edge_rows(&indexer, kind, "use.run");

    let fresh_tmp = tempfile::Builder::new()
        .prefix("lidx-py-kind-fresh-")
        .tempdir()
        .unwrap();
    common::write_files(fresh_tmp.path(), &base(after));
    let mut fresh_indexer = Indexer::new(
        fresh_tmp.path().to_path_buf(),
        fresh_tmp.path().join(".lidx").join(".lidx.sqlite"),
    )
    .unwrap();
    fresh_indexer.reindex().unwrap();
    let fresh = edge_rows(&fresh_indexer, kind, "use.run");
    assert_eq!(
        incremental, fresh,
        "{kind} rows after {before:?} -> {after:?}"
    );
    assert_eq!(fresh.0, 2, "two uses on one line keep two rows");
}

const TWO_CALLS: &str = "from pkg import table as t\n\n\ndef run():\n    return t(1), t(2)\n";
const TWO_REFS: &str = "from pkg import table as t\n\n\ndef run():\n    return [t, map(t, [1])]\n";
const AS_DEF: &str = "def table(x):\n    return x\n";
const AS_CLASS: &str = "class table:\n    pass\n";

#[test]
fn a_def_made_a_class_keeps_both_calls_on_a_line() {
    sync_kind_change(AS_DEF, AS_CLASS, TWO_CALLS, "CALLS", false);
}

#[test]
fn a_def_made_a_class_keeps_both_calls_on_a_line_on_reindex() {
    sync_kind_change(AS_DEF, AS_CLASS, TWO_CALLS, "CALLS", true);
}

#[test]
fn a_class_made_a_def_keeps_both_calls_on_a_line() {
    sync_kind_change(AS_CLASS, AS_DEF, TWO_CALLS, "CALLS", false);
}

#[test]
fn a_class_made_a_def_keeps_both_calls_on_a_line_on_reindex() {
    sync_kind_change(AS_CLASS, AS_DEF, TWO_CALLS, "CALLS", true);
}

#[test]
fn a_def_made_a_class_keeps_both_references_on_a_line() {
    sync_kind_change(AS_DEF, AS_CLASS, TWO_REFS, "REFERENCES", false);
}

#[test]
fn a_def_made_a_class_keeps_both_references_on_a_line_on_reindex() {
    sync_kind_change(AS_DEF, AS_CLASS, TWO_REFS, "REFERENCES", true);
}

#[test]
fn a_class_made_a_def_keeps_both_references_on_a_line() {
    sync_kind_change(AS_CLASS, AS_DEF, TWO_REFS, "REFERENCES", false);
}

#[test]
fn a_class_made_a_def_keeps_both_references_on_a_line_on_reindex() {
    sync_kind_change(AS_CLASS, AS_DEF, TWO_REFS, "REFERENCES", true);
}

fn delete_init(reindex: bool, files: Files) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-py-init-deleted-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, &files);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();

    std::fs::remove_file(root.join("pkg/__init__.py")).unwrap();
    if reindex {
        indexer.reindex().unwrap();
    } else {
        indexer
            .sync_rel_paths(&["pkg/__init__.py".to_string()])
            .unwrap();
    }
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&files[1..]);
    common::assert_matches_fresh(&snapshot, &fresh);
    assert!(
        snapshot
            .iter()
            .any(|e| e.kind == "CALLS" && e.source_qualname == "use.run"),
        "the importer keeps its call, now to an external stub: {snapshot:#?}"
    );
}

fn init_with_submodule() -> Files {
    vec![
        ("pkg/__init__.py", "from .table import table\n"),
        ("pkg/table.py", TABLE),
        ("use.py", USE),
    ]
}

/// The package holds nothing but its `__init__`, which defines the name.
fn init_alone() -> Files {
    vec![("pkg/__init__.py", TABLE), ("use.py", USE)]
}

#[test]
fn a_deleted_init_leaves_the_importer_as_a_fresh_index_has_it() {
    delete_init(false, init_with_submodule());
}

#[test]
fn a_deleted_init_leaves_the_importer_as_a_fresh_index_has_it_on_reindex() {
    delete_init(true, init_with_submodule());
}

#[test]
fn a_deleted_only_init_leaves_the_importer_with_its_external_stub() {
    delete_init(false, init_alone());
}

#[test]
fn a_deleted_only_init_leaves_the_importer_with_its_external_stub_on_reindex() {
    delete_init(true, init_alone());
}
