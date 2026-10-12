//! Value references through incremental sync: a deleted or renamed target
//! leaves no stale `REFERENCES` edge and no `unresolved_references` row, and
//! the result matches a fresh reindex of the same tree.

mod common;

use common::golden;
use lidx::indexer::Indexer;

const USER_PY: &str = "from lib import helper\n\n\ndef register(cb):\n    return cb\n\n\ndef run():\n    register(helper)\n";
const LIB_PY: &str = "def helper():\n    return 1\n";
const LIB_RENAMED_PY: &str = "def helper2():\n    return 1\n";

fn references(indexer: &Indexer) -> Vec<(String, Option<String>)> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'REFERENCES' AND e.graph_version = ?",
        )
        .unwrap();
    stmt.query_map([gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn unresolved_rows(indexer: &Indexer) -> i64 {
    let conn = indexer.db().read_conn().unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM unresolved_references WHERE edge_kind = 'REFERENCES'",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

fn indexed(files: &[(&str, &str)]) -> (tempfile::TempDir, std::path::PathBuf, Indexer) {
    let (tmp, root, db_path) = common::index_repo("lidx-valref-sync-", files);
    let indexer = Indexer::new(root.clone(), db_path).unwrap();
    (tmp, root, indexer)
}

#[test]
fn deleting_the_target_drops_the_edge_like_a_fresh_index() {
    let (_tmp, root, mut indexer) = indexed(&[("lib.py", LIB_PY), ("user.py", USER_PY)]);
    assert_eq!(
        references(&indexer),
        [("user.run".to_string(), Some("lib.helper".to_string()))]
    );

    std::fs::remove_file(root.join("lib.py")).unwrap();
    indexer.sync_rel_paths(&["lib.py".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    assert!(
        references(&indexer).is_empty(),
        "{:?}",
        references(&indexer)
    );
    assert_eq!(unresolved_rows(&indexer), 0);

    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&[("user.py", USER_PY)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn renaming_the_target_drops_the_edge_like_a_fresh_index() {
    let (_tmp, root, mut indexer) = indexed(&[("lib.py", LIB_PY), ("user.py", USER_PY)]);

    common::write_files(&root, &[("lib.py", LIB_RENAMED_PY)]);
    indexer.sync_rel_paths(&["lib.py".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    assert!(
        references(&indexer).is_empty(),
        "{:?}",
        references(&indexer)
    );
    assert_eq!(unresolved_rows(&indexer), 0);

    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) =
        common::index_files(&[("lib.py", LIB_RENAMED_PY), ("user.py", USER_PY)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn editing_the_referencing_file_moves_the_edge() {
    let (_tmp, root, mut indexer) = indexed(&[("lib.py", LIB_PY), ("user.py", USER_PY)]);
    let called = USER_PY.replace("register(helper)", "register(helper)\n    helper()");

    common::write_files(&root, &[("user.py", &called)]);
    indexer.sync_rel_paths(&["user.py".to_string()]).unwrap();

    // The pair now has a CALLS edge, so the reference is dropped.
    assert!(
        references(&indexer).is_empty(),
        "{:?}",
        references(&indexer)
    );
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&[("lib.py", LIB_PY), ("user.py", &called)]);
    common::assert_matches_fresh(&snapshot, &fresh);
}
