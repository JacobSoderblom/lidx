//! Golden edge harness (#71): whole-graph correctness scoreboard for the
//! Python fixture under `tests/fixtures/golden/python/` (see that
//! directory's `expected_edges.txt` for the expected-edges format and the
//! `xfail` marker convention, and `tests/common/golden.rs` for the
//! snapshot/comparison support these tests share).
//!
//! Covers, per issue #71: plain call, `from x import y` call, method call
//! on self, receiver-typed call, inherited method call, ambiguous bare
//! name, and a call into an external library -- plus an incremental
//! (edit-caller-only + `sync_rel_paths`) scenario.

mod common;

use common::golden::{self, ExpectedEdge};
use lidx::indexer::Indexer;

/// Precision/recall floors, asserted at today's measured baseline: 1.0/1.0
/// (7/7 CALLS edges) -- see `expected_edges.txt`'s header for the
/// measurement date. Raise these only alongside a real resolver fix that
/// moves the measured baseline, never speculatively.
const PRECISION_FLOOR: f64 = 1.0;
const RECALL_FLOOR: f64 = 1.0;

fn expected_edges() -> Vec<ExpectedEdge> {
    let text =
        std::fs::read_to_string(golden::fixture_path("golden/python/expected_edges.txt")).unwrap();
    golden::parse_expected_edges(&text)
}

#[test]
fn full_reindex_matches_expected_edges() {
    let (repo_root, db_path) = golden::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let graph_version = indexer.db().current_graph_version().unwrap();

    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let report = golden::compare(&snapshot, &expected_edges());
    report.assert_floors(PRECISION_FLOOR, RECALL_FLOOR);

    let _ = std::fs::remove_dir_all(&repo_root);
}

/// Incremental scenario: edit the caller file only (its content hash
/// changes; no call sites move) and sync just that path. Watch mode's
/// single-file sync must not regress edges belonging to files it didn't
/// touch -- carry-forward, dangling-id repair, and NULL-target
/// re-resolution all run on this path (see `Indexer::sync_abs_paths`).
#[test]
fn incremental_sync_after_editing_caller_matches_expected_edges() {
    let (repo_root, db_path) = golden::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let caller_path = repo_root.join("caller.py");
    let mut contents = std::fs::read_to_string(&caller_path).unwrap();
    contents.push_str("\n# a harmless trailing comment, to change caller's hash only\n");
    std::fs::write(&caller_path, contents).unwrap();
    indexer.sync_rel_paths(&["caller.py".to_string()]).unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let report = golden::compare(&snapshot, &expected_edges());
    report.assert_floors(PRECISION_FLOOR, RECALL_FLOOR);

    let _ = std::fs::remove_dir_all(&repo_root);
}
