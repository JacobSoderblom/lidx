//! Golden edge harness (#71): whole-graph correctness scoreboard for the
//! Python fixture under `tests/fixtures/golden/python/` (see that
//! directory's `expected_edges.txt` for the expected-edges format and the
//! `xfail` marker convention, and `tests/common/golden.rs` for the
//! snapshot/comparison support these tests share).
//!
//! Covers, per issue #71: plain call, `from x import y` call, method call
//! on self, receiver-typed call, inherited method call, ambiguous bare
//! name, and a call into an external library -- plus an incremental
//! (edit-caller-only + `sync_rel_paths`) scenario covering NULL-target
//! re-resolution, and a cross-file incoming edge that must survive that
//! sync intact (see the incremental test's own doc below for exactly what
//! that scenario does and doesn't exercise).

mod common;

use common::golden::{self, ExpectedEdge};
use lidx::indexer::Indexer;

/// Precision/recall floors, asserted at today's measured baseline -- see
/// `expected_edges.txt`'s header for the measurement date and the current
/// numbers. Raise these only alongside a real resolver fix that moves the
/// measured baseline, never speculatively.
const PRECISION_FLOOR: f64 = 1.0;
const RECALL_FLOOR: f64 = 1.0;

fn expected_edges() -> Vec<ExpectedEdge> {
    let text =
        std::fs::read_to_string(golden::fixture_path("golden/python/expected_edges.txt")).unwrap();
    golden::parse_expected_edges(&text)
}

fn fixture_modules() -> std::collections::HashSet<String> {
    golden::fixture_source_modules("golden/python")
}

#[test]
fn full_reindex_matches_expected_edges() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let graph_version = indexer.db().current_graph_version().unwrap();

    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let report = golden::compare(&snapshot, &expected_edges(), &fixture_modules());
    report.assert_floors("python", PRECISION_FLOOR, RECALL_FLOOR);
}

/// Incremental scenario: edit the caller file only (its content hash
/// changes; no call sites move) and sync just that path.
///
/// What this genuinely exercises (verified against `Indexer::sync_abs_paths`
/// and `Db::update_file_symbols`/`resolve_null_target_edges`, per PR #82
/// review):
///
/// - NULL-target re-resolution (`resolve_null_target_edges`) does real
///   work here -- `caller.py`'s own edges are deleted and reinserted on
///   every sync (`index_file`'s "still use delete-all-insert for now" for
///   edges), and this pass re-links whatever `insert_edges` didn't bind
///   inline.
/// - `downstream.py` (see the fixture) gives the sync a genuine incoming
///   edge -- `downstream.use_entry CALLS caller.entry` -- that must still
///   resolve to `caller.entry` afterward, not go dangling or unresolved,
///   even though only `caller.py` was re-synced.
///
/// What it does *not* exercise, despite being a plausible-sounding claim:
/// dangling-symbol-id repair (`repair_dangling_symbol_ids`). A content-only
/// edit like this one classifies `caller.py`'s symbols as "modified", which
/// `Db::update_file_symbols` handles with an `UPDATE ... WHERE stable_id =
/// ?` that keeps the existing row id -- so `caller.entry`'s id, and
/// `downstream.use_entry`'s edge pointing at it, never actually change.
/// Dangling ids only arise from a genuine rename (`diff.deleted` +
/// `diff.added`), and even then `update_file_symbols` already NULLs other
/// files' references to the deleted rowid inline, before
/// `repair_dangling_symbol_ids` ever runs -- confirmed by this test's own
/// output never logging `"nullified ... dangling symbol id(s)"`. Adding a
/// caller file (per the review's second suggested fix) doesn't change
/// that; it was verified empirically rather than assumed.
///
/// It also does *not* exercise `create_graph_version`/`carry_forward_files`
/// -- those only run in `reindex`, not `sync_abs_paths`.
#[test]
fn incremental_sync_after_editing_caller_matches_expected_edges() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let caller_path = repo_root.join("caller.py");
    let mut contents = std::fs::read_to_string(&caller_path).unwrap();
    contents.push_str("\n# a harmless trailing comment, to change caller's hash only\n");
    std::fs::write(&caller_path, contents).unwrap();
    indexer.sync_rel_paths(&["caller.py".to_string()]).unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let report = golden::compare(&snapshot, &expected_edges(), &fixture_modules());
    report.assert_floors("python", PRECISION_FLOOR, RECALL_FLOOR);
}
