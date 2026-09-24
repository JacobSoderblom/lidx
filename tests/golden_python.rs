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

/// Incremental scenario (issue #76): rename `caller.entry` to
/// `caller.entry_renamed` in `caller.py`, then sync just that path.
/// `downstream.py` (never resynced) still calls the old name via
/// `downstream.use_entry CALLS caller.entry`.
///
/// The rename deletes `caller.entry`'s symbol row and inserts a fresh one
/// for `caller.entry_renamed` -- on a plain (non-`AUTOINCREMENT`) rowid
/// table, SQLite can hand the new symbol the exact rowid just freed by the
/// old one. Without a never-reused id sequence and an on-delete-set-null
/// foreign key, `downstream.use_entry`'s edge -- never touched by this
/// sync, so it still carries the old row id in `target_symbol_id` -- can
/// silently end up pointing at whatever unrelated symbol now holds that
/// reused id, instead of going unresolved. This asserts the graph never
/// lets that happen: the old name is gone from every edge in the
/// snapshot, and `downstream.use_entry` is either unresolved or correctly
/// rebound to the new name, never silently wrong.
#[test]
fn incremental_rename_leaves_no_dangling_or_wrong_targets() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let caller_path = repo_root.join("caller.py");
    let contents = std::fs::read_to_string(&caller_path).unwrap();
    let renamed = contents.replace("def entry(", "def entry_renamed(");
    assert_ne!(
        contents, renamed,
        "fixture must define caller.entry as `def entry(...)` for this test's rename to apply"
    );
    std::fs::write(&caller_path, renamed).unwrap();
    indexer.sync_rel_paths(&["caller.py".to_string()]).unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();

    let stale: Vec<_> = snapshot
        .iter()
        .filter(|e| e.target_qualname.as_deref() == Some("caller.entry"))
        .collect();
    assert!(
        stale.is_empty(),
        "an edge still resolves to the renamed-away caller.entry: {stale:?}"
    );

    let downstream_edge = snapshot
        .iter()
        .find(|e| e.source_qualname == "downstream.use_entry" && e.kind == "CALLS")
        .expect("downstream.use_entry CALLS edge missing from snapshot entirely");
    assert!(
        matches!(
            downstream_edge.target_qualname.as_deref(),
            None | Some("caller.entry_renamed")
        ),
        "downstream.use_entry CALLS resolved to an unexpected target: {:?}",
        downstream_edge.target_qualname
    );
}

/// Incremental scenario (issue #76): delete `caller.py` (which
/// `downstream.py` imports and calls into), then sync the deletion. No
/// edge may keep pointing at a symbol that no longer exists, and none may
/// silently rebind to an unrelated symbol that happens to reuse a freed
/// row id.
#[test]
fn incremental_delete_file_leaves_no_dangling_or_wrong_targets() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let caller_path = repo_root.join("caller.py");
    std::fs::remove_file(&caller_path).unwrap();
    indexer.sync_rel_paths(&["caller.py".to_string()]).unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();

    let stale: Vec<_> = snapshot
        .iter()
        .filter(|e| {
            e.target_qualname
                .as_deref()
                .is_some_and(|q| q.starts_with("caller."))
        })
        .collect();
    assert!(
        stale.is_empty(),
        "an edge still resolves into deleted caller.py: {stale:?}"
    );

    let downstream_edge = snapshot
        .iter()
        .find(|e| e.source_qualname == "downstream.use_entry" && e.kind == "CALLS");
    if let Some(edge) = downstream_edge {
        assert_eq!(
            edge.target_qualname, None,
            "downstream.use_entry CALLS should be unresolved after caller.py is deleted, \
             not silently pointing at an unrelated symbol: {:?}",
            edge.target_qualname
        );
    }
}
