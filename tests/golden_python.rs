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

use common::golden::{self, EdgeKey, ExpectedEdge};
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

/// Every edge's `source_symbol_id`/`target_symbol_id` is either NULL or
/// references a row that still exists in `symbols` -- the property schema
/// v17's `edges` foreign key (`ON DELETE SET NULL` onto `symbols(id)`,
/// issue #76) makes true by construction. Checked against the raw ids
/// directly, not through `golden::snapshot_edges`'s qualname-joined view:
/// that view's `LEFT JOIN` renders a dangling id exactly like a properly
/// NULLed one (both come out `None`), so it can't tell "resolved to
/// nothing" apart from "silently left pointing at a row that's gone".
fn assert_no_dangling_edge_targets(db: &lidx::db::Db) {
    let conn = db.read_conn().unwrap();
    for column in ["source_symbol_id", "target_symbol_id"] {
        let dangling: i64 = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM edges
                     WHERE {column} IS NOT NULL
                       AND {column} NOT IN (SELECT id FROM symbols)"
                ),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            dangling, 0,
            "{dangling} edge(s) have a dangling {column} (references a symbol row \
             that no longer exists in the symbols table)"
        );
    }
}

/// The extra call this test (see below) gives `downstream.py`, purely in
/// its temp-repo copy -- never in the checked-in fixture, so it can't
/// affect any other test in this file. `from x import y` matches the shape
/// (and expected `import` resolution tier) of `call_imported_helper CALLS
/// helper.format_greeting import` already in `expected_edges.txt`.
const DOWNSTREAM_EXTRA_CALL_SOURCE: &str = "\
from other_module import local_util


def use_bump_target() -> str:
    return local_util()
";

/// `expected_edges()`, plus one line for `use_bump_target`'s call, gone
/// UNRESOLVED: the rename test's setup (see its doc comment) retargets
/// `use_bump_target` to call a symbol it then renames away without ever
/// resyncing `downstream.py` again, so this call's stored `target_qualname`
/// text can no longer resolve to anything.
fn expected_edges_after_rename() -> Vec<ExpectedEdge> {
    let mut edges = expected_edges();
    edges.push(ExpectedEdge {
        key: EdgeKey {
            source_qualname: "downstream.use_bump_target".to_string(),
            kind: "CALLS".to_string(),
            target_qualname: None,
            resolution_kind: None,
        },
        xfail: false,
    });
    edges
}

/// `expected_edges()`, transformed for the state after `caller.py` is
/// deleted and synced: every line sourced from `caller.py` is dropped (the
/// file, and everything it defined, no longer exists to be a source at
/// all), and `downstream.use_entry`'s call into it goes UNRESOLVED.
fn expected_edges_after_caller_deleted() -> Vec<ExpectedEdge> {
    expected_edges()
        .into_iter()
        .filter(|edge| !edge.key.source_qualname.starts_with("caller."))
        .map(|mut edge| {
            if edge.key.source_qualname == "downstream.use_entry"
                && edge.key.target_qualname.as_deref() == Some("caller.entry")
            {
                edge.key.target_qualname = None;
                edge.key.resolution_kind = None;
            }
            edge
        })
        .collect()
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
/// What it does *not* exercise: any dangling-symbol-id path. A
/// content-only edit like this one classifies `caller.py`'s symbols as
/// "modified", which `Db::update_file_symbols` handles with an
/// `UPDATE ... WHERE stable_id = ?` that keeps the existing row id -- so
/// `caller.entry`'s id, and `downstream.use_entry`'s edge pointing at it,
/// never actually change. Only a genuine rename (`diff.deleted` +
/// `diff.added`) frees a row id; see
/// `incremental_rename_leaves_no_dangling_or_wrong_targets` for that.
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

/// Incremental scenario (issue #76): rename whichever `other_module.py`
/// symbol currently holds the table's highest row id, then sync just that
/// path. `downstream.py`'s `use_bump_target` (added below, then never
/// resynced again) keeps calling the old name.
///
/// The three-step setup below deliberately arranges for the renamed symbol
/// to hold the table's current max row id right before the rename: only
/// then does freeing it force the very next insert to compete for that
/// exact id, exercising real SQLite rowid reuse rather than a merely
/// dangling reference. Renaming a symbol whose id isn't already the max
/// (e.g. `caller.py`'s `entry`) can't reach that.
///
/// Checks two ways: the replacement symbol gets a genuinely fresh id,
/// never the freed one, and the whole post-sync snapshot matches the
/// fixture's expected edges with the rename applied (no edge is wrong,
/// none is missing).
#[test]
fn incremental_rename_leaves_no_dangling_or_wrong_targets() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");

    let downstream_path = repo_root.join("downstream.py");
    let mut downstream_src = std::fs::read_to_string(&downstream_path).unwrap();
    downstream_src.push_str(DOWNSTREAM_EXTRA_CALL_SOURCE);
    std::fs::write(&downstream_path, downstream_src).unwrap();

    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Step 2: bump `local_util` to a fresh id that's the table's new max.
    // Its own, separate sync -- not combined with any other change -- so
    // it's the sync's only added symbol: `compute_symbol_diff` keys added
    // symbols by a `HashMap`, whose iteration order isn't stable across
    // process runs, so a sync that added more than one row here couldn't
    // deterministically guarantee which one lands on the max id.
    let other_module_path = repo_root.join("other_module.py");
    let contents = std::fs::read_to_string(&other_module_path).unwrap();
    let bumped = contents.replacen("def local_util(", "def local_util_bumped(", 1);
    assert_ne!(
        contents, bumped,
        "fixture must define other_module.local_util as `def local_util(...)` for this test's \
         setup to apply"
    );
    std::fs::write(&other_module_path, &bumped).unwrap();
    indexer
        .sync_rel_paths(&["other_module.py".to_string()])
        .unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();
    let bumped_id = indexer
        .db()
        .get_symbol_by_qualname("other_module.local_util_bumped", graph_version)
        .unwrap()
        .expect("other_module.local_util_bumped must exist after the bump sync")
        .id;
    let max_symbol_id: i64 = indexer
        .db()
        .read_conn()
        .unwrap()
        .query_row("SELECT MAX(id) FROM symbols", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        bumped_id, max_symbol_id,
        "precondition: other_module.local_util_bumped (id {bumped_id}) must hold the table's \
         current max symbol id ({max_symbol_id}) -- it was just synced alone as this sync's \
         only added symbol, so nothing else should have claimed a higher one"
    );

    // Step 3: retarget `use_bump_target` (a content-only change -- its
    // qualname/signature/kind, and so its stable id, don't move) to call
    // `local_util_bumped`, without adding any new symbol row.
    let downstream_src = std::fs::read_to_string(&downstream_path).unwrap();
    let retargeted = downstream_src.replace("local_util", "local_util_bumped");
    std::fs::write(&downstream_path, retargeted).unwrap();
    indexer
        .sync_rel_paths(&["downstream.py".to_string()])
        .unwrap();

    let max_symbol_id_after_retarget: i64 = indexer
        .db()
        .read_conn()
        .unwrap()
        .query_row("SELECT MAX(id) FROM symbols", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        max_symbol_id_after_retarget, bumped_id,
        "precondition: retargeting use_bump_target must not insert any new symbol row -- \
         local_util_bumped (id {bumped_id}) should still hold the table's max id \
         ({max_symbol_id_after_retarget})"
    );

    // The actual rename under test: this frees the table's current max id.
    // Without schema v17's AUTOINCREMENT sequence, SQLite's plain
    // max(rowid) + 1 allocation would hand that exact freed id right back
    // out to the replacement symbol inserted below (nothing else gets
    // inserted in between) -- an *actual* reuse, not merely a vanished id.
    let contents = std::fs::read_to_string(&other_module_path).unwrap();
    let renamed = contents.replacen("def local_util_bumped(", "def local_util_renamed(", 1);
    assert_ne!(contents, renamed);
    std::fs::write(&other_module_path, renamed).unwrap();
    indexer
        .sync_rel_paths(&["other_module.py".to_string()])
        .unwrap();

    let renamed_id = indexer
        .db()
        .get_symbol_by_qualname("other_module.local_util_renamed", graph_version)
        .unwrap()
        .expect("other_module.local_util_renamed must exist after the final rename sync")
        .id;
    assert!(
        renamed_id > bumped_id,
        "other_module.local_util_renamed (id {renamed_id}) must be a genuinely fresh id, \
         strictly greater than local_util_bumped's freed id ({bumped_id}) -- schema v17's \
         never-reused id sequence (issue #76) must never hand a freed id back out. Without it, \
         SQLite's plain max(rowid) + 1 allocation would reuse {bumped_id} exactly here, since \
         it was the table's max id at the moment this rename freed it (see this test's setup) \
         and nothing else was inserted in between."
    );

    assert_no_dangling_edge_targets(indexer.db());

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let report = golden::compare(
        &snapshot,
        &expected_edges_after_rename(),
        &fixture_modules(),
    );
    report.assert_floors("python (post-rename)", PRECISION_FLOOR, RECALL_FLOOR);
}

/// Incremental scenario (issue #76): delete `caller.py` (which
/// `downstream.py` imports and calls into), then sync the deletion.
/// `downstream.use_entry`'s edge is never touched by this sync, so the same
/// raw-id and full-snapshot checks as the rename test apply: no edge may
/// keep pointing at a symbol that no longer exists, and the whole
/// post-sync snapshot must match the fixture's expected edges with every
/// `caller.py`-sourced line removed and the incoming call from
/// `downstream.py` gone unresolved.
///
/// Doesn't need the rename test's rowid-reuse setup: a pure deletion has no
/// competing insert within the same sync to reuse the freed id, so there's
/// no "valid but wrong" outcome reachable here for the foreign key to
/// specifically guard against -- only "dangling", which the raw-id check
/// still verifies directly.
#[test]
fn incremental_delete_file_leaves_no_dangling_or_wrong_targets() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let caller_path = repo_root.join("caller.py");
    std::fs::remove_file(&caller_path).unwrap();
    indexer.sync_rel_paths(&["caller.py".to_string()]).unwrap();

    assert_no_dangling_edge_targets(indexer.db());

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let report = golden::compare(
        &snapshot,
        &expected_edges_after_caller_deleted(),
        &fixture_modules(),
    );
    report.assert_floors("python (post-delete)", PRECISION_FLOOR, RECALL_FLOOR);
}
