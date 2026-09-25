//! Golden edge harness (#71): whole-graph correctness scoreboard for the
//! Python fixture under `tests/fixtures/golden/python/` (see that
//! directory's `expected_edges.txt` for the expected-edges format and the
//! `xfail` marker convention, and `tests/common/golden.rs` for the
//! snapshot/comparison support these tests share).
//!
//! Covers, per issue #71: plain call, `from x import y` call, method call
//! on self, receiver-typed call, inherited method call, ambiguous bare
//! name, and a call into an external library -- plus incremental
//! scenarios covering NULL-target re-resolution and a cross-file incoming
//! edge that must survive a sync intact.
//!
//! Issue #77's four named incremental scenarios (edit callee, rename
//! callee, delete file, add file defining a previously unresolved name)
//! each get their own test below, against this fixture; every one also
//! asserts its post-sync snapshot against `common::fresh_reindex_snapshot`
//! -- a *fresh* copy of the fixture, brought to the same final-tree state
//! in one shot and fully reindexed -- per that issue's acceptance
//! criteria: an incremental result must be indistinguishable from a fresh
//! full reindex of the same final tree, not merely satisfy the fixture's
//! expected-edges file. The rest of issue #77's incremental/ambiguity
//! regressions (deleted-and-restored files, ambiguity-rule edge cases,
//! synthetic non-fixture trees) live in `tests/incremental_sync.rs`.

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

/// The edit-callee incremental test's mutation (issue #77): change
/// `caller.entry`'s signature, not just surrounding text. Unlike a
/// same-signature edit (`Db::update_file_symbols` handles that with an
/// in-place `UPDATE ... WHERE stable_id = ?`, never freeing the row), a
/// signature change moves `entry`'s stable_id -- its old row is deleted and
/// a new one inserted under a fresh id. `downstream.py`'s
/// `downstream.use_entry CALLS caller.entry` edge (never itself resynced)
/// must reattach to that new row by name, not go dangling or stay
/// unresolved just because it kept the same qualname under a different id.
fn add_entry_parameter(root: &std::path::Path) {
    let caller_path = root.join("caller.py");
    let contents = std::fs::read_to_string(&caller_path).unwrap();
    let edited = contents.replacen(
        "def entry() -> str:",
        "def entry(loud: bool = False) -> str:",
        1,
    );
    assert_ne!(
        contents, edited,
        "fixture must define caller.entry as `def entry() -> str:` for this test's edit to apply"
    );
    std::fs::write(&caller_path, edited).unwrap();
}

/// A new file this suite's add-file test (issue #77) introduces: a
/// top-level (non-method) `process`, giving the guarded name-fallback
/// tier a legal candidate for `bare_call_method.bare_caller`'s bare
/// `process()` call -- see `bare_call_method.py`'s own docstring for why
/// that call stays UNRESOLVED without it (the only other `process` in the
/// fixture is a method, which the bare-call guard refuses).
const WORKER_SOURCE: &str = "\
def process() -> str:
    # Top-level function, not a method: the name-fallback tier's first
    # legal candidate for bare_call_method.bare_caller's bare call.
    return \"worker\"
";

/// `expected_edges()`, with `bare_call_method.bare_caller`'s line
/// (UNRESOLVED before `worker.py` exists) retargeted to `worker.process`
/// via the `bare_name` tier.
fn expected_edges_after_worker_added() -> Vec<ExpectedEdge> {
    let mut edges = expected_edges();
    for edge in &mut edges {
        if edge.key.source_qualname == "bare_call_method.bare_caller"
            && edge.key.kind == "CALLS"
            && edge.key.target_qualname.is_none()
        {
            edge.key.target_qualname = Some("worker.process".to_string());
            edge.key.resolution_kind = Some("bare_name".to_string());
        }
    }
    edges
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

/// Incremental scenario (issue #77): edit a callee's *signature*, not just
/// surrounding text, and sync just its file.
///
/// Unlike a same-signature content edit (`Db::update_file_symbols` keeps
/// the existing row id via `UPDATE ... WHERE stable_id = ?`), changing
/// `entry`'s signature moves its stable_id: `caller.py`'s sync deletes the
/// old `entry` row and inserts a new one under a fresh id. `downstream.py`
/// (see the fixture) gives this a genuine incoming edge --
/// `downstream.use_entry CALLS caller.entry` -- that must reattach to the
/// new row, not go dangling or unresolved, even though `downstream.py`
/// itself is never resynced.
#[test]
fn incremental_sync_after_editing_callee_signature_reattaches_incoming_edge() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let graph_version = indexer.db().current_graph_version().unwrap();
    let entry_id_before = indexer
        .db()
        .get_symbol_by_qualname("caller.entry", graph_version)
        .unwrap()
        .expect("caller.entry must exist before the edit")
        .id;

    add_entry_parameter(&repo_root);
    indexer.sync_rel_paths(&["caller.py".to_string()]).unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();
    let entry_id_after = indexer
        .db()
        .get_symbol_by_qualname("caller.entry", graph_version)
        .unwrap()
        .expect("caller.entry must still exist after the edit")
        .id;
    assert_ne!(
        entry_id_before, entry_id_after,
        "precondition: changing entry's signature must free its old row and insert a new one -- \
         otherwise this test isn't exercising a dangling-id path at all (see \
         incremental_rename_leaves_no_dangling_or_wrong_targets for that shape)"
    );
    common::assert_no_dangling_edge_targets(indexer.db());

    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let incoming_target = snapshot
        .iter()
        .find(|edge| edge.source_qualname == "downstream.use_entry" && edge.kind == "CALLS")
        .and_then(|edge| edge.target_qualname.as_deref());
    assert_eq!(
        incoming_target,
        Some("caller.entry"),
        "downstream.use_entry's incoming edge must reattach to the new caller.entry row, not \
         go dangling or unresolved, even though downstream.py itself was never resynced"
    );

    let report = golden::compare(&snapshot, &expected_edges(), &fixture_modules());
    report.assert_floors("python", PRECISION_FLOOR, RECALL_FLOOR);

    let fresh = common::fresh_reindex_snapshot("golden/python", add_entry_parameter);
    common::assert_matches_fresh(&snapshot, &fresh);
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

    common::assert_no_dangling_edge_targets(indexer.db());

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let report = golden::compare(
        &snapshot,
        &expected_edges_after_rename(),
        &fixture_modules(),
    );
    report.assert_floors("python (post-rename)", PRECISION_FLOOR, RECALL_FLOOR);

    // The final tree this scenario reaches: `other_module.local_util`
    // renamed straight to `local_util_renamed` (the intermediate
    // `local_util_bumped` step above only exists to force a genuine rowid
    // reuse race, not to shape the final tree), `downstream.py` still
    // calling the never-resynced `local_util_bumped`.
    let fresh = common::fresh_reindex_snapshot("golden/python", |root| {
        let downstream_path = root.join("downstream.py");
        let mut downstream_src = std::fs::read_to_string(&downstream_path).unwrap();
        downstream_src.push_str(DOWNSTREAM_EXTRA_CALL_SOURCE);
        let downstream_src = downstream_src.replace("local_util", "local_util_bumped");
        std::fs::write(&downstream_path, downstream_src).unwrap();

        let other_module_path = root.join("other_module.py");
        let contents = std::fs::read_to_string(&other_module_path).unwrap();
        let renamed = contents.replacen("def local_util(", "def local_util_renamed(", 1);
        std::fs::write(&other_module_path, renamed).unwrap();
    });
    common::assert_matches_fresh(&snapshot, &fresh);
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

    common::assert_no_dangling_edge_targets(indexer.db());

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let report = golden::compare(
        &snapshot,
        &expected_edges_after_caller_deleted(),
        &fixture_modules(),
    );
    report.assert_floors("python (post-delete)", PRECISION_FLOOR, RECALL_FLOOR);

    let fresh = common::fresh_reindex_snapshot("golden/python", |root| {
        std::fs::remove_file(root.join("caller.py")).unwrap();
    });
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// Incremental scenario (issue #77): add a new file (`worker.py`)
/// defining a symbol that an existing call site could not previously
/// resolve to anything -- `bare_call_method.bare_caller`'s bare
/// `process()` call, UNRESOLVED in the base fixture because the only
/// other `process` in the repo is a method (see `bare_call_method.py`'s
/// docstring) -- then sync just that path.
///
/// Exercises the same NULL-target repair pass
/// (`Db::resolve_null_target_edges`, run by `Indexer::sync_abs_paths`
/// after every sync that touches a file) as the edit-callee test above,
/// but from the opposite direction: instead of an existing target
/// surviving a sync of its own file, a previously-unresolved *caller*
/// (never resynced itself) picks up a brand new target once one exists.
#[test]
fn incremental_add_file_resolves_previously_unresolved_name() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    std::fs::write(repo_root.join("worker.py"), WORKER_SOURCE).unwrap();
    indexer.sync_rel_paths(&["worker.py".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let report = golden::compare(
        &snapshot,
        &expected_edges_after_worker_added(),
        &fixture_modules(),
    );
    report.assert_floors("python (post-add)", PRECISION_FLOOR, RECALL_FLOOR);

    let fresh = common::fresh_reindex_snapshot("golden/python", |root| {
        std::fs::write(root.join("worker.py"), WORKER_SOURCE).unwrap();
    });
    common::assert_matches_fresh(&snapshot, &fresh);
}
