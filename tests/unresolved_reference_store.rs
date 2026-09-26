//! Issue #78: the unresolved-reference store and its targeted retry.
//!
//! `Db::insert_edges` records every `Resolver::resolve` `Unresolved`
//! outcome in the new `unresolved_references` table, keyed by the
//! reference's name and trailing name segment (`name_tail`).
//! `Db::retry_unresolved_references` retries only the rows a newly
//! inserted symbol's qualname or bare name could satisfy, instead of
//! `resolve_null_target_edges`'s rescan (which pulls unresolved edges in
//! batches of 1000 and gives up the moment a whole batch makes no
//! progress). This is exactly the scenario that cap breaks: a pile of
//! permanently-unresolved bare calls, inserted ahead of the one call that
//! later resolves, previously starved the rescan before it ever reached
//! the real target.

mod common;

use common::golden;
use lidx::indexer::Indexer;
use lidx::model::UnresolvedReferenceSummary;
use std::path::PathBuf;

/// `count` calls to distinct names nothing ever defines, all in one
/// function -- a pile of permanently-unresolved rows ahead of the real
/// target in insertion order. Mirrors `tests/incremental_sync.rs`'s helper
/// of the same shape (kept as its own copy, per this test directory's
/// convention -- see `tests/common/mod.rs`'s doc comment).
fn many_undefined_calls_source(count: usize) -> String {
    let mut src = String::from("def noisy():\n");
    for i in 0..count {
        src.push_str(&format!("    undefined_{i}()\n"));
    }
    src
}

/// Writes `files` into a fresh temp dir and runs one full `reindex()`,
/// returning the live `Indexer` (and its temp dir/repo root) for a further
/// incremental sync.
fn indexed_tree(files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-unresolved-store-")
        .tempdir()
        .unwrap();
    let repo_root = tmp.path().to_path_buf();
    common::write_files(&repo_root, files);
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, repo_root, indexer)
}

/// The golden scenario from issue #78's acceptance criteria: a bare call
/// to a name nothing defines yet is recorded in the unresolved-reference
/// store; adding a file that defines that name and syncing resolves the
/// call and removes the store row -- even with 1200 never-resolving calls
/// sorted ahead of it (`a.py` < `z.py`), which starve
/// `resolve_null_target_edges`'s LIMIT-1000 rescan before it ever reaches
/// `z.py`'s edge.
#[test]
fn adding_a_file_resolves_a_previously_unresolved_bare_call() {
    let noise = many_undefined_calls_source(1200);
    let caller_py = "def caller():\n    helper()\n";

    let (_tmp, repo_root, mut indexer) = indexed_tree(&[("a.py", &noise), ("z.py", caller_py)]);

    let graph_version = indexer.db().current_graph_version().unwrap();
    let before = indexer
        .db()
        .unresolved_reference_summary(graph_version)
        .unwrap();
    let no_candidates_count = |rows: &[lidx::model::UnresolvedReferenceSummary]| -> i64 {
        rows.iter()
            .find(|row| row.language == "python" && row.reason == "no_candidates")
            .map(|row| row.count)
            .unwrap_or(0)
    };
    let before_count = no_candidates_count(&before);
    // 1200 noise calls plus `caller`'s own call to `helper` -- all recorded
    // with the same reason, before `helper.py` exists to satisfy any of them.
    assert_eq!(
        before_count, 1201,
        "expected every noise call plus caller's own unresolved call recorded: {before:?}"
    );

    common::write_files(
        &repo_root,
        &[("helper.py", "def helper() -> None:\n    pass\n")],
    );
    indexer.sync_rel_paths(&["helper.py".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();

    let edge = snapshot
        .iter()
        .find(|e| e.source_qualname == "z.caller" && e.kind == "CALLS")
        .unwrap_or_else(|| panic!("z.caller must have a CALLS edge: {snapshot:#?}"));
    assert_eq!(
        edge.target_qualname.as_deref(),
        Some("helper.helper"),
        "caller's bare call to helper() must resolve once helper.py exists, even behind \
         1200 never-resolving calls: {edge:?}"
    );

    let after = indexer
        .db()
        .unresolved_reference_summary(graph_version)
        .unwrap();
    assert_eq!(
        no_candidates_count(&after),
        before_count - 1,
        "resolving caller's call must remove exactly its own row from the store, leaving \
         the 1200 never-resolving noise calls behind: {after:?}"
    );

    let (_fresh_tmp, fresh) = common::index_files(&[
        ("a.py", &noise),
        ("z.py", caller_py),
        ("helper.py", "def helper() -> None:\n    pass\n"),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// `unresolved_reference_summary` for a fresh, one-shot `reindex()` of
/// `files` in its own temp dir -- the fixed point every incremental
/// scenario below must match (issue #78 follow-up: a repair pass must never
/// leave the store *behind* the edges it repairs).
fn fresh_summary(files: &[(&str, &str)]) -> Vec<UnresolvedReferenceSummary> {
    let (_tmp, _repo_root, indexer) = indexed_tree(files);
    let graph_version = indexer.db().current_graph_version().unwrap();
    indexer
        .db()
        .unresolved_reference_summary(graph_version)
        .unwrap()
}

/// `Db::carry_forward_files` copies an unchanged file's edges into the new
/// graph version wholesale, but not their `unresolved_references` rows --
/// so a permanently-unresolved call carried forward this way used to vanish
/// from the store even though the edge it describes is still exactly as
/// unresolved as before. `a.py` (unchanged) carries its `nowhere()` call
/// forward while `b.py` is edited (forcing a real reparse rather than a
/// carry-forward) so the reindex takes the carry-forward path at all.
#[test]
fn carry_forward_files_preserves_unresolved_reference_row() {
    let a_py = "def caller():\n    nowhere()\n";
    let (_tmp, repo_root, mut indexer) =
        indexed_tree(&[("a.py", a_py), ("b.py", "def x():\n    pass\n")]);

    let graph_version = indexer.db().current_graph_version().unwrap();
    let before = indexer
        .db()
        .unresolved_reference_summary(graph_version)
        .unwrap();
    assert_eq!(
        before,
        vec![UnresolvedReferenceSummary {
            language: "python".to_string(),
            reason: "no_candidates".to_string(),
            count: 1,
        }],
        "the initial reindex must record caller's unresolved call to nowhere(): {before:?}"
    );

    // Only b.py changes -- a.py is unchanged and takes the carry-forward
    // path on this reindex, dragging its still-unresolved edge along.
    let b_py_after = "def x():\n    pass\n\n\ndef y():\n    pass\n";
    common::write_files(&repo_root, &[("b.py", b_py_after)]);
    indexer.reindex().unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let after = indexer
        .db()
        .unresolved_reference_summary(graph_version)
        .unwrap();
    assert_eq!(
        after,
        fresh_summary(&[("a.py", a_py), ("b.py", b_py_after)]),
        "carrying a.py forward unchanged must not drop its unresolved nowhere() call from the \
         store: {after:?}"
    );
}

/// Bug G2(ii) (issue #78/#79 follow-up): a reindex that finds nothing
/// changed on disk still creates a new graph version, carrying every file
/// forward. When nothing was indexed or deleted, `reindex`'s `needs_repair`
/// gate falls back to comparing the new version's NULL-target-edge count
/// against the `unresolved_edge_floor` recorded by the previous reindex --
/// and a permanently-unresolved call's carried-forward edge contributes the
/// same count both times, so that gate stays false and the repair pass never
/// runs at all. Without `carry_forward_files` copying the store row itself,
/// the row silently disappears even though the edge it describes is exactly
/// as unresolved as before.
#[test]
fn no_change_reindex_keeps_the_store_matching_a_fresh_reindex() {
    let a_py = "def f():\n    nothing_defines_this()\n";
    let b_py = "def g():\n    pass\n";
    let (_tmp, _repo_root, mut indexer) = indexed_tree(&[("a.py", a_py), ("b.py", b_py)]);

    // Second reindex, nothing changed on disk -- every file takes the
    // carry-forward path.
    indexer.reindex().unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let after = indexer
        .db()
        .unresolved_reference_summary(graph_version)
        .unwrap();
    assert_eq!(
        after,
        fresh_summary(&[("a.py", a_py), ("b.py", b_py)]),
        "a no-op reindex must not drop the store's row for f's still-unresolved call: {after:?}"
    );
}

/// Bug F2 (issue #78 follow-up): an edge that resolved cleanly at insert
/// time -- so `Db::insert_edges` never wrote it a store row -- can still go
/// NULL-target later, here via the `edges` foreign key's `ON DELETE SET
/// NULL` when its target symbol's file is deleted. Neither
/// `retry_unresolved_references` nor `resolve_null_target_edges` gives that
/// kind of miss a store row, so the reference silently drops out of the
/// scoreboard even though the edge is exactly as unresolved as a fresh
/// index of the same tree would show.
#[test]
fn deleting_target_file_records_unresolved_reference_for_orphaned_call() {
    let a_py = "from b import g\n\ndef f():\n    g()\n";
    let b_py = "def g():\n    pass\n";
    let (_tmp, repo_root, mut indexer) = indexed_tree(&[("a.py", a_py), ("b.py", b_py)]);

    let graph_version = indexer.db().current_graph_version().unwrap();
    let before = indexer
        .db()
        .unresolved_reference_summary(graph_version)
        .unwrap();
    assert!(
        before.is_empty(),
        "g() must resolve cleanly before b.py is deleted: {before:?}"
    );

    std::fs::remove_file(repo_root.join("b.py")).unwrap();
    indexer.sync_rel_paths(&["b.py".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let after = indexer
        .db()
        .unresolved_reference_summary(graph_version)
        .unwrap();
    assert!(
        !after.is_empty(),
        "deleting b.py must leave f's now-orphaned call to g() recorded in the store"
    );
    assert_eq!(
        after,
        fresh_summary(&[("a.py", a_py)]),
        "the incrementally-synced store must match a fresh index of the same final tree: {after:?}"
    );
}

/// Same bug as above, reached by renaming the target instead of deleting
/// its file: `g`'s symbol row still goes away (the `ON DELETE SET NULL`
/// foreign key nulls `a.py`'s edge the same way), and the newly-added `g2`
/// symbol also runs `unbind_edges_for_qualnames` -- neither path gives the
/// now-orphaned call a store row.
#[test]
fn renaming_target_symbol_records_unresolved_reference_for_orphaned_call() {
    let a_py = "from b import g\n\ndef f():\n    g()\n";
    let b_py_before = "def g():\n    pass\n";
    let (_tmp, repo_root, mut indexer) = indexed_tree(&[("a.py", a_py), ("b.py", b_py_before)]);

    let graph_version = indexer.db().current_graph_version().unwrap();
    let before = indexer
        .db()
        .unresolved_reference_summary(graph_version)
        .unwrap();
    assert!(
        before.is_empty(),
        "g() must resolve cleanly before b.py's g is renamed: {before:?}"
    );

    let b_py_after = "def g2():\n    pass\n";
    common::write_files(&repo_root, &[("b.py", b_py_after)]);
    indexer.sync_rel_paths(&["b.py".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    let after = indexer
        .db()
        .unresolved_reference_summary(graph_version)
        .unwrap();
    assert!(
        !after.is_empty(),
        "renaming g to g2 must leave f's now-orphaned call to g() recorded in the store"
    );
    assert_eq!(
        after,
        fresh_summary(&[("a.py", a_py), ("b.py", b_py_after)]),
        "the incrementally-synced store must match a fresh index of the same final tree: {after:?}"
    );
}
