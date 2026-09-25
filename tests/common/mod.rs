// Shared test helpers, compiled fresh into every integration test binary
// that declares `mod common;` -- not every binary calls every helper here,
// so an unused one there is expected, not a real dead-code bug.
#![allow(dead_code)]

pub mod golden;

use golden::EdgeKey;
use lidx::db::Db;
use lidx::indexer::Indexer;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Recursively copy `src`'s contents into `dst` (which is created if it
/// doesn't exist). Used only to materialize a fixture into a fresh temp
/// dir -- other integration tests keep their own, separate copies of this
/// (not refactored here; see `setup_repo`'s doc).
fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&path, &target);
        } else {
            std::fs::copy(&path, &target).unwrap();
        }
    }
}

/// Copy `fixture` (a path under `tests/fixtures/`, may contain `/`) into a
/// fresh `tempfile` temp dir and return `(guard, repo_root, db_path)`.
///
/// Keep `guard` bound (e.g. `let (_tmp, repo_root, db_path) = ...`) for as
/// long as the test needs the directory -- its `Drop` removes the temp dir
/// automatically, so callers don't need a manual `remove_dir_all` at the
/// end of the test.
///
/// This is the golden-harness-only home for what used to be
/// `tests/common/golden.rs`'s private `setup_repo`/`copy_dir`/
/// `temp_repo_dir` helpers (plus a `TEMP_COUNTER`, now unnecessary --
/// `tempfile` already guarantees a unique directory per call). About 16
/// other integration test files under `tests/` still carry their own,
/// separate copies of a similar handmade helper; those are intentionally
/// left alone here rather than folded into a repo-wide refactor.
pub fn setup_repo(fixture: &str) -> (tempfile::TempDir, PathBuf, PathBuf) {
    let src = golden::fixture_path(fixture);
    let prefix = format!("lidx-{}-", fixture.replace('/', "-"));
    let tmp = tempfile::Builder::new().prefix(&prefix).tempdir().unwrap();
    copy_dir(&src, tmp.path());
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    (tmp, repo_root, db_path)
}

/// Writes `files` (repo-relative path, source text) under `root`, creating
/// parent directories as needed.
pub fn write_files(root: &Path, files: &[(&str, &str)]) {
    for (rel, src) in files {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, src).unwrap();
    }
}

/// `write_files` into a fresh temp dir (no checked-in fixture) followed by
/// one full `reindex()`, returning its edge snapshot -- the synthetic-tree
/// counterpart to `fresh_reindex_snapshot`, for scenarios with no fixture
/// to copy at all. Keep the returned `TempDir` bound for as long as the
/// snapshot (or anything derived from it) is needed.
pub fn index_files(files: &[(&str, &str)]) -> (tempfile::TempDir, BTreeSet<EdgeKey>) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-sync-")
        .tempdir()
        .unwrap();
    write_files(tmp.path(), files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    (tmp, snapshot)
}

/// Builds a *fresh* copy of `fixture`, applies `build_final_tree` to reach
/// the same final state an incremental scenario ends at, does one full
/// `reindex()`, and returns its edge snapshot -- issue #77 requires every
/// incremental scenario's own result to match this, not merely a fixture's
/// hand-authored expected-edges file.
pub fn fresh_reindex_snapshot(
    fixture: &str,
    build_final_tree: impl FnOnce(&Path),
) -> BTreeSet<EdgeKey> {
    let (_tmp, repo_root, db_path) = setup_repo(fixture);
    build_final_tree(&repo_root);
    let mut indexer = Indexer::new(repo_root, db_path).unwrap();
    indexer.reindex().unwrap();
    let graph_version = indexer.db().current_graph_version().unwrap();
    golden::snapshot_edges(indexer.db(), graph_version).unwrap()
}

/// Raw-id sanity check shared by every incremental scenario: every edge's
/// `source_symbol_id`/`target_symbol_id` is either NULL or references a
/// row that still exists in `symbols` -- schema v17's `ON DELETE SET NULL`
/// foreign key (issue #76) makes this true by construction.
pub fn assert_no_dangling_edge_targets(db: &Db) {
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

/// Every incremental scenario's shared final assertion (issue #77): its
/// post-sync snapshot must be identical to a fresh full reindex of the
/// same final tree, not merely satisfy a fixture's expected-edges file.
pub fn assert_matches_fresh(snapshot: &BTreeSet<EdgeKey>, fresh: &BTreeSet<EdgeKey>) {
    assert_eq!(
        snapshot, fresh,
        "incremental sync result must match a fresh full reindex of the same final tree (issue #77)"
    );
}
