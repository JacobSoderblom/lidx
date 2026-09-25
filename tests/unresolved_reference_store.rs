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
