//! Issue #80: known-external calls bind to a reusable stub symbol
//! (`kind = 'external'`, qualname `ext:...`) instead of staying unresolved.
//!
//! Golden-fixture coverage (per-language external-call lines) lives in
//! `tests/golden_python.rs` / `tests/golden_languages.rs`. This file covers
//! the query-surface and lifecycle acceptance criteria: `explain_symbol`
//! lists a stub's callers, repo-internal listings exclude stubs, and a
//! stub's incremental lifecycle (appear, survive a carry-forward reindex,
//! disappear once orphaned) matches a fresh reindex of the same tree.

mod common;

use common::golden;
use lidx::indexer::Indexer;
use lidx::rpc;
use rusqlite::OptionalExtension;

const CALLER_WITH_EXTERNAL_CALL: &str =
    "import requests\n\n\ndef fetch():\n    return requests.get(\"https://example.com\")\n";

fn temp_indexer(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-ext-stub-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    (tmp, indexer)
}

fn external_stub_count(indexer: &Indexer) -> i64 {
    let graph_version = indexer.db().current_graph_version().unwrap();
    indexer
        .db()
        .read_conn()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM symbols WHERE graph_version = ? AND kind = 'external'",
            [graph_version],
            |row| row.get(0),
        )
        .unwrap()
}

fn stub_exists(indexer: &Indexer, qualname: &str) -> bool {
    let graph_version = indexer.db().current_graph_version().unwrap();
    indexer
        .db()
        .read_conn()
        .unwrap()
        .query_row(
            "SELECT 1 FROM symbols WHERE graph_version = ? AND kind = 'external' AND qualname = ?",
            rusqlite::params![graph_version, qualname],
            |_| Ok(()),
        )
        .optional()
        .unwrap()
        .is_some()
}

#[test]
fn explain_symbol_on_external_stub_lists_its_callers() {
    let (_tmp, mut indexer) = temp_indexer(&[("caller.py", CALLER_WITH_EXTERNAL_CALL)]);
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "explain_symbol",
        serde_json::json!({"qualname": "ext:requests.get", "sections": ["callers"]}),
    )
    .unwrap();

    assert_eq!(result["symbol"]["kind"], "external", "{result:#}");
    let callers = result["callers"].as_array().expect("callers array");
    assert_eq!(
        callers.len(),
        1,
        "explain_symbol on a stub must list its one real caller: {result:#}"
    );
    assert_eq!(callers[0]["symbol"]["qualname"], "caller.fetch");
}

#[test]
fn dead_symbols_excludes_external_stubs() {
    let (_tmp, mut indexer) = temp_indexer(&[(
        "caller.py",
        "import requests\n\n\ndef dead():\n    pass\n\n\ndef fetch():\n    return requests.get(\"https://example.com\")\n",
    )]);
    indexer.reindex().unwrap();

    let result = rpc::handle_method(&mut indexer, "dead_symbols", serde_json::json!({})).unwrap();
    let qualnames: Vec<String> = result["dead_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["qualname"].as_str().map(String::from))
        .collect();
    assert!(
        qualnames.contains(&"caller.dead".to_string()),
        "a genuinely uncalled function must still be reported: {qualnames:?}"
    );
    assert!(
        !qualnames.iter().any(|q| q.starts_with("ext:")),
        "an external stub must never be reported as a dead symbol: {qualnames:?}"
    );
}

#[test]
fn repo_map_excludes_external_stub_module_and_symbol_counts() {
    let (_tmp, mut indexer) = temp_indexer(&[("caller.py", CALLER_WITH_EXTERNAL_CALL)]);
    indexer.reindex().unwrap();

    let result = rpc::handle_method(&mut indexer, "repo_map", serde_json::json!({})).unwrap();
    let text = result["text"].as_str().unwrap();
    assert!(
        !text.contains("<external>"),
        "the synthetic external pseudo-file must never show up as its own module: {text}"
    );
    assert!(
        !text.contains("ext:"),
        "an external stub must never show up in the repo map: {text}"
    );
}

#[test]
fn top_complexity_excludes_external_stubs() {
    let (_tmp, mut indexer) = temp_indexer(&[(
        "caller.py",
        "import requests\n\n\ndef fetch(flag):\n    if flag:\n        return requests.get(\"https://example.com\")\n    return None\n",
    )]);
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "top_complexity",
        serde_json::json!({"min_complexity": 0}),
    )
    .unwrap();
    let results = result.as_array().expect("top_complexity returns an array");
    assert!(
        !results.iter().any(|s| s["symbol"]["qualname"]
            .as_str()
            .unwrap_or("")
            .starts_with("ext:")),
        "an external stub must never show up in top_complexity: {results:?}"
    );
}

/// Incremental sync (issue #80's own lifecycle criterion): adding an
/// external call creates the stub, and removing that call's last surviving
/// reference to it removes the stub again -- both matching what a fresh
/// reindex of the same final tree would produce (issue #77's invariant).
#[test]
fn incremental_stub_appears_and_disappears_matching_fresh() {
    let (tmp, mut indexer) = temp_indexer(&[("caller.py", "def local_only():\n    return 1\n")]);
    indexer.reindex().unwrap();
    assert_eq!(
        external_stub_count(&indexer),
        0,
        "no external call yet, so no stub"
    );

    common::write_files(tmp.path(), &[("caller.py", CALLER_WITH_EXTERNAL_CALL)]);
    indexer.sync_rel_paths(&["caller.py".to_string()]).unwrap();
    assert!(
        stub_exists(&indexer, "ext:requests.get"),
        "the stub must appear once an external call exists"
    );

    {
        let graph_version = indexer.db().current_graph_version().unwrap();
        let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
        let (_fresh_tmp, fresh) = common::index_files(&[("caller.py", CALLER_WITH_EXTERNAL_CALL)]);
        common::assert_matches_fresh(&snapshot, &fresh);
    }

    // Remove the external call again -- its only caller reverts to calling
    // nothing external at all.
    common::write_files(
        tmp.path(),
        &[("caller.py", "def local_only():\n    return 1\n")],
    );
    indexer.sync_rel_paths(&["caller.py".to_string()]).unwrap();
    assert_eq!(
        external_stub_count(&indexer),
        0,
        "the stub must be pruned once its last caller is gone"
    );

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let (_fresh_tmp, fresh) =
        common::index_files(&[("caller.py", "def local_only():\n    return 1\n")]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// A stub is not owned by any scanned file, so `Indexer::reindex`'s
/// carry-forward-unchanged-files path (`Db::carry_forward_files`) never
/// sees it directly through `caller.py`'s own file id -- it must still
/// survive into the new graph_version when `caller.py` itself is
/// unchanged and only carried forward, not re-parsed.
#[test]
fn stub_survives_being_carried_forward_by_an_unrelated_reindex() {
    let (tmp, mut indexer) = temp_indexer(&[
        ("caller.py", CALLER_WITH_EXTERNAL_CALL),
        ("other.py", "def a():\n    return 1\n"),
    ]);
    indexer.reindex().unwrap();
    assert_eq!(external_stub_count(&indexer), 1);

    // Only `other.py` changes -- `caller.py`'s content hash is unchanged,
    // so it's carried forward rather than re-parsed.
    common::write_files(tmp.path(), &[("other.py", "def a():\n    return 2\n")]);
    indexer.reindex().unwrap();
    assert_eq!(
        external_stub_count(&indexer),
        1,
        "the stub must survive a reindex that only carries its owning file forward"
    );
    assert!(stub_exists(&indexer, "ext:requests.get"));

    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&[
        ("caller.py", CALLER_WITH_EXTERNAL_CALL),
        ("other.py", "def a():\n    return 2\n"),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// Regression: the synthetic external pseudo-file never appears in a real
/// filesystem scan, so `Indexer::changed_files` and the "mark deleted"
/// sweep at the end of `Indexer::reindex` must never treat it as a repo
/// file that just disappeared -- `Db::list_files` excludes it for exactly
/// this reason. Before that fix, a second reindex stamped its
/// `deleted_version` with the very version it was carried into, which
/// would have made every stub on it invisible to any query that also
/// checks its own file's `deleted_version` starting in that version (not
/// caught by `assert_matches_fresh` above, since `edges_snapshot` only
/// checks the *source* side's file).
#[test]
fn external_pseudo_file_is_never_reported_changed_or_marked_deleted() {
    let (tmp, mut indexer) = temp_indexer(&[("caller.py", CALLER_WITH_EXTERNAL_CALL)]);
    indexer.reindex().unwrap();
    assert_eq!(external_stub_count(&indexer), 1);

    let changed = indexer.changed_files(None).unwrap();
    assert!(
        changed.deleted.is_empty(),
        "the synthetic external file must never show up as deleted: {changed:?}"
    );

    // A second reindex, with nothing on disk touched at all, exercises the
    // same "existing file not seen in this scan -> mark deleted" sweep.
    common::write_files(tmp.path(), &[("caller.py", CALLER_WITH_EXTERNAL_CALL)]);
    indexer.reindex().unwrap();
    assert_eq!(
        external_stub_count(&indexer),
        1,
        "the stub must still be visible after a second reindex"
    );

    let deleted_version: Option<i64> = indexer
        .db()
        .read_conn()
        .unwrap()
        .query_row(
            "SELECT deleted_version FROM files WHERE language = 'external'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        deleted_version, None,
        "the synthetic external file must never be marked deleted"
    );

    // And a stub is still resolvable by qualname, not just visible in a raw
    // symbols-table count -- the concrete way a `deleted_version` mixup
    // would surface to a real caller.
    let result = rpc::handle_method(
        &mut indexer,
        "explain_symbol",
        serde_json::json!({"qualname": "ext:requests.get", "sections": ["callers"]}),
    )
    .unwrap();
    assert_eq!(result["callers"].as_array().unwrap().len(), 1, "{result:#}");
}
