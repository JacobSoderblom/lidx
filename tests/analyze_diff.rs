//! Issue #106: `analyze_diff {paths: [...]}` (no `diff` text, so there are no
//! hunk ranges to compare against) marked every symbol in the file
//! `change_type: "modified"` even when nothing in the file actually changed,
//! and listed its callers under `downstream` with `relationship: "caller"` --
//! callers are upstream (things that depend on the changed symbol), not
//! downstream.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;
use std::path::PathBuf;

fn call(repo_root: PathBuf, db_path: PathBuf, method: &str, params: &str) -> Value {
    let raw = rpc::call(repo_root, db_path, method.to_string(), params, "1").unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "{method} returned an error: {envelope}"
    );
    envelope["result"].clone()
}

const TARGET_SOURCE: &str = "def target():\n    return 1\n";
const CALLER_SOURCE: &str = "from target import target\n\n\ndef wrapper():\n    return target()\n";

/// `paths`-only mode has no diff text and no git comparison, so there's no
/// basis to call every symbol in the file "modified" -- most of them likely
/// didn't change at all. It should use a neutral label instead.
#[test]
fn paths_only_mode_labels_symbols_neutrally_not_modified() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-analyze-diff-paths-label-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), &[("target.py", TARGET_SOURCE)]);
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    let result = call(
        repo_root,
        db_path,
        "analyze_diff",
        r#"{"paths":["target.py"]}"#,
    );

    let changed_symbols = result["changed_symbols"]
        .as_array()
        .expect("changed_symbols array");
    assert!(!changed_symbols.is_empty(), "{result:?}");
    for cs in changed_symbols {
        assert_ne!(
            cs["change_type"].as_str(),
            Some("modified"),
            "paths-only mode has no diff to confirm a modification against; \
             change_type must not claim \"modified\" without evidence: {cs:?}"
        );
    }
}

/// `crate::main`-equivalent `wrapper()` calls `target()`; in `paths`-only
/// mode, `wrapper` is a caller of the changed symbol `target`, which makes it
/// upstream (a consumer of `target`), not downstream.
#[test]
fn paths_only_mode_lists_callers_under_upstream_not_downstream() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-analyze-diff-paths-upstream-")
        .tempdir()
        .unwrap();
    common::write_files(
        tmp.path(),
        &[("target.py", TARGET_SOURCE), ("caller.py", CALLER_SOURCE)],
    );
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    let result = call(
        repo_root,
        db_path,
        "analyze_diff",
        r#"{"paths":["target.py"]}"#,
    );

    assert!(
        result.get("downstream").is_none(),
        "callers must not be reported under a \"downstream\" key: {result:?}"
    );

    let upstream = result["upstream"]
        .as_array()
        .expect("upstream array missing from analyze_diff response");
    assert!(
        upstream
            .iter()
            .any(|u| u["symbol"]["qualname"].as_str() == Some("caller.wrapper")),
        "expected caller.wrapper (which calls target.target) to show up as an \
         upstream caller: {result:?}"
    );
    for u in upstream {
        assert!(
            u["relationship"]
                .as_str()
                .is_some_and(|r| r.starts_with("caller")),
            "unexpected relationship in upstream entry: {u:?}"
        );
    }
}
