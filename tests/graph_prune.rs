//! Issue #322: graph-version prune must succeed on repeated no-op reindexes
//! and a prune failure must surface in the reindex result without failing it.

mod common;

use lidx::db::DEFAULT_GRAPH_VERSION_RETENTION;
use lidx::indexer::Indexer;
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::process::Command;

const SRC: &str = "from typing import Protocol\n\n\
class X(Protocol):\n    def a(self) -> None: ...\n\n\
class Y(Protocol):\n    def a(self) -> None: ...\n";

/// Enough no-op reindexes that several versions fall outside retention.
const REINDEXES: i64 = DEFAULT_GRAPH_VERSION_RETENTION * 2;

fn git(root: &Path, args: &[&str]) {
    let status = Command::new("git")
        .args(["-c", "user.name=t", "-c", "user.email=t@t"])
        .args(args)
        .current_dir(root)
        .status()
        .unwrap();
    assert!(status.success(), "git {args:?}");
}

fn git_repo() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let tmp = tempfile::Builder::new()
        .prefix("graph-prune")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), &[("a.py", SRC)]);
    git(tmp.path(), &["init", "-q"]);
    git(tmp.path(), &["add", "."]);
    git(tmp.path(), &["commit", "-q", "-m", "init"]);
    let root = tmp.path().to_path_buf();
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    (tmp, root, db_path)
}

fn versions(db_path: &Path, table: &str) -> Vec<i64> {
    let conn = Connection::open(db_path).unwrap();
    let mut stmt = conn
        .prepare(&format!(
            "SELECT DISTINCT graph_version FROM {table} ORDER BY 1"
        ))
        .unwrap();
    stmt.query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn force_prune_failure(db_path: &Path) {
    Connection::open(db_path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER force_prune_fail BEFORE DELETE ON unresolved_references
             BEGIN SELECT RAISE(ABORT, 'forced prune failure'); END;",
        )
        .unwrap();
}

#[test]
fn prune_keeps_only_retained_versions_after_many_reindexes() {
    let (_tmp, root, db_path) = git_repo();
    let mut indexer = Indexer::new(root, db_path.clone()).unwrap();
    for _ in 0..REINDEXES {
        let stats = indexer.reindex().unwrap();
        assert_eq!(stats.prune_error, None);
    }
    let newest = indexer.db().current_graph_version().unwrap();
    let expected: Vec<i64> = (newest - DEFAULT_GRAPH_VERSION_RETENTION + 1..=newest).collect();
    for table in ["symbols", "edges", "unresolved_references"] {
        assert_eq!(versions(&db_path, table), expected, "table {table}");
    }
    // Nothing left outside retention, so a further prune is a clean no-op.
    let (symbols_deleted, edges_deleted, _, _) = indexer.db().prune_and_maybe_vacuum().unwrap();
    assert_eq!((symbols_deleted, edges_deleted), (0, 0));
}

#[test]
fn successful_reindex_result_has_no_prune_error_field() {
    let (_tmp, root, db_path) = git_repo();
    let mut indexer = Indexer::new(root, db_path).unwrap();
    let stats = indexer.reindex().unwrap();
    assert!(
        !serde_json::to_string(&stats)
            .unwrap()
            .contains("prune_error")
    );
}

#[test]
fn prune_failure_is_reported_but_reindex_succeeds() {
    let (_tmp, root, db_path) = git_repo();
    let mut indexer = Indexer::new(root, db_path.clone()).unwrap();
    for _ in 0..=DEFAULT_GRAPH_VERSION_RETENTION {
        indexer.reindex().unwrap();
    }
    force_prune_failure(&db_path);
    let stats = indexer.reindex().unwrap();
    let err = stats.prune_error.as_deref().expect("prune_error set");
    assert!(err.contains("forced prune failure"), "{err}");
    let json = serde_json::to_value(&stats).unwrap();
    assert!(
        json["prune_error"]
            .as_str()
            .unwrap()
            .contains("forced prune failure")
    );
}

#[test]
fn rpc_reindex_result_carries_prune_error() {
    let (_tmp, root, db_path) = git_repo();
    let mut indexer = Indexer::new(root, db_path.clone()).unwrap();
    for _ in 0..=DEFAULT_GRAPH_VERSION_RETENTION {
        indexer.reindex().unwrap();
    }
    let ok = lidx::rpc::handle_method(&mut indexer, "reindex", serde_json::json!({})).unwrap();
    assert!(ok.get("prune_error").is_none(), "{ok}");

    force_prune_failure(&db_path);
    for params in [serde_json::json!({}), serde_json::json!({"summary": true})] {
        let out = lidx::rpc::handle_method(&mut indexer, "reindex", params).unwrap();
        assert!(
            out["prune_error"]
                .as_str()
                .unwrap()
                .contains("forced prune failure"),
            "{out}"
        );
    }
}
