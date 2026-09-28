//! Issue #62: surface `resolution_kind` on every edge.
//!
//! `dead_symbols`'s `unused_imports` list is the one live RPC response that
//! serializes raw `Edge` objects directly (`Db::unused_imports`, which
//! unions resolved `edges` rows with pending, never-bound
//! `unresolved_references` rows -- see `src/rpc/handlers.rs`'s
//! `handle_dead_symbols`). That makes it the seam this test exercises: a
//! same-repo unused import resolved by exact qualname match must report
//! its resolution tier, and an unused import that was never bound (an
//! external package the write path could not attribute) must omit the
//! field entirely rather than reporting `null`.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;

const HELPER_SOURCE: &str = "\
def unused_fn():
    return \"never called\"
";

const MAIN_SOURCE: &str = "\
import requests

from helper import unused_fn


def entry():
    return \"no calls to helper here\"
";

#[test]
fn unused_imports_expose_resolution_kind_and_omit_it_when_unresolved() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-resolution-kind-")
        .tempdir()
        .unwrap();
    common::write_files(
        tmp.path(),
        &[("helper.py", HELPER_SOURCE), ("main.py", MAIN_SOURCE)],
    );
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let raw = rpc::call(
        repo_root,
        db_path,
        "dead_symbols".to_string(),
        "{}",
        "1",
    )
    .unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "dead_symbols returned an error: {envelope}"
    );
    let unused_imports = envelope["result"]["unused_imports"]
        .as_array()
        .expect("unused_imports should be an array");
    assert_eq!(
        unused_imports.len(),
        2,
        "expected exactly 2 unused imports, got {unused_imports:?}"
    );

    let resolved = unused_imports
        .iter()
        .find(|e| e["target_qualname"] == "helper.unused_fn")
        .expect("expected an unused import targeting helper.unused_fn");
    assert_eq!(
        resolved["resolution_kind"], "exact",
        "a same-repo unused import resolved by exact qualname match must report its tier: {resolved}"
    );

    let unresolved = unused_imports
        .iter()
        .find(|e| e["target_qualname"] == "requests")
        .expect("expected an unused import targeting requests");
    assert!(
        unresolved.get("resolution_kind").is_none(),
        "an unresolved (never-bound) unused import must omit resolution_kind rather than report null: {unresolved}"
    );
}
