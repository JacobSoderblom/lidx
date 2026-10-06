//! Issue #319: Python method calls whose receiver is itself a call
//! (`make().stage(1).storage()`) are recorded, and bound only where the
//! receiver call's callee has a return annotation. Nothing is inferred from
//! function bodies.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use rusqlite::params;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn index(files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf, Indexer) {
    let (tmp, root, db) = common::index_repo("pychain", files);
    let mut indexer = Indexer::new(root.clone(), db).unwrap();
    indexer.reindex().unwrap();
    (tmp, root, indexer)
}

fn gv(indexer: &Indexer) -> i64 {
    indexer.db().current_graph_version().unwrap()
}

/// `(source qualname, bound target qualname)` of every bound CALLS edge.
fn bound_calls(indexer: &Indexer) -> BTreeSet<(String, String)> {
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.graph_version = ?",
        )
        .unwrap();
    stmt.query_map(params![gv(indexer)], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

/// `(reference_name, reason)` of the unresolved CALLS rows of `source`.
fn unresolved(indexer: &Indexer, source: &str) -> BTreeSet<(String, String)> {
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT u.reference_name, u.reason FROM unresolved_references u
             JOIN symbols s ON s.id = u.source_symbol_id
             WHERE u.edge_kind = 'CALLS' AND s.qualname = ? AND u.graph_version = ?",
        )
        .unwrap();
    stmt.query_map(params![source, gv(indexer)], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn external(names: &[&str]) -> BTreeSet<(String, String)> {
    names
        .iter()
        .map(|n| (n.to_string(), "external".to_string()))
        .collect()
}

fn calls(indexer: &Indexer, source: &str, target: &str) -> bool {
    bound_calls(indexer).contains(&(source.to_string(), target.to_string()))
}

const UNANNOTATED: &str = "class Builder:
    def stage(self, x):
        return self
    def storage(self):
        return self

def make():
    return Builder()

def use():
    return make().stage(1).storage()
";

const ANNOTATED: &str = "class Builder:
    def stage(self, x) -> \"Builder\":
        return self
    def storage(self) -> \"Builder\":
        return self

def make() -> Builder:
    return Builder()

def use():
    return make().stage(1).storage()
";

#[test]
fn unannotated_chain_is_unresolved_external_and_never_bound() {
    let (_t, _r, indexer) = index(&[("b.py", UNANNOTATED)]);
    assert_eq!(
        unresolved(&indexer, "b.use"),
        // Rows are named by the callee's source text now (the bare-method-name
        // placeholder died with the receiver-type inference).
        external(&["make().stage", "make().stage(1).storage"])
    );
    let bound = bound_calls(&indexer);
    assert!(
        !bound
            .iter()
            .any(|(s, t)| s == "b.use" && (t.ends_with(".stage") || t.ends_with(".storage"))),
        "no return annotation, nothing inferred from the body: {bound:?}"
    );
}

#[test]
fn annotated_chain_binds_through_receiver_type() {
    let (_t, _r, indexer) = index(&[("b.py", ANNOTATED)]);
    assert!(calls(&indexer, "b.use", "b.Builder.stage"));
    assert!(calls(&indexer, "b.use", "b.Builder.storage"));
    assert!(unresolved(&indexer, "b.use").is_empty());
    let conn = indexer.db().read_conn().unwrap();
    let kind: Option<String> = conn
        .query_row(
            "SELECT e.resolution_kind FROM edges e JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND t.qualname = 'b.Builder.storage' AND e.graph_version = ?",
            params![gv(&indexer)],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(kind.as_deref(), Some("receiver_type"));
}

#[test]
fn unannotated_middle_method_binds_first_link_only() {
    // The unannotated middle method must not return `self` (an unannotated
    // `return self` is typed as `Self` now), so its result is unknown.
    let src = ANNOTATED.replace(
        "def stage(self, x) -> \"Builder\":\n        return self",
        "def stage(self, x):\n        return Builder()",
    );
    let (_t, _r, indexer) = index(&[("b.py", &src)]);
    assert!(calls(&indexer, "b.use", "b.Builder.stage"));
    assert!(!calls(&indexer, "b.use", "b.Builder.storage"));
    assert_eq!(
        unresolved(&indexer, "b.use"),
        external(&["make().stage(1).storage"])
    );
}

#[test]
fn optional_return_annotation_unwraps() {
    let src = ANNOTATED.replace("def make() -> Builder:", "def make() -> Optional[Builder]:");
    let (_t, _r, indexer) = index(&[("b.py", &src)]);
    assert!(calls(&indexer, "b.use", "b.Builder.stage"));
    assert!(calls(&indexer, "b.use", "b.Builder.storage"));
}

#[test]
fn multiline_await_and_self_chains_are_recorded() {
    let src = "class Builder:
    def stage(self, x) -> \"Builder\":
        return self
    def storage(self) -> \"Builder\":
        return self
    def make(self) -> \"Builder\":
        return Builder()
    def inner(self):
        return self.make().stage(1)

def make() -> Builder:
    return Builder()

async def amake() -> Builder:
    return Builder()

def dsl():
    return (
        make()
        .stage(1)
        .storage()
    )

async def waits():
    return await amake().stage(1)

def untyped(x):
    return (
        x()
        .stage(1)
    )
";
    let (_t, _r, indexer) = index(&[("b.py", src)]);
    assert!(calls(&indexer, "b.dsl", "b.Builder.stage"));
    assert!(calls(&indexer, "b.dsl", "b.Builder.storage"));
    // `self.make()` is a method of the enclosing class, annotated.
    assert!(calls(&indexer, "b.Builder.inner", "b.Builder.stage"));
    // `await amake().stage(1)`: recorded (the un-awaited coroutine call is
    // the receiver, which is never a Builder).
    let recorded = unresolved(&indexer, "b.waits");
    let bound = bound_calls(&indexer);
    assert!(
        recorded.iter().any(|(n, _)| n.ends_with(".stage"))
            || bound.contains(&("b.waits".into(), "b.Builder.stage".into())),
        "await chain call dropped: {recorded:?}"
    );
    assert!(
        unresolved(&indexer, "b.untyped")
            .iter()
            .any(|(n, r)| n.ends_with(".stage") && r == "external")
    );
}

#[test]
fn external_library_chain_stays_external_beside_decoy_methods() {
    let decoy = "class Decoy:
    def get(self, k):
        return k
    def json(self):
        return {}
";
    let user = "import requests

def fetch(u):
    return requests.get(u).json().get(\"k\")
";
    let (_t, _r, indexer) = index(&[("decoy.py", decoy), ("user.py", user)]);
    let bound = bound_calls(&indexer);
    assert!(
        !bound
            .iter()
            .any(|(s, t)| s == "user.fetch" && t.starts_with("decoy.")),
        "external chain bound to a repo method: {bound:?}"
    );
    let rows = unresolved(&indexer, "user.fetch");
    assert!(
        rows.iter()
            .any(|(n, r)| n.ends_with(".json") && r == "external")
    );
    assert!(
        rows.iter()
            .any(|(n, r)| n.ends_with(".get") && r == "external")
    );
}

fn rpc_json(root: &Path, indexer_db: &Path, method: &str, params: &str) -> serde_json::Value {
    let response = rpc::call(
        root.to_path_buf(),
        indexer_db.to_path_buf(),
        method.to_string(),
        params,
        "1",
    )
    .unwrap();
    serde_json::from_str::<serde_json::Value>(&response).unwrap()["result"].clone()
}

#[test]
fn upstream_traversal_reaches_chain_caller() {
    let (_t, root, indexer) = index(&[("b.py", ANNOTATED)]);
    let db = root.join(".lidx").join(".lidx.sqlite");
    drop(indexer);
    let impact = rpc_json(
        &root,
        &db,
        "analyze_impact",
        r#"{"qualname":"b.Builder.stage","direction":"upstream","max_depth":3}"#,
    );
    assert!(
        impact["affected"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["symbol"]["qualname"] == "b.use"),
        "analyze_impact upstream must list b.use: {impact}"
    );
    let trace = rpc_json(
        &root,
        &db,
        "trace_flow",
        r#"{"start_qualname":"b.Builder.stage","direction":"upstream","max_hops":4}"#,
    );
    assert!(
        trace["paths_found"].as_u64().unwrap_or(0) > 0,
        "trace_flow upstream must be non-empty: {trace}"
    );
}

#[test]
fn incremental_sync_matches_fresh_index() {
    let (_t, root, mut indexer) = index(&[("b.py", UNANNOTATED)]);
    // Annotate `make`, then re-sync only that file.
    common::write_files(&root, &[("b.py", ANNOTATED)]);
    indexer.sync_rel_paths(&["b.py".to_string()]).unwrap();
    let edges = common::golden::snapshot_edges(indexer.db(), gv(&indexer)).unwrap();
    let rows = (unresolved(&indexer, "b.use"), bound_calls(&indexer));

    let (_t2, _r2, fresh) = index(&[("b.py", ANNOTATED)]);
    let fresh_edges = common::golden::snapshot_edges(fresh.db(), gv(&fresh)).unwrap();
    common::assert_matches_fresh(&edges, &fresh_edges);
    assert_eq!(rows.0, unresolved(&fresh, "b.use"));
    assert_eq!(rows.1, bound_calls(&fresh));
}
