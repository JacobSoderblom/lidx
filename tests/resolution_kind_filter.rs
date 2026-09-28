//! Issue #81: resolution-kind filter and lower-bound indicator for
//! `trace_flow` and `analyze_impact`.
//!
//! The filtered-vs-unfiltered scenario reuses `tests/fixtures/golden/python`
//! plus the same `worker.py` incremental addition
//! `tests/golden_python.rs`'s `incremental_add_file_resolves_previously_unresolved_name`
//! test uses: before `worker.py` exists, `bare_call_method.bare_caller`'s
//! bare `process()` call is UNRESOLVED (its only same-named candidate,
//! `Widget.process`, is a method the bare-call guard refuses). Once
//! `worker.py` exists, the guarded name-fallback tier binds it via
//! `bare_name` -- the one heuristic CALLS edge this fixture produces at
//! all, computed from the graph itself below rather than hardcoded, so a
//! resolver change that shifts which edge is heuristic here fails loudly.
//!
//! The lower-bound scenario uses the base fixture's own permanently
//! unresolved reference (`caller.call_ambiguous`, an ambiguous bare `run`)
//! instead -- no incremental step needed.

mod common;

use common::golden;
use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;
use std::path::{Path, PathBuf};

const WORKER_SOURCE: &str = "\
def process() -> str:
    # Top-level function, not a method: the name-fallback tier's first
    # legal candidate for bare_call_method.bare_caller's bare call.
    return \"worker\"
";

/// Reindex golden/python, then add and sync `worker.py` -- the state where
/// `bare_call_method.bare_caller CALLS worker.process` resolves via the
/// `bare_name` tier. Returns the temp dir guard, repo root and db path;
/// keep the guard bound for as long as the paths are used.
fn setup_with_worker_added() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let (tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    std::fs::write(repo_root.join("worker.py"), WORKER_SOURCE).unwrap();
    indexer.sync_rel_paths(&["worker.py".to_string()]).unwrap();
    (tmp, repo_root, db_path)
}

fn call(repo_root: &Path, db_path: &Path, method: &str, params: &str) -> Value {
    let raw = rpc::call(
        repo_root.to_path_buf(),
        db_path.to_path_buf(),
        method.to_string(),
        params,
        "1",
    )
    .unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "rpc call '{method}' with params {params} returned an error: {envelope}"
    );
    let result = envelope["result"].clone();
    assert!(
        !result.is_null(),
        "rpc call '{method}' with params {params} returned no result: {envelope}"
    );
    result
}

fn trace_hop_qualnames(result: &Value) -> Vec<String> {
    result["trace"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["symbol"]["qualname"].as_str().unwrap().to_string())
        .collect()
}

fn impact_affected_qualnames(result: &Value) -> Vec<String> {
    result["affected"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["symbol"]["qualname"].as_str().unwrap().to_string())
        .collect()
}

/// The one heuristic (`bare_name`/`two_segment`) CALLS edge present once
/// `worker.py` is added -- read straight off the graph snapshot, not
/// hardcoded, and asserted to be exactly this fixture's known shape.
fn the_heuristic_edge(db: &lidx::db::Db, graph_version: i64) -> golden::EdgeKey {
    let snapshot = golden::snapshot_edges(db, graph_version).unwrap();
    let heuristic: Vec<&golden::EdgeKey> = snapshot
        .iter()
        .filter(|e| {
            e.kind == "CALLS"
                && matches!(
                    e.resolution_kind.as_deref(),
                    Some("bare_name") | Some("two_segment")
                )
        })
        .collect();
    assert_eq!(
        heuristic.len(),
        1,
        "expected exactly one heuristic CALLS edge once worker.py is added, got {:?}",
        heuristic
    );
    let edge = heuristic[0].clone();
    assert_eq!(edge.source_qualname, "bare_call_method.bare_caller");
    assert_eq!(edge.target_qualname.as_deref(), Some("worker.process"));
    edge
}

#[test]
fn filtered_trace_flow_excludes_exactly_the_heuristic_edge() {
    let (_tmp, repo_root, db_path) = setup_with_worker_added();
    let target = {
        let indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
        let graph_version = indexer.db().current_graph_version().unwrap();
        the_heuristic_edge(indexer.db(), graph_version)
            .target_qualname
            .unwrap()
    };

    let unfiltered = call(
        &repo_root,
        &db_path,
        "trace_flow",
        r#"{"start_qualname":"bare_call_method.bare_caller","direction":"downstream","kinds":["CALLS"]}"#,
    );
    let filtered = call(
        &repo_root,
        &db_path,
        "trace_flow",
        r#"{"start_qualname":"bare_call_method.bare_caller","direction":"downstream","kinds":["CALLS"],
            "exclude_resolution_kinds":["bare_name","two_segment"]}"#,
    );

    let unfiltered_hops = trace_hop_qualnames(&unfiltered);
    let filtered_hops = trace_hop_qualnames(&filtered);

    assert!(
        unfiltered_hops.contains(&target),
        "unfiltered trace must reach the heuristically-resolved target, got {:?}",
        unfiltered_hops
    );
    assert!(
        !filtered_hops.contains(&target),
        "filtered trace must not traverse the excluded heuristic edge, got {:?}",
        filtered_hops
    );

    let mut diff: Vec<String> = unfiltered_hops
        .iter()
        .filter(|h| !filtered_hops.contains(h))
        .cloned()
        .collect();
    diff.sort();
    assert_eq!(
        diff,
        vec![target],
        "filtered vs unfiltered trace must differ by exactly the fixture's heuristic edge"
    );
}

#[test]
fn filtered_analyze_impact_excludes_exactly_the_heuristic_edge() {
    let (_tmp, repo_root, db_path) = setup_with_worker_added();
    let target = {
        let indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
        let graph_version = indexer.db().current_graph_version().unwrap();
        the_heuristic_edge(indexer.db(), graph_version)
            .target_qualname
            .unwrap()
    };

    let unfiltered = call(
        &repo_root,
        &db_path,
        "analyze_impact",
        r#"{"qualname":"bare_call_method.bare_caller","direction":"downstream","kinds":["CALLS"]}"#,
    );
    let filtered = call(
        &repo_root,
        &db_path,
        "analyze_impact",
        r#"{"qualname":"bare_call_method.bare_caller","direction":"downstream","kinds":["CALLS"],
            "exclude_resolution_kinds":["bare_name","two_segment"]}"#,
    );

    let unfiltered_affected = impact_affected_qualnames(&unfiltered);
    let filtered_affected = impact_affected_qualnames(&filtered);

    assert!(
        unfiltered_affected.contains(&target),
        "unfiltered impact must reach the heuristically-resolved target, got {:?}",
        unfiltered_affected
    );
    assert!(
        !filtered_affected.contains(&target),
        "filtered impact must not traverse the excluded heuristic edge, got {:?}",
        filtered_affected
    );

    let mut diff: Vec<String> = unfiltered_affected
        .iter()
        .filter(|h| !filtered_affected.contains(h))
        .cloned()
        .collect();
    diff.sort();
    assert_eq!(
        diff,
        vec![target],
        "filtered vs unfiltered impact must differ by exactly the fixture's heuristic edge"
    );
}

#[test]
fn trace_flow_next_hops_suggest_filtered_and_unfiltered_variants() {
    let (_tmp, repo_root, db_path) = setup_with_worker_added();

    let unfiltered = call(
        &repo_root,
        &db_path,
        "trace_flow",
        r#"{"start_qualname":"bare_call_method.bare_caller","direction":"downstream","kinds":["CALLS"]}"#,
    );
    let hops = unfiltered["next_hops"].as_array().unwrap();
    assert!(
        hops.iter().any(|h| h["method"] == "trace_flow"
            && h["params"].get("exclude_resolution_kinds").is_some()),
        "an unfiltered, non-empty trace should suggest a resolution-kind-filtered retry, got {:?}",
        hops
    );

    let filtered = call(
        &repo_root,
        &db_path,
        "trace_flow",
        r#"{"start_qualname":"bare_call_method.bare_caller","direction":"downstream","kinds":["CALLS"],
            "exclude_resolution_kinds":["bare_name"]}"#,
    );
    let hops = filtered["next_hops"].as_array().unwrap();
    assert!(
        hops.iter().any(|h| h["method"] == "trace_flow"
            && h["params"].get("exclude_resolution_kinds").is_none()
            && h["description"].as_str().unwrap().contains("unfiltered")),
        "a filtered trace should suggest an unfiltered retry, got {:?}",
        hops
    );

    // Issue #81 also requires every suggested method to be real -- covered
    // repo-wide by tests/next_hops_validity.rs, checked again here for this
    // handler specifically.
    for h in hops {
        assert!(rpc::METHOD_LIST.contains(&h["method"].as_str().unwrap()));
    }
}

#[test]
fn trace_flow_lower_bound_set_only_when_traversed_symbol_has_pending_unresolved_reference() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    // caller.call_ambiguous's own CALLS reference never resolves (two
    // same-named, unimported candidates) -- a pending unresolved_references
    // row keyed on this exact symbol.
    let ambiguous = call(
        &repo_root,
        &db_path,
        "trace_flow",
        r#"{"start_qualname":"caller.call_ambiguous","direction":"downstream","kinds":["CALLS"]}"#,
    );
    assert_eq!(
        ambiguous["lower_bound"]["is_lower_bound"],
        Value::Bool(true)
    );
    assert_eq!(ambiguous["lower_bound"]["unresolved_count"], Value::from(1));

    // caller.entry's own downstream closure (itself + caller.local_util)
    // has no pending unresolved reference at all.
    let clean = call(
        &repo_root,
        &db_path,
        "trace_flow",
        r#"{"start_qualname":"caller.entry","direction":"downstream","kinds":["CALLS"]}"#,
    );
    assert_eq!(clean["lower_bound"]["is_lower_bound"], Value::Bool(false));
    assert_eq!(clean["lower_bound"]["unresolved_count"], Value::from(0));
}

#[test]
fn analyze_impact_lower_bound_set_only_when_traversed_symbol_has_pending_unresolved_reference() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    let ambiguous = call(
        &repo_root,
        &db_path,
        "analyze_impact",
        r#"{"qualname":"caller.call_ambiguous","direction":"downstream","kinds":["CALLS"]}"#,
    );
    assert_eq!(
        ambiguous["lower_bound"]["is_lower_bound"],
        Value::Bool(true)
    );
    assert_eq!(ambiguous["lower_bound"]["unresolved_count"], Value::from(1));

    let clean = call(
        &repo_root,
        &db_path,
        "analyze_impact",
        r#"{"qualname":"caller.entry","direction":"downstream","kinds":["CALLS"]}"#,
    );
    assert_eq!(clean["lower_bound"]["is_lower_bound"], Value::Bool(false));
    assert_eq!(clean["lower_bound"]["unresolved_count"], Value::from(0));
}

/// Default behaviour is unchanged (acceptance criterion): omitting
/// `exclude_resolution_kinds` entirely must traverse the heuristic edge
/// exactly as before issue #81.
#[test]
fn omitting_exclude_resolution_kinds_keeps_default_behaviour() {
    let (_tmp, repo_root, db_path) = setup_with_worker_added();

    let result = call(
        &repo_root,
        &db_path,
        "trace_flow",
        r#"{"start_qualname":"bare_call_method.bare_caller","direction":"downstream","kinds":["CALLS"]}"#,
    );
    assert!(trace_hop_qualnames(&result).contains(&"worker.process".to_string()));

    let result = call(
        &repo_root,
        &db_path,
        "analyze_impact",
        r#"{"qualname":"bare_call_method.bare_caller","direction":"downstream","kinds":["CALLS"]}"#,
    );
    assert!(impact_affected_qualnames(&result).contains(&"worker.process".to_string()));
}
