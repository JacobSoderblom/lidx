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

/// Issue #81 (R1): `analyze_impact`'s test layer (`impact/layers/test.rs`'s
/// `discover_call_tests`, on by default) walks CALLS edges via
/// `edges_for_symbol` just like the direct layer -- the orchestrator used to
/// pass `exclude_resolution_kinds` only to the direct layer, so a test found
/// solely through a heuristically-resolved CALLS edge still surfaced even
/// with that resolution kind excluded.
#[test]
fn test_layer_honors_resolution_kind_filter() {
    let (_tmp, repo_root, db_path) = setup_with_worker_added();

    // `from worker import *` never populates import candidates for `process`
    // (a star import), so the bare `process()` call inside `test_alpha`
    // resolves via the guarded name-fallback tier, same shape as
    // `bare_call_method.bare_caller`'s heuristic edge above.
    std::fs::create_dir_all(repo_root.join("tests")).unwrap();
    std::fs::write(
        repo_root.join("tests").join("test_zzz.py"),
        "from worker import *\n\ndef test_alpha():\n    process()\n",
    )
    .unwrap();
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer
        .sync_rel_paths(&["tests/test_zzz.py".to_string()])
        .unwrap();
    drop(indexer);

    let unfiltered = call(
        &repo_root,
        &db_path,
        "analyze_impact",
        r#"{"qualname":"worker.process","direction":"upstream"}"#,
    );
    let unfiltered_affected = impact_affected_qualnames(&unfiltered);
    assert!(
        unfiltered_affected.contains(&"tests.test_zzz.test_alpha".to_string()),
        "precondition: unfiltered upstream impact must find the test via the call-based \
         test layer, got {:?}",
        unfiltered_affected
    );

    let filtered = call(
        &repo_root,
        &db_path,
        "analyze_impact",
        r#"{"qualname":"worker.process","direction":"upstream",
            "exclude_resolution_kinds":["bare_name","two_segment"]}"#,
    );
    let filtered_affected = impact_affected_qualnames(&filtered);
    assert!(
        !filtered_affected.contains(&"tests.test_zzz.test_alpha".to_string()),
        "R1: the test layer must also refuse a heuristically-resolved CALLS edge once \
         excluded, got {:?}",
        filtered_affected
    );
}

/// Issue #81 (R2): a `trace_flow` call started via `query` (or
/// `start_query`) has neither `start_qualname` nor `start_id` in its own
/// params, so a retry hop built by copying only those two fields had no
/// start at all -- following it failed with "trace_flow requires start_id,
/// start_qualname, or query". The retry hop must fall back to the symbol
/// `resolve_symbol` already resolved the query to.
#[test]
fn trace_flow_query_started_retry_hop_is_followable() {
    let (_tmp, repo_root, db_path) = setup_with_worker_added();

    let result = call(
        &repo_root,
        &db_path,
        "trace_flow",
        r#"{"query":"bare_caller","direction":"downstream","kinds":["CALLS"]}"#,
    );
    let hops = result["next_hops"].as_array().unwrap();
    let hop = hops
        .iter()
        .find(|h| {
            h["method"] == "trace_flow" && h["params"].get("exclude_resolution_kinds").is_some()
        })
        .unwrap_or_else(|| panic!("expected an exclude-heuristics retry hop, got {:?}", hops));
    assert!(
        hop["params"].get("start_id").is_some() || hop["params"].get("start_qualname").is_some(),
        "R2: a query-started trace's retry hop must carry a resolvable start, got {:?}",
        hop
    );

    let raw = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "trace_flow".to_string(),
        &serde_json::to_string(&hop["params"]).unwrap(),
        "2",
    )
    .unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "R2: following the retry hop from a query-started trace must not error, got {envelope}"
    );
}

/// Issue #81 (R4): a `trace_flow` retry hop used to be rebuilt from scratch
/// (only start + direction), dropping `end_qualname`/`end_id`, `kinds`,
/// `max_hops` and `include_snippets` -- the retry silently became a
/// different, broader trace instead of the same one with only the filter
/// toggled.
#[test]
fn trace_flow_retry_hop_preserves_end_kinds_max_hops_and_snippets() {
    let (_tmp, repo_root, db_path) = setup_with_worker_added();

    let result = call(
        &repo_root,
        &db_path,
        "trace_flow",
        r#"{"start_qualname":"bare_call_method.bare_caller","end_qualname":"worker.process",
            "direction":"downstream","kinds":["CALLS"],"max_hops":3,"include_snippets":false}"#,
    );
    let hops = result["next_hops"].as_array().unwrap();
    let hop = hops
        .iter()
        .find(|h| {
            h["method"] == "trace_flow" && h["params"].get("exclude_resolution_kinds").is_some()
        })
        .unwrap_or_else(|| panic!("expected an exclude-heuristics retry hop, got {:?}", hops));

    assert_eq!(hop["params"]["end_qualname"], Value::from("worker.process"));
    assert_eq!(hop["params"]["kinds"], serde_json::json!(["CALLS"]));
    assert_eq!(hop["params"]["max_hops"], Value::from(3));
    assert_eq!(hop["params"]["include_snippets"], Value::from(false));
}

/// Issue #81 (R4): an `analyze_impact` retry hop used to keep only
/// `{id: seed_ids.first(), direction}`, dropping `max_depth`, `kinds`,
/// `include_tests` and the layer enable/disable toggles -- the retry
/// silently became a different, unbounded analysis instead of the same one
/// with only the filter toggled.
#[test]
fn analyze_impact_retry_hop_preserves_max_depth_kinds_include_tests_and_layer_config() {
    let (_tmp, repo_root, db_path) = setup_with_worker_added();

    let result = call(
        &repo_root,
        &db_path,
        "analyze_impact",
        r#"{"qualname":"bare_call_method.bare_caller","direction":"downstream","kinds":["CALLS"],
            "max_depth":2,"include_tests":true,"enable_historical":false}"#,
    );
    let hops = result["next_hops"].as_array().unwrap();
    let hop = hops
        .iter()
        .find(|h| {
            h["method"] == "analyze_impact" && h["params"].get("exclude_resolution_kinds").is_some()
        })
        .unwrap_or_else(|| panic!("expected an exclude-heuristics retry hop, got {:?}", hops));

    assert_eq!(
        hop["params"]["qualname"],
        Value::from("bare_call_method.bare_caller")
    );
    assert_eq!(hop["params"]["max_depth"], Value::from(2));
    assert_eq!(hop["params"]["kinds"], serde_json::json!(["CALLS"]));
    assert_eq!(hop["params"]["include_tests"], Value::from(true));
    assert_eq!(hop["params"]["enable_historical"], Value::from(false));
}

/// Issue #81 (R3): an unknown or wrong-case resolution kind used to be
/// silently ignored (matching nothing), while the response still offered
/// "retry without the filter" as if the filter had done something. Both
/// handlers must reject it with a clear error instead.
#[test]
fn exclude_resolution_kinds_rejects_unknown_values() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    let raw = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "trace_flow".to_string(),
        r#"{"start_qualname":"caller.entry","exclude_resolution_kinds":["BARE_NAME"]}"#,
        "1",
    )
    .unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    let message = envelope["error"]["message"].as_str().unwrap_or_default();
    assert!(
        !message.is_empty(),
        "trace_flow must reject a wrong-case resolution kind, got {envelope}"
    );
    assert!(
        message.contains("BARE_NAME"),
        "error should name the offending value, got {message:?}"
    );
    assert!(
        message.contains("bare_name"),
        "error should list a valid kind so the caller can self-correct, got {message:?}"
    );

    let raw = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "analyze_impact".to_string(),
        r#"{"qualname":"caller.entry","exclude_resolution_kinds":["bogus"]}"#,
        "1",
    )
    .unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    let message = envelope["error"]["message"].as_str().unwrap_or_default();
    assert!(
        !message.is_empty(),
        "analyze_impact must reject an unknown resolution kind, got {envelope}"
    );
    assert!(
        message.contains("bogus"),
        "error should name the offending value, got {message:?}"
    );
}
