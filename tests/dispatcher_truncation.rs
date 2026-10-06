//! Issue #221: the dispatcher's generic truncation must not second-guess
//! methods that budget themselves, must split by size, and must not change
//! the response shape.

mod common;

use lidx::rpc;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::path::PathBuf;

const FANOUT: usize = 160;
const DEFAULT_CAP: usize = 30_000;

type Fixture = (tempfile::TempDir, PathBuf, PathBuf);

fn setup() -> Fixture {
    let mut hub = String::new();
    let mut callers = String::new();
    // A Python call binds through imports: the hub star-imports its leaves.
    hub.push_str("from leaves import *\n\n\ndef hub_root():\n");
    for i in 0..FANOUT {
        hub.push_str(&format!("    leaf_function_number_{i:03}()\n"));
        callers.push_str(&format!(
            "def leaf_function_number_{i:03}():\n    return shared_target_function()\n\n"
        ));
    }
    callers.push_str("def shared_target_function():\n    return 1\n");
    common::index_repo(
        "lidx-dispatcher-truncation-",
        &[("hub.py", &hub), ("leaves.py", &callers)],
    )
}

fn call(fx: &Fixture, method: &str, params: &Value) -> Value {
    let raw = rpc::call(
        fx.1.clone(),
        fx.2.clone(),
        method.to_string(),
        &params.to_string(),
        "1",
    )
    .unwrap_or_else(|e| panic!("{method} {params} failed: {e:#}"));
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(Value::is_null),
        "{method} {params} errored: {envelope}"
    );
    envelope["result"].clone()
}

fn hop_keys(result: &Value) -> Vec<String> {
    result["trace"]
        .as_array()
        .unwrap_or_else(|| panic!("no .trace in {result}"))
        .iter()
        .map(|h| h.to_string())
        .collect()
}

fn base() -> Value {
    json!({"start_qualname": "hub.hub_root", "max_hops": 2, "include_snippets": false})
}

#[test]
fn continuation_walk_returns_every_hop_exactly_once() {
    let fx = setup();
    let mut full_params = base();
    full_params["max_bytes"] = json!(200_000);
    let full = call(&fx, "trace_flow", &full_params);
    assert_eq!(full["truncated"], json!(false), "full trace must fit");
    let expected = hop_keys(&full);
    assert!(expected.len() >= FANOUT);

    let mut result = call(&fx, "trace_flow", &base());
    assert_eq!(result["truncated"], json!(true), "default budget must cut");
    let mut seen = hop_keys(&result);
    let mut rounds = 0;
    while result["truncated"] == json!(true) {
        rounds += 1;
        assert!(rounds < 50, "continuation never terminates");
        let next = result["next_hops"]
            .as_array()
            .unwrap()
            .iter()
            .find(|h| {
                h["description"]
                    .as_str()
                    .is_some_and(|d| d.starts_with("Continue trace"))
            })
            .unwrap_or_else(|| panic!("truncated without continuation: {result}"));
        result = call(&fx, "trace_flow", &next["params"]);
        seen.extend(hop_keys(&result));
    }
    let unique: BTreeSet<_> = seen.iter().collect();
    assert_eq!(unique.len(), seen.len(), "duplicate hops across pages");
    assert_eq!(seen, expected, "pages must concatenate to the full trace");
}

#[test]
fn trace_flow_stays_within_its_own_budget_and_shape() {
    let fx = setup();
    let truncated = call(&fx, "trace_flow", &base());
    let budget = &truncated["budget"];
    assert!(
        budget["used_bytes"].as_u64() <= budget["budget_bytes"].as_u64(),
        "{budget}"
    );
    let mut big = base();
    big["max_bytes"] = json!(200_000);
    let whole = call(&fx, "trace_flow", &big);
    assert_eq!(truncated["truncated"], json!(true));
    assert_eq!(whole["truncated"], json!(false));
    // Same top-level shape whether or not truncation fired.
    let keys = |v: &Value| -> BTreeSet<String> { v.as_object().unwrap().keys().cloned().collect() };
    assert_eq!(keys(&truncated), keys(&whole));
    assert!(truncated.get("data").is_none());
}

#[test]
fn analyze_impact_is_not_cut_by_an_equal_split() {
    let fx = setup();
    let r = call(
        &fx,
        "analyze_impact",
        &json!({"qualname": "leaves.shared_target_function", "direction": "upstream", "max_depth": 1}),
    );
    let total = r["summary"]["total_affected"].as_u64().unwrap() as usize;
    assert!(total >= FANOUT, "{total}");
    assert!(r.get("data").is_none(), "shape must be stable");
    let kept = r["affected"].as_array().unwrap().len();
    // The affected symbols cannot all fit the cap, so the cut must be
    // reported with the full count.
    assert!(kept < total, "fixture must exceed the cap: kept {kept}");
    assert_eq!(r["truncated"], json!(true));
    assert_eq!(
        r["affected_total_available"].as_u64().unwrap() as usize,
        total
    );
    // The cap is shared by size, not equally: `affected` is nearly all of the
    // payload, so it keeps nearly all of the budget (an equal split over the
    // object's arrays gave it at most 1/n of the cap).
    let affected_bytes = serde_json::to_string(&r["affected"]).unwrap().len();
    let rest_bytes = r.to_string().len() - affected_bytes;
    assert!(
        affected_bytes > 2 * rest_bytes,
        "affected {affected_bytes}B vs rest {rest_bytes}B"
    );
    assert!(r.to_string().len() <= DEFAULT_CAP);
}

/// Valid params for every dispatchable method, on the fan-out fixture.
fn probe_params(method: &str) -> Value {
    match method {
        "search" => json!({"query": "leaf_function"}),
        "outline" => json!({"path": "hub.py"}),
        "read_symbol" => json!({"qualname": "hub.hub_root"}),
        "explain_symbol" => json!({"qualname": "leaves.shared_target_function"}),
        "trace_flow" => base(),
        "analyze_impact" => {
            json!({"qualname": "leaves.shared_target_function", "direction": "upstream"})
        }
        "analyze_diff" => json!({"paths": ["leaves.py"]}),
        "gather_context" => json!({"seeds": [{"type": "symbol", "qualname": "hub.hub_root"}]}),
        "context" => json!({"path": "hub.py"}),
        "orient" | "onboard" | "reindex" | "top_complexity" | "repo_map" | "dead_symbols" => {
            json!({})
        }
        other => panic!("no probe params for method {other}"),
    }
}

#[test]
fn every_method_returns_an_object_so_truncation_never_changes_shape() {
    let fx = setup();
    for method in rpc::METHOD_LIST {
        let r = call(&fx, method, &probe_params(method));
        assert!(r.is_object(), "{method} must return an object, got {r}");
    }
}

#[test]
fn budget_reporting_methods_are_self_budgeting_and_not_recut_by_default() {
    let fx = setup();
    let mut reporting = Vec::new();
    for method in rpc::METHOD_LIST {
        let r = call(&fx, method, &probe_params(method));
        if r.get("budget").is_some() || r.get("budget_bytes").is_some() {
            reporting.push(*method);
            assert!(
                rpc::is_self_budgeting(method),
                "{method} reports a budget but is not declared self-budgeting"
            );
        }
        if rpc::is_self_budgeting(method) {
            assert!(
                r.get("max_response_bytes").is_none(),
                "{method} is self-budgeting yet was cut by the default cap: {r}"
            );
        }
    }
    for expected in ["explain_symbol", "trace_flow", "gather_context"] {
        assert!(
            reporting.contains(&expected),
            "{expected} not probed: {reporting:?}"
        );
    }
}

#[test]
fn explicit_cap_is_respected_by_every_self_budgeting_method() {
    let fx = setup();
    const CAP: usize = 6_000;
    let mut checked = 0;
    for method in rpc::METHOD_LIST
        .iter()
        .filter(|m| rpc::is_self_budgeting(m))
    {
        let mut params = probe_params(method);
        params["max_response_bytes"] = json!(CAP);
        let r = call(&fx, method, &params);
        let n = r.to_string().len();
        assert!(n <= CAP, "{method}: {n} bytes exceeds explicit cap {CAP}");
        checked += 1;
    }
    assert_eq!(checked, 7);
}

#[test]
fn trace_flow_continuation_stays_honest_under_an_explicit_cap() {
    let fx = setup();
    let mut full_params = base();
    full_params["max_bytes"] = json!(200_000);
    let expected = hop_keys(&call(&fx, "trace_flow", &full_params));

    let mut params = base();
    params["max_response_bytes"] = json!(12_000);
    let mut result = call(&fx, "trace_flow", &params);
    let mut seen = hop_keys(&result);
    let mut rounds = 0;
    while result["truncated"] == json!(true) {
        rounds += 1;
        assert!(rounds < 100, "continuation never terminates");
        let next = result["next_hops"]
            .as_array()
            .unwrap()
            .iter()
            .find(|h| {
                h["description"]
                    .as_str()
                    .is_some_and(|d| d.starts_with("Continue trace"))
            })
            .unwrap_or_else(|| panic!("truncated without continuation: {result}"));
        result = call(&fx, "trace_flow", &next["params"]);
        seen.extend(hop_keys(&result));
    }
    assert_eq!(
        seen.len(),
        expected.len(),
        "page walk lost or repeated hops"
    );
    assert_eq!(seen, expected);
}

#[test]
fn truncated_and_untruncated_search_have_the_same_top_level_keys() {
    let fx = setup();
    let keys = |v: &Value| -> BTreeSet<String> { v.as_object().unwrap().keys().cloned().collect() };
    let small = call(
        &fx,
        "search",
        &json!({"query": "leaf_function_number_00", "limit": 50}),
    );
    let mut big_params = json!({"query": "leaf_function", "limit": 500});
    big_params["max_response_bytes"] = json!(2_000);
    let big = call(&fx, "search", &big_params);
    assert_eq!(
        small.get("truncated"),
        None,
        "small search must not truncate"
    );
    assert_eq!(big["truncated"], json!(true));
    assert!(small["results"].is_array() && big["results"].is_array());
    let mut big_keys = keys(&big);
    for k in [
        "truncated",
        "max_response_bytes",
        "total_available",
        "total_available_is_lower_bound",
    ] {
        big_keys.remove(k);
    }
    assert_eq!(keys(&small), big_keys);
}

#[test]
fn trace_flow_budget_is_never_exceeded_except_by_a_lone_first_hop() {
    let fx = setup();
    // Default params: within budget.
    let r = call(&fx, "trace_flow", &base());
    assert!(r["budget"]["used_bytes"].as_u64() <= r["budget"]["budget_bytes"].as_u64());
    // A budget smaller than any hop: exactly one hop is kept (so the
    // continuation advances) and it is the only way to exceed the budget.
    let mut tiny = base();
    tiny["max_bytes"] = json!(1);
    let r = call(&fx, "trace_flow", &tiny);
    assert_eq!(r["trace"].as_array().unwrap().len(), 1, "{r}");
    assert_eq!(r["truncated"], json!(true));
    assert!(r["budget"]["used_bytes"].as_u64() > r["budget"]["budget_bytes"].as_u64());
}
