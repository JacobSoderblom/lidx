//! Issue #230: every next_hop is a complete, followable request -- it
//! preserves each parameter of the originating request except the ones it
//! deliberately overrides (`kinds`, and `max_bytes` for the truncation hop).

mod common;

use lidx::rpc;
use serde_json::{Value, json};
use std::path::PathBuf;

const OPTIONS: &str = r#"
namespace Acme.Config;

public class DatabaseOptions {
    public string ConnectionString { get; set; }
}
"#;

const REPO: &str = r#"
namespace Acme.Data;

public class MssqlRepositoryBase {
    public MssqlRepositoryBase(IOptions<DatabaseOptions> options) {
        _options = options;
    }
}
"#;

const CHAIN: &str = r#"
def step_one_of_a_long_chain():
    return step_two_of_a_long_chain()

def step_two_of_a_long_chain():
    return step_three_of_a_long_chain()

def step_three_of_a_long_chain():
    return step_four_of_a_long_chain()

def step_four_of_a_long_chain():
    return 1
"#;

fn setup() -> (tempfile::TempDir, PathBuf, PathBuf) {
    common::index_repo(
        "lidx-trace-flow-next-hop-params-",
        &[
            ("Options.cs", OPTIONS),
            ("Repo.cs", REPO),
            ("chain.py", CHAIN),
        ],
    )
}

/// Result payload of `method`, with an error envelope or a truncation wrapper
/// normalised away. Panics on an error response: a followed hop must work.
fn run(fx: &(tempfile::TempDir, PathBuf, PathBuf), method: &str, params: &Value) -> Value {
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
        "{method} {params} returned an error: {envelope}"
    );
    let result = envelope["result"].clone();
    if result.get("truncated").is_some() && result.get("data").is_some() {
        return result["data"].clone();
    }
    result
}

fn hops(result: &Value) -> Vec<Value> {
    result["next_hops"].as_array().cloned().unwrap_or_default()
}

/// The trace_flow hops whose `kinds` are CONFIG-only.
fn config_hops(result: &Value) -> Vec<Value> {
    hops(result)
        .into_iter()
        .filter(|h| {
            h["method"] == "trace_flow"
                && h["params"]["kinds"].as_array().is_some_and(|k| {
                    !k.is_empty() && k.iter().all(|k| k.as_str().unwrap().starts_with("CONFIG"))
                })
        })
        .collect()
}

/// `original` with the `overrides` applied: what a hop's params must equal.
fn expected_hop_params(original: &Value, overrides: Value) -> Value {
    let mut expected = original.clone();
    for (k, v) in overrides.as_object().unwrap() {
        expected[k] = v.clone();
    }
    expected
}

/// A followed CONFIG hop succeeded and traced from the original start.
fn assert_config_trace_from_repo(trace: &Value) {
    assert_eq!(trace["start"]["name"], "MssqlRepositoryBase", "{trace}");
    for h in trace["trace"].as_array().unwrap() {
        assert!(
            h["edge_kind"].as_str().unwrap().starts_with("CONFIG"),
            "only CONFIG edges: {h}"
        );
    }
}

#[test]
fn query_started_upstream_config_hop_is_followable() {
    let fx = setup();
    let original = json!({"query": "MssqlRepositoryBase", "direction": "upstream", "max_hops": 4});
    let result = run(&fx, "trace_flow", &original);
    assert!(result["trace"].as_array().unwrap().is_empty());

    let config = config_hops(&result);
    assert_eq!(config.len(), 1, "one CONFIG hop expected: {result}");
    assert_eq!(
        config[0]["params"],
        expected_hop_params(
            &original,
            json!({"kinds": ["CONFIG_SOURCE", "CONFIG_READ", "CONFIG_BIND"]})
        )
    );
    // Verbatim: succeeds, still query-started (so the start is resolved).
    assert_config_trace_from_repo(&run(&fx, "trace_flow", &config[0]["params"]));
}

#[test]
fn qualname_started_upstream_config_hop_keeps_direction() {
    let fx = setup();
    let original = json!({
        "start_qualname": "Acme.Data.MssqlRepositoryBase",
        "direction": "upstream",
        "max_hops": 3,
        "include_snippets": false,
    });
    let result = run(&fx, "trace_flow", &original);
    let config = config_hops(&result);
    assert_eq!(config.len(), 1, "one CONFIG hop expected: {result}");
    // Dropping `direction` silently turns the hop downstream.
    assert_eq!(
        config[0]["params"],
        expected_hop_params(
            &original,
            json!({"kinds": ["CONFIG_SOURCE", "CONFIG_READ", "CONFIG_BIND"]})
        )
    );
    assert_config_trace_from_repo(&run(&fx, "trace_flow", &config[0]["params"]));
}

#[test]
fn truncation_hop_overrides_only_kinds_and_max_bytes() {
    let fx = setup();
    let original = json!({
        "query": "step_four_of_a_long_chain",
        "direction": "upstream",
        "max_hops": 5,
        "max_bytes": 300,
        "include_snippets": false,
        "languages": ["python"],
    });
    let result = run(&fx, "trace_flow", &original);
    assert_eq!(result["truncated"], true, "fixture must truncate: {result}");

    let narrow = config_hops(&result);
    assert_eq!(narrow.len(), 1, "one narrowing hop expected: {result}");
    assert_eq!(
        narrow[0]["params"],
        expected_hop_params(
            &original,
            json!({
                "kinds": ["CONFIG_SOURCE", "CONFIG_READ", "CONFIG_BIND"],
                "max_bytes": 600,
            })
        )
    );
    run(&fx, "trace_flow", &narrow[0]["params"]);
}

#[test]
fn every_next_hop_is_followable() {
    let fx = setup();
    let requests = [
        // empty trace: analyze_impact + CONFIG retry hops
        json!({"query": "DatabaseOptions", "direction": "upstream"}),
        json!({"start_qualname": "Acme.Config.DatabaseOptions", "direction": "upstream"}),
        // truncated trace: continue + narrowing + explain_symbol hops
        json!({"query": "step_one_of_a_long_chain", "max_bytes": 300, "include_snippets": false}),
        // heuristic-filter retry hop on a query-started trace
        json!({"query": "step_one_of_a_long_chain", "exclude_resolution_kinds": ["bare_name"]}),
    ];
    for original in requests {
        let result = run(&fx, "trace_flow", &original);
        let next = hops(&result);
        assert!(!next.is_empty(), "no next_hops for {original}: {result}");
        for hop in next {
            let method = hop["method"].as_str().unwrap();
            run(&fx, method, &hop["params"]);
        }
    }
}

#[test]
fn analyze_impact_recovery_hops_keep_original_params() {
    let fx = setup();
    let original = json!({
        "query": "step_one_of_a_long_chain",
        "direction": "upstream",
        "max_depth": 2,
        "limit": 7,
        "languages": ["python"],
    });
    let result = run(&fx, "analyze_impact", &original);
    let flip = hops(&result)
        .into_iter()
        .find(|h| h["method"] == "analyze_impact")
        .unwrap_or_else(|| panic!("recovery hop expected: {result}"));
    let params = &flip["params"];
    assert_eq!(params["direction"], "downstream");
    assert_eq!(params["max_depth"], 2);
    assert_eq!(params["limit"], 7);
    assert_eq!(params["languages"], json!(["python"]));
    run(&fx, "analyze_impact", params);
}
