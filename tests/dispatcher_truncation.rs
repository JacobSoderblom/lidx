//! Issue #221: the dispatcher's generic truncation must not second-guess
//! methods that budget themselves, must split by size, and must not change
//! the response shape.

mod common;

use lidx::rpc;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::path::PathBuf;

const FANOUT: usize = 160;

type Fixture = (tempfile::TempDir, PathBuf, PathBuf);

fn setup() -> Fixture {
    let mut hub = String::new();
    let mut callers = String::new();
    hub.push_str("def hub_root():\n");
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
    if kept < total {
        assert_eq!(r["truncated"], json!(true));
        assert_eq!(
            r["affected_total_available"].as_u64().unwrap() as usize,
            total
        );
    }
    // The 30KB cap is shared by size, not equally: `affected` is nearly all
    // of the payload, so it keeps most of the budget (6 of 156 before).
    assert!(kept >= 50, "kept {kept} of {total}");
    assert!(r.to_string().len() <= 30_000 + 1_000);
}

#[test]
fn every_method_reporting_a_budget_is_self_budgeting() {
    let fx = setup();
    let probes = [
        ("explain_symbol", json!({"qualname": "hub.hub_root"})),
        ("trace_flow", base()),
        (
            "gather_context",
            json!({"seeds": [{"type": "symbol", "value": "hub.hub_root"}]}),
        ),
        ("analyze_impact", json!({"qualname": "hub.hub_root"})),
        ("analyze_diff", json!({})),
        ("orient", json!({})),
        ("onboard", json!({})),
        ("repo_map", json!({})),
        ("context", json!({})),
        ("top_complexity", json!({})),
        ("dead_symbols", json!({})),
        ("search", json!({"query": "leaf"})),
        ("outline", json!({"path": "hub.py"})),
        ("read_symbol", json!({"qualname": "hub.hub_root"})),
    ];
    let mut reported = 0;
    for (method, params) in probes {
        let Ok(raw) = rpc::call(
            fx.1.clone(),
            fx.2.clone(),
            method.into(),
            &params.to_string(),
            "1",
        ) else {
            continue;
        };
        let env: Value = serde_json::from_str(&raw).unwrap();
        let Some(result) = env.get("result").filter(|r| r.is_object()) else {
            continue;
        };
        if result.get("budget").is_some() || result.get("budget_bytes").is_some() {
            reported += 1;
            assert!(
                rpc::is_self_budgeting(method),
                "{method} reports a budget but is not declared self-budgeting"
            );
        }
    }
    assert!(reported >= 2, "probe should hit budget-reporting methods");
    for m in rpc::METHOD_LIST {
        let _ = rpc::is_self_budgeting(m); // every listed method has a spec
    }
}
