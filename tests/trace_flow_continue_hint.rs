//! Issue #354: hop-ceiling truncation must not offer an offset continuation
//! (it returned an empty trace and repeated itself); it offers a deeper
//! re-trace instead. Byte-budget truncation still pages with `trace_offset`.
mod common;

use lidx::rpc;
use serde_json::{Value, json};

const CHAIN: &str = "def a():\n    b()\n\ndef b():\n    c()\n\ndef c():\n    d()\n\ndef d():\n    e()\n\ndef e():\n    pass\n";

fn trace(repo: &std::path::Path, db: &std::path::Path, params: &Value) -> Value {
    let raw = rpc::call(
        repo.to_path_buf(),
        db.to_path_buf(),
        "trace_flow".to_string(),
        &params.to_string(),
        "1",
    )
    .unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "{envelope}"
    );
    envelope["result"].clone()
}

fn hop_names(result: &Value) -> Vec<String> {
    result["trace"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["symbol"]["qualname"].as_str().unwrap().to_string())
        .collect()
}

fn hint<'a>(result: &'a Value, prefix: &str) -> Option<&'a Value> {
    result["next_hops"].as_array().and_then(|hs| {
        hs.iter().find(|h| {
            h["description"]
                .as_str()
                .is_some_and(|d| d.starts_with(prefix))
        })
    })
}

#[test]
fn hop_ceiling_offers_deeper_retrace_not_offset() {
    let (_tmp, repo, db) = common::index_repo("lidx-continue-hint-depth-", &[("a.py", CHAIN)]);
    let params = json!({"start_qualname": "a.a", "direction": "downstream", "max_hops": 2});
    let r = trace(&repo, &db, &params);
    assert_eq!(r["trace"].as_array().unwrap().len(), 2, "{r}");
    assert_eq!(r["depth_limited"], json!(true), "{r}");
    assert_eq!(r["truncated"], json!(true), "{r}");
    assert_eq!(r["budget"]["truncated"], json!(false), "{r}");
    assert!(hint(&r, "Continue trace").is_none(), "{r}");
    let deeper = hint(&r, "Re-trace deeper").expect("deeper hint");
    assert_eq!(deeper["params"]["max_hops"], json!(3));
    assert!(deeper["params"].get("trace_offset").is_none());
}

#[test]
fn complete_trace_at_ceiling_is_not_depth_limited() {
    let (_tmp, repo, db) = common::index_repo("lidx-continue-hint-fits-", &[("a.py", CHAIN)]);
    // max_hops 10 covers the whole 4-hop chain, so there is nothing to deepen.
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.a", "direction": "downstream", "max_hops": 10}),
    );
    assert_ne!(r["depth_limited"], json!(true), "{r}");
    assert!(hint(&r, "Re-trace deeper").is_none(), "{r}");
}

#[test]
fn no_deeper_hint_beyond_the_ten_hop_cap() {
    // A 13-function chain: max_hops 10 is the hard cap and still cuts it.
    let mut src = String::new();
    for i in 0..13 {
        src.push_str(&format!("def f{i}():\n    f{}()\n\n", i + 1));
    }
    src.push_str("def f13():\n    pass\n");
    let (_tmp, repo, db) = common::index_repo("lidx-continue-hint-cap-", &[("a.py", &src)]);
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.f0", "direction": "downstream", "max_hops": 10}),
    );
    assert_eq!(r["trace"].as_array().unwrap().len(), 10, "{r}");
    assert_eq!(r["depth_limited"], json!(true), "{r}");
    assert!(hint(&r, "Re-trace deeper").is_none(), "{r}");
    assert!(hint(&r, "Continue trace").is_none(), "{r}");
    // Asking for more than the cap is clamped, not a way around it.
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.f0", "direction": "downstream", "max_hops": 50}),
    );
    assert_eq!(r["trace"].as_array().unwrap().len(), 10, "{r}");
    assert!(hint(&r, "Re-trace deeper").is_none(), "{r}");
}

#[test]
fn config_only_hint_on_paged_call_drops_trace_offset() {
    let (_tmp, repo, db) = common::index_repo("lidx-continue-hint-cfg-", &[("a.py", CHAIN)]);
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.a", "direction": "downstream", "max_hops": 5, "max_bytes": 1, "trace_offset": 1}),
    );
    assert_eq!(r["budget"]["truncated"], json!(true), "{r}");
    assert!(hint(&r, "Continue trace").is_some(), "{r}");
    let cfg = hint(&r, "Re-trace with only CONFIG").expect("config hint");
    assert!(cfg["params"].get("trace_offset").is_none(), "{cfg}");
}

#[test]
fn depth_only_truncation_has_no_config_only_hint() {
    let (_tmp, repo, db) = common::index_repo("lidx-continue-hint-cfgd-", &[("a.py", CHAIN)]);
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.a", "direction": "downstream", "max_hops": 2}),
    );
    assert_eq!(r["depth_limited"], json!(true), "{r}");
    assert!(hint(&r, "Re-trace with only CONFIG").is_none(), "{r}");
}

#[test]
fn byte_budget_and_hop_ceiling_together_end_with_deeper_hint_only() {
    let (_tmp, repo, db) = common::index_repo("lidx-continue-hint-both-", &[("a.py", CHAIN)]);
    let mut r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.a", "direction": "downstream", "max_hops": 2, "max_bytes": 500}),
    );
    assert_eq!(r["depth_limited"], json!(true), "{r}");
    assert_eq!(r["budget"]["truncated"], json!(true), "{r}");
    let mut seen = hop_names(&r);
    assert!(hint(&r, "Continue trace").is_some(), "{r}");
    assert!(hint(&r, "Re-trace deeper").is_some(), "{r}");
    for _ in 0..5 {
        let Some(next) = hint(&r, "Continue trace") else {
            break;
        };
        r = trace(&repo, &db, &next["params"]);
        seen.extend(hop_names(&r));
    }
    seen.sort();
    assert_eq!(seen, vec!["a.b".to_string(), "a.c".to_string()], "{r}");
    assert!(hint(&r, "Continue trace").is_none(), "{r}");
    assert_eq!(r["depth_limited"], json!(true), "{r}");
    assert_eq!(r["truncated"], json!(true), "{r}");
    assert!(hint(&r, "Re-trace deeper").is_some(), "{r}");
}

#[test]
fn byte_budget_offset_continuation_returns_remaining_hops() {
    let (_tmp, repo, db) = common::index_repo("lidx-continue-hint-bytes-", &[("a.py", CHAIN)]);
    let base = json!({"start_qualname": "a.a", "direction": "downstream", "max_hops": 5});
    let full = trace(&repo, &db, &base);
    let full_names = hop_names(&full);
    assert_eq!(full_names.len(), 4, "{full}");

    let mut params = base.clone();
    params["max_bytes"] = json!(1);
    let mut seen: Vec<String> = Vec::new();
    let mut r = trace(&repo, &db, &params);
    for _ in 0..10 {
        seen.extend(hop_names(&r));
        let Some(next) = hint(&r, "Continue trace") else {
            break;
        };
        assert_ne!(r["depth_limited"], json!(true), "{r}");
        r = trace(&repo, &db, &next["params"]);
    }
    seen.sort();
    let mut expected = full_names;
    expected.sort();
    assert_eq!(seen, expected, "union of pages must equal the full trace");
}

#[test]
fn offset_past_end_returns_empty_not_truncated() {
    let (_tmp, repo, db) = common::index_repo("lidx-continue-hint-past-", &[("a.py", CHAIN)]);
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.a", "direction": "downstream", "max_hops": 2, "trace_offset": 2}),
    );
    assert!(r["trace"].as_array().unwrap().is_empty(), "{r}");
    assert_eq!(r["truncated"], json!(false), "{r}");
    assert_eq!(r["no_more_results"], json!(true), "{r}");
    assert!(hint(&r, "Continue trace").is_none(), "{r}");
    // No hint that would return empty or repeat on an exhausted page.
    assert!(hint(&r, "Re-trace with CONFIG").is_none(), "{r}");
    assert!(hint(&r, "Re-trace with only CONFIG").is_none(), "{r}");
    assert!(hint(&r, "Try analyze_impact").is_none(), "{r}");
    // The trace was still depth-limited: keep that and the way forward.
    assert_eq!(r["depth_limited"], json!(true), "{r}");
    let deeper = hint(&r, "Re-trace deeper").expect("deeper hint");
    assert!(deeper["params"].get("trace_offset").is_none(), "{deeper}");
}
