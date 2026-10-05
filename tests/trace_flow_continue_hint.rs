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
fn deeper_retrace_is_capped_at_ten() {
    let (_tmp, repo, db) = common::index_repo("lidx-continue-hint-cap-", &[("a.py", CHAIN)]);
    // max_hops 10 is never depth-limited by a 4-hop chain, so no hint at all.
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.a", "direction": "downstream", "max_hops": 10}),
    );
    assert_ne!(r["depth_limited"], json!(true), "{r}");
    assert!(hint(&r, "Re-trace deeper").is_none(), "{r}");
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
}
