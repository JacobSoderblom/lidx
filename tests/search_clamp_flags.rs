//! Issue #368: `search` reports clamped `limit`/`context_lines` under
//! `_meta.clamped` and flags results cut by the limit as `truncated`, with
//! `total_available_is_lower_bound`. Unclamped, uncut requests add nothing.

mod common;

use lidx::rpc;
use serde_json::{Value, json};
use std::path::PathBuf;

type Fixture = (tempfile::TempDir, PathBuf, PathBuf);

fn setup() -> Fixture {
    let thousand: String = (1..=1000).map(|i| format!("{i}\n")).collect();
    let ten: String = (0..10).map(|i| format!("rare_marker_{i}\n")).collect();
    common::index_repo(
        "lidx-search-clamp-",
        &[("a.txt", &thousand), ("b.txt", &ten)],
    )
}

fn search(fx: &Fixture, params: Value) -> Value {
    let raw = rpc::call(
        fx.1.clone(),
        fx.2.clone(),
        "search".to_string(),
        &params.to_string(),
        "1",
    )
    .unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(Value::is_null),
        "{envelope}"
    );
    envelope["result"].clone()
}

fn n(v: &Value) -> usize {
    v["results"].as_array().unwrap().len()
}

#[test]
fn limit_above_cap_is_reported_and_truncated() {
    let fx = setup();
    let r = search(
        &fx,
        json!({"query": "^[0-9]+$", "path": "a.txt", "limit": 2000}),
    );
    assert_eq!(n(&r), 500);
    assert_eq!(r["truncated"], json!(true));
    assert_eq!(r["total_available_is_lower_bound"], json!(true));
    assert_eq!(
        r["_meta"]["clamped"]["limit"],
        json!({"requested": 2000, "applied": 500})
    );
}

#[test]
fn limit_exactly_cap_truncates_without_clamp_flag() {
    let fx = setup();
    let r = search(
        &fx,
        json!({"query": "^[0-9]+$", "path": "a.txt", "limit": 500}),
    );
    assert_eq!(n(&r), 500);
    assert_eq!(r["truncated"], json!(true));
    assert!(r.get("_meta").is_none(), "{r}");
}

#[test]
fn small_result_set_adds_no_fields() {
    let fx = setup();
    let r = search(&fx, json!({"query": "rare_marker_"}));
    assert_eq!(n(&r), 10);
    let keys: Vec<_> = r.as_object().unwrap().keys().collect();
    assert_eq!(keys, vec!["results"], "{r}");
}

#[test]
fn context_lines_clamp_is_reported() {
    let fx = setup();
    let r = search(
        &fx,
        json!({"query": "^500$", "path": "a.txt", "context_lines": 100, "limit": 1}),
    );
    assert_eq!(
        r["_meta"]["clamped"]["context_lines"],
        json!({"requested": 100, "applied": 50})
    );
    assert!(r["_meta"]["clamped"].get("limit").is_none());
    assert!(r.get("truncated").is_none(), "{r}");
}

#[test]
fn byte_budget_truncation_is_flagged() {
    let fx = setup();
    let r = search(
        &fx,
        json!({"query": "^[0-9]+$", "path": "a.txt", "limit": 500, "max_response_bytes": 5000}),
    );
    assert_eq!(r["truncated"], json!(true));
    assert!(n(&r) < 500);
    assert_eq!(r["total_available_is_lower_bound"], json!(true));
}
