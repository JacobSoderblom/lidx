//! Issue #241: invalid values for known params must be errors, never an
//! empty result that reads as "the index has no match". Covers `orient`
//! (unknown `view`), `context` (unindexed / absolute / escaping path, unknown
//! `format`), `search` (`limit` of 0, negative, absurdly large) and the audit
//! of the same shape in the other methods.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};

const FILES: &[(&str, &str)] = &[
    (
        "src/app.py",
        "def ensure_registered():\n    pass\n\n\
         def other():\n    ensure_registered()\n    ensure_registered()\n    ensure_registered()\n",
    ),
    ("src/util.py", "def helper():\n    return 1\n"),
    ("docs/guide.md", "# Guide\nensure_registered docs\n"),
];

fn build() -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-invalid-input-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), FILES);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

fn call(ix: &mut Indexer, method: &str, params: Value) -> anyhow::Result<Value> {
    rpc::handle_method(ix, method, params)
}

fn err_of(ix: &mut Indexer, method: &str, params: Value) -> String {
    call(ix, method, params.clone())
        .err()
        .unwrap_or_else(|| panic!("{method} {params} should be an error"))
        .to_string()
}

fn hit_count(result: &Value) -> usize {
    result
        .as_array()
        .or_else(|| result["results"].as_array())
        .map(Vec::len)
        .unwrap_or(0)
}

// ---- orient ----

#[test]
fn orient_unknown_view_errors_listing_valid_views() {
    let (_t, mut ix) = build();
    let err = err_of(&mut ix, "orient", json!({"view": "bogus"}));
    assert!(err.contains("bogus"), "{err}");
    for v in ["overview", "map", "modules", "all"] {
        assert!(err.contains(v), "should list '{v}': {err}");
    }
}

#[test]
fn orient_valid_views_still_work() {
    let (_t, mut ix) = build();
    for (view, key) in [
        ("overview", "overview"),
        ("map", "map"),
        ("modules", "modules"),
    ] {
        let r = call(&mut ix, "orient", json!({"view": view})).unwrap();
        assert!(r.get(key).is_some(), "{view}: {r}");
    }
    let all = call(&mut ix, "orient", json!({"view": "all"})).unwrap();
    assert!(all.get("overview").is_some() && all.get("map").is_some());
}

// ---- context ----

#[test]
fn context_unindexed_path_errors_like_outline() {
    let (_t, mut ix) = build();
    let ctx = err_of(&mut ix, "context", json!({"path": "nope.cs"}));
    let outline = err_of(&mut ix, "outline", json!({"path": "nope.cs"}));
    assert_eq!(ctx, outline, "context must reuse outline's message");
    assert!(ctx.contains("is not indexed"), "{ctx}");
}

#[test]
fn context_absolute_path_to_indexed_file_errors_not_empty() {
    let (t, mut ix) = build();
    let abs = t.path().join("src/app.py");
    let err = err_of(&mut ix, "context", json!({"path": abs.to_str().unwrap()}));
    assert!(err.contains("relative"), "{err}");
}

#[test]
fn context_path_escaping_repo_is_rejected() {
    let (_t, mut ix) = build();
    let err = err_of(&mut ix, "context", json!({"path": "../../etc/passwd"}));
    assert!(err.contains("escapes the repo root"), "{err}");
}

#[test]
fn context_unknown_format_errors() {
    let (_t, mut ix) = build();
    let err = err_of(
        &mut ix,
        "context",
        json!({"path": "src/app.py", "format": "xml"}),
    );
    assert!(
        err.contains("xml") && err.contains("json") && err.contains("text"),
        "{err}"
    );
}

#[test]
fn context_valid_path_still_works() {
    let (_t, mut ix) = build();
    let r = call(&mut ix, "context", json!({"path": "src/app.py"})).unwrap();
    assert!(r["context"].is_string());
    let j = call(
        &mut ix,
        "context",
        json!({"path": "src/app.py", "format": "json"}),
    )
    .unwrap();
    assert_eq!(j["path"], "src/app.py");
}

// ---- search limit ----

#[test]
fn search_limit_zero_is_an_error_not_an_empty_result() {
    let (_t, mut ix) = build();
    let baseline = call(&mut ix, "search", json!({"query": "ensure_registered"})).unwrap();
    assert_eq!(
        hit_count(&baseline),
        5,
        "fixture should have 5 hits: {baseline}"
    );
    let err = err_of(
        &mut ix,
        "search",
        json!({"query": "ensure_registered", "limit": 0}),
    );
    assert!(err.contains("limit") && err.contains("at least 1"), "{err}");
}

#[test]
fn search_negative_limit_errors_naming_the_param() {
    let (_t, mut ix) = build();
    let err = err_of(
        &mut ix,
        "search",
        json!({"query": "ensure_registered", "limit": -5}),
    );
    assert!(err.contains("limit") && err.contains("-5"), "{err}");
}

#[test]
fn search_fractional_limit_errors_naming_the_param() {
    let (_t, mut ix) = build();
    let err = err_of(
        &mut ix,
        "search",
        json!({"query": "ensure_registered", "limit": 1.5}),
    );
    assert!(err.contains("limit"), "{err}");
}

#[test]
fn search_huge_limit_is_clamped_and_still_returns_hits() {
    let (_t, mut ix) = build();
    for limit in [json!(1_000_000), json!(u64::MAX)] {
        let r = call(
            &mut ix,
            "search",
            json!({"query": "ensure_registered", "limit": limit}),
        )
        .unwrap();
        assert_eq!(hit_count(&r), 5, "{r}");
    }
}

#[test]
fn search_limit_one_still_caps() {
    let (_t, mut ix) = build();
    let r = call(
        &mut ix,
        "search",
        json!({"query": "ensure_registered", "limit": 1}),
    )
    .unwrap();
    assert_eq!(hit_count(&r), 1, "{r}");
}

#[test]
fn genuine_no_match_search_keeps_empty_result_and_recovery_hops() {
    let (_t, mut ix) = build();
    let r = call(&mut ix, "search", json!({"query": "zzz_no_such_thing_zzz"})).unwrap();
    assert_eq!(hit_count(&r), 0);
    assert!(
        r["next_hops"].as_array().is_some_and(|h| !h.is_empty()),
        "{r}"
    );
}

// ---- audit: same shape in other methods ----

#[test]
fn zero_limits_error_in_other_methods() {
    let (_t, mut ix) = build();
    for (method, params) in [
        ("top_complexity", json!({"limit": 0})),
        ("dead_symbols", json!({"limit": 0})),
        (
            "analyze_impact",
            json!({"qualname": "src.app.other", "limit": 0}),
        ),
        (
            "trace_flow",
            json!({"start_qualname": "src.app.other", "max_hops": 0}),
        ),
        (
            "explain_symbol",
            json!({"qualname": "src.app.other", "max_refs": 0}),
        ),
        (
            "gather_context",
            json!({"seeds": [{"type": "search", "query": "other", "limit": 0}]}),
        ),
    ] {
        let err = err_of(&mut ix, method, params);
        assert!(err.contains("at least 1"), "{method}: {err}");
    }
}

#[test]
fn unknown_enum_values_error_in_other_methods() {
    let (_t, mut ix) = build();
    for (method, params, bad) in [
        (
            "explain_symbol",
            json!({"qualname": "src.app.other", "format": "bogus"}),
            "bogus",
        ),
        (
            "trace_flow",
            json!({"start_qualname": "src.app.other", "format": "bogus"}),
            "bogus",
        ),
        (
            "gather_context",
            json!({"seeds": [{"type": "symbol", "qualname": "src.app.other"}], "strategy": "bogus"}),
            "bogus",
        ),
    ] {
        let err = err_of(&mut ix, method, params);
        assert!(
            err.contains(bad) && err.contains("valid"),
            "{method}: {err}"
        );
    }
}
