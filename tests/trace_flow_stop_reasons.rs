//! Issue #410: every leaf hop of a trace says why the trace ends there, so an
//! unresolved call is not mistaken for "execution ends here".
mod common;

use lidx::rpc;
use serde_json::{Value, json};

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

/// The hop for `qualname`.
fn hop<'a>(result: &'a Value, qualname: &str) -> &'a Value {
    result["trace"]
        .as_array()
        .unwrap()
        .iter()
        .find(|h| h["symbol"]["qualname"] == json!(qualname))
        .unwrap_or_else(|| panic!("no hop {qualname} in {result}"))
}

fn stop<'a>(result: &'a Value, qualname: &str) -> &'a str {
    hop(result, qualname)["stop"]
        .as_str()
        .unwrap_or_else(|| panic!("hop {qualname} has no stop: {result}"))
}

const CHAIN: &str = "def a():\n    b()\n\ndef b():\n    c()\n\ndef c():\n    d()\n\ndef d():\n    e()\n\ndef e():\n    pass\n";

// ---- downstream ------------------------------------------------------

#[test]
fn downstream_no_callees_vs_unresolved_calls() {
    let src = "def start():\n    clean()\n    broken()\n\ndef clean():\n    pass\n\ndef broken():\n    undefined_thing()\n";
    let (_t, repo, db) = common::index_repo("lidx-stop-calls-", &[("a.py", src)]);
    let r = trace(&repo, &db, &json!({"start_qualname": "a.start"}));
    assert_eq!(stop(&r, "a.clean"), "no_callees", "{r}");
    assert!(hop(&r, "a.clean").get("stop_detail").is_none(), "{r}");
    assert_eq!(stop(&r, "a.broken"), "unresolved_calls", "{r}");
    let detail = &hop(&r, "a.broken")["stop_detail"];
    assert_eq!(detail["count"], json!(1), "{r}");
    assert_eq!(detail["top"][0][0], json!("a.undefined_thing"), "{r}");
    assert!(detail["top"][0][1].is_string(), "reason missing: {r}");
    assert_eq!(
        r["stop_summary"],
        json!({"no_callees": 1, "unresolved_calls": 1})
    );
}

#[test]
fn unresolved_calls_lists_at_most_three_distinct_names_but_counts_all() {
    let src = "def start():\n    leaf()\n\ndef leaf():\n    n1()\n    n2()\n    n3()\n    n4()\n    n1()\n";
    let (_t, repo, db) = common::index_repo("lidx-stop-top3-", &[("a.py", src)]);
    let r = trace(&repo, &db, &json!({"start_qualname": "a.start"}));
    assert_eq!(stop(&r, "a.leaf"), "unresolved_calls", "{r}");
    let detail = &hop(&r, "a.leaf")["stop_detail"];
    assert!(detail["count"].as_u64().unwrap() >= 4, "{r}");
    assert_eq!(detail["top"].as_array().unwrap().len(), 3, "{r}");
}

#[test]
fn downstream_depth_limit_is_on_the_ceiling_hop_only() {
    let (_t, repo, db) = common::index_repo("lidx-stop-depth-", &[("a.py", CHAIN)]);
    let r = trace(&repo, &db, &json!({"start_qualname": "a.a", "max_hops": 2}));
    assert_eq!(stop(&r, "a.c"), "depth_limit", "{r}");
    assert!(hop(&r, "a.b").get("stop").is_none(), "non-leaf hop: {r}");
    assert_eq!(r["stop_summary"], json!({"depth_limit": 1}));
    // A complete trace has no depth_limit: the last hop is a genuine leaf.
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.a", "max_hops": 10}),
    );
    assert_eq!(stop(&r, "a.e"), "no_callees", "{r}");
}

#[test]
fn downstream_cycle() {
    let src = "def a():\n    b()\n\ndef b():\n    a()\n";
    let (_t, repo, db) = common::index_repo("lidx-stop-cycle-", &[("a.py", src)]);
    let r = trace(&repo, &db, &json!({"start_qualname": "a.a"}));
    assert_eq!(stop(&r, "a.b"), "cycle", "{r}");
}

const FILTER_A: &str = "from b import h\n\ndef g():\n    h()\n\ndef f():\n    g()\n";
const FILTER_B: &str = "def h():\n    pass\n";

#[test]
fn downstream_filtered_reports_what_was_declined() {
    let (_t, repo, db) = common::index_repo(
        "lidx-stop-filtered-",
        &[("a.py", FILTER_A), ("b.py", FILTER_B)],
    );
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.f", "exclude_resolution_kinds": ["import"]}),
    );
    assert_eq!(stop(&r, "a.g"), "filtered", "{r}");
    assert_eq!(hop(&r, "a.g")["stop_detail"], json!({"import": 1}), "{r}");
    // Without the filter the same hop is followed and h is the genuine leaf.
    let r = trace(&repo, &db, &json!({"start_qualname": "a.f"}));
    assert_eq!(stop(&r, "b.h"), "no_callees", "{r}");
}

#[test]
fn downstream_filtered_by_kinds() {
    // Only CALLS is followed, so the XREF out of `g` is declined by `kinds`.
    let (_t, repo, db) = common::index_repo(
        "lidx-stop-kinds-",
        &[("a.py", FILTER_A), ("b.py", FILTER_B)],
    );
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.f", "kinds": ["CALLS"], "exclude_resolution_kinds": ["import"]}),
    );
    assert_eq!(stop(&r, "a.g"), "filtered", "{r}");
}

#[test]
fn downstream_truncated_by_byte_budget() {
    let (_t, repo, db) = common::index_repo("lidx-stop-trunc-", &[("a.py", CHAIN)]);
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.a", "max_bytes": 1}),
    );
    assert_eq!(r["trace"].as_array().unwrap().len(), 1, "{r}");
    assert_eq!(stop(&r, "a.b"), "truncated", "{r}");
    assert_eq!(r["stop_summary"], json!({"truncated": 1}));
}

#[test]
fn downstream_external_stub_leaf() {
    let src = "import requests\n\ndef f():\n    requests.get('x')\n";
    let (_t, repo, db) = common::index_repo("lidx-stop-ext-", &[("a.py", src)]);
    let r = trace(&repo, &db, &json!({"start_qualname": "a.f"}));
    assert_eq!(stop(&r, "ext:requests.get"), "external", "{r}");
}

#[test]
fn empty_trace_reports_why_at_the_start_symbol() {
    let src = "def leaf():\n    pass\n\ndef broken():\n    undefined_thing()\n";
    let (_t, repo, db) = common::index_repo("lidx-stop-start-", &[("a.py", src)]);
    let r = trace(&repo, &db, &json!({"start_qualname": "a.leaf"}));
    assert!(r["trace"].as_array().unwrap().is_empty(), "{r}");
    assert_eq!(r["start_stop"]["stop"], json!("no_callees"), "{r}");
    let r = trace(&repo, &db, &json!({"start_qualname": "a.broken"}));
    assert_eq!(r["start_stop"]["stop"], json!("unresolved_calls"), "{r}");
    assert_eq!(r["start_stop"]["stop_detail"]["count"], json!(1), "{r}");
    // A trace with hops reports per-hop stops, not a start stop.
    let (_t, repo, db) = common::index_repo("lidx-stop-start2-", &[("a.py", CHAIN)]);
    let r = trace(&repo, &db, &json!({"start_qualname": "a.a"}));
    assert!(r.get("start_stop").is_none(), "{r}");
}

#[test]
fn summary_counts_leaves_and_matches_paths_found() {
    let src = "def start():\n    x()\n    y()\n\ndef x():\n    pass\n\ndef y():\n    nowhere()\n";
    let (_t, repo, db) = common::index_repo("lidx-stop-sum-", &[("a.py", src)]);
    let r = trace(&repo, &db, &json!({"start_qualname": "a.start"}));
    let total: u64 = r["stop_summary"]
        .as_object()
        .unwrap()
        .values()
        .map(|v| v.as_u64().unwrap())
        .sum();
    assert_eq!(total, r["paths_found"].as_u64().unwrap(), "{r}");
}

#[test]
fn end_target_trace_has_no_stop_reasons() {
    let (_t, repo, db) = common::index_repo("lidx-stop-end-", &[("a.py", CHAIN)]);
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.a", "end_qualname": "a.c"}),
    );
    assert_eq!(r["reached_target"], json!(true), "{r}");
    for h in r["trace"].as_array().unwrap() {
        assert!(h.get("stop").is_none(), "{r}");
    }
    assert!(r.get("stop_summary").is_none(), "{r}");
}

// ---- upstream --------------------------------------------------------

#[test]
fn upstream_no_callers_vs_unresolved_callers() {
    // `caller(thing)` calls `thing.target()` on an untyped receiver: the call
    // never bound to K.target, so it is a possible caller, not a real one.
    let src = "class K:\n    def target(self):\n        pass\n\ndef caller(thing):\n    thing.target()\n\ndef direct():\n    solo()\n\ndef solo():\n    pass\n";
    let (_t, repo, db) = common::index_repo("lidx-stop-up-unres-", &[("a.py", src)]);
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.solo", "direction": "upstream"}),
    );
    assert_eq!(stop(&r, "a.direct"), "no_callers", "{r}");
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.K.target", "direction": "upstream"}),
    );
    assert_eq!(r["start_stop"]["stop"], json!("unresolved_callers"), "{r}");
    assert_eq!(
        r["start_stop"]["stop_detail"]["top"][0][0],
        json!("a.caller")
    );
}

#[test]
fn upstream_unresolved_callers_on_a_leaf_hop() {
    let src = "class K:\n    def target(self):\n        pass\n\ndef direct():\n    helper()\n\ndef helper(thing):\n    thing.direct()\n";
    let (_t, repo, db) = common::index_repo("lidx-stop-up-unres2-", &[("a.py", src)]);
    // `thing.direct()` is an untyped receiver and `direct` a module
    // function, so it never binds: direct's caller gap, named by qualname.
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.helper", "direction": "upstream"}),
    );
    assert_eq!(stop(&r, "a.direct"), "unresolved_callers", "{r}");
    let detail = &hop(&r, "a.direct")["stop_detail"];
    assert_eq!(detail["count"], json!(1), "{r}");
    assert_eq!(detail["top"][0][0], json!("a.helper"), "{r}");
}

#[test]
fn upstream_depth_limit() {
    let (_t, repo, db) = common::index_repo("lidx-stop-up-depth-", &[("a.py", CHAIN)]);
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.e", "direction": "upstream", "max_hops": 2}),
    );
    assert_eq!(stop(&r, "a.c"), "depth_limit", "{r}");
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.e", "direction": "upstream", "max_hops": 10}),
    );
    assert_eq!(stop(&r, "a.a"), "no_callers", "{r}");
}

#[test]
fn upstream_cycle() {
    let src = "def a():\n    b()\n\ndef b():\n    a()\n";
    let (_t, repo, db) = common::index_repo("lidx-stop-up-cycle-", &[("a.py", src)]);
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.a", "direction": "upstream"}),
    );
    assert_eq!(stop(&r, "a.b"), "cycle", "{r}");
}

#[test]
fn upstream_filtered() {
    // run -> f is an import-resolved edge; excluding it declines run's call.
    let a = "def g():\n    pass\n\ndef f():\n    g()\n";
    let m = "from a import f\n\ndef run():\n    f()\n";
    let (_t, repo, db) = common::index_repo("lidx-stop-up-filt-", &[("a.py", a), ("m.py", m)]);
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.g", "direction": "upstream", "exclude_resolution_kinds": ["import"]}),
    );
    assert_eq!(stop(&r, "a.f"), "filtered", "{r}");
    assert_eq!(hop(&r, "a.f")["stop_detail"], json!({"import": 1}), "{r}");
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.g", "direction": "upstream"}),
    );
    assert_eq!(stop(&r, "m.run"), "no_callers", "{r}");
}

#[test]
fn upstream_truncated_by_byte_budget() {
    let (_t, repo, db) = common::index_repo("lidx-stop-up-trunc-", &[("a.py", CHAIN)]);
    let r = trace(
        &repo,
        &db,
        &json!({"start_qualname": "a.e", "direction": "upstream", "max_bytes": 1}),
    );
    assert_eq!(r["trace"].as_array().unwrap().len(), 1, "{r}");
    assert_eq!(stop(&r, "a.d"), "truncated", "{r}");
}
