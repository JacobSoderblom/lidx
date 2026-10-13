//! Issues #478, #480 and #481: a call through a function value is a gap and
//! never a genuine leaf, the depth ceiling says what it cut, and a call through
//! an interface costs one depth level.

mod common;

use lidx::rpc;
use serde_json::Value;

const GO_MOD: &str = "module example.com/m\n\ngo 1.22\n";

/// `Run -> runFlagActions -> (ActionableFlag.RunAction) -> Uint64Flag.RunAction
/// -> Context.Uint64`: three calls, one of them through an interface with two
/// implementations.
const FLAGS: &str = "package app

type Context struct{}

func (c *Context) Uint64(name string) uint64 { return 0 }

type ActionableFlag interface {
	RunAction(c *Context) error
}

type Uint64Flag struct{}

func (f *Uint64Flag) RunAction(c *Context) error {
	c.Uint64(\"n\")
	return nil
}

type BoolFlag struct{}

func (f *BoolFlag) RunAction(c *Context) error { return nil }

func runFlagActions(c *Context, af ActionableFlag) error {
	return af.RunAction(c)
}

func Run(c *Context, af ActionableFlag) error {
	return runFlagActions(c, af)
}
";

const VALUES: &str = "package app

func apply(fn func(int), v int) {
	fn(v)
}

func chain(ms []func(int), v int) {
	ms[0](v)
}

func leaf() {}

func Top() {
	apply(nil, 1)
	chain(nil, 2)
	leaf()
}
";

fn call(root: &std::path::Path, method: &str, params: &str) -> Value {
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let response = rpc::call(root.to_path_buf(), db_path, method.to_string(), params, "1").unwrap();
    let value: Value = serde_json::from_str(&response).unwrap();
    value["result"].clone()
}

fn qualnames(result: &Value, key: &str) -> Vec<String> {
    result[key]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|e| {
                    e["symbol"]["qualname"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string()
                })
                .collect()
        })
        .unwrap_or_default()
}

fn distance_of(result: &Value, qualname: &str) -> Option<u64> {
    result["affected"]
        .as_array()?
        .iter()
        .find(|e| e["symbol"]["qualname"] == qualname)
        .and_then(|e| e["distance"].as_u64())
}

fn flags_repo() -> (tempfile::TempDir, std::path::PathBuf) {
    let (tmp, root, _db) = common::index_repo(
        "lidx-depth-flags-",
        &[("go.mod", GO_MOD), ("app.go", FLAGS)],
    );
    (tmp, root)
}

#[test]
fn a_call_through_an_interface_costs_one_depth_level_upstream() {
    let (_tmp, root) = flags_repo();
    let result = call(
        &root,
        "analyze_impact",
        r#"{"qualname":"app.Context.Uint64","direction":"upstream","max_depth":3,
            "kinds":["CALLS","DISPATCH"],"enable_test":false,"enable_historical":false}"#,
    );
    let found = qualnames(&result, "affected");
    for q in [
        "app.Uint64Flag.RunAction",
        "app.ActionableFlag.RunAction",
        "app.runFlagActions",
        "app.Run",
    ] {
        assert!(found.iter().any(|f| f == q), "{q} missing from {found:?}");
    }
    // The implementation and the interface method are one call away, its
    // caller two, and `Run` three.
    assert_eq!(distance_of(&result, "app.Uint64Flag.RunAction"), Some(1));
    assert_eq!(
        distance_of(&result, "app.ActionableFlag.RunAction"),
        Some(1)
    );
    assert_eq!(distance_of(&result, "app.runFlagActions"), Some(2));
    assert_eq!(distance_of(&result, "app.Run"), Some(3));
    assert!(result.get("depth_limited").is_none(), "{result}");
}

#[test]
fn a_dispatch_hop_at_the_depth_ceiling_is_still_taken() {
    let (_tmp, root) = flags_repo();
    // depth 1: the implementation is one call away, so the interface method
    // it pairs with is reached too, but `runFlagActions` is a second call.
    let result = call(
        &root,
        "analyze_impact",
        r#"{"qualname":"app.Context.Uint64","direction":"upstream","max_depth":1,
            "kinds":["CALLS","DISPATCH"],"enable_test":false,"enable_historical":false}"#,
    );
    let found = qualnames(&result, "affected");
    assert!(
        found.contains(&"app.Uint64Flag.RunAction".to_string()),
        "{found:?}"
    );
    assert!(
        found.contains(&"app.ActionableFlag.RunAction".to_string()),
        "{found:?}"
    );
    assert!(
        !found.contains(&"app.runFlagActions".to_string()),
        "{found:?}"
    );
}

#[test]
fn a_cut_depth_is_reported_with_what_lies_beyond_it() {
    let (_tmp, root) = flags_repo();
    let result = call(
        &root,
        "analyze_impact",
        r#"{"qualname":"app.Context.Uint64","direction":"upstream","max_depth":2,
            "kinds":["CALLS","DISPATCH"],"enable_test":false,"enable_historical":false}"#,
    );
    let found = qualnames(&result, "affected");
    assert!(!found.contains(&"app.Run".to_string()), "{found:?}");
    assert_eq!(result["depth_limited"], true, "{result}");
    assert_eq!(result["beyond_depth"], 1, "{result}");

    // Deep enough to see everything: nothing is hidden, nothing is claimed.
    let result = call(
        &root,
        "analyze_impact",
        r#"{"qualname":"app.Context.Uint64","direction":"upstream","max_depth":5,
            "kinds":["CALLS","DISPATCH"],"enable_test":false,"enable_historical":false}"#,
    );
    assert!(result.get("depth_limited").is_none(), "{result}");
    assert!(result.get("beyond_depth").is_none(), "{result}");
}

#[test]
fn trace_flow_downstream_reaches_through_an_interface_in_three_hops() {
    let (_tmp, root) = flags_repo();
    let result = call(
        &root,
        "trace_flow",
        r#"{"start_qualname":"app.Run","direction":"downstream","max_hops":3}"#,
    );
    let hops = result["trace"].as_array().expect("trace");
    let at = |q: &str| {
        hops.iter()
            .find(|h| h["symbol"]["qualname"] == q)
            .and_then(|h| h["distance"].as_u64())
    };
    assert_eq!(at("app.runFlagActions"), Some(1), "{result}");
    assert_eq!(at("app.ActionableFlag.RunAction"), Some(2), "{result}");
    assert_eq!(at("app.Uint64Flag.RunAction"), Some(2), "{result}");
    assert_eq!(at("app.Context.Uint64"), Some(3), "{result}");
    // Nothing is cut: the walk ends where the graph does.
    assert_eq!(result["truncated"], false, "{result}");
}

#[test]
fn trace_flow_upstream_through_an_interface_counts_the_call_once() {
    let (_tmp, root) = flags_repo();
    let result = call(
        &root,
        "trace_flow",
        r#"{"start_qualname":"app.Context.Uint64","direction":"upstream","max_hops":3}"#,
    );
    let hops = result["trace"].as_array().expect("trace");
    let at = |q: &str| {
        hops.iter()
            .find(|h| h["symbol"]["qualname"] == q)
            .and_then(|h| h["distance"].as_u64())
    };
    assert_eq!(at("app.Uint64Flag.RunAction"), Some(1), "{result}");
    assert_eq!(at("app.ActionableFlag.RunAction"), Some(1), "{result}");
    assert_eq!(at("app.runFlagActions"), Some(2), "{result}");
    assert_eq!(at("app.Run"), Some(3), "{result}");
}

#[test]
fn trace_flow_hop_ceiling_still_reaches_the_implementation_behind_an_interface() {
    let (_tmp, root) = flags_repo();
    // runFlagActions (1) -> ActionableFlag.RunAction (2) -> impls (2).
    let result = call(
        &root,
        "trace_flow",
        r#"{"start_qualname":"app.Run","direction":"downstream","max_hops":2}"#,
    );
    let names: Vec<String> = result["trace"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["symbol"]["qualname"].as_str().unwrap().to_string())
        .collect();
    assert!(
        names.contains(&"app.Uint64Flag.RunAction".to_string()),
        "{names:?}"
    );
    assert!(
        !names.contains(&"app.Context.Uint64".to_string()),
        "{names:?}"
    );
    // The implementation that calls on is a depth-limited leaf, not a "genuine" one.
    let leaf = result["trace"]
        .as_array()
        .unwrap()
        .iter()
        .find(|h| h["symbol"]["qualname"] == "app.Uint64Flag.RunAction")
        .unwrap();
    assert_eq!(leaf["stop"], "depth_limit", "{leaf}");
}

#[test]
fn a_call_of_a_function_value_is_a_gap_not_a_genuine_leaf() {
    let (_tmp, root, _db) =
        common::index_repo("lidx-fnvalue-", &[("go.mod", GO_MOD), ("app.go", VALUES)]);
    let result = call(
        &root,
        "trace_flow",
        r#"{"start_qualname":"app.Top","direction":"downstream","max_hops":3}"#,
    );
    let hops = result["trace"].as_array().expect("trace");
    let stop_of = |q: &str| -> Value {
        hops.iter()
            .find(|h| h["symbol"]["qualname"] == q)
            .unwrap_or_else(|| panic!("{q} not in trace: {result}"))["stop"]
            .clone()
    };
    for q in ["app.apply", "app.chain"] {
        assert_eq!(stop_of(q), "unresolved_calls", "{q}: {result}");
    }
    // A function that calls nothing is still a genuine leaf.
    assert_eq!(stop_of("app.leaf"), "no_callees", "{result}");

    let apply = hops
        .iter()
        .find(|h| h["symbol"]["qualname"] == "app.apply")
        .unwrap();
    let detail = apply["stop_detail"].to_string();
    assert!(detail.contains("fn"), "the callee is named: {detail}");
    assert!(detail.contains("function_value"), "{detail}");
}

#[test]
fn a_function_value_call_makes_an_impact_answer_a_lower_bound() {
    let (_tmp, root, _db) = common::index_repo(
        "lidx-fnvalue-lb-",
        &[("go.mod", GO_MOD), ("app.go", VALUES)],
    );
    let result = call(
        &root,
        "analyze_impact",
        r#"{"qualname":"app.Top","direction":"downstream","enable_test":false,"enable_historical":false}"#,
    );
    assert_eq!(result["lower_bound"]["is_lower_bound"], true, "{result}");
}

#[test]
fn a_function_value_call_is_never_retried_against_a_same_named_symbol() {
    // A declaration named like the called value must not capture the call
    // when a later file adds it.
    let (_tmp, root, db) = common::index_repo(
        "lidx-fnvalue-retry-",
        &[("go.mod", GO_MOD), ("app.go", VALUES)],
    );
    common::write_files(&root, &[("other.go", "package app\n\nfunc fn(v int) {}\n")]);
    let mut indexer = lidx::indexer::Indexer::new(root.clone(), db).unwrap();
    indexer.sync_rel_paths(&["other.go".to_string()]).unwrap();
    drop(indexer);
    let result = call(
        &root,
        "trace_flow",
        r#"{"start_qualname":"app.apply","direction":"downstream","max_hops":3}"#,
    );
    let names = qualnames_of_trace(&result);
    assert!(!names.contains(&"other.fn".to_string()), "{names:?}");
}

fn qualnames_of_trace(result: &Value) -> Vec<String> {
    result["trace"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|h| h["symbol"]["qualname"].as_str().unwrap().to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn chain_source(len: usize) -> String {
    let mut src = String::from("package app\n\nfunc c0() {}\n");
    for i in 1..=len {
        src.push_str(&format!("\nfunc c{i}() {{ c{}() }}\n", i - 1));
    }
    src
}

#[test]
fn without_max_depth_the_default_depth_says_what_it_cut_and_offers_a_deeper_retry() {
    let src = chain_source(7);
    let (_tmp, root, _db) = common::index_repo(
        "lidx-default-chain-",
        &[("go.mod", GO_MOD), ("app.go", &src)],
    );
    let result = call(
        &root,
        "analyze_impact",
        r#"{"qualname":"app.c0","direction":"upstream","enable_test":false,"enable_historical":false}"#,
    );
    assert_eq!(qualnames(&result, "affected").len(), 3, "{result}");
    assert_eq!(result["config"]["max_depth"], 3, "{result}");
    assert_eq!(result["depth_limited"], true, "{result}");
    assert_eq!(result["beyond_depth"], 1, "{result}");
    let hops = result["next_hops"].to_string();
    assert!(
        hops.contains("\"max_depth\":6"),
        "a deeper retry is offered: {hops}"
    );

    // Deep enough to see everything: nothing is hidden, nothing is claimed.
    let result = call(
        &root,
        "analyze_impact",
        r#"{"qualname":"app.c0","direction":"upstream","max_depth":9,"enable_test":false,"enable_historical":false}"#,
    );
    assert_eq!(qualnames(&result, "affected").len(), 7, "{result}");
    assert!(result.get("depth_limited").is_none(), "{result}");
    assert!(result.get("beyond_depth").is_none(), "{result}");
}

#[test]
fn a_large_closure_cut_by_the_depth_counts_what_lies_beyond_it() {
    // `leaf` has 120 callers, each with a caller of its own, and so on.
    let mut src = String::from("package app\n\nfunc leaf() {}\n");
    for i in 0..120 {
        src.push_str(&format!("\nfunc h{i}() {{ leaf() }}\nfunc g{i}() {{ h{i}() }}\nfunc f{i}() {{ g{i}() }}\nfunc e{i}() {{ f{i}() }}\n"));
    }
    let (_tmp, root, _db) = common::index_repo(
        "lidx-adaptive-hub-",
        &[("go.mod", GO_MOD), ("app.go", &src)],
    );
    let result = call(
        &root,
        "analyze_impact",
        r#"{"qualname":"app.leaf","direction":"upstream","limit":2000,"max_response_bytes":10000000,
            "enable_test":false,"enable_historical":false}"#,
    );
    assert_eq!(result["config"]["max_depth"], 3, "{result}");
    assert_eq!(result["depth_limited"], true, "{result}");
    assert_eq!(result["beyond_depth"], 120, "{result}");
    let found = qualnames(&result, "affected");
    assert!(
        found.iter().any(|q| q == "app.f0"),
        "the three floor levels are walked"
    );
    assert!(
        !found.iter().any(|q| q == "app.e0"),
        "nothing past the budget level"
    );
    let hops = result["next_hops"].to_string();
    assert!(
        hops.contains("max_depth"),
        "a deeper retry is offered: {hops}"
    );
}
