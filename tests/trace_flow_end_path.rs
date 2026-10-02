//! Issue #226: `trace_flow` with an end target returns the path to it (with
//! per-hop predecessors) instead of the whole visited BFS frontier.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;
use std::path::PathBuf;

/// The issue's fixture: `start` calls helper1, helper2 and `mid`; `mid`
/// calls `target` and `other_leaf`.
const ISSUE_FLOW: &str = "def helper1():\n    return 1\n\n\ndef helper2():\n    return 2\n\n\ndef other_leaf():\n    return 3\n\n\ndef target():\n    return 4\n\n\ndef mid():\n    other_leaf()\n    return target()\n\n\ndef start():\n    helper1()\n    helper2()\n    return mid()\n";

/// Two distinct routes from `start` to `target` (via `alt` and via `mid`).
const TWO_ROUTE_FLOW: &str = "def helper1():\n    return 1\n\n\ndef target():\n    return 4\n\n\ndef mid():\n    return target()\n\n\ndef alt():\n    return target()\n\n\ndef start():\n    helper1()\n    alt()\n    return mid()\n";

struct Env {
    _tmp: tempfile::TempDir,
    repo_root: PathBuf,
    db_path: PathBuf,
}

fn setup(files: &[(&str, &str)]) -> Env {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-trace-flow-end-path-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);
    Env {
        _tmp: tmp,
        repo_root,
        db_path,
    }
}

fn call(env: &Env, params: &str) -> Value {
    let raw = rpc::call(
        env.repo_root.clone(),
        env.db_path.clone(),
        "trace_flow".to_string(),
        params,
        "1",
    )
    .unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "{envelope}"
    );
    let result = envelope["result"].clone();
    if result.get("truncated").is_some() && result.get("data").is_some() {
        return result["data"].clone();
    }
    result
}

fn qualnames(r: &Value) -> Vec<String> {
    r["trace"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["symbol"]["qualname"].as_str().unwrap().to_string())
        .collect()
}

/// Follows `predecessor` from the hop for `end` until it leaves the trace;
/// returns the qualnames visited, end first.
fn chain_from(r: &Value, end: &str) -> Vec<String> {
    let trace = r["trace"].as_array().unwrap();
    let mut cur = trace
        .iter()
        .find(|h| h["symbol"]["qualname"] == end)
        .unwrap();
    let mut chain = vec![end.to_string()];
    loop {
        let pred = cur["predecessor"].as_str().expect("every hop has one");
        chain.push(pred.to_string());
        match trace.iter().find(|h| h["symbol"]["qualname"] == pred) {
            Some(h) => cur = h,
            None => return chain,
        }
    }
}

#[test]
fn issue_fixture_trace_is_exactly_start_mid_target() {
    let env = setup(&[("flow.py", ISSUE_FLOW)]);
    let r = call(
        &env,
        r#"{"start_qualname":"flow.start","end_qualname":"flow.target","max_hops":3}"#,
    );
    assert_eq!(r["reached_target"], true, "{r}");
    assert_eq!(r["paths_found"], 1, "{r}");
    // `start` is the seed, not a hop; the hops in order are mid, target.
    assert_eq!(qualnames(&r), ["flow.mid", "flow.target"], "{r}");
    assert_eq!(
        chain_from(&r, "flow.target"),
        ["flow.target", "flow.mid", "flow.start"],
        "{r}"
    );
}

#[test]
fn two_routes_return_one_shortest_path_and_count_it() {
    let env = setup(&[("flow.py", TWO_ROUTE_FLOW)]);
    let r = call(
        &env,
        r#"{"start_qualname":"flow.start","end_qualname":"flow.target","max_hops":3}"#,
    );
    assert_eq!(r["reached_target"], true, "{r}");
    // Only one route is returned, and paths_found says exactly that.
    let chain = chain_from(&r, "flow.target");
    assert_eq!(chain.len(), 3, "{chain:?}");
    assert_eq!(chain.last().unwrap(), "flow.start");
    assert_eq!(r["trace"].as_array().unwrap().len(), 2, "{r}");
    assert_eq!(r["paths_found"], 1, "{r}");
    assert!(!qualnames(&r).contains(&"flow.helper1".to_string()), "{r}");
}

#[test]
fn unreachable_or_too_far_end_target_yields_empty_unreached_trace() {
    let env = setup(&[("flow.py", ISSUE_FLOW)]);
    let r = call(
        &env,
        r#"{"start_qualname":"flow.helper1","end_qualname":"flow.target","max_hops":3}"#,
    );
    assert_eq!(r["reached_target"], false, "{r}");
    assert_eq!(r["paths_found"], 0, "{r}");
    assert!(r["trace"].as_array().unwrap().is_empty(), "{r}");

    // target is 2 hops away; max_hops 1 cannot reach it.
    let r = call(
        &env,
        r#"{"start_qualname":"flow.start","end_qualname":"flow.target","max_hops":1}"#,
    );
    assert_eq!(r["reached_target"], false, "{r}");
    assert_eq!(r["paths_found"], 0, "{r}");
    assert!(r["trace"].as_array().unwrap().is_empty(), "{r}");
    // A missed end-target trace has no hops, so no pointless continuation.
    let hops = r["next_hops"].as_array().cloned().unwrap_or_default();
    assert!(
        !hops.iter().any(|h| h["description"]
            .as_str()
            .unwrap_or("")
            .starts_with("Continue trace")),
        "{r}"
    );
}

#[test]
fn unresolved_end_qualname_is_an_explicit_signal_not_a_frontier() {
    let env = setup(&[("flow.py", ISSUE_FLOW)]);
    let r = call(
        &env,
        r#"{"start_qualname":"flow.start","end_qualname":"flow.no_such_symbol","max_hops":3}"#,
    );
    assert_eq!(r["end_resolved"], false, "{r}");
    assert_eq!(r["resolved"], false, "{r}");
    assert!(r.get("trace").is_none(), "{r}");
}

#[test]
fn path_crosses_a_channel_bridge_with_predecessors() {
    let env = setup(&[
        (
            "pub.py",
            "def send(msg):\n    _bus.publish(\"order-created\", msg)\n\n\ndef entry(msg):\n    send(msg)\n",
        ),
        (
            "sub.py",
            "@router.subscribe(topic=\"order-created\")\ndef handle(msg):\n    pass\n",
        ),
    ]);
    let r = call(
        &env,
        r#"{"start_qualname":"pub.entry","end_qualname":"sub.handle","max_hops":3}"#,
    );
    assert_eq!(r["reached_target"], true, "{r}");
    assert_eq!(r["paths_found"], 1, "{r}");
    assert_eq!(qualnames(&r), ["pub.send", "sub.handle"], "{r}");
    assert_eq!(
        chain_from(&r, "sub.handle"),
        ["sub.handle", "pub.send", "pub.entry"],
        "{r}"
    );
    let bridged = &r["trace"][1];
    assert_eq!(bridged["cross_language"], true, "{r}");
    assert_eq!(bridged["boundary_type"], "message_bus", "{r}");
}

/// Strips the field this issue added, the byte count it inflates, and
/// run-dependent row ids.
fn scrub(v: &mut Value) {
    match v {
        Value::Object(m) => {
            m.remove("predecessor");
            // Sized from the serialized hops, which now carry predecessors.
            m.remove("used_bytes");
            // Db row ids depend on parallel-indexing order.
            m.remove("id");
            for x in m.values_mut() {
                scrub(x);
            }
        }
        Value::Array(a) => a.iter_mut().for_each(scrub),
        _ => {}
    }
}

/// A no-end-target trace is unchanged: compared with a snapshot captured
/// from the code before issue #226 (minus the new `predecessor` field;
/// regenerate with `UPDATE_GOLDEN=1` only when the response changes on
/// purpose).
#[test]
fn no_end_target_frontier_matches_pre_change_golden() {
    let env = setup(&[("flow.py", ISSUE_FLOW)]);
    let mut r = call(&env, r#"{"start_qualname":"flow.start","max_hops":3}"#);
    scrub(&mut r);
    let actual = serde_json::to_string_pretty(&r).unwrap() + "\n";
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("snapshots")
        .join("trace_flow_no_end_target.json");
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, &actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path).expect("golden snapshot missing");
    assert_eq!(actual, expected, "no-end-target trace differs from golden");
}

#[test]
fn no_end_target_returns_frontier_with_predecessors() {
    let env = setup(&[("flow.py", ISSUE_FLOW)]);
    let r = call(&env, r#"{"start_qualname":"flow.start","max_hops":3}"#);
    let names = qualnames(&r);
    for n in [
        "flow.helper1",
        "flow.helper2",
        "flow.mid",
        "flow.other_leaf",
        "flow.target",
    ] {
        assert!(names.contains(&n.to_string()), "{names:?}");
    }
    for h in r["trace"].as_array().unwrap() {
        assert!(h["predecessor"].is_string(), "{h}");
    }
    assert_eq!(
        chain_from(&r, "flow.other_leaf"),
        ["flow.other_leaf", "flow.mid", "flow.start"]
    );
}
