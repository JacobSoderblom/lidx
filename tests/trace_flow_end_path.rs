//! Issue #226: `trace_flow` with an end target returns the path to it (with
//! per-hop predecessors) instead of the whole visited BFS frontier.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;

const FLOW: &str = "def helper1():\n    return 1\n\n\ndef helper2():\n    return 2\n\n\ndef other_leaf():\n    return 3\n\n\ndef target():\n    return 4\n\n\ndef mid():\n    other_leaf()\n    return target()\n\n\ndef alt():\n    return target()\n\n\ndef start():\n    helper1()\n    helper2()\n    alt()\n    return mid()\n";

fn setup() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-trace-flow-end-path-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), &[("flow.py", FLOW)]);
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);
    (tmp, repo_root, db_path)
}

fn call(env: &(tempfile::TempDir, std::path::PathBuf, std::path::PathBuf), params: &str) -> Value {
    let raw = rpc::call(
        env.1.clone(),
        env.2.clone(),
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

#[test]
fn end_target_returns_only_the_path_with_a_followable_predecessor_chain() {
    let env = setup();
    let r = call(
        &env,
        r#"{"start_qualname":"flow.start","end_qualname":"flow.target","max_hops":3}"#,
    );
    assert_eq!(r["reached_target"], true, "{r}");
    // Two distinct routes exist (via alt and via mid); one shortest path is
    // returned and counted honestly.
    assert_eq!(r["paths_found"], 1, "{r}");
    let trace = r["trace"].as_array().unwrap();
    assert_eq!(trace.len(), 2, "{r}");
    for noise in ["flow.helper1", "flow.helper2", "flow.other_leaf"] {
        assert!(!qualnames(&r).contains(&noise.to_string()), "{r}");
    }

    // Follow predecessors from the end hop back to the start.
    let mut cur = trace
        .iter()
        .find(|h| h["symbol"]["qualname"] == "flow.target")
        .unwrap();
    let mut chain = vec!["flow.target".to_string()];
    loop {
        let pred = cur["predecessor"].as_str().expect("every hop has one");
        chain.push(pred.to_string());
        match trace.iter().find(|h| h["symbol"]["qualname"] == pred) {
            Some(h) => cur = h,
            None => break,
        }
    }
    assert_eq!(chain.last().unwrap(), "flow.start", "{chain:?}");
    assert_eq!(chain.len(), 3, "{chain:?}");
}

#[test]
fn unreachable_or_too_far_end_target_yields_empty_unreached_trace() {
    let env = setup();
    let r = call(
        &env,
        r#"{"start_qualname":"flow.helper1","end_qualname":"flow.target","max_hops":3}"#,
    );
    assert_eq!(r["reached_target"], false, "{r}");
    assert_eq!(r["paths_found"], 0, "{r}");
    assert!(r["trace"].as_array().unwrap().is_empty(), "{r}");

    // target is 2 hops away via mid/alt; max_hops 1 cannot reach it.
    let r = call(
        &env,
        r#"{"start_qualname":"flow.start","end_qualname":"flow.target","max_hops":1}"#,
    );
    assert_eq!(r["reached_target"], false, "{r}");
    assert_eq!(r["paths_found"], 0, "{r}");
    assert!(r["trace"].as_array().unwrap().is_empty(), "{r}");
}

#[test]
fn no_end_target_returns_frontier_with_predecessors() {
    let env = setup();
    let r = call(&env, r#"{"start_qualname":"flow.start","max_hops":3}"#);
    let names = qualnames(&r);
    for n in [
        "flow.helper1",
        "flow.helper2",
        "flow.mid",
        "flow.alt",
        "flow.other_leaf",
        "flow.target",
    ] {
        assert!(names.contains(&n.to_string()), "{names:?}");
    }
    for h in r["trace"].as_array().unwrap() {
        assert!(h["predecessor"].is_string(), "{h}");
    }
    let leaf = r["trace"]
        .as_array()
        .unwrap()
        .iter()
        .find(|h| h["symbol"]["qualname"] == "flow.other_leaf")
        .unwrap();
    assert_eq!(leaf["predecessor"], "flow.mid");
}
