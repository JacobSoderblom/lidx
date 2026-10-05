//! Regression test for issue #121's fix (PR #137): the BFS dequeue guard was
//! changed from `dist > max_hops` to `dist >= max_hops` so no returned hop
//! ever exceeds `max_hops`, but that change also dropped `truncated = true`
//! at the depth-limit break site. `handle_trace_flow` only emits the
//! "Continue trace" and "narrow by kind" `next_hops` when `truncated` is
//! true (see `src/rpc/handlers.rs`), so a trace that stopped purely because
//! it hit `max_hops` -- with more graph genuinely beyond it -- silently lost
//! its continuation hint.
//!
//! These checks pin both directions of that fix: a depth cutoff that really
//! does discard reachable graph must report `truncated: true` plus the
//! continuation hops, and a depth cutoff that lands exactly on the end of
//! the reachable graph (nothing left to explore) must not be reported as
//! truncated at all.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;

const TARGET_SOURCE: &str = "def target():\n    return 1\n";
const MIDDLE_SOURCE: &str = "from target import target\n\n\ndef middle():\n    return target()\n";
const WRAPPER_SOURCE: &str = "from middle import middle\n\n\ndef wrapper():\n    return middle()\n";

fn call(repo_root: std::path::PathBuf, db_path: std::path::PathBuf, params: &str) -> Value {
    let raw = rpc::call(repo_root, db_path, "trace_flow".to_string(), params, "1").unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "trace_flow returned an error: {envelope}"
    );
    envelope["result"].clone()
}

fn setup() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-trace-flow-depth-truncation-")
        .tempdir()
        .unwrap();
    common::write_files(
        tmp.path(),
        &[
            ("target.py", TARGET_SOURCE),
            ("middle.py", MIDDLE_SOURCE),
            ("wrapper.py", WRAPPER_SOURCE),
        ],
    );
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);
    (tmp, repo_root, db_path)
}

/// The real chain is `wrapper -> middle -> target` (2 hops). With
/// `max_hops: 1`, the BFS stops at `middle` -- which itself still calls
/// `target`, real graph the depth cutoff discarded. This must be reported
/// as `truncated: true`, and `next_hops` must offer the "continue trace"
/// and "narrow by kind" suggestions so the caller isn't left without a way
/// forward.
#[test]
fn depth_limited_trace_reports_truncated_and_offers_continuation() {
    let (_tmp, repo_root, db_path) = setup();

    let result = call(
        repo_root,
        db_path,
        r#"{"start_qualname":"wrapper.wrapper","direction":"downstream","max_hops":1}"#,
    );

    let trace = result["trace"].as_array().expect("trace array");
    assert_eq!(
        trace.len(),
        1,
        "max_hops=1 should return exactly the first hop: {result:?}"
    );
    for hop in trace {
        let distance = hop["distance"].as_u64().expect("hop distance");
        assert!(
            distance <= 1,
            "no hop should exceed max_hops=1, got distance={distance}: {hop:?}"
        );
    }

    assert_eq!(
        result["truncated"],
        Value::Bool(true),
        "a depth cutoff that stops short of real, further graph must report truncated: true, got: {result:?}"
    );

    let next_hops = result["next_hops"].as_array().expect("next_hops array");
    let methods_and_descriptions: Vec<(&str, &str)> = next_hops
        .iter()
        .map(|h| {
            (
                h["method"].as_str().unwrap_or(""),
                h["description"].as_str().unwrap_or(""),
            )
        })
        .collect();

    assert!(
        methods_and_descriptions
            .iter()
            .any(|(m, d)| *m == "trace_flow" && d.starts_with("Re-trace deeper")),
        "truncated depth-limited trace must offer a deeper re-trace next_hop, got: {methods_and_descriptions:?}"
    );
    assert!(
        !methods_and_descriptions
            .iter()
            .any(|(_, d)| d.starts_with("Continue trace")),
        "a depth-limited trace must not offer an offset continuation (#354), got: {methods_and_descriptions:?}"
    );
    assert!(
        methods_and_descriptions
            .iter()
            .any(|(m, d)| *m == "trace_flow" && d.contains("CONFIG edges")),
        "truncated depth-limited trace must offer the narrow-by-kind next_hop, got: {methods_and_descriptions:?}"
    );
}

/// With `max_hops: 2`, the BFS reaches `target`, which has no further
/// outgoing edges of its own -- the reachable graph ends exactly at the
/// ceiling. This is a complete trace, not a truncated one: reporting
/// `truncated: true` here would be a false positive from treating "hit the
/// ceiling" as "discarded real work" without checking whether there was any
/// work left to discard.
#[test]
fn depth_that_exactly_covers_the_reachable_graph_is_not_truncated() {
    let (_tmp, repo_root, db_path) = setup();

    let result = call(
        repo_root,
        db_path,
        r#"{"start_qualname":"wrapper.wrapper","direction":"downstream","max_hops":2}"#,
    );

    let trace = result["trace"].as_array().expect("trace array");
    assert_eq!(
        trace.len(),
        2,
        "max_hops=2 should reach the full 2-hop chain: {result:?}"
    );

    assert_eq!(
        result["truncated"],
        Value::Bool(false),
        "a trace that exhausts the whole reachable graph exactly at max_hops must not be reported as truncated, got: {result:?}"
    );
}

fn setup_files(
    files: &[(&str, &str)],
) -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-trace-flow-upstream-truncation-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);
    (tmp, repo_root, db_path)
}

const LEAF_SOURCE: &str = "def leaf():\n    return 1\n";
// `mid` has outgoing edges with no resolved target: a call to an undefined
// function and an env read (a NULL-target CONFIG_READ edge).
const MID_SOURCE: &str = "import os\nfrom leaf import leaf\n\n\ndef mid():\n    os.environ[\"SOME_VAR\"]\n    undefined_thing()\n    return leaf()\n";
const TOP_SOURCE: &str = "from mid import mid\n\n\ndef top():\n    return mid()\n";

/// Upstream, `mid` is the last node within `max_hops: 1`. Its only other
/// edges are outgoing and unresolved, which are not callers: the trace
/// must not claim more graph lies beyond it.
#[test]
fn upstream_depth_cutoff_ignores_unresolved_outgoing_edges() {
    let (_tmp, repo_root, db_path) =
        setup_files(&[("leaf.py", LEAF_SOURCE), ("mid.py", MID_SOURCE)]);
    let result = call(
        repo_root,
        db_path,
        r#"{"start_qualname":"leaf.leaf","direction":"upstream","max_hops":1}"#,
    );
    let trace = result["trace"].as_array().expect("trace array");
    assert_eq!(trace.len(), 1, "{result:?}");
    assert_eq!(
        result["truncated"],
        Value::Bool(false),
        "unresolved outgoing edges are not further callers: {result:?}"
    );
}

/// Same shape, but `mid` has a real caller beyond the cutoff.
#[test]
fn upstream_depth_cutoff_with_a_real_caller_is_truncated() {
    let (_tmp, repo_root, db_path) = setup_files(&[
        ("leaf.py", LEAF_SOURCE),
        ("mid.py", MID_SOURCE),
        ("top.py", TOP_SOURCE),
    ]);
    let result = call(
        repo_root,
        db_path,
        r#"{"start_qualname":"leaf.leaf","direction":"upstream","max_hops":1}"#,
    );
    let trace = result["trace"].as_array().expect("trace array");
    assert_eq!(trace.len(), 1, "{result:?}");
    assert_eq!(result["truncated"], Value::Bool(true), "{result:?}");
}
