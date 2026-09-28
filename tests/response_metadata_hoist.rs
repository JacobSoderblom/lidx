//! Issue #66 finding: `explain_symbol` hoists `graph_version`/`commit_sha`
//! to the response envelope and strips the redundant copies off every
//! nested `Symbol` (see `tests/explain_symbol_payload_trim.rs`), but that
//! fix was hard-coded to `explain_symbol` alone. `trace_flow`'s `start` and
//! every `trace[].symbol`, and `analyze_diff`'s `changed_symbols[].symbol`
//! and `downstream[].symbol`, still repeat both fields on every entry even
//! though they're constant for the whole response. These seam-A checks pin
//! the same hoist for both methods through the shared dispatch-boundary
//! mechanism (`rpc::handle_method`) rather than a second hard-coded strip.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;
use std::path::PathBuf;

fn call(repo_root: PathBuf, db_path: PathBuf, method: &str, params: &str) -> Value {
    let raw = rpc::call(repo_root, db_path, method.to_string(), params, "1").unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "{method} returned an error: {envelope}"
    );
    let result = envelope["result"].clone();
    if result.get("truncated").is_some() && result.get("data").is_some() {
        return result["data"].clone();
    }
    result
}

const TARGET_SOURCE: &str = "def target():\n    return 1\n";
const CALLER_SOURCE: &str = "from target import target\n\n\ndef wrapper():\n    return target()\n";

#[test]
fn trace_flow_hoists_graph_version_once_and_strips_nested_symbols() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-trace-flow-hoist-")
        .tempdir()
        .unwrap();
    common::write_files(
        tmp.path(),
        &[("target.py", TARGET_SOURCE), ("caller.py", CALLER_SOURCE)],
    );
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    let result = call(
        repo_root,
        db_path,
        "trace_flow",
        r#"{"start_qualname":"caller.wrapper","direction":"downstream"}"#,
    );

    let envelope_graph_version = result.get("graph_version").unwrap_or_else(|| {
        panic!("trace_flow must carry graph_version at the envelope: {result:?}")
    });
    assert!(
        envelope_graph_version.as_i64().is_some_and(|v| v > 0),
        "graph_version must be a positive integer: {result:?}"
    );

    assert!(
        result["start"].get("graph_version").is_none(),
        "start symbol must not repeat graph_version now that it's hoisted: {:?}",
        result["start"]
    );

    let trace = result["trace"].as_array().expect("trace array");
    assert!(!trace.is_empty(), "expected at least one hop: {result:?}");
    for hop in trace {
        assert!(
            hop["symbol"].get("graph_version").is_none(),
            "a hop's nested symbol must not repeat graph_version: {hop:?}"
        );
    }
}

#[test]
fn analyze_diff_hoists_graph_version_once_and_strips_nested_symbols() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-analyze-diff-hoist-")
        .tempdir()
        .unwrap();
    common::write_files(
        tmp.path(),
        &[("target.py", TARGET_SOURCE), ("caller.py", CALLER_SOURCE)],
    );
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    let result = call(
        repo_root,
        db_path,
        "analyze_diff",
        r#"{"paths":["target.py"]}"#,
    );

    let envelope_graph_version = result.get("graph_version").unwrap_or_else(|| {
        panic!("analyze_diff must carry graph_version at the envelope: {result:?}")
    });
    assert!(
        envelope_graph_version.as_i64().is_some_and(|v| v > 0),
        "graph_version must be a positive integer: {result:?}"
    );

    let changed_symbols = result["changed_symbols"]
        .as_array()
        .expect("changed_symbols array");
    assert!(!changed_symbols.is_empty(), "{result:?}");
    for cs in changed_symbols {
        assert!(
            cs["symbol"].get("graph_version").is_none(),
            "a changed symbol must not repeat graph_version: {cs:?}"
        );
    }

    let downstream = result["downstream"].as_array().expect("downstream array");
    assert!(
        !downstream.is_empty(),
        "expected caller.wrapper to show up downstream of target.target: {result:?}"
    );
    for d in downstream {
        assert!(
            d["symbol"].get("graph_version").is_none(),
            "a downstream entry must not repeat graph_version: {d:?}"
        );
    }
}
