//! Issue #62: surface `resolution_kind` on every edge.
//!
//! Four seam-A checks, one per response shape that carries a
//! `resolution_kind`-bearing edge:
//!
//! - `dead_symbols`'s `unused_imports` list is the one live RPC response
//!   that serializes raw `Edge` objects directly (`Db::unused_imports`,
//!   which unions resolved `edges` rows with pending, never-bound
//!   `unresolved_references` rows -- see `src/rpc/handlers.rs`'s
//!   `handle_dead_symbols`). A same-repo unused import resolved by exact
//!   qualname match must report its resolution tier, and an unused import
//!   that was never bound (an external package the write path could not
//!   attribute) must omit the field entirely rather than reporting `null`.
//! - `explain_symbol`'s `callers`/`callees` entries (`ExplainRef`) must
//!   carry the tier of the edge each ref was built from -- checked against
//!   `tests/fixtures/golden/python`, which documents (in its
//!   `expected_edges.txt`) exactly which qualname binds at which tier.
//! - `trace_flow`'s hops (`TraceHop`) must carry the tier of the edge each
//!   hop traversed, including the case where a String-Targeted Edge Kind's
//!   own target was never bound to a symbol at all (a Bridge Edge kind's
//!   cross-process join key, or -- as exercised below -- a CONFIG_SOURCE
//!   kind's config key/secret URI): its query-time complement lookup
//!   matches that target text exactly, not through the name tiers, so the
//!   hop must omit the field rather than report a tier that was never
//!   computed.
//! - `analyze_impact`'s `affected[].path.steps` (`PathStep`, reconstructed
//!   from the direct layer's BFS `parent_map`) must carry the tier of the
//!   edge each step traversed, across two different tiers in the same
//!   fixture as the `explain_symbol` check above: `exact` downstream from
//!   `caller.entry` to `caller.local_util`, and `import` upstream from
//!   `caller.entry` to `downstream.use_entry`.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;

const HELPER_SOURCE: &str = "\
def unused_fn():
    return \"never called\"
";

const MAIN_SOURCE: &str = "\
import requests

from helper import unused_fn


def entry():
    return \"no calls to helper here\"
";

#[test]
fn unused_imports_expose_resolution_kind_and_omit_it_when_unresolved() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-resolution-kind-")
        .tempdir()
        .unwrap();
    common::write_files(
        tmp.path(),
        &[("helper.py", HELPER_SOURCE), ("main.py", MAIN_SOURCE)],
    );
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let raw = rpc::call(repo_root, db_path, "dead_symbols".to_string(), "{}", "1").unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "dead_symbols returned an error: {envelope}"
    );
    let unused_imports = envelope["result"]["unused_imports"]
        .as_array()
        .expect("unused_imports should be an array");
    assert_eq!(
        unused_imports.len(),
        2,
        "expected exactly 2 unused imports, got {unused_imports:?}"
    );

    let resolved = unused_imports
        .iter()
        .find(|e| e["target_qualname"] == "helper.unused_fn")
        .expect("expected an unused import targeting helper.unused_fn");
    assert_eq!(
        resolved["resolution_kind"], "exact",
        "a same-repo unused import resolved by exact qualname match must report its tier: {resolved}"
    );

    let unresolved = unused_imports
        .iter()
        .find(|e| e["target_qualname"] == "requests")
        .expect("expected an unused import targeting requests");
    assert!(
        unresolved.get("resolution_kind").is_none(),
        "an unresolved (never-bound) unused import must omit resolution_kind rather than report null: {unresolved}"
    );
}

fn call_rpc(
    repo_root: std::path::PathBuf,
    db_path: std::path::PathBuf,
    method: &str,
    params: &str,
) -> Value {
    let raw = rpc::call(repo_root, db_path, method.to_string(), params, "1").unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "{method} returned an error: {envelope}"
    );
    envelope["result"].clone()
}

/// `explain_symbol`'s `callers`/`callees` refs carry the tier of the edge
/// each ref was built from, across two different tiers in one fixture:
/// `caller.entry CALLS caller.local_util` binds `exact` (same-module,
/// unambiguous target), and `downstream.use_entry CALLS caller.entry`
/// binds `import` (cross-file, bound by name through the caller's own
/// import) -- both pinned in `tests/fixtures/golden/python/expected_edges.txt`.
#[test]
fn explain_symbol_refs_expose_resolution_kind_across_tiers() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = call_rpc(
        repo_root,
        db_path,
        "explain_symbol",
        r#"{"qualname":"caller.entry","sections":["callers","callees"]}"#,
    );

    let callees = result["callees"]
        .as_array()
        .expect("callees should be an array");
    let callee = callees
        .iter()
        .find(|r| r["symbol"]["qualname"] == "caller.local_util")
        .expect("expected a callee ref for caller.local_util");
    assert_eq!(
        callee["resolution_kind"], "exact",
        "caller.entry's call to caller.local_util binds exact: {callee}"
    );

    let callers = result["callers"]
        .as_array()
        .expect("callers should be an array");
    let caller = callers
        .iter()
        .find(|r| r["symbol"]["qualname"] == "downstream.use_entry")
        .expect("expected a caller ref for downstream.use_entry");
    assert_eq!(
        caller["resolution_kind"], "import",
        "downstream.use_entry's call to caller.entry binds import: {caller}"
    );
}

const TF_SETTINGS_SOURCE: &str = "\
import os


def format_url(url):
    return url.strip()


def read_url():
    raw = os.getenv(\"DATABASE_URL\")
    return format_url(raw)
";

const TF_DEPLOY_YAML: &str = "\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
spec:
  template:
    spec:
      containers:
        - name: app
          image: app:latest
          env:
            - name: DATABASE_URL
              value: \"postgres://localhost/app\"
";

/// `trace_flow`'s hops carry the tier of the edge each hop traversed. This
/// fixture's `settings.read_url` reaches three hops in one downstream
/// trace: a direct `CALLS` to `format_url` (binds `exact`), a direct
/// `CALLS` to the external `os.getenv` stub (binds `external`), and a
/// `CONFIG_SOURCE` hop crossing into `k8s/deploy.yaml` -- a
/// String-Targeted Edge Kind, not a Bridge Edge pair, whose own target is
/// a `env://DATABASE_URL` config-key URI, not a symbol, so the write path
/// never binds it and its hop must omit `resolution_kind` entirely rather
/// than report a tier that was never computed.
#[test]
fn trace_flow_hops_expose_resolution_kind_and_omit_it_when_unresolved() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-trace-flow-resolution-kind-")
        .tempdir()
        .unwrap();
    common::write_files(
        tmp.path(),
        &[
            ("settings.py", TF_SETTINGS_SOURCE),
            ("k8s/deploy.yaml", TF_DEPLOY_YAML),
        ],
    );
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = call_rpc(
        repo_root,
        db_path,
        "trace_flow",
        r#"{"start_qualname":"settings.read_url","direction":"downstream"}"#,
    );
    let trace = result["trace"]
        .as_array()
        .expect("trace should be an array");

    let direct_hop = trace
        .iter()
        .find(|h| h["symbol"]["qualname"] == "settings.format_url")
        .expect("expected a direct CALLS hop to settings.format_url");
    assert_eq!(
        direct_hop["resolution_kind"], "exact",
        "the direct call to format_url binds exact: {direct_hop}"
    );

    let bridged_hop = trace
        .iter()
        .find(|h| h["edge_kind"] == "CONFIG_SOURCE")
        .expect("expected a bridged CONFIG_SOURCE hop into k8s/deploy.yaml");
    assert!(
        bridged_hop.get("resolution_kind").is_none(),
        "a CONFIG_SOURCE edge's target is a URI, never bound to a symbol, so its hop must omit resolution_kind rather than report null: {bridged_hop}"
    );
}

/// Find the one path step landing on `to_symbol`, or panic with the full
/// steps list for debugging.
fn find_step<'a>(steps: &'a [Value], to_symbol: &str) -> &'a Value {
    steps
        .iter()
        .find(|s| s["to_symbol"] == to_symbol)
        .unwrap_or_else(|| panic!("expected a path step to {to_symbol}, got {steps:?}"))
}

/// `analyze_impact`'s `affected[].path.steps` carry the tier of the edge
/// each step traversed. `caller.entry CALLS caller.local_util` binds
/// `exact` (same-module, unambiguous target) -- see
/// `tests/fixtures/golden/python/expected_edges.txt`.
#[test]
fn analyze_impact_path_steps_expose_resolution_kind_exact_tier() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = call_rpc(
        repo_root,
        db_path,
        "analyze_impact",
        r#"{"qualname":"caller.entry","direction":"downstream","kinds":["CALLS"]}"#,
    );
    let affected = result["affected"]
        .as_array()
        .expect("affected should be an array");
    let entry = affected
        .iter()
        .find(|e| e["symbol"]["qualname"] == "caller.local_util")
        .expect("expected an affected entry for caller.local_util");
    let steps = entry["path"]["steps"]
        .as_array()
        .expect("expected path.steps on the affected entry");
    let step = find_step(steps, "caller.local_util");
    assert_eq!(
        step["resolution_kind"], "exact",
        "caller.entry's call to caller.local_util binds exact: {step}"
    );
}

/// A path step whose parent is the seed itself must still carry a real
/// `from_symbol`. `reconstruct_path_steps` (`src/impact/orchestrator.rs`)
/// looks the parent up in a `symbol_map`; before this was fixed, that map
/// was built only from non-seed impacted symbols, so a seed parent's
/// qualname rendered as `""`. `caller.entry` is a distance-1 seed off
/// `caller.local_util` downstream, so the first (and only) step's
/// `from_symbol` must equal the seed's own qualname.
#[test]
fn analyze_impact_path_step_from_seed_has_real_from_symbol() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = call_rpc(
        repo_root,
        db_path,
        "analyze_impact",
        r#"{"qualname":"caller.entry","direction":"downstream","kinds":["CALLS"]}"#,
    );
    let affected = result["affected"]
        .as_array()
        .expect("affected should be an array");
    let entry = affected
        .iter()
        .find(|e| e["symbol"]["qualname"] == "caller.local_util")
        .expect("expected an affected entry for caller.local_util");
    let steps = entry["path"]["steps"]
        .as_array()
        .expect("expected path.steps on the affected entry");
    let step = find_step(steps, "caller.local_util");
    assert_eq!(
        step["from_symbol"], "caller.entry",
        "the step off the seed must name the seed, not render \"\": {step}"
    );
}

/// Same response shape, `import` tier: `downstream.use_entry CALLS
/// caller.entry` binds `import` (cross-file, bound by name through the
/// caller's own import) -- reached from `caller.entry` via an upstream
/// analysis.
#[test]
fn analyze_impact_path_steps_expose_resolution_kind_import_tier() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = call_rpc(
        repo_root,
        db_path,
        "analyze_impact",
        r#"{"qualname":"caller.entry","direction":"upstream","kinds":["CALLS"]}"#,
    );
    let affected = result["affected"]
        .as_array()
        .expect("affected should be an array");
    let entry = affected
        .iter()
        .find(|e| e["symbol"]["qualname"] == "downstream.use_entry")
        .expect("expected an affected entry for downstream.use_entry");
    let steps = entry["path"]["steps"]
        .as_array()
        .expect("expected path.steps on the affected entry");
    // Issue #103: steps read caller -> callee even for an upstream walk.
    let step = steps
        .iter()
        .find(|s| s["from_symbol"] == "downstream.use_entry")
        .unwrap_or_else(|| panic!("expected a step from downstream.use_entry, got {steps:?}"));
    assert_eq!(step["to_symbol"], "caller.entry");
    assert_eq!(
        step["resolution_kind"], "import",
        "downstream.use_entry's call to caller.entry binds import: {step}"
    );
}
