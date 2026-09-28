//! Issue #67: `min_resolution` filter on `explain_symbol`.
//!
//! Reuses `tests/fixtures/golden/python`, which already documents
//! exact/import/receiver_type/inherited/external tiers per-edge in its own
//! `expected_edges.txt`, plus issue #81's `worker.py` incremental-add
//! trick (see `tests/resolution_kind_filter.rs`) for the guarded
//! name-fallback tier. The tier ordering asserted here is the same
//! `db::resolver::ALL_RESOLUTION_KINDS` order issue #81 already filters
//! `trace_flow`/`analyze_impact` on (exact, import, receiver_type,
//! inherited, two_segment, bare_name, external) -- not a second ordering
//! invented for this ticket.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;
use std::path::Path;

fn call_rpc(repo_root: &Path, db_path: &Path, method: &str, params: &str) -> Value {
    let raw = rpc::call(
        repo_root.to_path_buf(),
        db_path.to_path_buf(),
        method.to_string(),
        params,
        "1",
    )
    .unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "{method} returned an error: {envelope}"
    );
    envelope["result"].clone()
}

fn ref_qualnames(refs: &Value) -> Vec<String> {
    refs.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|r| r["symbol"]["qualname"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Omitting `min_resolution` entirely must change nothing about the
/// response (acceptance criterion): `caller.entry`'s known exact callee
/// and import-tier caller (pinned by issue #62's
/// `explain_symbol_refs_expose_resolution_kind_across_tiers`) both still
/// appear, and there are no filter-related warnings.
#[test]
fn omitting_min_resolution_leaves_response_unchanged() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = call_rpc(
        &repo_root,
        &db_path,
        "explain_symbol",
        r#"{"qualname":"caller.entry","sections":["callers","callees"]}"#,
    );
    assert!(
        ref_qualnames(&result["callees"]).contains(&"caller.local_util".to_string()),
        "expected caller.local_util among callees, got {result}"
    );
    assert!(
        ref_qualnames(&result["callers"]).contains(&"downstream.use_entry".to_string()),
        "expected downstream.use_entry among callers, got {result}"
    );
    assert!(
        result
            .get("warnings")
            .is_none_or(|w| w.as_array().unwrap().is_empty()),
        "expected no warnings, got {result}"
    );
}

/// Filtering at the exact/import tier boundary: `caller.entry`'s single
/// callee (`caller.local_util`) binds `exact`, and its single caller
/// (`downstream.use_entry`) binds `import` (both pinned by
/// `expected_edges.txt`). `min_resolution: "exact"` must keep the exact
/// callee but drop the import-tier caller entirely (not just cap it --
/// `callers_total` must reflect the exclusion too); `min_resolution:
/// "import"` must keep both.
#[test]
fn min_resolution_exact_excludes_import_tier_caller() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let strict = call_rpc(
        &repo_root,
        &db_path,
        "explain_symbol",
        r#"{"qualname":"caller.entry","sections":["callers","callees"],"min_resolution":"exact"}"#,
    );
    assert!(
        ref_qualnames(&strict["callees"]).contains(&"caller.local_util".to_string()),
        "an exact-tier callee must survive an exact-only floor: {strict}"
    );
    assert!(
        !ref_qualnames(&strict["callers"]).contains(&"downstream.use_entry".to_string()),
        "an import-tier caller must be excluded by an exact-only floor: {strict}"
    );
    assert_eq!(
        strict["callers_total"],
        Value::from(0),
        "the excluded caller must not count toward callers_total either: {strict}"
    );

    let lenient = call_rpc(
        &repo_root,
        &db_path,
        "explain_symbol",
        r#"{"qualname":"caller.entry","sections":["callers","callees"],"min_resolution":"import"}"#,
    );
    assert!(ref_qualnames(&lenient["callees"]).contains(&"caller.local_util".to_string()));
    assert!(ref_qualnames(&lenient["callers"]).contains(&"downstream.use_entry".to_string()));
}

/// Filtering at the `receiver_type`/`inherited` tier boundary:
/// `caller.call_inherited` CALLS `animals.Animal.speak` binds `inherited`
/// (pinned by `expected_edges.txt`, one tier weaker than `receiver_type`).
/// `min_resolution: "receiver_type"` must exclude it; `min_resolution:
/// "inherited"` must keep it.
#[test]
fn min_resolution_filters_callees_at_inherited_tier_boundary() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let strict = call_rpc(
        &repo_root,
        &db_path,
        "explain_symbol",
        r#"{"qualname":"caller.call_inherited","sections":["callees"],"min_resolution":"receiver_type"}"#,
    );
    assert!(
        !ref_qualnames(&strict["callees"]).contains(&"animals.Animal.speak".to_string()),
        "an inherited-tier callee must be excluded once the floor is receiver_type: {strict}"
    );
    assert_eq!(strict["callees_total"], Value::from(0));

    let lenient = call_rpc(
        &repo_root,
        &db_path,
        "explain_symbol",
        r#"{"qualname":"caller.call_inherited","sections":["callees"],"min_resolution":"inherited"}"#,
    );
    assert!(ref_qualnames(&lenient["callees"]).contains(&"animals.Animal.speak".to_string()));
}

/// An unknown tier name produces a warning in the existing `warnings`
/// field, consistent with how an unknown `sections` value behaves, rather
/// than a hard error -- and the filter has no effect, same as omitting
/// `min_resolution` entirely.
#[test]
fn unknown_min_resolution_warns_instead_of_erroring() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = call_rpc(
        &repo_root,
        &db_path,
        "explain_symbol",
        r#"{"qualname":"caller.entry","sections":["callers","callees"],"min_resolution":"bogus"}"#,
    );
    let warnings = result["warnings"].as_array().unwrap();
    assert!(
        warnings
            .iter()
            .any(|w| w.as_str().unwrap_or_default().contains("bogus")),
        "an unknown min_resolution tier should be named in a warning, got {warnings:?}"
    );
    assert!(ref_qualnames(&result["callees"]).contains(&"caller.local_util".to_string()));
    assert!(
        ref_qualnames(&result["callers"]).contains(&"downstream.use_entry".to_string()),
        "an unrecognized tier must not filter anything, same as omitting min_resolution: {result}"
    );
}

/// `analyze_impact`'s `min_confidence` is a different, untouched knob
/// (issue #60's spec is explicit this ticket must not redefine it) -- a
/// smoke check that it still parses and runs.
#[test]
fn analyze_impact_min_confidence_is_untouched() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = call_rpc(
        &repo_root,
        &db_path,
        "analyze_impact",
        r#"{"qualname":"caller.entry","direction":"both","min_confidence":0.0}"#,
    );
    assert!(
        result.get("affected").is_some(),
        "analyze_impact with min_confidence should still return an 'affected' field: {result}"
    );
}

/// `min_resolution` also filters the `tests` section: reuses issue #81's
/// `worker.py` incremental-add trick and its `test_layer_honors_resolution_kind_filter`
/// test-file shape (`tests/resolution_kind_filter.rs`) to produce a CALLS
/// edge from a test function into `worker.process` resolved by the
/// guarded name-fallback tier. Which of `bare_name`/`two_segment` applies
/// is read off the graph rather than assumed (not this ticket's concern);
/// `inherited` ranks strictly above both, so it reliably excludes the
/// edge regardless of which one it is.
#[test]
fn min_resolution_filters_tests_section_at_heuristic_tier() {
    let (_tmp, repo_root, db_path) = common::setup_repo("golden/python");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    std::fs::write(
        repo_root.join("worker.py"),
        "def process() -> str:\n    return \"worker\"\n",
    )
    .unwrap();
    indexer.sync_rel_paths(&["worker.py".to_string()]).unwrap();
    std::fs::create_dir_all(repo_root.join("tests")).unwrap();
    std::fs::write(
        repo_root.join("tests").join("test_zzz.py"),
        "from worker import *\n\ndef test_alpha():\n    process()\n",
    )
    .unwrap();
    indexer
        .sync_rel_paths(&["tests/test_zzz.py".to_string()])
        .unwrap();

    let heuristic_kind = {
        let graph_version = indexer.db().current_graph_version().unwrap();
        let snapshot = common::golden::snapshot_edges(indexer.db(), graph_version).unwrap();
        snapshot
            .iter()
            .find(|e| e.source_qualname == "tests.test_zzz.test_alpha" && e.kind == "CALLS")
            .and_then(|e| e.resolution_kind.clone())
            .expect("expected tests.test_zzz.test_alpha's CALLS edge to resolve with a tier")
    };
    drop(indexer);

    let unfiltered = call_rpc(
        &repo_root,
        &db_path,
        "explain_symbol",
        r#"{"qualname":"worker.process","sections":["tests"]}"#,
    );
    assert!(
        ref_qualnames(&unfiltered["tests"]).contains(&"tests.test_zzz.test_alpha".to_string()),
        "precondition: the unfiltered tests section must find test_alpha, got {unfiltered}"
    );

    let strict = call_rpc(
        &repo_root,
        &db_path,
        "explain_symbol",
        r#"{"qualname":"worker.process","sections":["tests"],"min_resolution":"inherited"}"#,
    );
    assert!(
        !ref_qualnames(&strict["tests"]).contains(&"tests.test_zzz.test_alpha".to_string()),
        "a heuristically-resolved test call must be excluded once the floor outranks it: {strict}"
    );
    assert_eq!(strict["tests_total"], Value::from(0));

    let lenient = call_rpc(
        &repo_root,
        &db_path,
        "explain_symbol",
        &format!(
            r#"{{"qualname":"worker.process","sections":["tests"],"min_resolution":"{heuristic_kind}"}}"#
        ),
    );
    assert!(
        ref_qualnames(&lenient["tests"]).contains(&"tests.test_zzz.test_alpha".to_string()),
        "min_resolution set exactly to the edge's own tier must keep it: {lenient}"
    );
}
