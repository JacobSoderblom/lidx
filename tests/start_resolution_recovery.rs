//! Tests for structured recovery payloads when trace_flow / analyze_impact
//! start-symbol resolution fails.
//!
//! Issue #44: When resolution of the start symbol itself fails (not just an
//! empty-result traversal), both handlers must return a structured payload with
//! search hits, config-URI candidates, and next_hops — not a flat `{error}`.

use lidx::indexer::Indexer;
use lidx::rpc;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn temp_repo_dir(label: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("lidx-start-recovery-{label}-{nanos}-{counter}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&path, &target);
        } else {
            std::fs::copy(&path, &target).unwrap();
        }
    }
}

struct TempRepo {
    pub repo_root: PathBuf,
    pub db_path: PathBuf,
}

impl Drop for TempRepo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.repo_root);
    }
}

impl TempRepo {
    fn new(fixture: &str) -> Self {
        let src = fixture_path(fixture);
        let repo_root = temp_repo_dir(fixture);
        copy_dir(&src, &repo_root);
        let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
        Self { repo_root, db_path }
    }
}

fn indexed_repo(fixture: &str) -> TempRepo {
    let temp = TempRepo::new(fixture);
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    temp
}

fn call_raw(temp: &TempRepo, method: &str, params: &str) -> serde_json::Value {
    let raw = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        method.to_string(),
        params,
        "1",
    )
    .unwrap();
    serde_json::from_str(&raw).unwrap()
}

fn rg_available() -> bool {
    std::process::Command::new("rg")
        .arg("--version")
        .output()
        .is_ok()
}

fn follow_hop(temp: &TempRepo, hop: &serde_json::Value) -> serde_json::Value {
    let method = hop["method"]
        .as_str()
        .expect("hop must have a method")
        .to_string();
    let params = serde_json::to_string(&hop["params"]).unwrap();
    let raw = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        method,
        &params,
        "2",
    )
    .unwrap();
    serde_json::from_str(&raw).unwrap()
}

// ---------------------------------------------------------------------------
// trace_flow: unresolvable start ref returns structured payload, not flat error
// ---------------------------------------------------------------------------

/// Core contract: an unresolvable start ref — whether passed as `query` or as a
/// (non-config-URI) `start_qualname` — must return a structured result object
/// with `next_hops`, not a bare `{error: ...}`. Both param keys route through the
/// same recovery boundary, so they are exercised in one table.
#[test]
fn trace_flow_unresolvable_start_returns_structured_payload() {
    let temp = indexed_repo("py_mvp");

    let cases = [
        ("query", r#"{"query":"xyzzy_totally_absent_symbol_9999"}"#),
        (
            "start_qualname",
            r#"{"start_qualname":"no.such.Symbol.AtAll"}"#,
        ),
    ];

    for (label, params) in cases {
        let envelope = call_raw(&temp, "trace_flow", params);

        // Must NOT be a flat error response
        assert!(
            envelope.get("error").is_none(),
            "[{label}] unresolvable trace_flow must not return a flat error, got: {envelope}"
        );

        let result = &envelope["result"];
        assert!(
            result.is_object(),
            "[{label}] trace_flow recovery payload must be an object, got: {result}"
        );

        // Must have a next_hops array with at least one well-formed entry
        let next_hops = result["next_hops"]
            .as_array()
            .expect("trace_flow recovery payload must have next_hops array");
        assert!(
            !next_hops.is_empty(),
            "[{label}] trace_flow recovery payload must contain at least one next_hop"
        );
        for hop in next_hops {
            let obj = hop.as_object().unwrap();
            assert!(
                obj.contains_key("method"),
                "each hop must have a method field"
            );
            assert!(
                obj.contains_key("description"),
                "each hop must have a description field"
            );
        }

        // Must have a human-readable message explaining what failed
        assert!(
            result.get("message").is_some(),
            "[{label}] recovery payload should include an explanatory message, got: {result}"
        );
    }
}

/// Resolvable trace_flow start refs must produce the standard result shape —
/// no recovery payload, no next_hops on non-empty traces. (Not a byte-identical
/// snapshot: this asserts the fields a caller actually depends on.)
#[test]
fn trace_flow_resolvable_ref_still_returns_standard_fields() {
    let temp = indexed_repo("py_mvp");

    let envelope = call_raw(&temp, "trace_flow", r#"{"query":"Greeter"}"#);

    // Must be a successful result (no error)
    assert!(
        envelope.get("error").is_none(),
        "resolvable trace_flow must succeed, got: {}",
        envelope
    );

    let result = &envelope["result"];
    // Must have the standard trace_flow fields
    assert!(
        result.get("start").is_some(),
        "resolvable trace_flow result must have 'start' field"
    );
    assert!(
        result.get("trace").is_some(),
        "resolvable trace_flow result must have 'trace' field"
    );
    // A resolved start must not carry the recovery payload shape.
    assert!(
        result.get("resolved").is_none(),
        "resolvable trace_flow must not return the recovery payload, got: {}",
        result
    );
    assert_eq!(
        result["start"]["qualname"].as_str(),
        Some("pkg.core.Greeter"),
        "resolvable trace_flow must resolve to the expected symbol, got: {}",
        result
    );
}

// ---------------------------------------------------------------------------
// analyze_impact: unresolvable seed returns structured payload, not flat error
// ---------------------------------------------------------------------------

/// An unresolvable single-path analyze_impact seed — whether `query` or
/// `qualname` — must return a structured result object with next_hops, not a flat
/// `{error: ...}`. Both keys route through the same recovery boundary.
#[test]
fn analyze_impact_unresolvable_seed_returns_structured_payload() {
    let temp = indexed_repo("py_mvp");

    let cases = [
        ("query", r#"{"query":"xyzzy_totally_absent_symbol_9999"}"#),
        ("qualname", r#"{"qualname":"no.such.Symbol.AtAll"}"#),
    ];

    for (label, params) in cases {
        let envelope = call_raw(&temp, "analyze_impact", params);

        assert!(
            envelope.get("error").is_none(),
            "[{label}] unresolvable analyze_impact must not return a flat error, got: {envelope}"
        );

        let result = &envelope["result"];
        assert!(
            result.is_object(),
            "[{label}] analyze_impact recovery payload must be an object, got: {result}"
        );

        let next_hops = result["next_hops"]
            .as_array()
            .expect("analyze_impact recovery payload must have next_hops array");
        assert!(
            !next_hops.is_empty(),
            "[{label}] analyze_impact recovery payload must contain at least one suggestion"
        );
        for hop in next_hops {
            let obj = hop.as_object().unwrap();
            assert!(
                obj.contains_key("method"),
                "each hop must have a method field"
            );
            assert!(
                obj.contains_key("description"),
                "each hop must have a description field"
            );
        }
    }
}

/// Resolvable analyze_impact seeds must produce the same structure as before.
#[test]
fn analyze_impact_resolvable_ref_is_unchanged() {
    let temp = indexed_repo("py_mvp");

    let envelope = call_raw(
        &temp,
        "analyze_impact",
        r#"{"qualname":"pkg.core.Greeter","max_depth":3}"#,
    );

    assert!(
        envelope.get("error").is_none(),
        "resolvable analyze_impact must succeed, got: {}",
        envelope
    );

    let result = &envelope["result"];
    assert!(
        result.get("seeds").is_some(),
        "resolvable analyze_impact must have 'seeds' field"
    );
    assert!(
        result.get("affected").is_some(),
        "resolvable analyze_impact must have 'affected' field"
    );
    // A resolved seed must not carry the recovery payload. (next_hops may still
    // appear on success, e.g. the heuristic-edge retry hop.)
    assert!(
        result.get("resolved").is_none(),
        "resolvable analyze_impact must not return the recovery payload, got: {}",
        result
    );
}

// ---------------------------------------------------------------------------
// Recovery hops are executable (no dead-end suggestions)
// ---------------------------------------------------------------------------

/// For a near-miss query ("Greeter" matches, "zzz" breaks the AND), the
/// trace_flow recovery payload must (a) suggest an `explain_symbol`/`trace_flow`
/// pivot off the matching token, and (b) every next_hop must be an executable RPC
/// call (no dead-end suggestions).
#[test]
fn trace_flow_recovery_hops_are_executable() {
    let temp = indexed_repo("py_mvp");

    // "Greeter zz" — "Greeter" is the longer token, so the single-longest-token
    // "did you mean" lookup (shared with resolve_by_query) lands on it.
    let envelope = call_raw(&temp, "trace_flow", r#"{"query":"Greeter zz"}"#);

    assert!(
        envelope.get("error").is_none(),
        "recovery payload must not be a flat error"
    );

    let result = &envelope["result"];
    let next_hops = result["next_hops"].as_array().unwrap();

    // The matching token must yield a pivot suggestion (explain_symbol or retry).
    let has_pivot = next_hops.iter().any(|h| {
        matches!(
            h["method"].as_str(),
            Some("explain_symbol") | Some("trace_flow")
        )
    });
    assert!(
        has_pivot,
        "recovery hops must include explain_symbol or trace_flow retry suggestions, got: {next_hops:?}"
    );

    for hop in next_hops {
        // Skip search hops when rg is not available — search relies on ripgrep
        if hop["method"].as_str() == Some("search") && !rg_available() {
            continue;
        }
        let followed = follow_hop(&temp, hop);
        assert!(
            followed.get("error").is_none(),
            "recovery hop {} must execute without top-level error, got: {}",
            hop,
            followed
        );
    }
}

/// Every next_hop in an analyze_impact recovery payload must be a valid RPC call.
#[test]
fn analyze_impact_recovery_hops_are_executable() {
    let temp = indexed_repo("py_mvp");

    // A near-miss that triggers "Did you mean" — "Greeter" is the longer token
    let envelope = call_raw(&temp, "analyze_impact", r#"{"query":"Greeter zz"}"#);

    assert!(
        envelope.get("error").is_none(),
        "recovery payload must not be a flat error"
    );

    let result = &envelope["result"];
    let next_hops = result["next_hops"].as_array().unwrap();

    for hop in next_hops {
        // Skip search hops when rg is not available — search relies on ripgrep
        if hop["method"].as_str() == Some("search") && !rg_available() {
            continue;
        }
        let followed = follow_hop(&temp, hop);
        assert!(
            followed.get("error").is_none(),
            "recovery hop {} must execute without top-level error, got: {}",
            hop,
            followed
        );
    }
}

// ---------------------------------------------------------------------------
// Degenerate start refs: empty / whitespace / punctuation / unicode / huge.
// The recovery payload must still be a structured object with at least one
// executable next_hop — never a flat error, never a panic.
// ---------------------------------------------------------------------------

/// Helper: assert the response is a structured recovery payload (not a flat
/// error) with a non-empty next_hops array, and that every hop is well-formed
/// (has method + params).
fn assert_structured_recovery(envelope: &serde_json::Value) {
    assert!(
        envelope.get("error").is_none(),
        "must not be a flat error, got: {envelope}"
    );
    let result = &envelope["result"];
    let next_hops = result["next_hops"]
        .as_array()
        .unwrap_or_else(|| panic!("recovery payload must have next_hops array, got: {result}"));
    assert!(
        !next_hops.is_empty(),
        "recovery payload must always contain at least one next_hop, got: {result}"
    );
    for hop in next_hops {
        let obj = hop.as_object().expect("each hop must be an object");
        assert!(obj.contains_key("method"), "each hop must have a method");
        assert!(obj.contains_key("params"), "each hop must have params");
    }
}

/// Every degenerate input is fed through both handlers. The point is robustness:
/// no panic, no flat error, always a structured payload with a fallback hop.
const DEGENERATE_QUERIES: &[(&str, &str)] = &[
    ("empty", ""),
    ("whitespace", "   "),
    ("tabs_newlines", " \t \n "),
    ("punctuation_only", "...."),
    ("symbols_only", "@#$%^&*()"),
    ("single_char", "x"),
    ("unicode_emoji", "日本語🦀"),
    ("regex_special", "a|b.*c+"),
];

#[test]
fn trace_flow_degenerate_query_always_structured() {
    let temp = indexed_repo("py_mvp");
    for (label, q) in DEGENERATE_QUERIES {
        let params = serde_json::json!({ "query": q }).to_string();
        let envelope = call_raw(&temp, "trace_flow", &params);
        assert_structured_recovery(&envelope);
        // Sanity: it should always have the always-present `search` fallback hop.
        let next_hops = envelope["result"]["next_hops"].as_array().unwrap();
        assert!(
            next_hops
                .iter()
                .any(|h| h["method"].as_str() == Some("search")),
            "[{label}] expected a search fallback hop, got: {next_hops:?}"
        );
    }
}

#[test]
fn analyze_impact_degenerate_query_always_structured() {
    let temp = indexed_repo("py_mvp");
    for (label, q) in DEGENERATE_QUERIES {
        let params = serde_json::json!({ "query": q }).to_string();
        let envelope = call_raw(&temp, "analyze_impact", &params);
        assert_structured_recovery(&envelope);
        let next_hops = envelope["result"]["next_hops"].as_array().unwrap();
        assert!(
            next_hops
                .iter()
                .any(|h| h["method"].as_str() == Some("search")),
            "[{label}] expected a search fallback hop, got: {next_hops:?}"
        );
    }
}

/// A very long ref must not panic, slice into a multi-byte char, or error out.
#[test]
fn trace_flow_very_long_ref_is_structured() {
    let temp = indexed_repo("py_mvp");
    let long = "λ".repeat(2000); // 2000 multi-byte chars, well under pattern_max_length
    let params = serde_json::json!({ "query": long }).to_string();
    let envelope = call_raw(&temp, "trace_flow", &params);
    assert_structured_recovery(&envelope);
}

/// The single always-present `search` fallback hop must itself be executable
/// even for a degenerate input (this is the core promise of issue #44).
#[test]
fn search_fallback_hop_is_executable_for_degenerate_input() {
    if !rg_available() {
        return;
    }
    let temp = indexed_repo("py_mvp");
    for (label, q) in DEGENERATE_QUERIES {
        let params = serde_json::json!({ "query": q }).to_string();
        let envelope = call_raw(&temp, "trace_flow", &params);
        let next_hops = envelope["result"]["next_hops"].as_array().unwrap();
        let search_hop = next_hops
            .iter()
            .find(|h| h["method"].as_str() == Some("search"))
            .unwrap_or_else(|| panic!("[{label}] no search hop"));
        let followed = follow_hop(&temp, search_hop);
        assert!(
            followed.get("error").is_none(),
            "[{label}] search fallback hop must execute, got: {followed}"
        );
    }
}

// ---------------------------------------------------------------------------
// ID-based start refs: a miss still flat-errors (no symbol to suggest).
// ---------------------------------------------------------------------------

/// trace_flow with a non-existent `start_id` must still return a flat error —
/// an ID either exists or it doesn't, there is nothing to suggest.
#[test]
fn trace_flow_missing_start_id_still_errors() {
    let temp = indexed_repo("py_mvp");
    let envelope = call_raw(&temp, "trace_flow", r#"{"start_id":999999999}"#);
    assert!(
        envelope.get("error").is_some(),
        "missing start_id must remain a flat error, got: {envelope}"
    );
    assert!(
        envelope.get("result").is_none(),
        "missing start_id must not produce a recovery result, got: {envelope}"
    );
}

/// analyze_impact with a non-existent `id` must still return a flat error.
#[test]
fn analyze_impact_missing_id_still_errors() {
    let temp = indexed_repo("py_mvp");
    let envelope = call_raw(&temp, "analyze_impact", r#"{"id":999999999}"#);
    assert!(
        envelope.get("error").is_some(),
        "missing id must remain a flat error, got: {envelope}"
    );
}

// ---------------------------------------------------------------------------
// Batch path: mix of good + bad entries produces correct per-entry shape and
// preserves pre-existing batch error semantics (layers.direct.error).
// ---------------------------------------------------------------------------

/// A good entry must omit `recovery` entirely (skip_serializing_if), while the
/// bad entry must carry `recovery` AND preserve the legacy `layers.direct.error`.
#[test]
fn analyze_impact_batch_mixed_entries_shape_and_legacy_error() {
    let temp = indexed_repo("py_mvp");
    let envelope = call_raw(
        &temp,
        "analyze_impact",
        r#"{"qualnames":["pkg.core.Greeter","xyzzy_totally_absent_symbol_9999"]}"#,
    );
    assert!(envelope.get("error").is_none(), "batch must succeed");
    let results = envelope["result"]["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);

    let good = results
        .iter()
        .find(|e| e["seed_qualname"].as_str() == Some("pkg.core.Greeter"))
        .unwrap();
    // skip_serializing_if = Option::is_none means the key must be ABSENT, not null.
    assert!(
        good.as_object().unwrap().get("recovery").is_none(),
        "successful entry must omit the recovery key entirely, got: {good}"
    );

    let bad = results
        .iter()
        .find(|e| e["seed_qualname"].as_str() == Some("xyzzy_totally_absent_symbol_9999"))
        .unwrap();
    let recovery = bad
        .get("recovery")
        .expect("failed entry must carry recovery");
    assert_eq!(recovery["resolved"], serde_json::json!(false));
    assert!(
        recovery["next_hops"]
            .as_array()
            .is_some_and(|h| !h.is_empty())
    );
    // Pre-existing semantics: the per-entry direct-layer error must be preserved.
    let direct_error = &bad["layers"]["direct"]["error"];
    assert!(
        direct_error.is_string(),
        "legacy layers.direct.error must be preserved on a failed batch entry, got: {bad}"
    );
}

/// A batch where EVERY entry fails must still succeed at the top level and give
/// each entry its own recovery payload (no batch-wide collapse to flat error).
#[test]
fn analyze_impact_batch_all_bad_entries_each_recover() {
    let temp = indexed_repo("py_mvp");
    let envelope = call_raw(
        &temp,
        "analyze_impact",
        r#"{"qualnames":["nope_one_zzz","nope_two_zzz"]}"#,
    );
    assert!(
        envelope.get("error").is_none(),
        "all-bad batch must not collapse to a flat error, got: {envelope}"
    );
    let results = envelope["result"]["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    for entry in results {
        assert!(
            entry.get("recovery").is_some(),
            "every failed entry must carry a recovery payload, got: {entry}"
        );
    }
}

// ---------------------------------------------------------------------------
// Issue #58 findings: a single typed "not found" error (not a duplicate
// suggestion algorithm), structured `suggestions`, id-based retry hops that
// carry the caller's params, a safe search fallback, and config-URI recovery
// on the single-seed path.
// ---------------------------------------------------------------------------

/// A query too large for the DB to plan (SQLite's expression-tree limit) must
/// propagate as a real error, not be swallowed into a "not found" recovery
/// payload. `explain_symbol` (which never catches resolution failure) already
/// surfaces the real DB error for the same input — trace_flow must match it
/// instead of reporting "not found".
#[test]
fn trace_flow_db_error_propagates_instead_of_becoming_recovery() {
    let temp = indexed_repo("py_mvp");
    let huge_query = "greet ".repeat(1200);
    let params = serde_json::json!({ "query": huge_query }).to_string();

    let envelope = call_raw(&temp, "trace_flow", &params);
    let error = envelope.get("error").unwrap_or_else(|| {
        panic!("expected a flat error for a DB-breaking query, got: {envelope}")
    });
    let msg = error["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("Expression tree is too large"),
        "expected the real SQLite error to propagate (matching explain_symbol's behavior \
         for the same input), got: {msg}"
    );
}

/// A recovery payload must carry its candidates as structured data (id,
/// qualname, kind, file_path, start_line), not only inside hop params/text.
#[test]
fn recovery_payload_includes_structured_suggestions() {
    let temp = indexed_repo("py_mvp");
    let envelope = call_raw(&temp, "trace_flow", r#"{"query":"Greeter zz"}"#);

    let result = &envelope["result"];
    let suggestions = result["suggestions"]
        .as_array()
        .expect("recovery payload must have a structured 'suggestions' array");
    assert!(
        !suggestions.is_empty(),
        "expected at least one candidate suggestion, got: {result}"
    );
    for s in suggestions {
        let obj = s.as_object().expect("each suggestion must be an object");
        for key in ["id", "qualname", "kind", "file_path", "start_line"] {
            assert!(
                obj.contains_key(key),
                "each suggestion must have '{key}', got: {s}"
            );
        }
    }
}

/// A batch entry whose qualname normalizes to a real config URI must surface
/// that URI in a structured `config_uri_candidates` array on its recovery
/// payload (not just buried in a hop description).
#[test]
fn analyze_impact_batch_recovery_includes_config_uri_candidates() {
    let temp = indexed_repo("py_config");
    let envelope = call_raw(&temp, "analyze_impact", r#"{"qualnames":["DATABASE_URL"]}"#);

    assert!(envelope.get("error").is_none(), "batch must succeed");
    let results = envelope["result"]["results"].as_array().unwrap();
    let entry = &results[0];
    let recovery = entry
        .get("recovery")
        .expect("failed entry must carry recovery");
    let config_candidates = recovery["config_uri_candidates"].as_array().unwrap_or_else(|| {
        panic!("recovery payload should surface config_uri_candidates for a config-key-shaped qualname, got: {recovery}")
    });
    assert!(
        config_candidates
            .iter()
            .any(|c| c.as_str() == Some("env://DATABASE_URL")),
        "expected 'env://DATABASE_URL' among config_uri_candidates, got: {config_candidates:?}"
    );
}

/// A retry hop for a same-name candidate must target it by id, not by name —
/// a name-based retry can resolve to a different, shorter-qualname symbol
/// that happens to share the same bare name.
#[test]
fn trace_flow_recovery_retry_hop_pins_candidate_by_id_not_name() {
    let temp = indexed_repo("py_greet_conflict");
    // "Greeter" is the longer token; the "did you mean" search on it
    // substring-matches both the Greeter class and its `greet` method
    // (pkg.core.Greeter.greet), plus the decoy pkg.util.greet shares the
    // method's bare name "greet" but has a much shorter qualname.
    let envelope = call_raw(&temp, "trace_flow", r#"{"query":"Greeter zz"}"#);
    assert!(envelope.get("error").is_none(), "got: {envelope}");

    let result = &envelope["result"];
    let next_hops = result["next_hops"].as_array().unwrap();

    let retry_hop = next_hops
        .iter()
        .find(|h| {
            h["method"].as_str() == Some("trace_flow")
                && h["description"]
                    .as_str()
                    .is_some_and(|d| d.contains("pkg.core.Greeter.greet"))
        })
        .unwrap_or_else(|| {
            panic!("expected a trace_flow retry hop for pkg.core.Greeter.greet, got: {next_hops:?}")
        });

    assert!(
        retry_hop["params"]["start_id"].is_i64(),
        "retry hop must target the candidate by start_id, got params: {}",
        retry_hop["params"]
    );
    assert!(
        retry_hop["params"].get("query").is_none(),
        "retry hop must not fall back to a name-based query, got params: {}",
        retry_hop["params"]
    );

    let followed = follow_hop(&temp, retry_hop);
    assert!(
        followed.get("error").is_none(),
        "retry hop must execute, got: {followed}"
    );
    assert_eq!(
        followed["result"]["start"]["qualname"].as_str(),
        Some("pkg.core.Greeter.greet"),
        "retry hop must resolve to the pinned candidate, not the same-name decoy, got: {followed}"
    );
}

/// A retry hop must carry the caller's other traversal params (direction,
/// etc.) forward, replacing only the start ref — not drop them.
#[test]
fn analyze_impact_recovery_retry_hop_carries_caller_params() {
    let temp = indexed_repo("py_mvp");
    let envelope = call_raw(
        &temp,
        "analyze_impact",
        r#"{"query":"Greeter zz","direction":"upstream"}"#,
    );
    assert!(envelope.get("error").is_none(), "got: {envelope}");

    let result = &envelope["result"];
    let next_hops = result["next_hops"].as_array().unwrap();

    let retry_hop = next_hops
        .iter()
        .find(|h| h["method"].as_str() == Some("analyze_impact") && h["params"].get("id").is_some())
        .unwrap_or_else(|| {
            panic!("expected an id-based analyze_impact retry hop, got: {next_hops:?}")
        });

    assert_eq!(
        retry_hop["params"]["direction"].as_str(),
        Some("upstream"),
        "retry hop must carry the caller's direction param, got params: {}",
        retry_hop["params"]
    );

    let followed = follow_hop(&temp, retry_hop);
    assert!(
        followed.get("error").is_none(),
        "retry hop must execute, got: {followed}"
    );
}

/// The always-present search fallback hop must use fixed_string mode so a
/// token containing regex metacharacters (unbalanced parens, etc.) doesn't
/// blow up the hop with a regex parse error.
#[test]
fn search_fallback_hop_uses_fixed_string_and_survives_regex_metacharacters() {
    let temp = indexed_repo("py_mvp");
    let envelope = call_raw(&temp, "trace_flow", r#"{"query":"handle_request("}"#);

    let result = &envelope["result"];
    let next_hops = result["next_hops"].as_array().unwrap();
    let search_hop = next_hops
        .iter()
        .find(|h| h["method"].as_str() == Some("search"))
        .expect("expected a search fallback hop");
    assert_eq!(
        search_hop["params"]["fixed_string"],
        serde_json::json!(true),
        "search fallback hop must use fixed_string mode, got: {search_hop}"
    );

    if rg_available() {
        let followed = follow_hop(&temp, search_hop);
        assert!(
            followed.get("error").is_none(),
            "search fallback hop with an unbalanced paren must not error, got: {followed}"
        );
    }
}

/// A batch entry whose seed resolves fine but whose analysis subsequently
/// fails (here: a `languages` filter that excludes the seed's own language,
/// so `load_seeds` finds zero matching symbols and the orchestrator errors
/// with "No valid seed symbols found") must NOT carry a `recovery` payload.
/// `recovery` means "the seed itself couldn't be found" — a
/// `build_resolution_recovery_payload` "Symbol '...' not found" message
/// would be actively wrong here, since the symbol WAS found. The real error
/// must still land in `layers.direct.error`, as before.
#[test]
fn analyze_impact_batch_resolved_seed_with_failed_analysis_has_no_recovery() {
    let temp = indexed_repo("py_mvp");
    let envelope = call_raw(
        &temp,
        "analyze_impact",
        r#"{"qualnames":["pkg.core.Greeter"],"languages":["rust"]}"#,
    );
    assert!(
        envelope.get("error").is_none(),
        "batch must succeed, got: {envelope}"
    );
    let results = envelope["result"]["results"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    let entry = &results[0];

    assert!(
        entry.as_object().unwrap().get("recovery").is_none(),
        "a resolved seed whose analysis fails afterward must not carry a recovery \
         payload (recovery means 'seed not found', not 'analysis failed'), got: {entry}"
    );
    let direct_error = &entry["layers"]["direct"]["error"];
    assert!(
        direct_error.is_string(),
        "the real analysis error must still be preserved in layers.direct.error, got: {entry}"
    );
}

/// An unmatched config URI passed directly as trace_flow's start_qualname
/// must return a structured recovery payload, not a flat error — matching
/// the batch path's existing recovery for the same kind of input.
#[test]
fn trace_flow_unmatched_config_uri_returns_recovery_not_error() {
    let temp = indexed_repo("py_config");
    let envelope = call_raw(
        &temp,
        "trace_flow",
        r#"{"start_qualname":"env://NOPE_VAR"}"#,
    );

    assert!(
        envelope.get("error").is_none(),
        "unmatched config URI must not flat-error, got: {envelope}"
    );
    let result = &envelope["result"];
    assert_eq!(result["resolved"], serde_json::json!(false));
    let next_hops = result["next_hops"].as_array().unwrap();
    assert!(!next_hops.is_empty());
    for hop in next_hops {
        if hop["method"].as_str() == Some("search") && !rg_available() {
            continue;
        }
        let followed = follow_hop(&temp, hop);
        assert!(
            followed.get("error").is_none(),
            "recovery hop {} must execute, got: {}",
            hop,
            followed
        );
    }
}

/// Same as above for analyze_impact's single-seed config-URI path.
#[test]
fn analyze_impact_unmatched_config_uri_returns_recovery_not_error() {
    let temp = indexed_repo("py_config");
    let envelope = call_raw(&temp, "analyze_impact", r#"{"qualname":"env://NOPE_VAR"}"#);

    assert!(
        envelope.get("error").is_none(),
        "unmatched config URI must not flat-error, got: {envelope}"
    );
    let result = &envelope["result"];
    assert_eq!(result["resolved"], serde_json::json!(false));
    let next_hops = result["next_hops"].as_array().unwrap();
    assert!(!next_hops.is_empty());
    for hop in next_hops {
        if hop["method"].as_str() == Some("search") && !rg_available() {
            continue;
        }
        let followed = follow_hop(&temp, hop);
        assert!(
            followed.get("error").is_none(),
            "recovery hop {} must execute, got: {}",
            hop,
            followed
        );
    }
}
