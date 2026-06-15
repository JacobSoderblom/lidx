//! Tests for recovery next_hops on empty search and zero-affected analyze_impact.
//!
//! Issue #40: When a query succeeds but finds nothing, the LLM gets an opaque payload
//! with no recovery path. This test file pins that both handlers now emit actionable
//! next_hops on empty-success paths — and that the suggested hops are themselves
//! valid calls (a hop that errors when followed is worse than no hop).

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
    dir.push(format!("lidx-recovery-{label}-{nanos}-{counter}"));
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

/// Copy a fixture to a temp dir and index it.
fn indexed_repo(fixture: &str) -> TempRepo {
    let temp = TempRepo::new(fixture);
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    temp
}

/// Call an RPC method and parse the full response envelope.
fn rpc_json(temp: &TempRepo, method: &str, params: &str) -> serde_json::Value {
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        method.to_string(),
        params,
        "1",
    )
    .unwrap();
    serde_json::from_str(&response).unwrap()
}

/// Execute a next_hop suggestion and return the full response envelope.
fn follow_hop(temp: &TempRepo, hop: &serde_json::Value) -> serde_json::Value {
    let method = hop["method"]
        .as_str()
        .expect("hop must have a method")
        .to_string();
    let params = serde_json::to_string(&hop["params"]).unwrap();
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        method,
        &params,
        "2",
    )
    .unwrap();
    serde_json::from_str(&response).unwrap()
}

fn rg_available() -> bool {
    std::process::Command::new("rg")
        .arg("--version")
        .output()
        .is_ok()
}

// ---------------------------------------------------------------------------
// search: empty-result recovery next_hops
// ---------------------------------------------------------------------------

#[test]
fn empty_search_includes_next_hops() {
    if !rg_available() {
        return; // rg not available — skip
    }
    let temp = indexed_repo("py_mvp");

    // case_sensitive=true: empty search should return an object (not a bare array)
    // with a drop-case retry hop plus the always-present explain_symbol hop.
    let value = rpc_json(
        &temp,
        "search",
        r#"{"query":"XYZZY_NO_MATCH_EVER_12345","case_sensitive":true,"limit":5}"#,
    );
    let result = value["result"]
        .as_object()
        .expect("empty search should return an object with next_hops, not a bare array");
    let next_hops = result["next_hops"]
        .as_array()
        .expect("empty search result must have next_hops array");
    for hop in next_hops {
        let hop_obj = hop.as_object().unwrap();
        assert!(
            hop_obj.contains_key("method"),
            "each next_hop must have a method field"
        );
        assert!(
            hop_obj.contains_key("description"),
            "each next_hop must have a description field"
        );
    }
    // The drop-case hop must have case_sensitive:false (not None/missing) so that
    // ripgrep receives -i and actually changes behaviour (C1 fix).
    let drop_case_hop = next_hops
        .iter()
        .find(|h| h["method"].as_str() == Some("search"))
        .expect("case_sensitive query should yield a drop-case search retry hop, got: {:?}");
    assert_eq!(
        drop_case_hop["params"]["case_sensitive"],
        serde_json::json!(false),
        "drop-case hop must explicitly set case_sensitive:false, got params: {}",
        drop_case_hop["params"]
    );

    // No flags, single token: explain_symbol is still suggested as a recovery hop.
    let value = rpc_json(
        &temp,
        "search",
        r#"{"query":"XYZZY_TOTALLY_ABSENT_TOKEN","limit":5}"#,
    );
    let next_hops = value["result"]["next_hops"]
        .as_array()
        .expect("next_hops must be present without case_sensitive too");
    assert!(
        next_hops
            .iter()
            .any(|h| h["method"].as_str() == Some("explain_symbol")),
        "should suggest explain_symbol as a recovery hop, got: {:?}",
        next_hops
    );
}

#[test]
fn empty_search_drop_case_hop_description_is_honest() {
    // C4: the explain_symbol hop description must not claim to definitively resolve the
    // symbol (it may error if the query doesn't name a symbol). The drop-case hop must
    // use case_sensitive:false rather than omitting the field.
    if !rg_available() {
        return;
    }
    let temp = indexed_repo("py_mvp");
    let value = rpc_json(
        &temp,
        "search",
        r#"{"query":"XYZZY_NOTHING_12345","case_sensitive":true,"limit":5}"#,
    );
    let next_hops = value["result"]["next_hops"].as_array().unwrap();

    // explain_symbol hop: description must not assert definitive resolution
    let es_hop = next_hops
        .iter()
        .find(|h| h["method"].as_str() == Some("explain_symbol"))
        .expect("explain_symbol hop must be present");
    let desc = es_hop["description"].as_str().unwrap_or("");
    assert!(
        !desc.contains("resolves"),
        "explain_symbol hop description must not claim to definitively resolve — \
         got: {:?}",
        desc
    );

    // drop-case search hop must have case_sensitive:false
    let cs_hop = next_hops
        .iter()
        .find(|h| h["method"].as_str() == Some("search"))
        .expect("drop-case hop must be present when case_sensitive:true");
    assert_eq!(
        cs_hop["params"]["case_sensitive"],
        serde_json::json!(false),
        "drop-case hop must explicitly set case_sensitive:false"
    );
}

#[test]
fn non_empty_search_is_unchanged() {
    if !rg_available() {
        return;
    }
    let temp = indexed_repo("py_mvp");

    // A pattern that DOES match
    let value = rpc_json(&temp, "search", r#"{"query":"def\\s+greet","limit":5}"#);

    // Non-empty search keeps existing bare-array format
    let hits = value["result"]
        .as_array()
        .expect("non-empty search should return a bare array (unchanged format)");
    assert!(!hits.is_empty());
}

#[test]
fn empty_search_multiword_suggests_first_token_widening() {
    if !rg_available() {
        return;
    }
    let temp = indexed_repo("py_mvp");

    let value = rpc_json(
        &temp,
        "search",
        r#"{"query":"XYZZY_ABSENT_TOK another_word","limit":5}"#,
    );

    let next_hops = value["result"]["next_hops"].as_array().unwrap();
    let widen_hop = next_hops
        .iter()
        .find(|h| {
            h["method"].as_str() == Some("search")
                && h["params"]["query"].as_str() == Some("XYZZY_ABSENT_TOK")
        })
        .expect("multi-word empty search must suggest widening to the first token");

    // Following the widen hop must be a valid call
    let followed = follow_hop(&temp, widen_hop);
    assert!(
        followed.get("error").is_none(),
        "widen hop must execute without error, got: {}",
        followed
    );
}

#[test]
fn empty_search_fixed_string_hops_carry_flag_and_are_executable() {
    if !rg_available() {
        return;
    }
    let temp = indexed_repo("py_mvp");

    // Fixed-string query with regex metacharacters: suggested retries must keep
    // fixed_string, otherwise they fail with a regex parse error when followed.
    let value = rpc_json(
        &temp,
        "search",
        r#"{"query":"absent_call(  XYZ","fixed_string":true,"case_sensitive":true}"#,
    );

    let next_hops = value["result"]["next_hops"].as_array().unwrap();
    let search_hops: Vec<&serde_json::Value> = next_hops
        .iter()
        .filter(|h| h["method"].as_str() == Some("search"))
        .collect();
    assert!(
        !search_hops.is_empty(),
        "case_sensitive + multi-word query should yield search retry hops"
    );

    for hop in search_hops {
        assert_eq!(
            hop["params"]["fixed_string"],
            serde_json::json!(true),
            "search retry hops must carry fixed_string, got: {}",
            hop
        );
        let followed = follow_hop(&temp, hop);
        assert!(
            followed.get("error").is_none(),
            "suggested search hop must execute without error, got: {}",
            followed
        );
    }
}

#[test]
fn empty_search_skips_widen_hop_when_first_token_is_invalid_regex() {
    if !rg_available() {
        return;
    }
    let temp = indexed_repo("py_mvp");

    // The full query is a valid regex, but its first whitespace token "(XYZZY_ABSENT"
    // is an unclosed group — suggesting it would produce a hop that errors.
    let value = rpc_json(
        &temp,
        "search",
        r#"{"query":"(XYZZY_ABSENT alpha)","limit":5}"#,
    );

    let next_hops = value["result"]["next_hops"].as_array().unwrap();
    assert!(
        !next_hops.is_empty(),
        "explain_symbol hop should still be suggested"
    );
    let has_broken_widen_hop = next_hops.iter().any(|h| {
        h["method"].as_str() == Some("search")
            && h["params"]["query"].as_str() == Some("(XYZZY_ABSENT")
    });
    assert!(
        !has_broken_widen_hop,
        "must not suggest an unparsable regex fragment, got: {:?}",
        next_hops
    );
}

// ---------------------------------------------------------------------------
// analyze_impact: zero-affected recovery next_hops
// ---------------------------------------------------------------------------

/// Analyze a symbol with all layers disabled so we reliably get zero affected
/// symbols, and return the result object (with the zero-affected precondition pinned).
fn zero_affected_impact(temp: &TempRepo, params: &str) -> serde_json::Value {
    let value = rpc_json(temp, "analyze_impact", params);
    let result = value["result"].clone();
    let affected = result["affected"].as_array().unwrap();
    assert_eq!(
        affected.len(),
        0,
        "precondition: should have zero affected symbols"
    );
    result
}

const HELPER_UPSTREAM_NO_LAYERS: &str = r#"{"qualname":"pkg.utils.Helper","direction":"upstream","enable_direct":false,"enable_test":false,"enable_historical":false}"#;

#[test]
fn zero_affected_analyze_impact_suggests_recovery_hops() {
    let temp = indexed_repo("py_mvp");
    let result = zero_affected_impact(&temp, HELPER_UPSTREAM_NO_LAYERS);

    let next_hops = result["next_hops"]
        .as_array()
        .expect("zero-affected analyze_impact must include next_hops");
    assert!(
        !next_hops.is_empty(),
        "zero-affected next_hops must contain at least one suggestion"
    );

    // Each hop must have method and description
    for hop in next_hops {
        let hop_obj = hop.as_object().unwrap();
        assert!(hop_obj.contains_key("method"), "each hop must have method");
        assert!(
            hop_obj.contains_key("description"),
            "each hop must have description"
        );
    }

    // Should suggest flipping direction (current is "upstream", so suggest "downstream")
    let has_direction_flip = next_hops.iter().any(|h| {
        h["params"]["direction"]
            .as_str()
            .map(|d| d != "upstream")
            .unwrap_or(false)
    });
    assert!(
        has_direction_flip,
        "should suggest a direction flip in next_hops, got: {:?}",
        next_hops
    );

    // CONFIG-kinds hop is suppressed when no kinds filter was passed (C2):
    // the default all-kinds traversal already covers every edge, so a restricted
    // subset retry cannot find more results.
    let has_config_kinds = next_hops.iter().any(|h| {
        let params_str = serde_json::to_string(&h["params"]).unwrap_or_default();
        params_str.contains("CONFIG")
    });
    assert!(
        !has_config_kinds,
        "CONFIG-kinds hop must NOT appear when the original call had no kinds filter \
         (it would be a dead-end subset), got: {:?}",
        next_hops
    );

    // Should suggest seeding the parent of pkg.utils.Helper, and that hop must
    // be a valid call when followed.
    let parent_hop = next_hops
        .iter()
        .find(|h| h["params"]["qualname"].as_str() == Some("pkg.utils"))
        .expect("should suggest seeding the parent of pkg.utils.Helper");
    let followed = follow_hop(&temp, parent_hop);
    assert!(
        followed.get("error").is_none(),
        "parent seed hop must execute without error, got: {}",
        followed
    );
}

#[test]
fn zero_affected_with_default_direction_omits_flip_hop() {
    let temp = indexed_repo("py_mvp");

    // No direction param — defaults to "both", which already traverses every edge.
    // A narrower upstream/downstream retry could never find more, so no flip hop
    // should appear; the other recovery hops must still be present.
    let result = zero_affected_impact(
        &temp,
        r#"{"qualname":"pkg.utils.Helper","enable_direct":false,"enable_test":false,"enable_historical":false}"#,
    );

    let next_hops = result["next_hops"].as_array().unwrap();
    assert!(
        !next_hops.is_empty(),
        "non-flip recovery hops must still be suggested"
    );
    let has_flip_hop = next_hops.iter().any(|h| {
        h["description"]
            .as_str()
            .is_some_and(|d| d.starts_with("Flip direction"))
    });
    assert!(
        !has_flip_hop,
        "direction 'both' already covers either direction — flipping cannot help, got: {:?}",
        next_hops
    );
}

#[test]
fn zero_affected_omits_parent_hop_when_parent_is_not_a_symbol() {
    let temp = indexed_repo("poly_mvp");

    // The SQL function dbo.get_user has qualname prefix "dbo" (a schema), which is
    // not an indexed symbol — a parent hop would error with "symbol not found".
    let result = zero_affected_impact(
        &temp,
        r#"{"qualname":"dbo.get_user","direction":"upstream","enable_direct":false,"enable_test":false,"enable_historical":false}"#,
    );

    let next_hops = result["next_hops"].as_array().unwrap();
    let has_dbo_hop = next_hops
        .iter()
        .any(|h| h["params"]["qualname"].as_str() == Some("dbo"));
    assert!(
        !has_dbo_hop,
        "must not suggest seeding 'dbo' — it does not resolve, got: {:?}",
        next_hops
    );

    // Every suggested analyze_impact hop must be a valid call
    for hop in next_hops
        .iter()
        .filter(|h| h["method"].as_str() == Some("analyze_impact"))
    {
        let followed = follow_hop(&temp, hop);
        assert!(
            followed.get("error").is_none(),
            "suggested hop must execute without error: {} -> {}",
            hop,
            followed
        );
    }
}

#[test]
fn non_zero_affected_analyze_impact_is_unchanged() {
    let temp = indexed_repo("py_mvp");

    // A symbol that definitely has callers
    let value = rpc_json(
        &temp,
        "analyze_impact",
        r#"{"qualname":"pkg.core.Greeter","max_depth":3}"#,
    );
    let result = value["result"].as_object().unwrap();

    let affected = result["affected"].as_array().unwrap();
    assert!(
        !affected.is_empty(),
        "precondition: Greeter must have affected symbols"
    );

    // Should NOT have next_hops when results are non-empty
    assert!(
        !result.contains_key("next_hops"),
        "non-zero affected analyze_impact should not add next_hops (response unchanged)"
    );
}

// ---------------------------------------------------------------------------
// C2: CONFIG-kinds hop only emitted when it could actually help
// ---------------------------------------------------------------------------

#[test]
fn zero_affected_no_kinds_filter_omits_config_hop() {
    // C2: when the original call passes no kinds filter (the default: all edge kinds),
    // the CONFIG-kinds hop must NOT be emitted — restricting to a subset of kinds
    // cannot surface more results than all-kinds did.
    let temp = indexed_repo("py_mvp");

    // No kinds param → defaults to all edge kinds
    let result = zero_affected_impact(
        &temp,
        r#"{"qualname":"pkg.utils.Helper","direction":"upstream","enable_direct":false,"enable_test":false,"enable_historical":false}"#,
    );
    let next_hops = result["next_hops"].as_array().unwrap();
    let has_config_hop = next_hops.iter().any(|h| {
        let params_str = serde_json::to_string(&h["params"]).unwrap_or_default();
        params_str.contains("CONFIG")
    });
    assert!(
        !has_config_hop,
        "CONFIG-kinds hop must be suppressed when no kinds filter was used, got: {:?}",
        next_hops
    );
}

#[test]
fn zero_affected_restrictive_kinds_emits_config_hop() {
    // C2: when the original call passes a restrictive kinds filter that excludes
    // CONFIG/CALLS kinds, the hop IS useful — it broadens to include those kinds.
    let temp = indexed_repo("py_mvp");

    // kinds=["EXTENDS"] — explicitly excludes CONFIG_BIND, CONFIG_SOURCE, CONFIG_READ, CALLS
    let result = zero_affected_impact(
        &temp,
        r#"{"qualname":"pkg.utils.Helper","direction":"upstream","kinds":["EXTENDS"],"enable_direct":false,"enable_test":false,"enable_historical":false}"#,
    );
    let next_hops = result["next_hops"].as_array().unwrap();
    let has_config_hop = next_hops.iter().any(|h| {
        let params_str = serde_json::to_string(&h["params"]).unwrap_or_default();
        params_str.contains("CONFIG_BIND")
    });
    assert!(
        has_config_hop,
        "CONFIG-kinds hop must appear when original kinds filter excluded CONFIG/CALLS, \
         got: {:?}",
        next_hops
    );
}

// ---------------------------------------------------------------------------
// C5: direction alias recognition in flip hop
// ---------------------------------------------------------------------------

#[test]
fn zero_affected_direction_alias_callers_produces_flip_hop() {
    // C5: the alias "callers" is equivalent to "upstream" per TraversalDirection::from.
    // A zero-affected query with direction:"callers" must still get a flip hop to "downstream".
    let temp = indexed_repo("py_mvp");

    let result = zero_affected_impact(
        &temp,
        r#"{"qualname":"pkg.utils.Helper","direction":"callers","enable_direct":false,"enable_test":false,"enable_historical":false}"#,
    );
    let next_hops = result["next_hops"].as_array().unwrap();
    let flip_hop = next_hops.iter().find(|h| {
        h["method"].as_str() == Some("analyze_impact")
            && h["params"]["direction"].as_str() == Some("downstream")
    });
    assert!(
        flip_hop.is_some(),
        "direction alias 'callers' (= upstream) must produce a flip hop to 'downstream', \
         got: {:?}",
        next_hops
    );
}

#[test]
fn zero_affected_direction_alias_callees_produces_flip_hop() {
    // C5: "callees" is an alias for "downstream" — zero-affected must flip to "upstream".
    let temp = indexed_repo("py_mvp");

    let result = zero_affected_impact(
        &temp,
        r#"{"qualname":"pkg.utils.Helper","direction":"callees","enable_direct":false,"enable_test":false,"enable_historical":false}"#,
    );
    let next_hops = result["next_hops"].as_array().unwrap();
    let flip_hop = next_hops.iter().find(|h| {
        h["method"].as_str() == Some("analyze_impact")
            && h["params"]["direction"].as_str() == Some("upstream")
    });
    assert!(
        flip_hop.is_some(),
        "direction alias 'callees' (= downstream) must produce a flip hop to 'upstream', \
         got: {:?}",
        next_hops
    );
}
