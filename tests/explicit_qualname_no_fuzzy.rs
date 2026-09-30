//! Issue #235: an explicit `qualname` that misses the exact lookup and is
//! answered by the fuzzy fallback must be disclosed, on every method that
//! resolves a qualname through `resolve::resolve_symbol`.
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
    dir.push(format!("lidx-explicit-qualname-{label}-{nanos}-{counter}"));
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

fn indexed_repo(fixture: &str) -> (TempRepo, Indexer) {
    let temp = TempRepo::new(fixture);
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    (temp, indexer)
}

/// Calls an RPC method and returns the response envelope as-is.
/// The envelope has the shape `{"id": ..., "result": {...}}` or `{"id": ..., "error": {...}}`.
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

/// Calls an RPC method and returns the inner result value (unwrapping the
/// `{"result": ...}` envelope and any truncation wrapper `{"data": ...}`).
/// Panics if the response contains an error.
fn call(temp: &TempRepo, method: &str, params: &str) -> serde_json::Value {
    let envelope = call_raw(temp, method, params);
    if let Some(err) = envelope.get("error") {
        panic!("RPC error for {}: {:?}", method, err);
    }
    let result = envelope["result"].clone();
    // Unwrap the truncation envelope if present so tests see the actual result.
    if result.get("truncated").is_some() && result.get("data").is_some() {
        return result["data"].clone();
    }
    result
}

use serde_json::Value;

/// A bare name is not any symbol's exact qualname (`pkg.core.Greeter` is), but
/// the fuzzy fallback resolves it.
const INEXACT: &str = "Greeter";
const EXACT: &str = "pkg.core.Greeter";
const NO_MATCH: &str = "Zzyzx.Nonexistent.Qqqrrsttuv";

/// Every method that resolves an explicit qualname through the shared
/// resolver: (method, param carrying the qualname, param carrying a query).
const METHODS: &[(&str, &str, &str)] = &[
    ("read_symbol", "qualname", "query"),
    ("explain_symbol", "qualname", "query"),
    ("trace_flow", "start_qualname", "query"),
    ("analyze_impact", "qualname", "query"),
    ("orient", "focus_qualname", "focus_query"),
];

const DISCLOSURE_KEYS: [&str; 4] = [
    "requested_qualname",
    "resolved_qualname",
    "resolved_via",
    "exact_match",
];

fn params(param: &str, value: &str) -> String {
    serde_json::json!({ param: value }).to_string()
}

/// The qualname the method itself reports for the symbol it resolved.
fn own_qualname(method: &str, v: &Value) -> String {
    let q = match method {
        "read_symbol" => &v["qualname"],
        "explain_symbol" => &v["symbol"]["qualname"],
        "trace_flow" => &v["start"]["qualname"],
        "analyze_impact" => &v["seeds"][0]["qualname"],
        "orient" => &v["focus_symbol"]["qualname"],
        _ => unreachable!(),
    };
    q.as_str()
        .unwrap_or_else(|| panic!("{method}: no returned qualname in {v}"))
        .to_string()
}

/// Strips only genuinely nondeterministic values: wall-clock timings, symbol
/// row ids (assigned in parallel-indexing order, so they vary run to run) and
/// `used_bytes`, which depends on the digit width of those ids. Everything
/// else must match exactly.
fn scrub(v: &mut Value) {
    match v {
        Value::Object(o) => {
            o.remove("duration_ms");
            o.remove("id");
            o.remove("used_bytes");
            o.values_mut().for_each(scrub);
        }
        Value::Array(a) => a.iter_mut().for_each(scrub),
        _ => {}
    }
}

fn assert_disclosed(method: &str, v: &Value) {
    assert_eq!(v["requested_qualname"], INEXACT, "{method}: {v}");
    assert_eq!(v["resolved_qualname"], EXACT, "{method}: {v}");
    assert_eq!(v["resolved_via"], "fuzzy_fallback", "{method}: {v}");
    assert_eq!(v["exact_match"], false, "{method}: {v}");
    let warnings = v["warnings"]
        .as_array()
        .unwrap_or_else(|| panic!("{method}: no warnings array: {v}"));
    assert!(
        warnings.iter().any(|w| {
            let w = w.as_str().unwrap_or_default();
            w.contains(INEXACT) && w.contains(EXACT) && w.contains("inexact")
        }),
        "{method}: warnings should name the substitution: {warnings:?}"
    );
}

#[test]
fn every_qualname_method_reports_an_inexact_match_at_the_top_level() {
    let (temp, _idx) = indexed_repo("py_mvp");
    for (method, param, _) in METHODS {
        let v = call(&temp, method, &params(param, INEXACT));
        assert_disclosed(method, &v);
        // The disclosed resolved qualname is the one the method itself
        // reports, and differs from what was asked for.
        assert_eq!(own_qualname(method, &v), EXACT, "{method}");
        assert_ne!(v["requested_qualname"], v["resolved_qualname"], "{method}");
    }
}

/// Exact-qualname responses are compared against a snapshot captured from
/// the code as it behaved before issue #235 (regenerate deliberately with
/// `UPDATE_GOLDEN=1` only when a response changes on purpose).
#[test]
fn exact_qualname_hit_matches_pre_change_golden() {
    let (temp, _idx) = indexed_repo("py_mvp");
    let mut actual = serde_json::Map::new();
    for (method, param, _) in METHODS {
        let v = call(&temp, method, &params(param, EXACT));
        for key in DISCLOSURE_KEYS {
            assert!(v.get(key).is_none(), "{method}: exact hit has {key}: {v}");
        }
        let mut v = v;
        if *method == "orient" {
            // The rest of orient's response embeds the temp repo path, index
            // timestamps and tie-ordered text, all run-dependent; what the
            // qualname path controls is `focus_symbol` and the key set.
            let mut keys: Vec<&String> = v.as_object().unwrap().keys().collect();
            keys.sort();
            v = serde_json::json!({ "keys": keys, "focus_symbol": v["focus_symbol"] });
        }
        scrub(&mut v);
        actual.insert(method.to_string(), v);
    }
    let actual = serde_json::to_string_pretty(&Value::Object(actual)).unwrap() + "\n";
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("snapshots")
        .join("explicit_qualname_exact.json");
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path).expect("golden snapshot missing");
    assert_eq!(
        actual, expected,
        "exact-qualname responses differ from the pre-change golden"
    );
}

#[test]
fn not_found_qualname_is_unchanged() {
    let (temp, _idx) = indexed_repo("py_mvp");
    for (method, param, _) in METHODS {
        if *method == "orient" {
            // orient has no recovery payload: a miss is a plain error.
            let env = call_raw(&temp, method, &params(param, NO_MATCH));
            assert!(env.get("error").is_some(), "orient: {env}");
            continue;
        }
        let v = call(&temp, method, &params(param, NO_MATCH));
        assert_eq!(v["resolved"], false, "{method}: {v}");
        assert!(
            v["next_hops"].as_array().is_some_and(|h| !h.is_empty()),
            "{method}: not-found keeps its next_hops: {v}"
        );
        for key in DISCLOSURE_KEYS {
            assert!(v.get(key).is_none(), "{method}: {key} on a miss: {v}");
        }
    }
}

#[test]
fn query_reports_resolved_qualname_without_marking_inexact() {
    let (temp, _idx) = indexed_repo("py_mvp");
    for (method, _, query_param) in METHODS {
        let v = call(&temp, method, &params(query_param, INEXACT));
        assert_eq!(v["resolved_qualname"], EXACT, "{method}: {v}");
        assert_eq!(own_qualname(method, &v), EXACT, "{method}");
        for key in ["requested_qualname", "resolved_via", "exact_match"] {
            assert!(v.get(key).is_none(), "{method}: query has {key}: {v}");
        }
        let has_substitution_warning = v["warnings"]
            .as_array()
            .is_some_and(|w| w.iter().any(|w| w.to_string().contains("inexact")));
        assert!(!has_substitution_warning, "{method}: {v}");
    }
}

#[test]
fn disclosure_survives_truncation_envelope() {
    let (temp, _idx) = indexed_repo("py_mvp");
    for (method, param) in [
        ("trace_flow", "start_qualname"),
        ("analyze_impact", "qualname"),
    ] {
        let raw = call_raw(
            &temp,
            method,
            &serde_json::json!({ param: INEXACT, "max_bytes": 300, "max_response_bytes": 300 })
                .to_string(),
        );
        let result = &raw["result"];
        assert_eq!(result["truncated"], true, "{method}: not wrapped: {result}");
        let data = &result["data"];
        for key in DISCLOSURE_KEYS {
            assert!(data.get(key).is_some(), "{method}: {key} lost: {data}");
        }
        assert_eq!(data["requested_qualname"], INEXACT, "{method}");
        assert_eq!(data["resolved_qualname"], EXACT, "{method}");
        assert_eq!(data["exact_match"], false, "{method}");
    }
}

#[test]
fn disclosure_survives_compact_format() {
    let (temp, _idx) = indexed_repo("py_mvp");
    let v = call(
        &temp,
        "trace_flow",
        &serde_json::json!({ "start_qualname": INEXACT, "format": "compact" }).to_string(),
    );
    assert_disclosed("trace_flow", &v);
    // The MCP compact text mode serializes the same value with
    // `serde_json::to_string`, so the fields are present in its text too.
    let text = serde_json::to_string(&v).unwrap();
    for key in DISCLOSURE_KEYS {
        assert!(text.contains(&format!("\"{key}\"")), "{key} missing");
    }
}
