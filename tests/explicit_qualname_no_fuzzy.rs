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
/// resolver: (method, request param name that carries the qualname). The
/// unit test `qualname_selector_methods_are_enumerated` in `rpc/mod.rs` pins
/// this list to the method schemas, so a new qualname-taking method can't be
/// added without also being added here.
const METHODS: &[(&str, &str)] = &[
    ("read_symbol", "qualname"),
    ("explain_symbol", "qualname"),
    ("trace_flow", "start_qualname"),
    ("analyze_impact", "qualname"),
    ("orient", "focus_qualname"),
];

fn params(param: &str, value: &str) -> String {
    serde_json::json!({ param: value }).to_string()
}

/// The qualname the response says it resolved to.
fn returned_qualname(method: &str, v: &Value) -> String {
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

/// Where the substitution fields live: the response object itself, except
/// `orient`, which nests the resolved symbol under `focus_symbol`.
fn disclosure<'a>(method: &str, v: &'a Value) -> &'a Value {
    if method == "orient" {
        &v["focus_symbol"]
    } else {
        v
    }
}

fn has_disclosure(method: &str, v: &Value) -> bool {
    let d = disclosure(method, v);
    ["requested_qualname", "resolved_via", "exact_match"]
        .iter()
        .any(|k| d.get(k).is_some())
}

#[test]
fn every_qualname_method_reports_an_inexact_match() {
    let (temp, _idx) = indexed_repo("py_mvp");
    for (method, param) in METHODS {
        let v = call(&temp, method, &params(param, INEXACT));
        let d = disclosure(method, &v);
        assert_eq!(
            d["requested_qualname"], INEXACT,
            "{method}: requested_qualname missing: {v}"
        );
        assert_eq!(d["exact_match"], false, "{method}: exact_match: {v}");
        assert_eq!(d["resolved_via"], "fuzzy_fallback", "{method}: {v}");
        // Substitution is detectable by comparing requested and returned
        // qualnames alone.
        let returned = returned_qualname(method, &v);
        assert_ne!(returned, INEXACT, "{method}: returned equals requested");
        assert_eq!(returned, EXACT, "{method}");
    }
}

#[test]
fn explain_symbol_warns_about_the_substitution() {
    let (temp, _idx) = indexed_repo("py_mvp");
    let v = call(&temp, "explain_symbol", &params("qualname", INEXACT));
    let warnings = v["warnings"].as_array().expect("warnings array");
    assert!(
        warnings.iter().any(|w| {
            let w = w.as_str().unwrap_or_default();
            w.contains(INEXACT) && w.contains(EXACT) && w.contains("inexact")
        }),
        "warnings should name the substitution: {warnings:?}"
    );
}

#[test]
fn exact_qualname_hit_is_unchanged() {
    let (temp, _idx) = indexed_repo("py_mvp");
    for (method, param) in METHODS {
        let exact = call(&temp, method, &params(param, EXACT));
        assert!(
            !has_disclosure(method, &exact),
            "{method}: exact hit gained disclosure fields: {exact}"
        );
        // Byte-identical to resolving the same symbol without the qualname
        // fallback path (query for read_symbol/orient, id otherwise).
        let other = match *method {
            "read_symbol" | "orient" => {
                let alt = if *method == "read_symbol" {
                    "query"
                } else {
                    "focus_query"
                };
                call(&temp, method, &params(alt, EXACT))
            }
            _ => {
                let id = match *method {
                    "explain_symbol" => exact["symbol"]["id"].as_i64().unwrap(),
                    "trace_flow" => exact["start"]["id"].as_i64().unwrap(),
                    _ => exact["seeds"][0]["id"].as_i64().unwrap(),
                };
                let key = if *method == "trace_flow" {
                    "start_id"
                } else {
                    "id"
                };
                call(&temp, method, &serde_json::json!({ key: id }).to_string())
            }
        };
        // `next_hops` legitimately echo the request's own selector
        // (start_qualname vs start_id), so they are excluded from the
        // comparison, as do wall-clock `duration_ms` timings and the
        // order of `by_file`; everything
        // else must match byte for byte.
        fn scrub(v: &mut Value) {
            match v {
                Value::Object(o) => {
                    o.remove("duration_ms");
                    // `summary.by_file` comes out of a HashMap in
                    // run-dependent order.
                    if let Some(Value::Array(files)) = o.get_mut("by_file") {
                        files.sort_by_key(|f| f.to_string());
                    }
                    o.values_mut().for_each(scrub);
                }
                Value::Array(a) => a.iter_mut().for_each(scrub),
                _ => {}
            }
        }
        let strip = |mut v: Value| {
            if let Some(o) = v.as_object_mut() {
                o.remove("next_hops");
            }
            scrub(&mut v);
            serde_json::to_string(&v).unwrap()
        };
        assert_eq!(
            strip(exact),
            strip(other),
            "{method}: exact-qualname response differs from the unannotated resolution"
        );
    }
}

#[test]
fn not_found_qualname_is_unchanged() {
    let (temp, _idx) = indexed_repo("py_mvp");
    for (method, param) in METHODS {
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
        assert!(!has_disclosure(method, &v), "{method}: {v}");
    }
}

#[test]
fn query_resolution_is_not_marked_inexact() {
    let (temp, _idx) = indexed_repo("py_mvp");
    let query_params = [
        ("read_symbol", "query"),
        ("explain_symbol", "query"),
        ("trace_flow", "query"),
        ("analyze_impact", "query"),
        ("orient", "focus_query"),
    ];
    for (method, param) in query_params {
        let v = call(&temp, method, &params(param, INEXACT));
        assert!(
            !has_disclosure(method, &v),
            "{method}: a query is fuzzy by contract: {v}"
        );
        assert_eq!(returned_qualname(method, &v), EXACT, "{method}");
    }
}
