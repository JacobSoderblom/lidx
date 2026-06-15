/// Integration test: every follow-up method a handler suggests (`next_hops[].method`
/// and `suggested_queries[].method`) must be a dispatchable method in `METHOD_LIST`.
/// This catches dead hops before they reach production and fail silently when an
/// LLM follows the suggestion.
///
/// Covers the handlers that emit hop suggestions: explain_symbol, analyze_diff,
/// trace_flow (non-empty and empty-trace branches), and onboard.
///
/// Fixture used: py_mvp — a small Python repo with a Greeter class, so the handlers
/// find real symbols. analyze_diff is exercised via the `paths` param pointing at a
/// file that contains a method/function, which is the condition that fires the
/// upstream-callers hop (formerly the dead `references` hop).
use lidx::indexer::Indexer;
use lidx::rpc::{self, METHOD_LIST};
use std::collections::HashSet;
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
    dir.push(format!("lidx-next-hops-{label}-{nanos}-{counter}"));
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

/// Keys whose array elements are LLM-followable method suggestions.
const HOP_KEYS: &[&str] = &["next_hops", "suggested_queries"];

/// Recursively collect every suggested `method` string from the response.
fn collect_hop_methods(value: &serde_json::Value) -> Vec<String> {
    let mut methods = Vec::new();
    collect_hop_methods_inner(value, &mut methods);
    methods
}

fn collect_hop_methods_inner(value: &serde_json::Value, out: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, v) in map {
                if HOP_KEYS.contains(&key.as_str())
                    && let Some(arr) = v.as_array()
                {
                    for hop in arr {
                        if let Some(m) = hop.get("method").and_then(|v| v.as_str()) {
                            out.push(m.to_string());
                        }
                    }
                }
                collect_hop_methods_inner(v, out);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                collect_hop_methods_inner(v, out);
            }
        }
        _ => {}
    }
}

/// Call an RPC method and return its `result`. Panics if the call returned an
/// error envelope, so hop assertions can never pass vacuously against a Null result.
fn call_and_get_result(temp: &TempRepo, method: &str, params: &str) -> serde_json::Value {
    let raw = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        method.to_string(),
        params,
        "1",
    )
    .unwrap();
    let envelope: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "rpc call '{method}' with params {params} returned an error: {envelope}"
    );
    let result = envelope["result"].clone();
    assert!(
        !result.is_null(),
        "rpc call '{method}' with params {params} returned no result: {envelope}"
    );
    result
}

/// Assert every hop method in `result` is dispatchable, and return the emitted hops
/// so callers can additionally assert the handler emitted any at all.
fn assert_all_hops_valid(result: &serde_json::Value, handler: &str) -> Vec<String> {
    let valid: HashSet<&str> = METHOD_LIST.iter().copied().collect();
    let emitted = collect_hop_methods(result);
    for method in &emitted {
        assert!(
            valid.contains(method.as_str()),
            "handler '{}' emitted hop with method '{}' which is not in METHOD_LIST.\n\
             METHOD_LIST: {:?}\n\
             All emitted hops: {:?}",
            handler,
            method,
            METHOD_LIST,
            emitted,
        );
    }
    emitted
}

#[test]
fn all_next_hops_methods_are_dispatchable() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    // explain_symbol — emits analyze_impact and gather_context hops
    let result = call_and_get_result(
        &temp,
        "explain_symbol",
        r#"{"qualname":"pkg.core.Greeter"}"#,
    );
    let hops = assert_all_hops_valid(&result, "explain_symbol");
    assert!(
        !hops.is_empty(),
        "explain_symbol emitted no hops; the validity check exercised nothing"
    );

    // analyze_diff — emits explain_symbol plus analyze_impact (upstream + both) hops.
    // The upstream hop (formerly the dead `references` hop) only fires when a changed
    // symbol is a method/function, so assert the fixture still satisfies that.
    let result = call_and_get_result(&temp, "analyze_diff", r#"{"paths":["pkg/core.py"]}"#);
    let has_function_change = result["changed_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .any(|cs| matches!(cs["symbol"]["kind"].as_str(), Some("method" | "function")));
    assert!(
        has_function_change,
        "fixture no longer produces a changed method/function; the upstream-callers hop branch is untested"
    );
    let hops = assert_all_hops_valid(&result, "analyze_diff");
    assert!(
        !hops.is_empty(),
        "analyze_diff emitted no hops; the validity check exercised nothing"
    );

    // trace_flow — emits explain_symbol hops per trace hop, plus pagination/
    // empty-trace-alternative hops depending on the trace
    let result = call_and_get_result(
        &temp,
        "trace_flow",
        r#"{"start_qualname":"pkg.core.make_greeter"}"#,
    );
    let hops = assert_all_hops_valid(&result, "trace_flow");
    assert!(
        !hops.is_empty(),
        "trace_flow emitted no hops; the validity check exercised nothing"
    );

    // trace_flow from a leaf symbol — exercises the empty-trace alternatives branch
    // (analyze_impact + CONFIG-only retrace suggestions) when the trace is empty
    let result = call_and_get_result(&temp, "trace_flow", r#"{"start_qualname":"pkg.core.Base"}"#);
    assert_all_hops_valid(&result, "trace_flow (leaf symbol)");

    // onboard — emits suggested_queries with method pointers
    let result = call_and_get_result(&temp, "onboard", r#"{}"#);
    let hops = assert_all_hops_valid(&result, "onboard");
    assert!(
        !hops.is_empty(),
        "onboard emitted no suggested_queries; the validity check exercised nothing"
    );
}
