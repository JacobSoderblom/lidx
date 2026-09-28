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

/// Execute a next_hop suggestion and return its `result` -- panics on an error
/// envelope, so a hop whose params don't actually validate against the target
/// method's schema fails loudly here rather than passing silently.
fn follow_hop(temp: &TempRepo, hop: &serde_json::Value) -> serde_json::Value {
    let method = hop["method"].as_str().expect("hop must have a method");
    let params = serde_json::to_string(&hop["params"]).unwrap();
    call_and_get_result(temp, method, &params)
}

fn rg_available() -> bool {
    std::process::Command::new("rg")
        .arg("--version")
        .output()
        .is_ok()
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

    // explain_symbol — emits analyze_impact and gather_context hops, plus
    // (#97) a read_symbol hop for the resolved symbol.
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
    let next_hops = result["next_hops"].as_array().unwrap();
    let read_symbol_hop = next_hops
        .iter()
        .find(|h| h["method"].as_str() == Some("read_symbol"))
        .expect("explain_symbol must emit a read_symbol hop for the resolved symbol");
    assert_eq!(
        read_symbol_hop["params"]["qualname"],
        serde_json::json!("pkg.core.Greeter"),
        "read_symbol hop must target the resolved symbol's qualname, got: {}",
        read_symbol_hop
    );
    // The hop's params must actually validate against read_symbol: following it
    // must resolve back to the same symbol and return its source, not error.
    let followed = follow_hop(&temp, read_symbol_hop);
    assert_eq!(
        followed["qualname"],
        serde_json::json!("pkg.core.Greeter"),
        "following the read_symbol hop must read the same symbol explain_symbol resolved, got: {}",
        followed
    );
    assert!(
        followed["source"].as_str().is_some_and(|s| !s.is_empty()),
        "following the read_symbol hop must return non-empty source, got: {}",
        followed
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

/// Writes `count` python files under a fresh temp repo, each containing a
/// shared marker string so a single search query matches all of them. The
/// first file matches on two separate lines, so the fixture also exercises
/// "one hop per file, not per hit" within a single file.
fn many_files_repo(count: usize) -> TempRepo {
    let dir = temp_repo_dir("many-files");
    for i in 0..count {
        let body = if i == 0 {
            "def marker_a():\n    return \"NEXT_HOPS_SEARCH_MARKER\"\n\n\ndef marker_b():\n    return \"NEXT_HOPS_SEARCH_MARKER\"\n"
        } else {
            "def marker():\n    return \"NEXT_HOPS_SEARCH_MARKER\"\n"
        };
        std::fs::write(dir.join(format!("file_{i}.py")), body).unwrap();
    }
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    let repo = TempRepo {
        repo_root: dir,
        db_path,
    };
    let mut indexer = Indexer::new(repo.repo_root.clone(), repo.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);
    repo
}

/// #97: `search` hits point at `outline` for their file, one hop per distinct
/// file (not per hit) -- no cap: every distinct matching file gets one.
#[test]
fn search_hits_include_outline_hops_deduplicated_per_file() {
    if !rg_available() {
        return; // rg not available -- skip
    }
    // 7 distinct files match; file_0.py matches twice, on two separate lines.
    let temp = many_files_repo(7);

    let result = call_and_get_result(
        &temp,
        "search",
        r#"{"query":"NEXT_HOPS_SEARCH_MARKER","limit":50}"#,
    );
    let hits = result
        .as_array()
        .expect("non-empty search must stay a bare array (unchanged format)");
    assert_eq!(
        hits.len(),
        8,
        "expected 8 hits (7 files, one with 2 matches), got: {:?}",
        hits
    );

    let hops = assert_all_hops_valid(&result, "search");
    assert!(
        !hops.is_empty(),
        "search emitted no hops; the validity check exercised nothing"
    );
    assert!(
        hops.iter().all(|m| m == "outline"),
        "search hits should only emit outline hops, got: {:?}",
        hops
    );

    // Collect the file path named by each hit that carries a hop, in hit order.
    let mut hop_paths: Vec<String> = Vec::new();
    for hit in hits {
        let Some(hop_list) = hit.get("next_hops").and_then(|v| v.as_array()) else {
            continue;
        };
        assert_eq!(
            hop_list.len(),
            1,
            "each hopped hit should carry exactly one outline hop, got: {:?}",
            hop_list
        );
        let hop = &hop_list[0];
        assert_eq!(hop["method"], serde_json::json!("outline"));
        assert_eq!(
            hop["params"]["path"], hit["path"],
            "outline hop must point at the hit's own file, got hop: {} hit: {}",
            hop, hit
        );
        hop_paths.push(hop["params"]["path"].as_str().unwrap().to_string());
    }

    // One hop per distinct file, uncapped: 7 files matched (8 hits, one file
    // matching twice), so 7 hits should carry a hop.
    assert_eq!(
        hop_paths.len(),
        7,
        "expected exactly 7 hits to carry an outline hop (one per distinct file, no cap), got {}: {:?}",
        hop_paths.len(),
        hop_paths
    );

    // Deduplicated: no file's hop appears twice, so file_0.py's second hit
    // (same file as its first) must not have added a second hop.
    let unique: HashSet<&String> = hop_paths.iter().collect();
    assert_eq!(
        unique.len(),
        hop_paths.len(),
        "outline hops must be deduplicated per file, got: {:?}",
        hop_paths
    );

    // The hop's params must actually validate against outline: following it
    // must succeed and outline the same file it named.
    let first_hop = hits
        .iter()
        .find_map(|h| h.get("next_hops"))
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .expect("at least one hit carries an outline hop");
    let followed = follow_hop(&temp, first_hop);
    assert_eq!(
        followed["path"], first_hop["params"]["path"],
        "following the outline hop must outline the file it named, got: {}",
        followed
    );
}

/// #97/standards follow-up: a search hit whose file `outline` can't handle
/// (not an indexed language, not Markdown -- e.g. Cargo.toml, a .json file)
/// must not carry an outline hop; a hit for an outline-able file still does.
#[test]
fn search_hits_skip_outline_hop_for_non_outlineable_files() {
    if !rg_available() {
        return; // rg not available -- skip
    }
    let dir = temp_repo_dir("non-outlineable");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"NEXT_HOPS_SEARCH_MARKER\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("data.json"),
        "{\"key\": \"NEXT_HOPS_SEARCH_MARKER\"}\n",
    )
    .unwrap();
    std::fs::write(dir.join("main.py"), "# NEXT_HOPS_SEARCH_MARKER\n").unwrap();
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    let repo = TempRepo {
        repo_root: dir,
        db_path,
    };
    let mut indexer = Indexer::new(repo.repo_root.clone(), repo.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    let result = call_and_get_result(
        &repo,
        "search",
        r#"{"query":"NEXT_HOPS_SEARCH_MARKER","limit":50}"#,
    );
    let hits = result.as_array().expect("non-empty search stays an array");
    assert_eq!(hits.len(), 3, "{:?}", hits);

    for hit in hits {
        let path = hit["path"].as_str().unwrap();
        let has_hop = hit.get("next_hops").is_some();
        if path.ends_with(".py") {
            assert!(
                has_hop,
                "an outline-able .py file should carry an outline hop: {hit:#?}"
            );
        } else {
            assert!(
                !has_hop,
                "a non-outline-able file ('{path}') should not carry an outline hop: {hit:#?}"
            );
        }
    }
}
