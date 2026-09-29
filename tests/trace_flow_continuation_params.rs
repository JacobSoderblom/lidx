/// Issue #119: trace_flow's "Continue trace" pagination next_hop must echo
/// every original request param plus the updated `trace_offset`, not a
/// hand-picked subset. The old implementation only copied `max_hops`,
/// `include_snippets`, `start_qualname`/`start_id`, `kinds`, and `format` --
/// dropping `direction`, `max_bytes`, `exclude_resolution_kinds`,
/// `languages`, and `end_qualname`, and leaving a `query`-started trace with
/// no start param at all in the continuation.
use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};
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
    dir.push(format!("lidx-trace-continuation-{label}-{nanos}-{counter}"));
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

fn call_and_get_result(temp: &TempRepo, method: &str, params: &str) -> Value {
    let raw = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        method.to_string(),
        params,
        "1",
    )
    .unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
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

/// The continuation hop must round-trip every param the caller originally
/// sent -- including a `query`-only start (no `start_qualname`/`start_id`
/// at all) -- and override only `trace_offset`.
#[test]
fn continuation_hop_echoes_every_original_param() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    // `max_bytes: 1` forces truncation on the very first hop (same technique
    // as `traversal::tests::byte_budget_truncation`), regardless of which
    // params are otherwise set. Started via `query` (not start_qualname/
    // start_id) -- the case the issue calls out as ending up with no start
    // at all in the continuation.
    let original = json!({
        "query": "run",
        "direction": "downstream",
        "max_hops": 3,
        "max_bytes": 1,
        "include_snippets": true,
        "exclude_resolution_kinds": ["bare_name"],
        "languages": ["python"],
        "kinds": ["CALLS"],
        "end_qualname": "pkg.utils.Helper.__init__",
    });

    let result = call_and_get_result(&temp, "trace_flow", &original.to_string());
    assert_eq!(
        result["truncated"],
        json!(true),
        "fixture must actually truncate for this test to exercise the continuation hop, got: {result}"
    );

    let next_hops = result["next_hops"]
        .as_array()
        .expect("trace_flow must emit next_hops when truncated");
    let continue_hop = next_hops
        .iter()
        .find(|h| {
            h["description"]
                .as_str()
                .is_some_and(|d| d.starts_with("Continue trace"))
        })
        .expect("truncated trace_flow must emit a 'Continue trace' next_hop");

    assert_eq!(continue_hop["method"], json!("trace_flow"));
    let continue_params = continue_hop["params"]
        .as_object()
        .expect("continuation hop params must be an object");

    // Every original param must be echoed back verbatim.
    let original_obj = original.as_object().unwrap();
    for (key, value) in original_obj {
        assert_eq!(
            continue_params.get(key),
            Some(value),
            "continuation params dropped or changed '{key}': got {continue_params:#?}"
        );
    }

    // Plus trace_offset, advanced past the hops already returned.
    let trace_len = result["trace"].as_array().map(Vec::len).unwrap_or(0);
    assert_eq!(
        continue_params.get("trace_offset"),
        Some(&json!(trace_len)),
        "continuation params must set trace_offset to the number of hops already returned, got: {continue_params:#?}"
    );

    // The continuation hop must actually be followable: since the start was
    // `query`-only, the continuation must still resolve a start symbol.
    let followed_params = serde_json::to_string(&continue_hop["params"]).unwrap();
    let followed = call_and_get_result(&temp, "trace_flow", &followed_params);
    assert!(
        followed.get("error").is_none(),
        "following the continuation hop must not error, got: {followed}"
    );
}
