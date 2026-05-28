use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::json;
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
    dir.push(format!("lidx-trace-{label}-{nanos}-{counter}"));
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

fn indexed(fixture: &str) -> (TempRepo, Indexer) {
    let temp = TempRepo::new(fixture);
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    (temp, indexer)
}

#[test]
fn trace_flow_by_query_returns_required_fields() {
    let (_temp, mut indexer) = indexed("py_mvp");

    let result = rpc::handle_method(&mut indexer, "trace_flow", json!({"query": "run"})).unwrap();

    assert!(result["start"]["id"].is_number());
    assert!(result["trace"].is_array());
    assert!(result["paths_found"].is_number());
    assert!(result["reached_target"].is_boolean());
    assert!(result["truncated"].is_boolean());
    assert!(result["budget"]["budget_bytes"].is_number());
    assert!(result["budget"]["used_bytes"].is_number());
}

#[test]
fn trace_flow_by_qualname_resolves_symbol() {
    let (_temp, mut indexer) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "trace_flow",
        json!({"start_qualname": "app.run"}),
    )
    .unwrap();

    assert_eq!(result["start"]["name"], "run");
    assert!(result["trace"].is_array());
}

#[test]
fn trace_flow_by_id_resolves_symbol() {
    let (_temp, mut indexer) = indexed("py_mvp");

    let sym = rpc::handle_method(&mut indexer, "explain_symbol", json!({"query": "run"})).unwrap();
    let id = sym["symbol"]["id"].as_i64().unwrap();

    let result = rpc::handle_method(&mut indexer, "trace_flow", json!({"start_id": id})).unwrap();

    assert_eq!(result["start"]["id"], id);
}

#[test]
fn trace_flow_missing_start_returns_error() {
    let (_temp, mut indexer) = indexed("py_mvp");

    let err = rpc::handle_method(&mut indexer, "trace_flow", json!({}));
    assert!(err.is_err());
    assert!(
        err.unwrap_err()
            .to_string()
            .contains("start_id, start_qualname, or query")
    );
}

#[test]
fn trace_flow_nonexistent_id_returns_error() {
    let (_temp, mut indexer) = indexed("py_mvp");

    let err = rpc::handle_method(&mut indexer, "trace_flow", json!({"start_id": 999999}));
    assert!(err.is_err());
}

#[test]
fn trace_flow_empty_trace_suggests_alternatives() {
    let (_temp, mut indexer) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "trace_flow",
        json!({
            "query": "helper",
            "kinds": ["NONEXISTENT_KIND"],
        }),
    )
    .unwrap();

    let trace = result["trace"].as_array().unwrap();
    assert!(trace.is_empty());

    let next_hops = result["next_hops"].as_array().unwrap();
    let methods: Vec<&str> = next_hops
        .iter()
        .filter_map(|h| h["method"].as_str())
        .collect();
    assert!(
        methods.contains(&"analyze_impact"),
        "empty trace should suggest analyze_impact, got: {:?}",
        methods
    );
}

#[test]
fn trace_flow_truncated_suggests_continuation() {
    let (_temp, mut indexer) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "trace_flow",
        json!({
            "query": "run",
            "max_bytes": 1,
        }),
    )
    .unwrap();

    assert!(result["truncated"].as_bool().unwrap());

    let next_hops = result["next_hops"].as_array().unwrap();
    let methods: Vec<&str> = next_hops
        .iter()
        .filter_map(|h| h["method"].as_str())
        .collect();
    assert!(
        methods.contains(&"trace_flow"),
        "truncated trace should suggest continuation, got: {:?}",
        methods
    );
}

#[test]
fn trace_flow_compact_format_shrinks_symbols() {
    let (_temp, mut indexer) = indexed("py_mvp");

    let full = rpc::handle_method(
        &mut indexer,
        "trace_flow",
        json!({"query": "run", "format": "full"}),
    )
    .unwrap();

    let compact = rpc::handle_method(
        &mut indexer,
        "trace_flow",
        json!({"query": "run", "format": "compact"}),
    )
    .unwrap();

    let full_str = serde_json::to_string(&full).unwrap();
    let compact_str = serde_json::to_string(&compact).unwrap();
    assert!(
        compact_str.len() <= full_str.len(),
        "compact format should not be larger than full format"
    );
}

#[test]
fn trace_flow_container_start_succeeds() {
    let (_temp, mut indexer) = indexed("py_mvp");

    let result =
        rpc::handle_method(&mut indexer, "trace_flow", json!({"query": "Greeter"})).unwrap();

    assert_eq!(result["start"]["kind"], "class");
    assert!(result["trace"].is_array());
    assert!(result["budget"]["budget_bytes"].is_number());
}

#[test]
fn trace_flow_config_uri_resolves_seeds() {
    let (_temp, mut indexer) = indexed("py_config");

    let result = rpc::handle_method(
        &mut indexer,
        "trace_flow",
        json!({"start_qualname": "env://DATABASE_URL"}),
    );

    match result {
        Ok(val) => {
            assert!(val["start"]["id"].is_number());
        }
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("no symbols found for config URI"),
                "config URI error should be descriptive: {msg}"
            );
        }
    }
}

#[test]
fn trace_flow_direction_upstream() {
    let (_temp, mut indexer) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "trace_flow",
        json!({
            "query": "helper",
            "direction": "upstream",
        }),
    )
    .unwrap();

    assert!(result["trace"].is_array());
}

#[test]
fn trace_flow_max_hops_limits_depth() {
    let (_temp, mut indexer) = indexed("py_mvp");

    let shallow = rpc::handle_method(
        &mut indexer,
        "trace_flow",
        json!({"query": "run", "max_hops": 1}),
    )
    .unwrap();

    let deep = rpc::handle_method(
        &mut indexer,
        "trace_flow",
        json!({"query": "run", "max_hops": 10}),
    )
    .unwrap();

    let shallow_count = shallow["trace"].as_array().unwrap().len();
    let deep_count = deep["trace"].as_array().unwrap().len();
    assert!(
        shallow_count <= deep_count,
        "shallow trace ({}) should have <= hops than deep ({})",
        shallow_count,
        deep_count
    );
}

#[test]
fn trace_flow_kinds_filter_restricts_edges() {
    let (_temp, mut indexer) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "trace_flow",
        json!({
            "query": "run",
            "kinds": ["NONEXISTENT_KIND"],
        }),
    )
    .unwrap();

    let trace = result["trace"].as_array().unwrap();
    assert!(
        trace.is_empty(),
        "filtering by nonexistent kind should yield empty trace"
    );
}
