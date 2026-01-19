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
    dir.push(format!("lidx-gather-{label}-{nanos}-{counter}"));
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

fn setup_repo(fixture: &str) -> (PathBuf, PathBuf) {
    let src = fixture_path(fixture);
    let repo_root = temp_repo_dir(fixture);
    copy_dir(&src, &repo_root);
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    (repo_root, db_path)
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
        let (repo_root, db_path) = setup_repo(fixture);
        Self { repo_root, db_path }
    }
}

#[test]
fn gather_context_returns_symbol_content() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        r#"{"seeds":[{"type":"symbol","qualname":"pkg.core.Greeter"}]}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    assert!(!result["items"].as_array().unwrap().is_empty());
    assert!(result["total_bytes"].as_u64().unwrap() > 0);
    assert!(!result["truncated"].as_bool().unwrap());
}

#[test]
fn gather_context_respects_byte_budget() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        r#"{"seeds":[{"type":"symbol","qualname":"pkg.core.Greeter"}],"max_bytes":50}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    assert!(result["total_bytes"].as_u64().unwrap() <= 50);
}

#[test]
fn gather_context_deduplicates_overlapping_regions() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Request same symbol twice - should only appear once
    // Disable include_related to avoid subgraph expansion adding more duplicates
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        r#"{"seeds":[
            {"type":"symbol","qualname":"pkg.core.Greeter"},
            {"type":"symbol","qualname":"pkg.core.Greeter"}
        ],"include_related":false}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let items = value["result"]["items"].as_array().unwrap();

    // Count items with this qualname
    let greeter_count = items
        .iter()
        .filter(|item| item["symbol"]["qualname"].as_str() == Some("pkg.core.Greeter"))
        .count();

    assert_eq!(greeter_count, 1);

    // Verify deduplication happened (exact count may vary due to subgraph)
    // but at least one item should be deduplicated
    let metadata = value["result"]["metadata"].as_object().unwrap();
    assert!(metadata["items_deduplicated"].as_u64().unwrap() >= 1);
}

#[test]
fn gather_context_expands_subgraph() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        r#"{"seeds":[{"type":"symbol","qualname":"app"}],"include_related":true,"depth":2}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let items = value["result"]["items"].as_array().unwrap();

    // Should include related symbols from call graph
    let has_subgraph = items
        .iter()
        .any(|item| item["source"]["source_type"].as_str() == Some("subgraph"));

    assert!(has_subgraph);
}

#[test]
fn gather_context_handles_search_seeds() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        r#"{"seeds":[{"type":"search","query":"greet","limit":3}]}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    assert!(!result["items"].as_array().unwrap().is_empty());
}

#[test]
fn gather_context_handles_file_seeds() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        r#"{"seeds":[{"type":"file","path":"pkg/core.py","start_line":1,"end_line":10}]}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let items = value["result"]["items"].as_array().unwrap();

    assert!(!items.is_empty());
    assert_eq!(items[0]["path"].as_str().unwrap(), "pkg/core.py");
}

#[test]
fn gather_context_output_is_deterministic() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let params = r#"{"seeds":[
        {"type":"symbol","qualname":"pkg.core.Greeter"},
        {"type":"symbol","qualname":"app"}
    ],"include_related":true}"#;

    let response1 = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        params,
        "1",
    )
    .unwrap();

    let response2 = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        params,
        "1",
    )
    .unwrap();

    // Parse and compare items (ignore timing metadata)
    let value1: serde_json::Value = serde_json::from_str(&response1).unwrap();
    let value2: serde_json::Value = serde_json::from_str(&response2).unwrap();

    let items1 = value1["result"]["items"].as_array().unwrap();
    let items2 = value2["result"]["items"].as_array().unwrap();

    assert_eq!(items1.len(), items2.len());
    for (a, b) in items1.iter().zip(items2.iter()) {
        assert_eq!(a["path"], b["path"]);
        assert_eq!(a["start_byte"], b["start_byte"]);
        assert_eq!(a["content"], b["content"]);
    }
}

#[test]
fn gather_context_rejects_path_traversal() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        r#"{"seeds":[{"type":"file","path":"../../../etc/passwd"}]}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let items = value["result"]["items"].as_array().unwrap();
    assert!(items.is_empty()); // Path should be silently skipped
}

#[test]
fn gather_context_enforces_hard_cap_on_max_bytes() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Request 10MB, should be capped at 2MB
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        r#"{"seeds":[{"type":"symbol","qualname":"pkg.core.Greeter"}],"max_bytes":10000000}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    // Budget should be capped at 2MB
    assert_eq!(result["budget_bytes"].as_u64().unwrap(), 2_000_000);
}

#[test]
fn gather_context_rejects_too_many_seeds() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Create 101 seeds (exceeds limit of 100)
    let mut seeds = Vec::new();
    for _ in 0..101 {
        seeds.push(r#"{"type":"symbol","qualname":"pkg.core.Greeter"}"#);
    }
    let seeds_json = format!(r#"{{"seeds":[{}]}}"#, seeds.join(","));

    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        &seeds_json,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    // Should return an error
    assert!(value["error"].is_object());
    assert!(
        value["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Too many seeds")
    );
}

#[test]
fn gather_context_handles_stale_files() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Modify a file after indexing to make it stale
    let file_path = temp.repo_root.join("pkg/core.py");
    let mut content = std::fs::read_to_string(&file_path).unwrap();
    content.push_str("\n# Modified after indexing\n");
    std::fs::write(&file_path, content).unwrap();

    // Request should skip stale content gracefully
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        r#"{"seeds":[{"type":"symbol","qualname":"pkg.core.Greeter"}]}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();

    // Should return successfully (symbols may be skipped due to stale content)
    assert!(result["items"].is_array());
}

#[test]
fn gather_context_uses_symbol_strategy_by_default() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Request with symbol seeds - should default to "symbol" strategy
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        r#"{"seeds":[{"type":"symbol","qualname":"pkg.core.Greeter"}],"include_related":false}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let items = value["result"]["items"].as_array().unwrap();

    assert!(!items.is_empty());

    // In symbol strategy, content should include symbol body with file header
    let content = items[0]["content"].as_str().unwrap();
    assert!(content.contains("// Symbol:") || content.contains("class Greeter"));
}

#[test]
fn gather_context_explicit_symbol_strategy() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Explicitly request symbol strategy
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        r#"{"seeds":[{"type":"symbol","qualname":"pkg.core.Greeter"}],"strategy":"symbol","include_related":false}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let items = value["result"]["items"].as_array().unwrap();

    assert!(!items.is_empty());

    // Should have content (not just metadata)
    let content = items[0]["content"].as_str().unwrap();
    assert!(!content.is_empty());
}

#[test]
fn gather_context_file_strategy_uses_full_files() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Explicitly request file strategy
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        r#"{"seeds":[{"type":"symbol","qualname":"pkg.core.Greeter"}],"strategy":"file","include_related":false}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let items = value["result"]["items"].as_array().unwrap();

    assert!(!items.is_empty());

    // File strategy should still work as before
    let content = items[0]["content"].as_str().unwrap();
    assert!(!content.is_empty());
}
