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

// Regression tests for issue #104: gather_context with a search seed must
// include the matched symbol itself (tagged as the actual search hit, not
// folded into unrelated "subgraph" expansion), must not let unrelated
// expansion starve that match's budget, and must never truncate an item to
// a partial byte-sliced fragment.

fn write_search_seed_fixture(repo_root: &Path) {
    // zzz_target_marker is the unique search match. aaa_caller_fn is a real
    // graph-connected neighbor (via CALLS) whose qualname sorts
    // alphabetically *before* the match, so it previously starved the
    // match's budget when related items were processed in alphabetical
    // order instead of prioritizing the search hit.
    std::fs::write(
        repo_root.join("zzz_target.py"),
        "def zzz_target_marker():\n    return \"UNIQUESEARCHTOKEN42\"\n",
    )
    .unwrap();
    std::fs::write(
        repo_root.join("aaa_caller.py"),
        "from zzz_target import zzz_target_marker\n\n\ndef aaa_caller_fn():\n    return zzz_target_marker()\n",
    )
    .unwrap();
}

#[test]
fn gather_context_search_seed_match_is_tagged_search_and_not_duplicated() {
    let temp = TempRepo::new("py_mvp");
    write_search_seed_fixture(&temp.repo_root);

    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        r#"{"seeds":[{"type":"search","query":"UNIQUESEARCHTOKEN42"}]}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let items = value["result"]["items"].as_array().unwrap();

    // Match on the function's own qualname rather than raw content, since
    // the whole-file "module" symbol enclosing it is a distinct, genuinely
    // graph-connected related item that also happens to contain this
    // single-function file's text -- that's not the duplication being
    // guarded against here.
    let matches: Vec<&serde_json::Value> = items
        .iter()
        .filter(|item| item["symbol"]["qualname"].as_str() == Some("zzz_target.zzz_target_marker"))
        .collect();

    // The search-seed match must appear exactly once, tagged as the actual
    // search hit -- not silently merged into generic subgraph "related"
    // items.
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one item containing the search match, got {items:#?}"
    );
    assert_eq!(
        matches[0]["source"]["source_type"].as_str(),
        Some("search"),
        "search-seed match must be tagged source_type=search"
    );

    // The graph-connected caller must still show up as a related expansion
    // item -- related items must remain graph-connected to the seed.
    let has_caller = items.iter().any(|item| {
        item["content"]
            .as_str()
            .is_some_and(|c| c.contains("aaa_caller_fn"))
            && item["source"]["source_type"].as_str() == Some("subgraph")
    });
    assert!(
        has_caller,
        "expected aaa_caller_fn as a graph-connected related item, got {items:#?}"
    );
}

#[test]
fn gather_context_search_seed_match_wins_tight_budget_without_partial_fragment() {
    let temp = TempRepo::new("py_mvp");
    write_search_seed_fixture(&temp.repo_root);

    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let call = |body: &str| {
        let response = rpc::call(
            temp.repo_root.clone(),
            temp.db_path.clone(),
            "gather_context".to_string(),
            body,
            "1",
        )
        .unwrap();
        serde_json::from_str::<serde_json::Value>(&response).unwrap()
    };

    // Generous budget: capture the full, untruncated content of the match.
    let baseline = call(r#"{"seeds":[{"type":"search","query":"UNIQUESEARCHTOKEN42"}]}"#);
    let baseline_items = baseline["result"]["items"].as_array().unwrap();
    let full_content = baseline_items
        .iter()
        .find(|item| item["symbol"]["qualname"].as_str() == Some("zzz_target.zzz_target_marker"))
        .and_then(|item| item["content"].as_str())
        .expect("search match present with generous budget")
        .to_string();

    // Budget tight enough to hold exactly the match's full content and
    // nothing else.
    let tight_budget = full_content.len();
    let params = format!(
        r#"{{"seeds":[{{"type":"search","query":"UNIQUESEARCHTOKEN42"}}],"max_bytes":{tight_budget}}}"#
    );
    let result = call(&params);
    let items = result["result"]["items"].as_array().unwrap();

    // The match must still win the budget, in full -- never a byte-sliced
    // fragment -- and the unrelated (from this angle) caller must not fit
    // alongside it.
    assert_eq!(
        items.len(),
        1,
        "only the full match should fit in the tight budget, got {items:#?}"
    );
    assert_eq!(items[0]["content"].as_str(), Some(full_content.as_str()));
    assert_eq!(items[0]["source"]["source_type"].as_str(), Some("search"));
    assert!(result["result"]["total_bytes"].as_u64().unwrap() <= tight_budget as u64);
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
    let seeds = vec![r#"{"type":"symbol","qualname":"pkg.core.Greeter"}"#; 101];
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

#[test]
fn gather_context_truncates_multibyte_content_at_budget() {
    let temp = TempRepo::new("py_mvp");

    // File of 2-byte chars with no newline: every odd byte offset is mid-char
    let multibyte_path = temp.repo_root.join("notes.md");
    std::fs::write(&multibyte_path, "é".repeat(100)).unwrap();

    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Budget of 5 bytes lands mid-char in the snippet
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        r#"{"seeds":[{"type":"file","path":"notes.md"}],"max_bytes":5,"include_related":false}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();
    assert!(result["total_bytes"].as_u64().unwrap() <= 5);
}

#[test]
fn gather_context_handles_multibyte_file_header() {
    let temp = TempRepo::new("py_mvp");

    // First line is > 500 bytes of 2-byte chars after a 1-byte "#",
    // so byte 500 falls mid-char when the header is capped
    let header_line = format!("#{}", "é".repeat(300));
    let source = format!("{header_line}\ndef uni_func():\n    return 1\n");
    std::fs::write(temp.repo_root.join("uni.py"), source).unwrap();

    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Symbol strategy reads the file header for tier-0 formatting
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        r#"{"seeds":[{"type":"symbol","qualname":"uni.uni_func"}],"strategy":"symbol","include_related":false}"#,
        "1",
    )
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let items = value["result"]["items"].as_array().unwrap();
    assert!(!items.is_empty());
    assert!(items[0]["content"].as_str().unwrap().contains("uni_func"));
}

#[test]
fn gather_context_handles_stale_multibyte_files() {
    let temp = TempRepo::new("py_mvp");

    let file_path = temp.repo_root.join("stale.py");
    std::fs::write(&file_path, "def stale_func():\n    return 42\n").unwrap();

    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Rewrite with multibyte content so the indexed byte offsets land mid-char
    std::fs::write(&file_path, "é".repeat(50)).unwrap();

    for strategy in ["file", "symbol"] {
        let response = rpc::call(
            temp.repo_root.clone(),
            temp.db_path.clone(),
            "gather_context".to_string(),
            &format!(
                r#"{{"seeds":[{{"type":"symbol","qualname":"stale.stale_func"}}],"strategy":"{strategy}","include_related":false}}"#
            ),
            "1",
        )
        .unwrap();

        let value: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert!(value["result"]["items"].is_array(), "strategy={strategy}");
    }
}

fn expansion_repo() -> TempRepo {
    let temp = TempRepo::new("py_mvp");
    let root = &temp.repo_root;
    std::fs::write(
        root.join("seedmod.py"),
        "from calleemod import helper\n\n\ndef seed(x):\n    return helper(x)\n",
    )
    .unwrap();
    std::fs::write(
        root.join("calleemod.py"),
        "def helper(x):\n    return x + 1\n",
    )
    .unwrap();
    std::fs::write(
        root.join("callermod.py"),
        "from seedmod import seed\n\n\ndef caller():\n    return seed(1)\n",
    )
    .unwrap();
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    temp
}

fn gather(temp: &TempRepo, params: &str) -> serde_json::Value {
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        params,
        "1",
    )
    .unwrap();
    serde_json::from_str::<serde_json::Value>(&response).unwrap()["result"].clone()
}

fn rel_of(result: &serde_json::Value, qualname: &str) -> Option<String> {
    result["items"].as_array().unwrap().iter().find_map(|i| {
        (i["symbol"]["qualname"] == qualname).then(|| {
            i["source"]["relationship"]
                .as_str()
                .unwrap_or("")
                .to_string()
        })
    })
}

#[test]
fn gather_context_no_snippets_still_expands() {
    let temp = expansion_repo();
    let r = gather(
        &temp,
        r#"{"seeds":[{"type":"symbol","qualname":"seedmod.seed"}],"include_snippets":false}"#,
    );
    assert!(rel_of(&r, "callermod.caller").is_some(), "{r}");
    assert!(rel_of(&r, "calleemod.helper").is_some(), "{r}");
}

#[test]
fn gather_context_dry_run_estimate_matches_real_run() {
    let temp = expansion_repo();
    let seeds = r#""seeds":[{"type":"symbol","qualname":"seedmod.seed"}]"#;
    let dry = gather(&temp, &format!("{{{seeds},\"dry_run\":true}}"));
    let real = gather(&temp, &format!("{{{seeds}}}"));
    assert!(rel_of(&dry, "callermod.caller").is_some(), "{dry}");
    assert_eq!(
        dry["estimated_bytes"], real["total_bytes"],
        "dry {dry}\nreal {real}"
    );
}

#[test]
fn gather_context_relationship_reflects_edge_direction() {
    let temp = expansion_repo();
    let r = gather(
        &temp,
        r#"{"seeds":[{"type":"symbol","qualname":"seedmod.seed"}]}"#,
    );
    assert_eq!(
        rel_of(&r, "callermod.caller").as_deref(),
        Some("caller"),
        "{r}"
    );
    assert_eq!(
        rel_of(&r, "calleemod.helper").as_deref(),
        Some("callee"),
        "{r}"
    );
}

#[test]
fn gather_context_no_snippets_cross_file_callers_are_stubs() {
    let temp = expansion_repo();
    let r = gather(
        &temp,
        r#"{"seeds":[{"type":"symbol","qualname":"seedmod.seed"}],"strategy":"file","depth":0,"include_snippets":false}"#,
    );
    let caller = r["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["symbol"]["qualname"] == "callermod.caller")
        .unwrap_or_else(|| panic!("{r}"));
    let content = caller["content"].as_str().unwrap();
    assert!(!content.contains("return seed(1)"), "{content}");
}

#[test]
fn gather_context_tight_budget_prefers_caller_over_module_stub() {
    let temp = expansion_repo();
    let r = gather(
        &temp,
        r#"{"seeds":[{"type":"symbol","qualname":"seedmod.seed"}],"max_bytes":240}"#,
    );
    assert!(rel_of(&r, "callermod.caller").is_some(), "{r}");
    let has_module = r["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|i| i["symbol"]["kind"] == "module");
    assert!(!has_module, "{r}");
}

fn write_cross_file_body_fixture(repo_root: &Path) {
    std::fs::write(
        repo_root.join("seedmod.py"),
        "from calleemod import callee_fn\n\n\ndef seed_fn():\n    return callee_fn()\n",
    )
    .unwrap();
    std::fs::write(
        repo_root.join("calleemod.py"),
        "def callee_fn():\n    first = 1\n    CALLEE_BODY_LINE_A = 2\n    CALLEE_BODY_LINE_B = 3\n    CALLEE_BODY_LINE_C = 4\n    CALLEE_BODY_LINE_D = 5\n    return first\n",
    )
    .unwrap();
    std::fs::write(
        repo_root.join("callermod.py"),
        "from seedmod import seed_fn\n\n\ndef caller_fn():\n    CALLER_BODY_LINE_A = 1\n    CALLER_BODY_LINE_B = 2\n    CALLER_BODY_LINE_C = 3\n    CALLER_BODY_LINE_D = 4\n    return seed_fn()\n",
    )
    .unwrap();
}

fn gather_symbol_seed(temp: &TempRepo, extra: &str) -> serde_json::Value {
    let params =
        format!(r#"{{"seeds":[{{"type":"symbol","qualname":"seedmod.seed_fn"}}]{extra}}}"#);
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        &params,
        "1",
    )
    .unwrap();
    serde_json::from_str::<serde_json::Value>(&response).unwrap()["result"].clone()
}

fn all_content(result: &serde_json::Value) -> String {
    result["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|i| i["content"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn gather_context_symbol_strategy_honors_include_snippets_for_cross_file_related() {
    let temp = TempRepo::new("py_mvp");
    write_cross_file_body_fixture(&temp.repo_root);
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let with = gather_symbol_seed(&temp, r#","include_snippets":true"#);
    let without = gather_symbol_seed(&temp, r#","include_snippets":false"#);
    let with_text = all_content(&with);
    let without_text = all_content(&without);

    assert!(
        with_text.contains("CALLEE_BODY_LINE_C"),
        "callee body expected with include_snippets=true: {with_text}"
    );
    assert!(
        with_text.contains("CALLER_BODY_LINE_C"),
        "caller body expected with include_snippets=true: {with_text}"
    );
    assert!(
        !without_text.contains("CALLEE_BODY_LINE_C"),
        "{without_text}"
    );
    assert!(
        !without_text.contains("CALLER_BODY_LINE_C"),
        "{without_text}"
    );
    // Stubs still present
    assert!(without_text.contains("calleemod.py"), "{without_text}");
    assert!(without_text.contains("callermod.py"), "{without_text}");
    assert!(
        with["total_bytes"].as_u64().unwrap() > without["total_bytes"].as_u64().unwrap(),
        "snippets output must be strictly larger"
    );
}

#[test]
fn gather_context_symbol_strategy_snippets_respect_small_budget() {
    let temp = TempRepo::new("py_mvp");
    write_cross_file_body_fixture(&temp.repo_root);
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Budget = the seed item alone plus a few bytes: no related body (or stub) can fit,
    // so several related items must be dropped and truncation is certain.
    let full = gather_symbol_seed(&temp, r#","include_snippets":true"#);
    let seed_bytes: usize = full["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["source"]["source_type"].as_str() == Some("direct_seed"))
        .map(|i| i["content"].as_str().unwrap().len())
        .sum();
    assert!(seed_bytes > 0, "{full}");
    let max = (seed_bytes + 10) as u64;
    let small = gather_symbol_seed(
        &temp,
        &format!(r#","include_snippets":true,"max_bytes":{max}"#),
    );
    assert!(
        small["items"].as_array().unwrap().len() < full["items"].as_array().unwrap().len(),
        "related items must have been dropped: {small}"
    );
    assert!(!all_content(&small).contains("CALLEE_BODY_LINE_C"));
    assert!(small["total_bytes"].as_u64().unwrap() <= max, "{small}");
    assert_eq!(small["truncated"].as_bool(), Some(true), "{small}");
}

#[test]
fn gather_context_symbol_strategy_honors_include_snippets_same_file() {
    let temp = TempRepo::new("py_mvp");
    let body: String = (0..10)
        .map(|i| format!("    SAMEFILE_B_LINE_{i} = {i}\n"))
        .collect();
    std::fs::write(
        temp.repo_root.join("samefile.py"),
        format!("def a():\n    return b()\n\n\ndef b():\n{body}    return 0\n"),
    )
    .unwrap();
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let run = |snip: bool| {
        let params = format!(
            r#"{{"seeds":[{{"type":"symbol","qualname":"samefile.a"}}],"include_snippets":{snip}}}"#
        );
        let r = rpc::call(
            temp.repo_root.clone(),
            temp.db_path.clone(),
            "gather_context".to_string(),
            &params,
            "1",
        )
        .unwrap();
        serde_json::from_str::<serde_json::Value>(&r).unwrap()["result"].clone()
    };
    let (with, without) = (run(true), run(false));
    assert!(all_content(&with).contains("SAMEFILE_B_LINE_9"), "{with}");
    assert!(
        !all_content(&without).contains("SAMEFILE_B_LINE_9"),
        "{without}"
    );
    assert!(with["total_bytes"].as_u64() > without["total_bytes"].as_u64());
}

#[test]
fn gather_context_symbol_strategy_large_cross_file_body_not_dropped_by_subcap() {
    let temp = TempRepo::new("py_mvp");
    write_cross_file_body_fixture(&temp.repo_root);
    // ~3 KB callee body: larger than the 1000-byte floor of the stub-mode cross-file cap.
    let big: String = (0..60)
        .map(|i| format!("    BIG_CALLEE_LINE_{i:03} = {i}  # padding padding\n"))
        .collect();
    std::fs::write(
        temp.repo_root.join("calleemod.py"),
        format!("def callee_fn():\n{big}    return 1\n"),
    )
    .unwrap();
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let with = gather_symbol_seed(&temp, r#","include_snippets":true,"max_bytes":20000"#);
    let without = gather_symbol_seed(&temp, r#","include_snippets":false,"max_bytes":20000"#);
    assert!(all_content(&with).contains("BIG_CALLEE_LINE_059"), "{with}");
    assert!(
        with["total_bytes"].as_u64().unwrap() > without["total_bytes"].as_u64().unwrap() + 2000
    );
}

// Regression tests for issue #359: test callers must not consume the node cap
// ahead of non-test callers, so `depth` takes effect.

fn write_test_caller_flood_fixture(repo_root: &Path) {
    std::fs::write(repo_root.join("lib.py"), "def target():\n    pass\n").unwrap();
    std::fs::write(
        repo_root.join("app.py"),
        "from lib import target\n\n\ndef svc():\n    target()\n\n\ndef api():\n    svc()\n",
    )
    .unwrap();
    let mut tests = String::from("from lib import target\n\n");
    for n in 0..60 {
        tests.push_str(&format!("\ndef test_{n}():\n    target()\n"));
    }
    std::fs::write(repo_root.join("test_lib.py"), tests).unwrap();
}

const TARGET_SEED: &str = r#"{"seeds":[{"type":"symbol","qualname":"lib.target"}]"#;

fn indexed_flood_repo() -> (TempRepo, Indexer) {
    let temp = TempRepo::new("py_mvp");
    write_test_caller_flood_fixture(&temp.repo_root);
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    (temp, indexer)
}

/// Run gather_context on `lib.target` with extra JSON params (e.g. `"depth":1`).
fn gather_target(temp: &TempRepo, extra: &str) -> Vec<serde_json::Value> {
    let params = format!("{TARGET_SEED},{extra}}}");
    let response = rpc::call(
        temp.repo_root.clone(),
        temp.db_path.clone(),
        "gather_context".to_string(),
        &params,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    value["result"]["items"].as_array().unwrap().clone()
}

fn names(items: &[serde_json::Value]) -> Vec<String> {
    items
        .iter()
        .filter_map(|i| i["symbol"]["qualname"].as_str().map(String::from))
        .collect()
}

fn is_test_item(item: &serde_json::Value) -> bool {
    lidx::indexer::test_detection::is_test_file(item["path"].as_str().unwrap())
}

#[test]
fn gather_context_test_callers_do_not_displace_non_test_callers() {
    let (temp, _indexer) = indexed_flood_repo();
    let found = names(&gather_target(&temp, r#""depth":2"#));
    assert!(found.iter().any(|n| n == "app.svc"), "{found:?}");
    assert!(found.iter().any(|n| n == "app.api"), "{found:?}");
}

#[test]
fn gather_context_caps_test_callers() {
    let (temp, _indexer) = indexed_flood_repo();
    let items = gather_target(&temp, r#""depth":2"#);
    let tests = items.iter().filter(|i| is_test_item(i)).count();
    assert!(tests > 0 && tests <= 8, "default cap is 8, got {tests}");

    let capped = gather_target(&temp, r#""depth":2,"max_test_nodes":2"#);
    // The cap also counts the test file's module node (not emitted as an item).
    let n = capped.iter().filter(|i| is_test_item(i)).count();
    assert!((1..=2).contains(&n), "{n}");
    let none = gather_target(&temp, r#""depth":2,"max_test_nodes":0"#);
    assert_eq!(none.iter().filter(|i| is_test_item(i)).count(), 0);
    assert!(names(&none).iter().any(|n| n == "app.api"));
}

#[test]
fn gather_context_orders_test_items_last() {
    let (temp, _indexer) = indexed_flood_repo();
    let items = gather_target(&temp, r#""depth":2"#);
    let first_test = items.iter().position(is_test_item).expect("a test caller");
    assert!(items[first_test..].iter().all(is_test_item));
}

#[test]
fn gather_context_depth_takes_effect_with_test_callers() {
    let (temp, _indexer) = indexed_flood_repo();
    let d1 = names(&gather_target(&temp, r#""depth":1"#));
    let d2 = names(&gather_target(&temp, r#""depth":2"#));
    assert!(d1.iter().any(|n| n == "app.svc"));
    assert!(!d1.iter().any(|n| n == "app.api"), "{d1:?}");
    assert_ne!(d1, d2);
}

#[test]
fn gather_context_incremental_matches_fresh_index() {
    let (temp, mut indexer) = indexed_flood_repo();
    // Incremental change: add an unrelated file and edit a test file.
    std::fs::write(temp.repo_root.join("other.py"), "def other():\n    pass\n").unwrap();
    let mut t = std::fs::read_to_string(temp.repo_root.join("test_lib.py")).unwrap();
    t.push_str("\n\ndef test_extra():\n    target()\n");
    std::fs::write(temp.repo_root.join("test_lib.py"), t).unwrap();
    indexer.reindex().unwrap();
    let incremental = gather_target(&temp, r#""depth":2"#);

    let fresh_temp = TempRepo::new("py_mvp");
    copy_dir(&temp.repo_root, &fresh_temp.repo_root);
    let _ = std::fs::remove_dir_all(fresh_temp.repo_root.join(".lidx"));
    let mut fresh_indexer =
        Indexer::new(fresh_temp.repo_root.clone(), fresh_temp.db_path.clone()).unwrap();
    fresh_indexer.reindex().unwrap();
    let fresh = gather_target(&fresh_temp, r#""depth":2"#);

    let shape = |items: &[serde_json::Value]| -> Vec<String> {
        items
            .iter()
            .map(|i| {
                format!(
                    "{}|{}|{}",
                    i["path"], i["symbol"]["qualname"], i["source"]["relationship"]
                )
            })
            .collect()
    };
    assert_eq!(shape(&incremental), shape(&fresh));
}
