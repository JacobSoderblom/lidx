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
    dir.push(format!("lidx-deadsyms-{label}-{nanos}-{counter}"));
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

/// Returns the list of qualnames in the dead_symbols array of the response.
fn dead_qualnames(result: &serde_json::Value) -> Vec<String> {
    result["dead_symbols"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|sym| sym["qualname"].as_str().map(|s| s.to_string()))
        .collect()
}

#[test]
fn dead_symbols_returns_required_response_shape() {
    let (repo_root, db_path) = setup_repo("dead_symbols");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(&mut indexer, "dead_symbols", serde_json::json!({})).unwrap();

    assert!(
        result.get("dead_symbols").is_some(),
        "Response must have 'dead_symbols' field: {:?}",
        result
    );
    assert!(
        result.get("unused_imports").is_some(),
        "Response must have 'unused_imports' field: {:?}",
        result
    );
    assert!(
        result.get("orphan_tests").is_some(),
        "Response must have 'orphan_tests' field: {:?}",
        result
    );
    assert!(
        result.get("counts").is_some(),
        "Response must have 'counts' field: {:?}",
        result
    );

    let counts = &result["counts"];
    assert!(
        counts.get("dead_symbols").is_some(),
        "'counts' must have 'dead_symbols' sub-field: {:?}",
        counts
    );
    assert!(
        counts.get("unused_imports").is_some(),
        "'counts' must have 'unused_imports' sub-field: {:?}",
        counts
    );
    assert!(
        counts.get("orphan_tests").is_some(),
        "'counts' must have 'orphan_tests' sub-field: {:?}",
        counts
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn dead_symbols_reports_unreachable_function() {
    let (repo_root, db_path) = setup_repo("dead_symbols");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(&mut indexer, "dead_symbols", serde_json::json!({})).unwrap();

    let qualnames = dead_qualnames(&result);
    assert!(
        qualnames.iter().any(|q| q.contains("dead_function")),
        "dead_function (never called) should appear in dead_symbols, got: {:?}",
        qualnames
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn dead_symbols_excludes_http_route_handler() {
    let (repo_root, db_path) = setup_repo("dead_symbols");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(&mut indexer, "dead_symbols", serde_json::json!({})).unwrap();

    let qualnames = dead_qualnames(&result);
    assert!(
        !qualnames.iter().any(|q| q.contains("health_check")),
        "health_check is an HTTP route handler (entry point) and must NOT appear in dead_symbols, got: {:?}",
        qualnames
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn dead_symbols_counts_match_all_three_categories() {
    let (repo_root, db_path) = setup_repo("dead_symbols");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(&mut indexer, "dead_symbols", serde_json::json!({})).unwrap();

    let ds_count = result["counts"]["dead_symbols"].as_u64().unwrap() as usize;
    let ds_len = result["dead_symbols"].as_array().unwrap().len();
    assert_eq!(ds_count, ds_len);

    let ui_count = result["counts"]["unused_imports"].as_u64().unwrap() as usize;
    let ui_len = result["unused_imports"].as_array().unwrap().len();
    assert_eq!(ui_count, ui_len);

    let ot_count = result["counts"]["orphan_tests"].as_u64().unwrap() as usize;
    let ot_len = result["orphan_tests"].as_array().unwrap().len();
    assert_eq!(ot_count, ot_len);

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn dead_symbols_include_flags_control_categories() {
    let (repo_root, db_path) = setup_repo("dead_symbols");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Disable unused_imports
    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"include_unused_imports": false}),
    )
    .unwrap();
    let ui_len = result["unused_imports"].as_array().unwrap().len();
    assert_eq!(
        ui_len, 0,
        "unused_imports should be empty when include_unused_imports=false, got {} items",
        ui_len
    );

    // Disable orphan_tests
    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"include_orphan_tests": false}),
    )
    .unwrap();
    let ot_len = result["orphan_tests"].as_array().unwrap().len();
    assert_eq!(
        ot_len, 0,
        "orphan_tests should be empty when include_orphan_tests=false, got {} items",
        ot_len
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn dead_symbols_limit_param_works() {
    let (repo_root, db_path) = setup_repo("dead_symbols");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"limit": 1}),
    )
    .unwrap();

    let ds_len = result["dead_symbols"].as_array().unwrap().len();
    assert!(
        ds_len <= 1,
        "With limit=1, dead_symbols array should have at most 1 item, got {}",
        ds_len
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn dead_symbols_excludes_called_functions() {
    let (repo_root, db_path) = setup_repo("dead_symbols");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(&mut indexer, "dead_symbols", serde_json::json!({})).unwrap();

    let qualnames = dead_qualnames(&result);
    assert!(
        !qualnames.iter().any(|q| q.contains("live_function")),
        "live_function is called by run() and must NOT appear in dead_symbols, got: {:?}",
        qualnames
    );
    assert!(
        !qualnames.iter().any(|q| q == "run"),
        "run is a top-level function that calls live_function and must NOT appear in dead_symbols, got: {:?}",
        qualnames
    );
    assert!(
        !qualnames.iter().any(|q| q.contains("helper_used")),
        "helper_used is called by live_function and must NOT appear in dead_symbols, got: {:?}",
        qualnames
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn dead_symbols_includes_unused_helper() {
    let (repo_root, db_path) = setup_repo("dead_symbols");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(&mut indexer, "dead_symbols", serde_json::json!({})).unwrap();

    let qualnames = dead_qualnames(&result);
    assert!(
        qualnames.iter().any(|q| q.contains("helper_unused")),
        "helper_unused (never referenced) should appear in dead_symbols, got: {:?}",
        qualnames
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn dead_symbols_limit_zero_returns_empty() {
    let (repo_root, db_path) = setup_repo("dead_symbols");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"limit": 0}),
    )
    .unwrap();

    let ds_len = result["dead_symbols"].as_array().unwrap().len();
    assert_eq!(
        ds_len, 0,
        "With limit=0, dead_symbols array should be empty, got {}",
        ds_len
    );

    let ui_len = result["unused_imports"].as_array().unwrap().len();
    assert_eq!(
        ui_len, 0,
        "With limit=0, unused_imports array should be empty, got {}",
        ui_len
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn dead_symbols_both_include_flags_false() {
    let (repo_root, db_path) = setup_repo("dead_symbols");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"include_unused_imports": false, "include_orphan_tests": false}),
    )
    .unwrap();

    // dead_symbols should still be returned even with both flags false
    let ds_len = result["dead_symbols"].as_array().unwrap().len();
    assert!(
        ds_len > 0,
        "dead_symbols should still return results when both include flags are false"
    );

    let ui_len = result["unused_imports"].as_array().unwrap().len();
    assert_eq!(ui_len, 0, "unused_imports should be empty");

    let ot_len = result["orphan_tests"].as_array().unwrap().len();
    assert_eq!(ot_len, 0, "orphan_tests should be empty");

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn dead_symbols_language_filter_excludes_other_languages() {
    let (repo_root, db_path) = setup_repo("dead_symbols");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Fixture only has Python files — filtering by rust should yield zero dead symbols
    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"languages": ["rust"]}),
    )
    .unwrap();
    let ds_len = result["dead_symbols"].as_array().unwrap().len();
    assert_eq!(
        ds_len, 0,
        "Filtering by rust on a Python-only fixture should return 0 dead symbols, got {}",
        ds_len
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn dead_symbols_unknown_language_returns_error() {
    let (repo_root, db_path) = setup_repo("dead_symbols");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"languages": ["cobol"]}),
    );
    assert!(
        result.is_err(),
        "Unknown language filter should return an error"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("cobol"),
        "Error should mention the unknown language, got: {}",
        err_msg
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn dead_symbols_negative_limit_returns_error() {
    let (repo_root, db_path) = setup_repo("dead_symbols");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Negative limit can't deserialize into usize
    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"limit": -1}),
    );
    assert!(
        result.is_err(),
        "Negative limit should fail deserialization"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}
