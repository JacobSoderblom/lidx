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
    dir.push(format!("lidx-repomap-{label}-{nanos}-{counter}"));
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

#[test]
fn repo_map_returns_required_fields() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(&mut indexer, "repo_map", serde_json::json!({})).unwrap();

    assert!(
        result.get("text").is_some(),
        "Expected 'text' field in repo_map response: {:?}",
        result
    );
    assert!(
        result.get("modules").is_some(),
        "Expected 'modules' field in repo_map response: {:?}",
        result
    );
    assert!(
        result.get("symbols").is_some(),
        "Expected 'symbols' field in repo_map response: {:?}",
        result
    );
    assert!(
        result.get("bytes").is_some(),
        "Expected 'bytes' field in repo_map response: {:?}",
        result
    );

    let text = result["text"].as_str().unwrap();
    assert!(
        !text.is_empty(),
        "text should be non-empty for a repo with symbols"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn repo_map_respects_max_bytes() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let max_bytes: usize = 2000;
    let result = rpc::handle_method(
        &mut indexer,
        "repo_map",
        serde_json::json!({"max_bytes": max_bytes}),
    )
    .unwrap();

    let bytes = result["bytes"].as_u64().unwrap() as usize;
    assert!(
        bytes <= max_bytes,
        "bytes ({}) should be <= max_bytes ({})",
        bytes,
        max_bytes
    );

    let text = result["text"].as_str().unwrap();
    assert!(
        text.len() <= max_bytes,
        "text length ({}) should be <= max_bytes ({})",
        text.len(),
        max_bytes
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn repo_map_languages_filter_works() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Filter to python only — should still return a valid response
    let result = rpc::handle_method(
        &mut indexer,
        "repo_map",
        serde_json::json!({"languages": ["python"]}),
    )
    .unwrap();

    assert!(
        result.get("text").is_some(),
        "Expected 'text' field with language filter: {:?}",
        result
    );
    assert!(
        result.get("modules").is_some(),
        "Expected 'modules' field with language filter: {:?}",
        result
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn repo_map_max_bytes_zero_clamps_to_minimum() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // max_bytes=0 should be clamped to 1000
    let result = rpc::handle_method(
        &mut indexer,
        "repo_map",
        serde_json::json!({"max_bytes": 0}),
    )
    .unwrap();

    let bytes = result["bytes"].as_u64().unwrap() as usize;
    assert!(
        bytes <= 1000,
        "bytes ({}) should be <= 1000 (clamped minimum)",
        bytes
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn repo_map_max_bytes_below_minimum_clamps() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // max_bytes=999 should be clamped to 1000
    let result = rpc::handle_method(
        &mut indexer,
        "repo_map",
        serde_json::json!({"max_bytes": 999}),
    )
    .unwrap();

    let bytes = result["bytes"].as_u64().unwrap() as usize;
    assert!(
        bytes <= 1000,
        "bytes ({}) should be <= 1000 when max_bytes=999 (clamped to minimum)",
        bytes
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn repo_map_max_bytes_at_exact_minimum() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "repo_map",
        serde_json::json!({"max_bytes": 1000}),
    )
    .unwrap();

    let bytes = result["bytes"].as_u64().unwrap() as usize;
    assert!(
        bytes <= 1000,
        "bytes ({}) should be <= 1000 at exact minimum boundary",
        bytes
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn repo_map_max_bytes_above_maximum_clamps() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // max_bytes=100000 should be clamped to 50000
    let result = rpc::handle_method(
        &mut indexer,
        "repo_map",
        serde_json::json!({"max_bytes": 100000}),
    )
    .unwrap();

    let bytes = result["bytes"].as_u64().unwrap() as usize;
    assert!(
        bytes <= 50000,
        "bytes ({}) should be <= 50000 (clamped maximum)",
        bytes
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn repo_map_bytes_field_equals_text_length() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(&mut indexer, "repo_map", serde_json::json!({})).unwrap();

    let bytes = result["bytes"].as_u64().unwrap() as usize;
    let text_len = result["text"].as_str().unwrap().len();
    assert_eq!(
        bytes, text_len,
        "bytes field ({}) should equal text.len() ({})",
        bytes, text_len
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn repo_map_unknown_language_returns_error() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "repo_map",
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
fn repo_map_negative_max_bytes_returns_error() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Negative max_bytes can't deserialize into usize
    let result = rpc::handle_method(
        &mut indexer,
        "repo_map",
        serde_json::json!({"max_bytes": -1}),
    );
    assert!(
        result.is_err(),
        "Negative max_bytes should fail deserialization"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn repo_map_paths_filter_with_array() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "repo_map",
        serde_json::json!({"paths": ["pkg"]}),
    )
    .unwrap();

    assert!(
        result.get("text").is_some(),
        "Should return valid response with paths filter"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}
