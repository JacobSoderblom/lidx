/// Issue #65: `top_complexity` and `repo_map` used to return a bare empty
/// list/counter when they found nothing, giving the caller no way to tell
/// "this codebase is uniformly simple" (or "the filter matched nothing")
/// from "the requested scope has no data at all". Both now report explicit
/// counts plus a diagnostic flag on the empty path, matching the
/// explicit-counts precedent set by `dead_symbols` (see `tests/dead_symbols.rs`).
/// Non-empty results are asserted unchanged (still a bare array for
/// `top_complexity`, still the same object shape for `repo_map`).
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
    dir.push(format!("lidx-emptycounts-{label}-{nanos}-{counter}"));
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

// ---------------------------------------------------------------------------
// top_complexity
// ---------------------------------------------------------------------------

#[test]
fn top_complexity_non_empty_stays_bare_array() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(&mut indexer, "top_complexity", serde_json::json!({})).unwrap();

    assert!(
        result.is_array(),
        "non-empty top_complexity must remain a bare array: {:?}",
        result
    );
    assert!(
        !result.as_array().unwrap().is_empty(),
        "py_mvp has functions, top_complexity should not be empty"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

/// A `min_complexity` floor above every function's actual complexity empties
/// the result even though complexity metrics genuinely exist for this scope
/// -- the codebase is just uniformly simple relative to the floor.
#[test]
fn top_complexity_empty_due_to_threshold_reports_metrics_exist() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "top_complexity",
        serde_json::json!({"min_complexity": 9999}),
    )
    .unwrap();

    assert!(
        result.is_object(),
        "empty top_complexity must be an explicit-counts object, got: {:?}",
        result
    );
    assert_eq!(
        result["results"].as_array().unwrap().len(),
        0,
        "results should be empty: {:?}",
        result
    );
    assert_eq!(
        result["counts"]["results"].as_u64(),
        Some(0),
        "counts.results should be explicit 0: {:?}",
        result
    );
    assert_eq!(
        result["metrics_exist"].as_bool(),
        Some(true),
        "python functions were extracted, complexity metrics do exist: {:?}",
        result
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

/// Filtering to a language that has no files in this repo means complexity
/// metrics never existed for the requested scope at all -- distinct from
/// "uniformly simple".
#[test]
fn top_complexity_empty_due_to_missing_language_reports_no_metrics() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "top_complexity",
        serde_json::json!({"languages": ["rust"]}),
    )
    .unwrap();

    assert!(result.is_object(), "expected object, got: {:?}", result);
    assert_eq!(result["results"].as_array().unwrap().len(), 0);
    assert_eq!(result["counts"]["results"].as_u64(), Some(0));
    assert_eq!(
        result["metrics_exist"].as_bool(),
        Some(false),
        "py_mvp is Python-only, rust has no complexity metrics at all: {:?}",
        result
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

// ---------------------------------------------------------------------------
// repo_map
// ---------------------------------------------------------------------------

#[test]
fn repo_map_non_empty_shape_unchanged() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(&mut indexer, "repo_map", serde_json::json!({})).unwrap();

    assert!(result.get("text").is_some());
    assert!(result.get("modules").is_some());
    assert!(result.get("symbols").is_some());
    assert!(result.get("bytes").is_some());
    assert!(
        result.get("index_empty").is_none(),
        "a non-empty repo_map should not carry the empty-path diagnostic field: {:?}",
        result
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

/// A language filter that matches no files in an otherwise-populated index:
/// the filter is what produced the empty result, not an empty index.
#[test]
fn repo_map_filter_matched_nothing_reports_index_not_empty() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "repo_map",
        serde_json::json!({"languages": ["rust"]}),
    )
    .unwrap();

    assert_eq!(
        result["modules"].as_u64(),
        Some(0),
        "rust matches nothing in a python-only repo: {:?}",
        result
    );
    assert_eq!(
        result["index_empty"].as_bool(),
        Some(false),
        "the repo is indexed, the language filter just matched nothing: {:?}",
        result
    );
    assert!(
        result["counts"].is_object(),
        "empty repo_map should carry an explicit counts object: {:?}",
        result
    );
    assert!(
        !result["warnings"].as_array().unwrap().is_empty(),
        "empty repo_map should explain itself: {:?}",
        result
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

/// A repo that has never been indexed (no reindex called) has zero files at
/// any graph version -- `repo_map` returning nothing here is a genuinely
/// empty index, not a filter mismatch.
#[test]
fn repo_map_empty_index_reports_index_empty() {
    let repo_root = temp_repo_dir("never-indexed");
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();

    let result = rpc::handle_method(&mut indexer, "repo_map", serde_json::json!({})).unwrap();

    assert_eq!(result["modules"].as_u64(), Some(0));
    assert_eq!(
        result["index_empty"].as_bool(),
        Some(true),
        "nothing was ever indexed: {:?}",
        result
    );
    assert!(!result["warnings"].as_array().unwrap().is_empty());

    let _ = std::fs::remove_dir_all(&repo_root);
}
