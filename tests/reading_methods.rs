/// Integration tests for the `outline` and `read_symbol` reading methods (issue #94).
///
/// These methods are registered (METHOD_LIST, param schema, dispatch table) but not
/// yet implemented -- follow-up tickets build the real behaviour on top of this seam.
/// For now, calling either through `rpc::handle_method` must:
///   - accept their final param shapes (so invalid params are rejected by validation,
///     not by the stub itself)
///   - return a clear "not implemented yet" error, never "unknown method"
///
/// Setup mirrors `tests/repo_map.rs` / `tests/next_hops_validity.rs`: copy a fixture repo
/// into a temp dir and index it. Follow-up tickets extend this same file (and can reuse
/// `setup_repo`) once outline/read_symbol grow real behaviour worth asserting on.
use lidx::indexer::Indexer;
use lidx::rpc::{self, METHOD_LIST};
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
    dir.push(format!("lidx-reading-methods-{label}-{nanos}-{counter}"));
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

/// Reusable setup helper -- copies `fixture` into a fresh temp dir and indexes it.
/// Follow-up tickets extending this file should keep using this.
fn setup_repo(fixture: &str) -> (PathBuf, PathBuf) {
    let src = fixture_path(fixture);
    let repo_root = temp_repo_dir(fixture);
    copy_dir(&src, &repo_root);
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    (repo_root, db_path)
}

fn indexed(fixture: &str) -> (Indexer, PathBuf) {
    let (repo_root, db_path) = setup_repo(fixture);
    let mut indexer = Indexer::new(repo_root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    (indexer, repo_root)
}

fn assert_not_unknown_method(err_msg: &str) {
    assert!(
        !err_msg.to_lowercase().contains("unknown method"),
        "expected a not-implemented error, got 'unknown method': {}",
        err_msg
    );
}

fn assert_not_implemented(err_msg: &str) {
    assert!(
        err_msg.to_lowercase().contains("not implemented"),
        "expected a 'not implemented' error, got: {}",
        err_msg
    );
}

// --- registration ---

#[test]
fn outline_and_read_symbol_are_registered_methods() {
    assert!(
        METHOD_LIST.contains(&"outline"),
        "outline should be in METHOD_LIST"
    );
    assert!(
        METHOD_LIST.contains(&"read_symbol"),
        "read_symbol should be in METHOD_LIST"
    );
}

// --- outline ---

#[test]
fn outline_returns_not_implemented_error() {
    let (mut indexer, _repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "outline",
        serde_json::json!({"path": "pkg/core.py"}),
    );

    let err_msg = result.unwrap_err().to_string();
    assert_not_unknown_method(&err_msg);
    assert_not_implemented(&err_msg);

    let _ = std::fs::remove_dir_all(&_repo_root);
}

#[test]
fn outline_accepts_kinds_and_max_depth_params() {
    let (mut indexer, _repo_root) = indexed("py_mvp");

    // kinds/max_depth are part of the final param schema, so passing them must not
    // fail validation -- the response should still be the not-implemented stub error.
    let result = rpc::handle_method(
        &mut indexer,
        "outline",
        serde_json::json!({"path": "pkg/core.py", "kinds": ["function", "class"], "max_depth": 2}),
    );

    let err_msg = result.unwrap_err().to_string();
    assert_not_unknown_method(&err_msg);
    assert_not_implemented(&err_msg);

    let _ = std::fs::remove_dir_all(&_repo_root);
}

#[test]
fn outline_missing_path_is_rejected() {
    let (mut indexer, _repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(&mut indexer, "outline", serde_json::json!({}));

    assert!(
        result.is_err(),
        "outline without 'path' should be rejected by validation"
    );
    let err_msg = result.unwrap_err().to_string();
    assert_not_unknown_method(&err_msg);
    assert!(
        !err_msg.to_lowercase().contains("not implemented"),
        "missing-path rejection should be a validation error, not the not-implemented stub: {}",
        err_msg
    );

    let _ = std::fs::remove_dir_all(&_repo_root);
}

// --- read_symbol ---

#[test]
fn read_symbol_with_qualname_returns_not_implemented_error() {
    let (mut indexer, _repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter"}),
    );

    let err_msg = result.unwrap_err().to_string();
    assert_not_unknown_method(&err_msg);
    assert_not_implemented(&err_msg);

    let _ = std::fs::remove_dir_all(&_repo_root);
}

#[test]
fn read_symbol_with_query_returns_not_implemented_error() {
    let (mut indexer, _repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"query": "Greeter"}),
    );

    let err_msg = result.unwrap_err().to_string();
    assert_not_unknown_method(&err_msg);
    assert_not_implemented(&err_msg);

    let _ = std::fs::remove_dir_all(&_repo_root);
}

#[test]
fn read_symbol_with_qualnames_list_returns_not_implemented_error() {
    let (mut indexer, _repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualnames": ["pkg.core.Greeter", "pkg.utils.helper"]}),
    );

    let err_msg = result.unwrap_err().to_string();
    assert_not_unknown_method(&err_msg);
    assert_not_implemented(&err_msg);

    let _ = std::fs::remove_dir_all(&_repo_root);
}

#[test]
fn read_symbol_accepts_skeleton_and_context_lines_params() {
    let (mut indexer, _repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter", "skeleton": true, "context_lines": 3}),
    );

    let err_msg = result.unwrap_err().to_string();
    assert_not_unknown_method(&err_msg);
    assert_not_implemented(&err_msg);

    let _ = std::fs::remove_dir_all(&_repo_root);
}

#[test]
fn read_symbol_with_no_selector_is_rejected() {
    let (mut indexer, _repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(&mut indexer, "read_symbol", serde_json::json!({}));

    assert!(
        result.is_err(),
        "read_symbol with none of qualname/query/qualnames should be rejected"
    );
    let err_msg = result.unwrap_err().to_string();
    assert_not_unknown_method(&err_msg);
    assert!(
        !err_msg.to_lowercase().contains("not implemented"),
        "no-selector rejection should be a validation error, not the not-implemented stub: {}",
        err_msg
    );

    let _ = std::fs::remove_dir_all(&_repo_root);
}

#[test]
fn read_symbol_with_multiple_selectors_is_rejected() {
    let (mut indexer, _repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter", "query": "Greeter"}),
    );

    assert!(
        result.is_err(),
        "read_symbol with more than one of qualname/query/qualnames should be rejected"
    );
    let err_msg = result.unwrap_err().to_string();
    assert_not_unknown_method(&err_msg);
    assert!(
        !err_msg.to_lowercase().contains("not implemented"),
        "multi-selector rejection should be a validation error, not the not-implemented stub: {}",
        err_msg
    );

    let _ = std::fs::remove_dir_all(&_repo_root);
}
