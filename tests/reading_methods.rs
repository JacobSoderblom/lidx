/// Integration tests for the `outline` and `read_symbol` reading methods (issue #93).
///
/// `read_symbol` with a single `qualname`/`query` selector is implemented (issue #96):
/// exact source cut from disk by stored byte span, line-numbered, with staleness
/// detection against the indexed file hash. `qualnames` (multi-symbol reads),
/// `skeleton`, and `context_lines` stay a follow-up (#98) and are still asserted
/// against a "not implemented" stub below. `outline` (issue #97) is unimplemented in
/// this file's scope and still asserted as a stub too.
///
/// Setup mirrors `tests/repo_map.rs` / `tests/next_hops_validity.rs`: copy a fixture repo
/// into a temp dir and index it. Follow-up tickets extend this same file (and can reuse
/// `setup_repo`/`indexed`) once outline grows real behaviour worth asserting on.
mod common;

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

/// Like `indexed`, but builds the repo from inline source (`(rel_path, contents)`
/// pairs) rather than a fixture directory -- used for the `ext:` external-stub case,
/// which needs a minimal file with an unresolved external call rather than a full
/// fixture from `tests/fixtures/`.
fn indexed_from_source(label: &str, files: &[(&str, &str)]) -> (Indexer, PathBuf) {
    let repo_root = temp_repo_dir(label);
    common::write_files(&repo_root, files);
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
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
fn read_symbol_qualname_returns_exact_source_with_line_numbers() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter"}),
    )
    .unwrap();

    assert_eq!(result["qualname"], "pkg.core.Greeter");
    assert_eq!(result["kind"], "class");
    assert_eq!(result["path"], "pkg/core.py");
    assert_eq!(result["stale"], false, "{result:#}");
    assert!(
        result.get("next_hops").is_none(),
        "a fresh (non-stale) read shouldn't emit a reindex hop: {result:#}"
    );

    let start_line = result["start_line"].as_i64().expect("start_line");
    let end_line = result["end_line"].as_i64().expect("end_line");
    assert_eq!(start_line, 9, "{result:#}");
    assert_eq!(end_line, 13, "{result:#}");

    // Build the expected line-numbered text straight from the file on disk using
    // the response's own start/end_line, rather than hardcoding fixture content.
    let content = std::fs::read_to_string(repo_root.join("pkg/core.py")).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    let expected_source = lines[(start_line as usize - 1)..(end_line as usize)]
        .iter()
        .enumerate()
        .map(|(i, line)| format!("{}: {}", start_line + i as i64, line))
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(
        result["source"].as_str().unwrap(),
        expected_source,
        "source should be byte-identical to the fixture slice, line-numbered"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_query_resolves_same_symbol_as_explain_symbol() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    let read_result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"query": "Greeter"}),
    )
    .unwrap();
    let explain_result = rpc::handle_method(
        &mut indexer,
        "explain_symbol",
        serde_json::json!({"query": "Greeter"}),
    )
    .unwrap();

    assert!(
        read_result.get("ambiguous").is_none(),
        "expected an unambiguous resolution: {read_result:#}"
    );
    assert_eq!(read_result["qualname"], "pkg.core.Greeter");
    assert_eq!(
        read_result["qualname"].as_str().unwrap(),
        explain_result["symbol"]["qualname"].as_str().unwrap(),
        "read_symbol and explain_symbol must resolve a partial query to the same symbol"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_ambiguous_query_returns_candidates_and_no_source() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    // py_mvp has both `pkg.b.helper` (function) and `pkg.utils.Helper` (class) --
    // a case-insensitive tie on the exact-name-match tier `find_symbols` ranks first.
    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"query": "helper"}),
    )
    .unwrap();

    assert_eq!(result["ambiguous"], true, "{result:#}");
    assert!(
        result.get("source").is_none(),
        "an ambiguous match must not return source: {result:#}"
    );
    let candidates = result["candidates"].as_array().expect("candidates array");
    let qualnames: Vec<&str> = candidates
        .iter()
        .filter_map(|c| c["qualname"].as_str())
        .collect();
    assert!(
        qualnames.contains(&"pkg.b.helper"),
        "expected pkg.b.helper among candidates: {qualnames:?}"
    );
    assert!(
        qualnames.contains(&"pkg.utils.Helper"),
        "expected pkg.utils.Helper among candidates: {qualnames:?}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_ext_qualname_is_not_found() {
    let (mut indexer, repo_root) = indexed_from_source(
        "ext-stub",
        &[(
            "caller.py",
            "import requests\n\n\ndef fetch():\n    return requests.get(\"https://example.com\")\n",
        )],
    );

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "ext:requests.get"}),
    );

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("not found"),
        "expected a not-found error for an ext: qualname, got: {err_msg}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_stale_file_flags_and_still_returns_text() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    let before = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter"}),
    )
    .unwrap();
    assert_eq!(before["stale"], false, "{before:#}");

    // Append a trailing comment on disk without reindexing -- doesn't touch
    // Greeter's byte span, so the slice stays valid, but the file hash changes.
    let file_path = repo_root.join("pkg/core.py");
    let mut content = std::fs::read_to_string(&file_path).unwrap();
    content.push_str("\n# trailing comment, added after indexing\n");
    std::fs::write(&file_path, content).unwrap();

    let after = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter"}),
    )
    .unwrap();

    assert_eq!(after["stale"], true, "{after:#}");
    assert_eq!(
        after["source"], before["source"],
        "stale text should still be returned, unchanged: {after:#}"
    );
    let hops = after["next_hops"]
        .as_array()
        .expect("stale read should emit next_hops");
    assert!(
        hops.iter().any(|h| h["method"] == "reindex"),
        "expected a reindex next hop on a stale read: {hops:?}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn read_symbol_missing_file_errors_with_reindex_hint() {
    let (mut indexer, repo_root) = indexed("py_mvp");

    std::fs::remove_file(repo_root.join("pkg/core.py")).unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter"}),
    );

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("reindex"),
        "expected a reindex hint in the missing-file error, got: {err_msg}"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
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
fn read_symbol_accepts_skeleton_and_context_lines_params_but_ignores_them() {
    let (mut indexer, _repo_root) = indexed("py_mvp");

    // skeleton/context_lines are part of the final param schema (#98 scope), so
    // passing them must not fail validation -- but #96 doesn't implement them yet,
    // so a single-symbol qualname read still succeeds normally, ignoring both.
    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "pkg.core.Greeter", "skeleton": true, "context_lines": 3}),
    )
    .unwrap();

    assert_eq!(result["qualname"], "pkg.core.Greeter");
    assert!(result.get("source").is_some(), "{result:#}");

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
