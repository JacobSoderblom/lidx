//! Issue #63: per-scope file counts on `orient` and `onboard`.
//!
//! Scope is classified query-time by `search::scope_allows` (the same
//! classifier `search`'s `scope` param uses) -- this aggregates it rather
//! than introducing a stored column. Asserts what a caller observes: the
//! `overview.scope_counts` object's fields, against a fixture with a known
//! mix of scopes and a second fixture with no test files at all.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;

fn build_indexer(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-scope-counts-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

fn scope_counts(result: &Value) -> &Value {
    &result["overview"]["scope_counts"]
}

const MIXED_SCOPE_FILES: &[(&str, &str)] = &[
    ("main.py", "def main():\n    pass\n"),
    ("helper.py", "def helper():\n    pass\n"),
    ("tests/test_main.py", "def test_main():\n    pass\n"),
    ("docs/guide.py", "def guide():\n    pass\n"),
    ("examples/demo.py", "def demo():\n    pass\n"),
];

const NO_TEST_FILES: &[(&str, &str)] = &[
    ("src/app.py", "def app():\n    pass\n"),
    ("src/utils.py", "def util():\n    pass\n"),
];

#[test]
fn orient_reports_per_scope_file_counts_for_known_mix() {
    let (_tmp, mut indexer) = build_indexer(MIXED_SCOPE_FILES);
    let result = rpc::handle_method(&mut indexer, "orient", serde_json::json!({})).unwrap();
    let counts = scope_counts(&result);
    assert_eq!(counts["code"].as_i64().unwrap(), 2, "counts: {counts}");
    assert_eq!(counts["tests"].as_i64().unwrap(), 1, "counts: {counts}");
    assert_eq!(counts["docs"].as_i64().unwrap(), 1, "counts: {counts}");
    assert_eq!(counts["examples"].as_i64().unwrap(), 1, "counts: {counts}");
}

#[test]
fn onboard_reports_per_scope_file_counts_for_known_mix() {
    let (_tmp, mut indexer) = build_indexer(MIXED_SCOPE_FILES);
    let result = rpc::handle_method(&mut indexer, "onboard", serde_json::json!({})).unwrap();
    let counts = scope_counts(&result);
    assert_eq!(counts["code"].as_i64().unwrap(), 2, "counts: {counts}");
    assert_eq!(counts["tests"].as_i64().unwrap(), 1, "counts: {counts}");
    assert_eq!(counts["docs"].as_i64().unwrap(), 1, "counts: {counts}");
    assert_eq!(counts["examples"].as_i64().unwrap(), 1, "counts: {counts}");
}

#[test]
fn orient_reports_explicit_zero_tests_count_when_no_test_files_exist() {
    let (_tmp, mut indexer) = build_indexer(NO_TEST_FILES);
    let result = rpc::handle_method(&mut indexer, "orient", serde_json::json!({})).unwrap();
    let counts = scope_counts(&result);
    assert!(
        counts.get("tests").is_some(),
        "'tests' must be present (an explicit zero) even when no test files exist: {counts}"
    );
    assert_eq!(counts["tests"].as_i64().unwrap(), 0, "counts: {counts}");
    assert_eq!(counts["code"].as_i64().unwrap(), 2, "counts: {counts}");
    assert_eq!(counts["docs"].as_i64().unwrap(), 0, "counts: {counts}");
    assert_eq!(counts["examples"].as_i64().unwrap(), 0, "counts: {counts}");
}

#[test]
fn onboard_reports_explicit_zero_tests_count_when_no_test_files_exist() {
    let (_tmp, mut indexer) = build_indexer(NO_TEST_FILES);
    let result = rpc::handle_method(&mut indexer, "onboard", serde_json::json!({})).unwrap();
    let counts = scope_counts(&result);
    assert!(
        counts.get("tests").is_some(),
        "'tests' must be present (an explicit zero) even when no test files exist: {counts}"
    );
    assert_eq!(counts["tests"].as_i64().unwrap(), 0, "counts: {counts}");
}

/// Issue #133: Markdown files are indexed as `files` rows, so `docs` counts
/// them and `onboard.languages` reports what is actually present.
#[test]
fn markdown_files_count_as_docs_and_appear_in_onboard_languages() {
    let (_tmp, mut indexer) = build_indexer(&[
        ("main.py", "def main():\n    pass\n"),
        ("README.md", "# Title\n\nbody\n"),
        ("docs/guide.md", "# Guide\n"),
    ]);
    let result = rpc::handle_method(&mut indexer, "onboard", serde_json::json!({})).unwrap();
    let counts = scope_counts(&result);
    assert_eq!(counts["docs"].as_i64().unwrap(), 2, "counts: {counts}");
    let langs: Vec<&str> = result["languages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(langs, vec!["markdown", "python"], "languages: {langs:?}");
}

/// Issue #133: every language `onboard` advertises must be accepted as a
/// `languages` filter.
#[test]
fn markdown_is_a_valid_language_filter() {
    let (_tmp, mut indexer) = build_indexer(&[
        ("main.py", "def main():\n    pass\n"),
        ("README.md", "# Title\n"),
    ]);
    let result = rpc::handle_method(
        &mut indexer,
        "onboard",
        serde_json::json!({"languages": ["markdown"]}),
    )
    .unwrap();
    assert_eq!(result["overview"]["files"].as_i64().unwrap(), 1, "{result}");
}
