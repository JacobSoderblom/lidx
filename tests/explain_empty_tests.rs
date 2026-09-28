//! Issue #68: an empty `tests` list on `explain_symbol` must say why when
//! the index holds zero test-scope files at all, and must stay quiet when
//! the index does hold test-scope files that simply don't reach the
//! queried symbol -- that second case is a genuine "no", and annotating it
//! would be noise.
//!
//! Reuses #63's per-scope file counts (the same `scope_allows`-backed query
//! `orient`/`onboard`'s `scope_counts` already runs) rather than a second
//! test-file tally, and appends to the existing `warnings` field rather
//! than a new channel.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;

fn build_indexer(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-explain-empty-tests-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

fn explain_tests_section(indexer: &mut Indexer, query: &str) -> Value {
    rpc::handle_method(
        indexer,
        "explain_symbol",
        serde_json::json!({"query": query, "sections": ["tests"]}),
    )
    .unwrap()
}

fn warnings(result: &Value) -> Vec<String> {
    result
        .get("warnings")
        .and_then(|w| w.as_array())
        .map(|w| {
            w.iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}

const NO_TEST_FILES: &[(&str, &str)] = &[("src/app.py", "def greet():\n    pass\n")];

const UNRELATED_TEST_FILES: &[(&str, &str)] = &[
    (
        "src/app.py",
        "def greet():\n    pass\n\n\ndef other():\n    pass\n",
    ),
    (
        "tests/test_other.py",
        "from src.app import other\n\n\ndef test_other():\n    other()\n",
    ),
];

#[test]
fn explain_symbol_explains_empty_tests_when_index_has_no_test_scope_files() {
    let (_tmp, mut indexer) = build_indexer(NO_TEST_FILES);
    let result = explain_tests_section(&mut indexer, "greet");

    assert_eq!(
        result["tests"].as_array().map(|t| t.len()),
        Some(0),
        "expected an empty tests list: {result}"
    );

    let warnings = warnings(&result);
    assert!(
        warnings.iter().any(|w| w.to_lowercase().contains("test")),
        "expected a warning explaining the empty tests list when the index holds no \
         test-scope files, got warnings: {warnings:?} (full response: {result})"
    );
    // Issue #67 finding 3: tests are detected by file path, so a crate
    // whose tests live inline (e.g. Rust's #[cfg(test)] modules) can have
    // zero test-scope *files* while still having tests. The warning must
    // not overstate that as "no tests were ever indexed".
    assert!(
        warnings
            .iter()
            .any(|w| w.to_lowercase().contains("file path")),
        "expected the warning to explain that tests are detected by file path, got \
         warnings: {warnings:?} (full response: {result})"
    );
    assert!(
        !warnings.iter().any(|w| w.contains("were ever indexed")),
        "the warning must not overstate this as 'no tests were ever indexed' -- \
         lidx only knows about test-scope *files*, not whether tests exist inline, \
         got warnings: {warnings:?} (full response: {result})"
    );
}

#[test]
fn explain_symbol_stays_quiet_when_tests_exist_but_dont_reach_the_symbol() {
    let (_tmp, mut indexer) = build_indexer(UNRELATED_TEST_FILES);
    let result = explain_tests_section(&mut indexer, "greet");

    assert_eq!(
        result["tests"].as_array().map(|t| t.len()),
        Some(0),
        "expected an empty tests list: {result}"
    );

    let warnings = warnings(&result);
    assert!(
        warnings.is_empty(),
        "a genuine 'no tests reach this symbol' must stay quiet when the index does \
         hold test-scope files, got warnings: {warnings:?} (full response: {result})"
    );
}
