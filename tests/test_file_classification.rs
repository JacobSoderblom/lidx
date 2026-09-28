//! Issue #67 finding 2: `test_detection::is_test_file` matched
//! `test_`/`_test.`/`.test.`/`.spec.`/`test.java` etc. as *substrings
//! anywhere in the whole path*, so a source file whose name merely
//! contains "test" as part of a longer word (`latest_version.py`,
//! `contest_rules.py`) was misclassified as a test file. Since #61 this
//! predicate also drives `search::classify_path`'s `tests` scope, so the
//! false positive surfaces on `orient`/`onboard`'s `scope_counts` too.
//!
//! Filename conventions must be matched against the file name component
//! only (prefix/suffix), not as a substring of the whole path. Directory
//! rules (`tests/`, `spec/`, etc.) are unchanged and still cover real
//! conventions.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;

fn build_indexer(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-test-file-classification-")
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

const FALSE_POSITIVE_FILES: &[(&str, &str)] = &[
    (
        "src/latest_version.py",
        "def get_latest_version():\n    return 1\n",
    ),
    ("src/contest_rules.py", "def rules():\n    return 1\n"),
];

#[test]
fn orient_does_not_classify_substring_matches_as_tests() {
    let (_tmp, mut indexer) = build_indexer(FALSE_POSITIVE_FILES);
    let result = rpc::handle_method(&mut indexer, "orient", serde_json::json!({})).unwrap();
    let counts = scope_counts(&result);
    assert_eq!(
        counts["tests"].as_i64().unwrap(),
        0,
        "neither file is a test -- \"test\" merely appears inside a longer word in \
         its name, got counts: {counts}"
    );
    assert_eq!(
        counts["code"].as_i64().unwrap(),
        FALSE_POSITIVE_FILES.len() as i64,
        "both files should be classified as code, got counts: {counts}"
    );
}

const REAL_TEST_CONVENTIONS: &[(&str, &str)] = &[
    ("tests/test_x.py", "def test_x():\n    pass\n"),
    ("test_foo.py", "def test_foo():\n    pass\n"),
    ("foo_test.go", "package foo\n\nfunc TestFoo() {}\n"),
    ("foo.spec.ts", "export const x = 1;\n"),
    ("src/__tests__/a.js", "const x = 1;\n"),
];

#[test]
fn orient_still_classifies_real_test_conventions_as_tests() {
    let (_tmp, mut indexer) = build_indexer(REAL_TEST_CONVENTIONS);
    let result = rpc::handle_method(&mut indexer, "orient", serde_json::json!({})).unwrap();
    let counts = scope_counts(&result);
    assert_eq!(
        counts["tests"].as_i64().unwrap(),
        REAL_TEST_CONVENTIONS.len() as i64,
        "every fixture file uses a real test-file convention, got counts: {counts}"
    );
}
