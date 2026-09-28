//! Issue #67 finding 4: `context.rs` carried its own `is_test_path` copy
//! (line ~281, used to bucket `cross_file_callers` into `test_files`) that
//! diverged from the canonical `indexer::test_detection::is_test_file` --
//! in particular it had no notion of an RSpec-style `spec/` directory at
//! all, so a caller living there leaked into `cross_file_callers` instead
//! of being classified as a test. Replacing the copy with the canonical
//! predicate fixes this and keeps `context` consistent with `orient`,
//! `explain_symbol` and `search`.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;

fn build_indexer(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-context-test-detection-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

fn context_json(indexer: &mut Indexer, path: &str) -> Value {
    rpc::handle_method(
        indexer,
        "context",
        serde_json::json!({"path": path, "format": "json"}),
    )
    .unwrap()
}

const FILES: &[(&str, &str)] = &[
    ("pkg/__init__.py", ""),
    ("pkg/target.py", "def helper():\n    return 1\n"),
    (
        "spec/caller_spec.py",
        "from pkg import target\n\n\ndef exercise():\n    return target.helper()\n",
    ),
];

#[test]
fn context_buckets_spec_dir_caller_into_test_files_not_callers() {
    let (_tmp, mut indexer) = build_indexer(FILES);
    let result = context_json(&mut indexer, "pkg/target.py");

    let test_files: Vec<String> = result["test_files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert!(
        test_files.iter().any(|f| f == "spec/caller_spec.py"),
        "expected spec/caller_spec.py in test_files (spec/ is a recognized test \
         directory under the canonical predicate), got: {result}"
    );

    let caller_files: Vec<String> = result["cross_file_callers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["file_path"].as_str().unwrap().to_string())
        .collect();
    assert!(
        !caller_files.iter().any(|f| f == "spec/caller_spec.py"),
        "spec/caller_spec.py is a test caller and must not also appear in \
         cross_file_callers, got: {result}"
    );
}
