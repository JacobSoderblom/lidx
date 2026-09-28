//! Issue #67 finding 1: Rust tests were dropped from `explain_symbol`.
//! `indexer::test_detection::is_test_symbol` looks for `#[test]` (and
//! friends) as a substring of the symbol's `signature`, but the Rust
//! extractor's `extract_signature` only ever built "{params} -> {ret}" --
//! the attribute never reached the signature, so the only way a Rust test
//! was ever recognized was the tests-dir + test-like-name fallback. A
//! `#[test]` fn inside a `#[cfg(test)]` mod in a `src/` file, or one with a
//! name that doesn't look like a test even inside `tests/`, was invisible.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::json;

fn build_indexer(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-rust-test-attr-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

fn explain_symbol_test_names(indexer: &mut Indexer, query: &str) -> Vec<String> {
    let result = rpc::handle_method(
        indexer,
        "explain_symbol",
        json!({"query": query, "sections": ["tests"]}),
    )
    .unwrap();
    result["tests"]
        .as_array()
        .unwrap_or_else(|| panic!("explain_symbol response missing 'tests' array: {result}"))
        .iter()
        .map(|t| t["symbol"]["name"].as_str().unwrap_or_default().to_string())
        .collect()
}

const CFG_TEST_MOD_FILE: &str = "src/parser.rs";
const CFG_TEST_MOD_SRC: &str = "pub fn parse(input: &str) -> usize {\n    input.len()\n}\n\n#[cfg(test)]\nmod tests {\n    use super::*;\n\n    #[test]\n    fn test_parse() {\n        parse(\"a\");\n    }\n}\n";

#[test]
fn explain_symbol_finds_rust_test_inside_cfg_test_mod() {
    let (_tmp, mut indexer) = build_indexer(&[(CFG_TEST_MOD_FILE, CFG_TEST_MOD_SRC)]);
    let tests = explain_symbol_test_names(&mut indexer, "parse");
    assert!(
        tests.iter().any(|n| n == "test_parse"),
        "expected 'test_parse' in explain_symbol's tests for 'parse', got: {tests:?}"
    );
}

const TESTS_DIR_FILE: &str = "tests/math.rs";
const TESTS_DIR_SRC: &str = "pub fn add(a: i32, b: i32) -> i32 {\n    a + b\n}\n\n#[test]\nfn adds_numbers() {\n    add(1, 2);\n}\n";

#[test]
fn explain_symbol_finds_rust_test_with_non_test_like_name_in_tests_dir() {
    let (_tmp, mut indexer) = build_indexer(&[(TESTS_DIR_FILE, TESTS_DIR_SRC)]);
    let tests = explain_symbol_test_names(&mut indexer, "add");
    assert!(
        tests.iter().any(|n| n == "adds_numbers"),
        "expected 'adds_numbers' in explain_symbol's tests for 'add', got: {tests:?}"
    );
}

const TOKIO_TEST_FILE: &str = "src/fetcher.rs";
const TOKIO_TEST_SRC: &str = "pub async fn fetch() -> u32 {\n    1\n}\n\n#[cfg(test)]\nmod tests {\n    use super::*;\n\n    #[tokio::test]\n    async fn returns_one() {\n        fetch().await;\n    }\n}\n";

#[test]
fn explain_symbol_finds_rust_tokio_test_attribute() {
    let (_tmp, mut indexer) = build_indexer(&[(TOKIO_TEST_FILE, TOKIO_TEST_SRC)]);
    let tests = explain_symbol_test_names(&mut indexer, "fetch");
    assert!(
        tests.iter().any(|n| n == "returns_one"),
        "expected 'returns_one' in explain_symbol's tests for 'fetch', got: {tests:?}"
    );
}
