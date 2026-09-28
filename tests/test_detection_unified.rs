//! Issue #61: `explain_symbol`, `analyze_diff`'s coverage step and the
//! search-scope classifier must all answer "is this a test" the same way,
//! using the single canonical predicate in `indexer::test_detection`.
//!
//! This pins the cross-method equivalence against a fixture spanning the
//! three conventions the old, divergent rules disagreed on: Go's
//! `_test.go` suffix, Rust's `#[test]` attribute, and C#'s `.Tests.`
//! namespace convention. Before this fix, `explain_symbol` used its own
//! inline `looks_like_test` (a path/name substring check) while
//! `analyze_diff` already used the canonical predicate -- a C# test class
//! in a `.Tests.` namespace whose file path and method name carry no
//! literal "test" substring was invisible to `explain_symbol` but visible
//! to `analyze_diff`.

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::json;
use std::collections::BTreeSet;
use std::path::Path;

fn write_files(root: &Path, files: &[(&str, &str)]) {
    for (rel, src) in files {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, src).unwrap();
    }
}

fn indexed_repo(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-test-detection-")
        .tempdir()
        .unwrap();
    write_files(tmp.path(), files);
    let repo_root = tmp.path().to_path_buf();
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root, db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

const GO_FILE: &str = "go/mathutil/add_test.go";
const GO_SRC: &str = "package mathutil\n\nfunc Add(a, b int) int {\n\treturn a + b\n}\n\nfunc TestAdd() {\n\tAdd(1, 2)\n}\n";

// Neither in a `tests/` directory nor named like a test (`verifies_addition`
// doesn't start with `test_`/end with `_test`), so this only passes via the
// `#[test]` attribute reaching `is_test_symbol` -- see issue #67 finding 1.
const RUST_FILE: &str = "rust/mathutil.rs";
const RUST_SRC: &str = "pub fn add(a: i32, b: i32) -> i32 {\n    a + b\n}\n\n#[cfg(test)]\nmod tests {\n    use super::*;\n\n    #[test]\n    fn verifies_addition() {\n        add(1, 2);\n    }\n}\n";

const CS_PROD_FILE: &str = "csharp/Billing/Calculator.cs";
const CS_PROD_SRC: &str = "namespace Billing\n{\n    public static class Calculator\n    {\n        public static int Sum(int a, int b) => a + b;\n    }\n}\n";

const CS_TEST_FILE: &str = "csharp/Billing/CalculatorChecks.cs";
const CS_TEST_SRC: &str = "namespace Billing.Tests\n{\n    public class CalculatorChecks\n    {\n        public void ReturnsCorrectTotal() => Billing.Calculator.Sum(1, 2);\n    }\n}\n";

/// Collect the `tests` section of `explain_symbol` as a set of qualnames.
fn explain_symbol_tests(indexer: &mut Indexer, symbol_id: i64) -> BTreeSet<String> {
    let result = rpc::handle_method(
        indexer,
        "explain_symbol",
        json!({"id": symbol_id, "sections": ["tests"]}),
    )
    .unwrap();
    result["tests"]
        .as_array()
        .unwrap_or_else(|| panic!("explain_symbol response missing 'tests' array: {result}"))
        .iter()
        .map(|t| {
            t["symbol"]["qualname"]
                .as_str()
                .expect("test ref missing symbol.qualname")
                .to_string()
        })
        .collect()
}

/// Collect `analyze_diff`'s test-coverage set for `qualname` as a set of
/// qualnames, from a diff over `file`.
fn analyze_diff_tests(indexer: &mut Indexer, file: &str, qualname: &str) -> BTreeSet<String> {
    let result = rpc::handle_method(
        indexer,
        "analyze_diff",
        json!({"paths": [file], "include_tests": true, "include_risk": false}),
    )
    .unwrap();
    let coverage = result["test_coverage"]
        .as_array()
        .unwrap_or_else(|| panic!("analyze_diff response missing 'test_coverage' array: {result}"));
    coverage
        .iter()
        .find(|c| c["symbol_qualname"].as_str() == Some(qualname))
        .map(|c| {
            c["tests"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| {
                    t["test_qualname"]
                        .as_str()
                        .expect("TestRef missing test_qualname")
                        .to_string()
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Find a changed symbol's `(id, qualname)` by name from an `analyze_diff`
/// response over `file`.
fn find_changed_symbol(indexer: &mut Indexer, file: &str, name: &str) -> (i64, String) {
    let result = rpc::handle_method(
        indexer,
        "analyze_diff",
        json!({"paths": [file], "include_tests": false, "include_risk": false}),
    )
    .unwrap();
    let changed = result["changed_symbols"]
        .as_array()
        .unwrap_or_else(|| panic!("analyze_diff response missing 'changed_symbols': {result}"));
    let sym = changed
        .iter()
        .find(|cs| cs["symbol"]["name"].as_str() == Some(name))
        .unwrap_or_else(|| {
            panic!("symbol '{name}' not found in changed_symbols for {file}: {changed:?}")
        });
    (
        sym["symbol"]["id"].as_i64().unwrap(),
        sym["symbol"]["qualname"].as_str().unwrap().to_string(),
    )
}

#[test]
fn explain_symbol_and_analyze_diff_agree_on_tests_across_languages() {
    let (_tmp, mut indexer) = indexed_repo(&[
        (GO_FILE, GO_SRC),
        (RUST_FILE, RUST_SRC),
        (CS_PROD_FILE, CS_PROD_SRC),
        (CS_TEST_FILE, CS_TEST_SRC),
    ]);

    let cases = [
        ("Go _test.go convention", GO_FILE, "Add"),
        ("Rust #[test] attribute convention", RUST_FILE, "add"),
        ("C# .Tests. namespace convention", CS_PROD_FILE, "Sum"),
    ];

    for (label, file, name) in cases {
        let (id, qualname) = find_changed_symbol(&mut indexer, file, name);

        let explain_tests = explain_symbol_tests(&mut indexer, id);
        let diff_tests = analyze_diff_tests(&mut indexer, file, &qualname);

        assert!(
            !diff_tests.is_empty(),
            "{label}: expected analyze_diff to find at least one test for {qualname}, found none"
        );
        assert_eq!(
            explain_tests, diff_tests,
            "{label}: explain_symbol and analyze_diff disagree on the test set for {qualname}"
        );
    }
}
