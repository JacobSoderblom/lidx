//! Issue #479: the test layer of `analyze_impact` recognises every function
//! `go test` runs -- `Test*`, `Benchmark*` and `Fuzz*` with the matching
//! `*testing` parameter -- and leaves out lowercase helpers, look-alike
//! names, `Example*` and methods.

mod common;

use lidx::rpc;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::Path;

const GO_MOD: &str = "module example.com/m\n\ngo 1.22\n";

const LIB: &str = "package m

func PathAllowed(p string) bool { return p != \"\" }
";

const TESTS: &str = "package m

import \"testing\"

func TestPathAllowed(t *testing.T) { PathAllowed(\"a\") }

func BenchmarkPathAllowed(b *testing.B) {
	for i := 0; i < b.N; i++ {
		PathAllowed(\"a\")
	}
}

func FuzzPathAllowed(f *testing.F) {
	f.Fuzz(func(t *testing.T, s string) { PathAllowed(s) })
}

func testHelperAllowed(t *testing.T) { PathAllowed(\"h\") }

func Testify(t *testing.T) { PathAllowed(\"y\") }

func BenchmarkMismatch(t *testing.T) { PathAllowed(\"m\") }

func ExamplePathAllowed() { PathAllowed(\"e\") }

func ExampleOutputAllowed() {
	PathAllowed(\"o\")
	// Output:
}

func BenchmarkLocalType(b *T) { PathAllowed(\"l\") }

type suite struct{}

func (s *suite) TestMethod(t *testing.T) { PathAllowed(\"s\") }
";

const ALIAS_TESTS: &str = "package m

import tst \"testing\"

func TestAlias(t *tst.T) { PathAllowed(\"x\") }

func BenchmarkAlias(b *tst.B) { PathAllowed(\"x\") }

func TestWrongAlias(t *other.T) { PathAllowed(\"x\") }

func TestWrongType(t *tst.B) { PathAllowed(\"x\") }
";

const DOT_TESTS: &str = "package m

import . \"testing\"

func TestDot(t *T) { PathAllowed(\"d\") }

func BenchmarkDot(b *B) { PathAllowed(\"d\") }

func FuzzDot(f *F) { PathAllowed(\"d\") }

func TestDotWrong(b *B) { PathAllowed(\"d\") }
";

/// Qualnames of the symbols the test layer of an upstream `analyze_impact`
/// of `lib.PathAllowed` reports.
fn test_layer(root: &Path) -> BTreeSet<String> {
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let response = rpc::call(
        root.to_path_buf(),
        db_path,
        "analyze_impact".to_string(),
        r#"{"qualname":"lib.PathAllowed","direction":"upstream","max_depth":3,
            "enable_test":true,"enable_historical":false}"#,
        "1",
    )
    .unwrap();
    let value: Value = serde_json::from_str(&response).unwrap();
    let result = &value["result"];
    result["affected"]
        .as_array()
        .unwrap_or_else(|| panic!("no affected list in {result}"))
        .iter()
        .filter(|e| e["relationship"] == "TEST")
        .filter_map(|e| e["symbol"]["qualname"].as_str().map(str::to_string))
        .collect()
}

fn assert_entries(found: &BTreeSet<String>) {
    for q in [
        "lib_test.TestPathAllowed",
        "lib_test.BenchmarkPathAllowed",
        "lib_test.FuzzPathAllowed",
        "lib_test.ExampleOutputAllowed",
        "alias_test.TestAlias",
        "alias_test.BenchmarkAlias",
        "dot_test.TestDot",
        "dot_test.BenchmarkDot",
        "dot_test.FuzzDot",
    ] {
        assert!(found.contains(q), "{q} missing from {found:?}");
    }
    for q in [
        "lib_test.testHelperAllowed",
        "lib_test.Testify",
        "lib_test.BenchmarkMismatch",
        "lib_test.ExamplePathAllowed",
        "lib_test.BenchmarkLocalType",
        "lib_test.suite.TestMethod",
        "alias_test.TestWrongAlias",
        "alias_test.TestWrongType",
        "dot_test.TestDotWrong",
    ] {
        assert!(!found.contains(q), "{q} wrongly a test in {found:?}");
    }
}

const FILES: &[(&str, &str)] = &[
    ("go.mod", GO_MOD),
    ("lib.go", LIB),
    ("lib_test.go", TESTS),
    ("alias_test.go", ALIAS_TESTS),
    ("dot_test.go", DOT_TESTS),
];

#[test]
fn benchmark_and_fuzz_functions_are_in_the_test_layer() {
    let (_tmp, root, _db) = common::index_repo("lidx-go-test-reach-", FILES);
    assert_entries(&test_layer(&root));
}

#[test]
fn a_test_file_added_by_sync_gives_what_a_fresh_index_gives() {
    let (_tmp, root, mut indexer) = common::live_indexer(
        "lidx-go-test-reach-sync-",
        &[("go.mod", GO_MOD), ("lib.go", LIB)],
    );
    common::write_files(&root, &FILES[2..]);
    let added: Vec<String> = FILES[2..].iter().map(|(p, _)| p.to_string()).collect();
    indexer.sync_rel_paths(&added).unwrap();
    let synced = test_layer(&root);
    assert_entries(&synced);

    let (_tmp2, fresh_root, _db) = common::index_repo("lidx-go-test-reach-fresh-", FILES);
    assert_eq!(synced, test_layer(&fresh_root));
}
