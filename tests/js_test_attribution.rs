//! Issue #209: JS/TS `describe`/`it`/`test` callbacks are anonymous, so calls
//! inside them are attributed to the test file's module symbol. That module
//! must count as a test (marked `file_level`), in `explain_symbol`'s tests
//! section and in the impact test layer, while a module in a non-test file
//! must not.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};

const FIXTURE: &[(&str, &str)] = &[
    (
        "src/kind.ts",
        "export function parseKind(name: string): string {\n  return name;\n}\n",
    ),
    // `test` and nested `describe` + `it`, arrow callbacks.
    (
        "test/sample.test.ts",
        "import { parseKind } from '../src/kind';\n\ntest('parses', () => {\n  parseKind('a');\n});\n\ndescribe('suite', () => {\n  it('parses nested', () => {\n    parseKind('b');\n  });\n});\n",
    ),
    // Modifier and `function` callback forms.
    (
        "test/forms.spec.js",
        "const { parseKind } = require('../src/kind');\n\nit.each([1, 2])('each %i', function (n) {\n  parseKind(n);\n});\n\ntest.only('only', function () {\n  parseKind('c');\n});\n\ndescribe.skip('skipped', () => {\n  suite('inner', () => {\n    parseKind('d');\n  });\n});\n",
    ),
    // Test file that calls nothing from the seed.
    (
        "test/empty.test.ts",
        "test('noop', () => {\n  const x = 1;\n  void x;\n});\n",
    ),
    // Non-test file: module-level call must not become a test.
    (
        "src/main.ts",
        "import { parseKind } from './kind';\n\nparseKind('main');\n",
    ),
];

fn build() -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-js-test-attribution-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), FIXTURE);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

fn test_files(refs: &Value) -> Vec<String> {
    let mut v: Vec<String> = refs
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["symbol"]["file_path"].as_str().unwrap().to_string())
        .collect();
    v.sort();
    v
}

#[test]
fn explain_symbol_reports_file_level_tests_for_js_callbacks() {
    let (_tmp, mut indexer) = build();
    let result = rpc::handle_method(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "src/kind.parseKind", "sections": ["tests"]}),
    )
    .unwrap();
    assert_eq!(result["tests_total"], 2, "{result}");
    assert_eq!(
        test_files(&result["tests"]),
        vec!["test/forms.spec.js", "test/sample.test.ts"],
        "{result}"
    );
    for t in result["tests"].as_array().unwrap() {
        assert_eq!(t["file_level"], true, "module entry must be marked: {t}");
    }
}

#[test]
fn impact_test_layer_finds_js_tests_through_graph_edges() {
    let (_tmp, mut indexer) = build();
    let result = rpc::handle_method(
        &mut indexer,
        "analyze_impact",
        json!({"qualname": "src/kind.parseKind", "direction": "upstream"}),
    )
    .unwrap();
    let tests: Vec<&Value> = result["affected"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["relationship"] == "TEST")
        .collect();
    let mut files: Vec<&str> = tests
        .iter()
        .map(|e| e["symbol"]["file_path"].as_str().unwrap())
        .collect();
    files.sort();
    assert_eq!(
        files,
        vec!["test/forms.spec.js", "test/sample.test.ts"],
        "{result}"
    );
    for t in &tests {
        assert_eq!(t["file_level"], true, "{t}");
    }
    assert!(result.get("test_layer").is_none(), "{result}");
}

#[test]
fn non_test_module_and_empty_test_file_are_not_tests() {
    let (_tmp, mut indexer) = build();
    let explained = rpc::handle_method(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "src/kind.parseKind", "sections": ["tests", "callers"]}),
    )
    .unwrap();
    let files = test_files(&explained["tests"]);
    assert!(!files.contains(&"src/main.ts".to_string()), "{explained}");
    assert!(
        !files.contains(&"test/empty.test.ts".to_string()),
        "{explained}"
    );
    // The non-test module is still an ordinary caller.
    assert!(
        explained["callers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["symbol"]["file_path"] == "src/main.ts"),
        "{explained}"
    );
}
