//! Issue #363: outline returns bare entries:[] with no note or next_hop for
//! indexed files with no symbols (GitHub workflow YAML, vitest test files).

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};

fn outline(files: &[(&str, &str)], path: &str) -> (Indexer, Value, tempfile::TempDir) {
    let (tmp, _) = common::index_files(files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    let result = rpc::handle_method(&mut indexer, "outline", json!({"path": path})).unwrap();
    (indexer, result, tmp)
}

fn assert_empty_outline(indexer: &mut Indexer, result: &Value, language: &str) {
    assert!(result["entries"].as_array().unwrap().is_empty());
    assert!(result["total_lines"].as_i64().unwrap() > 0);
    let note = result["note"].as_str().expect("note present");
    assert!(note.contains("no symbols"), "{note}");
    assert!(note.contains(&format!("language {language}")), "{note}");
    assert!(!note.contains("read_symbol"), "{note}");

    let hops = result["next_hops"].as_array().unwrap();
    assert!(!hops.is_empty(), "{result}");
    for hop in hops {
        let method = hop["method"].as_str().unwrap();
        rpc::handle_method(indexer, method, hop["params"].clone())
            .unwrap_or_else(|e| panic!("next_hop {hop} failed: {e}"));
    }
}

#[test]
fn outline_empty_yaml_includes_note_and_runnable_hops() {
    let (mut indexer, result, _tmp) = outline(
        &[(
            ".github/workflows/ci.yml",
            "name: ci\non: [push]\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        )],
        ".github/workflows/ci.yml",
    );
    assert_empty_outline(&mut indexer, &result, "yaml");
    assert!(result["note"].as_str().unwrap().contains("Kubernetes"));
}

#[test]
fn outline_empty_vitest_file_includes_note_and_runnable_hops() {
    let (mut indexer, result, _tmp) = outline(
        &[(
            "src/cron.test.ts",
            "import { describe, it, expect } from 'vitest';\n\ndescribe('cron', () => {\n  it('works', () => {\n    expect(1).toBe(1);\n  });\n});\n",
        )],
        "src/cron.test.ts",
    );
    if result["entries"].as_array().unwrap().is_empty() {
        assert_empty_outline(&mut indexer, &result, "typescript");
        assert!(result["note"].as_str().unwrap().contains("describe/it"));
    } else {
        panic!("vitest fixture unexpectedly produced symbols: {result}");
    }
}
