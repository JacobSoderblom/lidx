//! Issue #363: outline returns bare entries:[] with no note or next_hop for
//! indexed files with no symbols (GitHub workflow YAML, vitest test files).

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::json;

#[test]
fn outline_empty_entries_includes_note_and_next_hops() {
    let (tmp, _) = common::index_files(&[(
        ".github/workflows/ci.yml",
        "name: ci\non: [push]\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
    )]);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    let result = rpc::handle_method(
        &mut indexer,
        "outline",
        json!({"path": ".github/workflows/ci.yml"}),
    )
    .unwrap();

    let entries = result["entries"].as_array().unwrap();
    assert!(
        entries.is_empty(),
        "YAML workflow file should have no symbol entries"
    );

    let total_lines = result["total_lines"].as_i64().unwrap();
    assert!(total_lines > 0, "file should have multiple lines");

    let note = result.get("note").and_then(|n| n.as_str());
    assert!(
        note.is_some(),
        "empty outline should include a note, got: {}",
        result
    );
    assert!(
        note.unwrap().contains("no symbols"),
        "note should mention no symbols were extracted"
    );
    assert!(
        note.unwrap().contains("yaml"),
        "note should mention the language"
    );

    let next_hops = result["next_hops"].as_array().unwrap();
    assert!(
        !next_hops.is_empty(),
        "empty outline should include next_hops, got: {}",
        result
    );

    // At least one next hop should be a search or read
    let has_useful_hop = next_hops.iter().any(|hop| {
        let method = hop.get("method").and_then(|m| m.as_str());
        method == Some("search") || method == Some("read_symbol")
    });
    assert!(
        has_useful_hop,
        "next_hops should include search or read_symbol, got: {:?}",
        next_hops
    );
}
