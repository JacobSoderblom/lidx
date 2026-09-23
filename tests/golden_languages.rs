//! Golden edge harness (#72): baseline scoreboards for the Rust, C#,
//! TypeScript and Go fixtures under `tests/fixtures/golden/<language>/`.
//! Each fixture mirrors the Python fixture's call shapes (see
//! `golden_python.rs`) where the language has them; today's failures are
//! recorded as `# xfail` lines in each `expected_edges.txt`.

mod common;

use common::golden;
use lidx::indexer::Indexer;
use std::collections::HashSet;

/// Floors are asserted at today's baseline (xfail lines are excluded from
/// the score, see `tests/common/golden.rs`). Raise only alongside a real
/// resolver fix.
const PRECISION_FLOOR: f64 = 1.0;
const RECALL_FLOOR: f64 = 1.0;

/// Index `golden/<language>` and score its CALLS edges against the
/// source symbols rooted in `scope`.
fn assert_golden(language: &str, scope: HashSet<String>) {
    let fixture = format!("golden/{language}");
    let (_tmp, repo_root, db_path) = common::setup_repo(&fixture);
    let mut indexer = Indexer::new(repo_root, db_path).unwrap();
    indexer.reindex().unwrap();
    let graph_version = indexer.db().current_graph_version().unwrap();

    let text = std::fs::read_to_string(golden::fixture_path(&format!(
        "{fixture}/expected_edges.txt"
    )))
    .unwrap();
    let expected = golden::parse_expected_edges(&text);
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();
    golden::compare(&snapshot, &expected, &scope).assert_floors(
        language,
        PRECISION_FLOOR,
        RECALL_FLOOR,
    );
}

/// Every Rust qualname is rooted in `crate`, every C# one in the fixture's
/// `Golden` namespace.
fn root(name: &str) -> HashSet<String> {
    HashSet::from([name.to_string()])
}

#[test]
fn rust_fixture_matches_expected_edges() {
    assert_golden("rust", root("crate"));
}

#[test]
fn csharp_fixture_matches_expected_edges() {
    assert_golden("csharp", root("Golden"));
}

#[test]
fn typescript_fixture_matches_expected_edges() {
    assert_golden(
        "typescript",
        golden::fixture_source_modules("golden/typescript"),
    );
}

#[test]
fn go_fixture_matches_expected_edges() {
    assert_golden("go", golden::fixture_source_modules("golden/go"));
}
