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

/// Issue #77: the JS/TS extractor emits an `IMPORTS_FILE` edge for
/// every relative specifier, whether or not its target currently resolves
/// to a real file (`javascript::resolve_import_file_edges`) -- a plain
/// `import './app.css'` into a real, on-disk file that simply has no
/// symbols of its own must keep its edge (unresolved, since no CSS module
/// symbol ever exists to bind it to), not lose it entirely. Own temp repo,
/// not the shared `golden/typescript` fixture: this is a full-reindex
/// regression, unrelated to that fixture's CALLS scoring.
#[test]
fn typescript_css_import_keeps_its_unresolved_imports_file_edge() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    std::fs::write(
        root.join("index.ts"),
        "import './app.css';\nimport { h } from './lib';\nexport function f() { return h(); }\n",
    )
    .unwrap();
    std::fs::write(root.join("app.css"), "body {}\n").unwrap();
    std::fs::write(
        root.join("lib.ts"),
        "export function h(): number { return 1; }\n",
    )
    .unwrap();

    let mut indexer =
        Indexer::new(root.to_path_buf(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();

    let has_css_edge = snapshot.iter().any(|edge| {
        edge.source_qualname == "index"
            && edge.kind == "IMPORTS_FILE"
            && edge.target_qualname.is_none()
    });
    assert!(
        has_css_edge,
        "index's unresolved IMPORTS_FILE edge into app.css must survive a \
         fresh reindex, not be deleted: {snapshot:#?}"
    );
}

/// Issue #77: C# static overloads share one qualname (no signature in
/// it, see `csharp::build_qualname`) -- a call fully-qualified enough to
/// reach the exact tier directly (`Golden.App.Foo.Bar(1)`, mirroring the
/// shared fixture's `CallNamespacePath` shape) must still collapse to one
/// target on a full reindex, not go unresolved just because two symbols
/// share the name.
#[test]
fn csharp_static_overload_call_resolves_on_full_reindex() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    std::fs::write(
        root.join("Foo.cs"),
        "namespace Golden.App\n{\n    public class Foo\n    {\n        \
         public static string Bar(int x) => \"int\";\n        \
         public static string Bar(string x) => \"string\";\n    }\n}\n",
    )
    .unwrap();
    std::fs::write(
        root.join("Caller.cs"),
        "namespace Golden.App\n{\n    public class Caller\n    {\n        \
         public string Entry() => Golden.App.Foo.Bar(1);\n    }\n}\n",
    )
    .unwrap();

    let mut indexer =
        Indexer::new(root.to_path_buf(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    let graph_version = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), graph_version).unwrap();

    let bound = snapshot
        .iter()
        .find(|e| e.source_qualname == "Golden.App.Caller.Entry" && e.kind == "CALLS");
    assert_eq!(
        bound.and_then(|e| e.target_qualname.as_deref()),
        Some("Golden.App.Foo.Bar"),
        "an overload set (same file, same kind) must collapse to one target: {snapshot:#?}"
    );
}
