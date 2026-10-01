//! Issue #206: a C# `using` of a namespace declared in several files (or a
//! reference to a `partial` type split across files) must resolve, not be
//! dropped as ambiguous -- the declarations are parts of one entity. Two
//! unrelated symbols that merely share a name stay ambiguous.

mod common;

use common::golden::{self, EdgeKey};
use lidx::indexer::Indexer;
use std::collections::BTreeSet;

type Files = Vec<(String, String)>;
/// `(target qualname, kind of the bound target symbol if any)`.
type ImportEdge = (String, Option<String>);
/// IMPORTS edges of `A.cs` plus the reference names of `ambiguous` rows.
type ImportOutcome = (BTreeSet<ImportEdge>, Vec<String>);

const USING_SOURCE: &str =
    "using Foo.Bar;\nusing Foo.Baz;\nnamespace My.App;\npublic class A { public void M() { } }\n";

fn index(files: &Files) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-cs206-")
        .tempdir()
        .unwrap();
    let borrowed: Vec<(&str, &str)> = files
        .iter()
        .map(|(p, s)| (p.as_str(), s.as_str()))
        .collect();
    common::write_files(tmp.path(), &borrowed);
    let mut indexer = Indexer::new(
        tmp.path().to_path_buf(),
        tmp.path().join(".lidx").join(".lidx.sqlite"),
    )
    .unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

fn owned(files: &[(&str, &str)]) -> Files {
    files
        .iter()
        .map(|(p, s)| (p.to_string(), s.to_string()))
        .collect()
}

/// `declarations` plus the file under test, which sorts before them in the
/// scan (`first`) or after them. Its name always ends in `suffix`.
fn with_user_file(mut declarations: Files, dir_suffix: &str, source: &str, first: bool) -> Files {
    let user = (
        format!("{}/{dir_suffix}", if first { "0" } else { "z" }),
        source.to_string(),
    );
    if first {
        declarations.insert(0, user);
    } else {
        declarations.push(user);
    }
    declarations
}

/// A `Foo.Bar` file plus `n` files each declaring `Foo.Baz`.
fn namespace_declarations(n: usize) -> Files {
    let mut files = owned(&[("m/B.cs", "namespace Foo.Bar;\npublic class X {}\n")]);
    for i in 0..n {
        files.push((
            format!("m/Baz{i}.cs"),
            format!("namespace Foo.Baz;\npublic class Y{i} {{}}\n"),
        ));
    }
    files
}

fn using_tree(first: bool, n: usize) -> Files {
    with_user_file(namespace_declarations(n), "A.cs", USING_SOURCE, first)
}

const LATEST: &str = "AND e.graph_version = (SELECT MAX(graph_version) FROM edges)";

fn imports(indexer: &Indexer) -> ImportOutcome {
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(&format!(
            "SELECT e.target_qualname, t.kind FROM edges e
               JOIN files f ON f.id = e.file_id
               LEFT JOIN symbols t ON t.id = e.target_symbol_id
              WHERE e.kind = 'IMPORTS' AND f.path LIKE '%A.cs' {LATEST}"
        ))
        .unwrap();
    let edges = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let mut stmt = conn
        .prepare(
            "SELECT reference_name FROM unresolved_references
              WHERE edge_kind = 'IMPORTS' AND reason = 'ambiguous'",
        )
        .unwrap();
    let ambiguous = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    (edges, ambiguous)
}

fn using_outcome(first: bool, n: usize) -> ImportOutcome {
    let (_tmp, indexer) = index(&using_tree(first, n));
    imports(&indexer)
}

fn assert_resolved((edges, ambiguous): &ImportOutcome) {
    assert!(ambiguous.is_empty(), "ambiguous rows: {ambiguous:?}");
    for ns in ["Foo.Bar", "Foo.Baz"] {
        assert!(
            edges.contains(&(ns.to_string(), Some("namespace".to_string()))),
            "{ns} not resolved: {edges:?}"
        );
    }
}

#[test]
fn shared_namespace_using_resolves_in_both_scan_orders() {
    let first = using_outcome(true, 2);
    let last = using_outcome(false, 2);
    assert_resolved(&first);
    assert_resolved(&last);
    assert_eq!(first, last);
}

#[test]
fn namespace_in_five_files_behaves_like_two() {
    let five = using_outcome(true, 5);
    assert_resolved(&five);
    assert_eq!(five, using_outcome(true, 2));
    assert_eq!(using_outcome(false, 5), five);
}

/// Path of the file holding the symbol the `Foo.Baz` IMPORTS edge binds to.
fn baz_target_path(indexer: &Indexer) -> Option<String> {
    let conn = indexer.db().read_conn().unwrap();
    conn.query_row(
        &format!(
            "SELECT tf.path FROM edges e
               JOIN files f ON f.id = e.file_id
               JOIN symbols t ON t.id = e.target_symbol_id
               JOIN files tf ON tf.id = t.file_id
              WHERE e.kind = 'IMPORTS' AND e.target_qualname = 'Foo.Baz'
                AND f.path LIKE '%A.cs' {LATEST}"
        ),
        [],
        |r| r.get(0),
    )
    .ok()
}

/// The chosen declaring symbol is stable: the same file in both scan orders
/// and across reindexes of an unchanged tree, with an identical edge set.
#[test]
fn shared_namespace_target_is_deterministic_across_reindexes() {
    let mut seen = Vec::new();
    for first in [true, false] {
        let (_tmp, mut indexer) = index(&using_tree(first, 3));
        let before = (imports(&indexer), baz_target_path(&indexer));
        let edges_before = snapshot(&indexer);
        indexer.reindex().unwrap();
        assert_eq!(before, (imports(&indexer), baz_target_path(&indexer)));
        assert_eq!(
            edges_before,
            snapshot(&indexer),
            "reindex changed the edge set"
        );
        seen.push(before);
    }
    assert_eq!(seen[0], seen[1]);
    assert_eq!(seen[0].1.as_deref(), Some("m/Baz0.cs"));
}

fn snapshot(indexer: &Indexer) -> BTreeSet<EdgeKey> {
    let gv = indexer.db().current_graph_version().unwrap();
    golden::snapshot_edges(indexer.db(), gv).unwrap()
}

/// Adding or deleting a declaring file incrementally moves the canonical
/// target exactly as a fresh reindex of the final tree would.
#[test]
fn shared_namespace_target_follows_incremental_sync() {
    let base = using_tree(false, 2);
    let extra = ("a/First.cs", "namespace Foo.Baz;\npublic class F {}\n");
    let (tmp, mut indexer) = index(&base);
    assert_eq!(baz_target_path(&indexer).as_deref(), Some("m/Baz0.cs"));

    common::write_files(tmp.path(), &[extra]);
    indexer.sync_rel_paths(&[extra.0.to_string()]).unwrap();
    assert_eq!(baz_target_path(&indexer).as_deref(), Some("a/First.cs"));
    let mut with_extra = base.clone();
    with_extra.push((extra.0.to_string(), extra.1.to_string()));
    let borrowed: Vec<(&str, &str)> = with_extra
        .iter()
        .map(|(p, s)| (p.as_str(), s.as_str()))
        .collect();
    let (_fresh_tmp, fresh) = common::index_files(&borrowed);
    common::assert_matches_fresh(&snapshot(&indexer), &fresh);

    std::fs::remove_file(tmp.path().join(extra.0)).unwrap();
    indexer.sync_rel_paths(&[extra.0.to_string()]).unwrap();
    assert_eq!(baz_target_path(&indexer).as_deref(), Some("m/Baz0.cs"));
    let borrowed: Vec<(&str, &str)> = base.iter().map(|(p, s)| (p.as_str(), s.as_str())).collect();
    let (_fresh_tmp, fresh) = common::index_files(&borrowed);
    common::assert_matches_fresh(&snapshot(&indexer), &fresh);
}

/// `using Foo.Dup;` against `declarations`: the outcome for `A.cs`.
fn dup_outcome(declarations: &[(&str, &str)]) -> ImportOutcome {
    let files = with_user_file(
        owned(declarations),
        "A.cs",
        "using Foo.Dup;\nnamespace My.App;\npublic class A { }\n",
        true,
    );
    let (_tmp, indexer) = index(&files);
    imports(&indexer)
}

fn assert_dup_ambiguous(declarations: &[(&str, &str)]) {
    let (edges, ambiguous) = dup_outcome(declarations);
    assert_eq!(ambiguous, vec!["Foo.Dup".to_string()], "edges: {edges:?}");
    assert!(edges.is_empty(), "edges: {edges:?}");
}

/// Two different symbols that merely share a name stay ambiguous.
#[test]
fn distinct_symbols_sharing_a_name_stay_ambiguous() {
    assert_dup_ambiguous(&[
        ("B.cs", "namespace Foo;\npublic class Dup {}\n"),
        ("C.cs", "namespace Foo;\npublic class Dup {}\n"),
    ]);
}

/// A namespace and a class with one qualname are a genuine collision, not a
/// `using` that quietly binds to the class through a later tier.
#[test]
fn namespace_and_type_sharing_a_qualname_stay_ambiguous() {
    assert_dup_ambiguous(&[
        ("B.cs", "namespace Foo.Dup;\npublic class P {}\n"),
        ("C.cs", "namespace Foo;\npublic class Dup {}\n"),
    ]);
}

/// A `partial` type and an unrelated non-partial type of the same qualname
/// (class, or positional record) do not merge: a base-list reference to the
/// qualname resolves by exact match and must report ambiguous.
#[test]
fn partial_type_and_unrelated_twin_stay_ambiguous() {
    for twin in ["public record Dup(int X);", "public class Dup { }"] {
        let files = with_user_file(
            owned(&[
                ("m/P.cs", "namespace Foo;\npublic partial class Dup { }\n"),
                ("m/Q.cs", &format!("namespace Foo;\n{twin}\n")),
            ]),
            "U.cs",
            "namespace My.App;\npublic class U : Foo.Dup { }\n",
            true,
        );
        let (_tmp, indexer) = index(&files);
        assert_eq!(base_target(&indexer), None, "twin {twin} merged");
        let conn = indexer.db().read_conn().unwrap();
        let ambiguous: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM unresolved_references WHERE reason = 'ambiguous'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(ambiguous > 0, "twin {twin}: expected an ambiguous row");
    }
}

/// `(qualname, defining file)` the base-list edge of `My.App.U` binds to.
fn base_target(indexer: &Indexer) -> Option<(String, String)> {
    let conn = indexer.db().read_conn().unwrap();
    conn.query_row(
        &format!(
            "SELECT t.qualname, tf.path FROM edges e
               JOIN symbols s ON s.id = e.source_symbol_id
               JOIN symbols t ON t.id = e.target_symbol_id
               JOIN files tf ON tf.id = t.file_id
              WHERE s.qualname = 'My.App.U' AND e.kind IN ('EXTENDS', 'INHERITS')
                AND e.target_qualname != 'object' {LATEST}"
        ),
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .ok()
}

/// Partial types of every kind, split across files, resolve identically in
/// both scan orders and expose no `partial` marker in their signature.
#[test]
fn partial_types_split_across_files_are_not_ambiguous() {
    let cases = [
        ("partial class", "partial class", "class"),
        ("partial struct", "partial struct", "struct"),
        ("partial interface", "partial interface", "interface"),
        ("partial record", "partial record", "record"),
    ];
    for (label, decl, kind) in cases {
        let mut results = Vec::new();
        for first in [true, false] {
            let files = with_user_file(
                owned(&[
                    (
                        "m/P1.cs",
                        &format!("namespace Foo;\npublic {decl} Dup {{ }}\n"),
                    ),
                    (
                        "m/P2.cs",
                        &format!("namespace Foo;\npublic {decl} Dup {{ }}\n"),
                    ),
                ]),
                "U.cs",
                "namespace My.App;\npublic class U : Foo.Dup { }\n",
                first,
            );
            let (_tmp, indexer) = index(&files);
            // A struct/record base is not valid C#; only the edge's binding matters.
            let target = base_target(&indexer);
            assert_eq!(
                target,
                Some(("Foo.Dup".to_string(), "m/P1.cs".to_string())),
                "{label} (user first = {first})"
            );
            let sym = indexer
                .db()
                .get_symbol_by_qualname("Foo.Dup", indexer.db().current_graph_version().unwrap())
                .unwrap()
                .unwrap();
            assert_eq!(sym.kind, kind);
            assert_eq!(sym.signature, None, "{label}: partial marker leaked");
            results.push(target);
        }
        assert_eq!(results[0], results[1]);
    }
}

/// A positional `partial record` keeps its parameter list as its signature
/// and still merges with its other part.
#[test]
fn partial_positional_record_merges_and_shows_only_parameters() {
    let files = with_user_file(
        owned(&[
            (
                "m/P1.cs",
                "namespace Foo;\npublic partial record Dup(int X);\n",
            ),
            ("m/P2.cs", "namespace Foo;\npublic partial record Dup;\n"),
        ]),
        "U.cs",
        "namespace My.App;\npublic class U : Foo.Dup { }\n",
        true,
    );
    let (_tmp, indexer) = index(&files);
    assert_eq!(
        base_target(&indexer),
        Some(("Foo.Dup".to_string(), "m/P1.cs".to_string()))
    );
    let gv = indexer.db().current_graph_version().unwrap();
    let sigs: BTreeSet<Option<String>> = indexer
        .db()
        .get_symbols_by_qualname("Foo.Dup", gv)
        .unwrap()
        .into_iter()
        .map(|s| s.signature)
        .collect();
    assert_eq!(sigs, BTreeSet::from([None, Some("(int X)".to_string())]));
}

/// Calls through a partial class split across files all resolve.
#[test]
fn partial_class_members_resolve_in_both_scan_orders() {
    let mut results = Vec::new();
    for first in [true, false] {
        let files = with_user_file(
            owned(&[
                (
                    "m/P1.cs",
                    "namespace Foo;\npublic partial class Part { public void One() { } }\n",
                ),
                (
                    "m/P2.cs",
                    "namespace Foo;\npublic partial class Part { public void Two() { } }\n",
                ),
            ]),
            "U.cs",
            "namespace My.App;\npublic class U { public void Go(Foo.Part p) { p.One(); p.Two(); var n = new Foo.Part(); } }\n",
            first,
        );
        let (_tmp, indexer) = index(&files);
        let conn = indexer.db().read_conn().unwrap();
        let targets: Vec<Option<String>> = conn
            .prepare(&format!(
                "SELECT t.qualname FROM edges e
                   JOIN symbols s ON s.id = e.source_symbol_id
                   LEFT JOIN symbols t ON t.id = e.target_symbol_id
                  WHERE s.qualname = 'My.App.U.Go' AND e.kind = 'CALLS' {LATEST}
                  ORDER BY 1"
            ))
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        for expected in ["Foo.Part", "Foo.Part.One", "Foo.Part.Two"] {
            assert!(
                targets.iter().any(|t| t.as_deref() == Some(expected)),
                "{expected} unresolved (user first = {first}): {targets:?}"
            );
        }
        let ambiguous: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM unresolved_references WHERE reason = 'ambiguous'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ambiguous, 0, "user first = {first}");
        results.push(targets);
    }
    assert_eq!(results[0], results[1]);
}

/// Guards the downstream behaviour: a bare type name in a file whose `using`
/// of a shared namespace resolves binds to that namespace's type, in both scan
/// orders. (On main this already held, because the import tier reads the
/// `using` text from unresolved IMPORTS rows too; the fix keeps the edge.)
#[test]
fn bare_reference_binds_through_shared_namespace_using() {
    for first in [true, false] {
        let files = with_user_file(
            owned(&[
                ("m/Baz0.cs", "namespace Foo.Baz;\npublic class Y0 {}\n"),
                ("m/Baz1.cs", "namespace Foo.Baz;\npublic class Y1 {}\n"),
                ("m/Other.cs", "namespace Other;\npublic class Y0 {}\n"),
            ]),
            "U.cs",
            "using Foo.Baz;\nnamespace My.App;\npublic class U : Y0 { }\n",
            first,
        );
        let (_tmp, indexer) = index(&files);
        assert_eq!(
            base_target(&indexer).map(|(q, _)| q).as_deref(),
            Some("Foo.Baz.Y0"),
            "user first = {first}"
        );
    }
}
