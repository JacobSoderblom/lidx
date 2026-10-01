//! Issue #206: a C# `using` of a namespace declared in several files (or a
//! reference to a `partial` class split across files) must resolve, not be
//! dropped as ambiguous -- the declarations are parts of one entity.

mod common;

use lidx::indexer::Indexer;
use std::collections::BTreeSet;

fn index(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-cs206-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
    let mut indexer = Indexer::new(
        tmp.path().to_path_buf(),
        tmp.path().join(".lidx").join(".lidx.sqlite"),
    )
    .unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

fn user(path: &'static str) -> (&'static str, &'static str) {
    (
        path,
        "using Foo.Bar;\nusing Foo.Baz;\nnamespace My.App;\npublic class A { public void M() { } }\n",
    )
}

fn decls(n: usize) -> Vec<(String, String)> {
    let mut v = vec![(
        "B.cs".to_string(),
        "namespace Foo.Bar;\npublic class X {}\n".to_string(),
    )];
    for i in 0..n {
        v.push((
            format!("Baz{i}.cs"),
            format!("namespace Foo.Baz;\npublic class Y{i} {{}}\n"),
        ));
    }
    v
}

fn tree(user_first: bool, n: usize) -> Vec<(String, String)> {
    let u = user(if user_first { "0/A.cs" } else { "z/A.cs" });
    let mut files = decls(n);
    // Nest declaring files so the scan order flips with the user file's dir.
    for f in files.iter_mut() {
        f.0 = format!("m/{}", f.0);
    }
    let u = (u.0.to_string(), u.1.to_string());
    if user_first {
        files.insert(0, u);
    } else {
        files.push(u);
    }
    files
}

/// `(target qualname or None)` per IMPORTS edge of the user file, plus the
/// `ambiguous` unresolved IMPORTS rows.
fn imports(indexer: &Indexer) -> (BTreeSet<(String, Option<String>)>, Vec<String>) {
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT e.target_qualname, t.id, t.kind, f.path FROM edges e
               JOIN files f ON f.id = e.file_id
               LEFT JOIN symbols t ON t.id = e.target_symbol_id
              WHERE e.kind = 'IMPORTS' AND f.path LIKE '%A.cs'
                AND e.graph_version = (SELECT MAX(graph_version) FROM edges)",
        )
        .unwrap();
    let edges = stmt
        .query_map([], |r| {
            let q: String = r.get(0)?;
            let id: Option<i64> = r.get(1)?;
            let kind: Option<String> = r.get(2)?;
            Ok((q, id.map(|_| kind.unwrap_or_default())))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let mut stmt = conn
        .prepare(
            "SELECT reference_name FROM unresolved_references
              WHERE edge_kind = 'IMPORTS' AND reason = 'ambiguous'",
        )
        .unwrap();
    let amb = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    (edges, amb)
}

fn run(user_first: bool, n: usize) -> (BTreeSet<(String, Option<String>)>, Vec<String>) {
    let owned = tree(user_first, n);
    let files: Vec<(&str, &str)> = owned
        .iter()
        .map(|(p, s)| (p.as_str(), s.as_str()))
        .collect();
    let (_tmp, indexer) = index(&files);
    imports(&indexer)
}

fn assert_resolved(r: &(BTreeSet<(String, Option<String>)>, Vec<String>)) {
    assert!(r.1.is_empty(), "ambiguous rows: {:?}", r.1);
    for ns in ["Foo.Bar", "Foo.Baz"] {
        assert!(
            r.0.contains(&(ns.to_string(), Some("namespace".to_string()))),
            "{ns} not resolved: {:?}",
            r.0
        );
    }
}

#[test]
fn shared_namespace_using_resolves_in_both_scan_orders() {
    let first = run(true, 2);
    let last = run(false, 2);
    assert_resolved(&first);
    assert_resolved(&last);
    assert_eq!(first, last);
}

#[test]
fn namespace_in_five_files_behaves_like_two() {
    let five = run(true, 5);
    assert_resolved(&five);
    assert_eq!(five, run(true, 2));
    assert_eq!(run(false, 5), five);
}

/// The chosen declaring symbol is stable: same file for both scan orders and
/// across reindexes of an unchanged tree.
#[test]
fn shared_namespace_target_is_deterministic_across_reindexes() {
    let target = |indexer: &Indexer| -> (Vec<(String, Option<String>)>, String) {
        let conn = indexer.db().read_conn().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT e.target_qualname, tf.path FROM edges e
                   JOIN files f ON f.id = e.file_id
                   LEFT JOIN symbols t ON t.id = e.target_symbol_id
                   LEFT JOIN files tf ON tf.id = t.file_id
                  WHERE e.kind = 'IMPORTS' AND f.path LIKE '%A.cs'
                    AND e.graph_version = (SELECT MAX(graph_version) FROM edges)
                  ORDER BY 1",
            )
            .unwrap();
        let rows: Vec<(String, Option<String>)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        let baz = rows
            .iter()
            .find(|(q, _)| q == "Foo.Baz")
            .and_then(|(_, p)| p.clone())
            .unwrap_or_default();
        (rows, baz)
    };
    let mut seen = Vec::new();
    for user_first in [true, false] {
        let owned = tree(user_first, 3);
        let files: Vec<(&str, &str)> = owned
            .iter()
            .map(|(p, s)| (p.as_str(), s.as_str()))
            .collect();
        let (_tmp, mut indexer) = index(&files);
        let a = target(&indexer);
        indexer.reindex().unwrap();
        let b = target(&indexer);
        assert_eq!(a, b, "reindex changed IMPORTS targets");
        seen.push(a);
    }
    assert_eq!(seen[0], seen[1]);
    assert_eq!(seen[0].1, "m/Baz0.cs");
}

/// Two different symbols that merely share a name stay ambiguous.
#[test]
fn distinct_symbols_sharing_a_name_stay_ambiguous() {
    let files = [
        (
            "A.cs",
            "using Foo.Dup;\nnamespace My.App;\npublic class A { }\n",
        ),
        ("B.cs", "namespace Foo;\npublic class Dup {}\n"),
        ("C.cs", "namespace Foo;\npublic class Dup {}\n"),
    ];
    let (_tmp, indexer) = index(&files);
    let (edges, amb) = imports(&indexer);
    assert_eq!(amb, vec!["Foo.Dup".to_string()], "edges: {edges:?}");
}

/// Partial classes: the same qualname declared in several files.
fn partial_files(user_first: bool) -> Vec<(&'static str, &'static str)> {
    let user = (
        if user_first { "0/U.cs" } else { "z/U.cs" },
        "namespace My.App;\npublic class U : Foo.Part { public void Go(Foo.Part p) { p.One(); p.Two(); var n = new Foo.Part(); } }\n",
    );
    let mut files = vec![
        (
            "m/P1.cs",
            "namespace Foo;\npublic partial class Part { public void One() { } }\n",
        ),
        (
            "m/P2.cs",
            "namespace Foo;\npublic partial class Part { public void Two() { } }\n",
        ),
    ];
    if user_first {
        files.insert(0, user);
    } else {
        files.push(user);
    }
    files
}

fn go_targets(indexer: &Indexer) -> (Vec<(String, Option<String>)>, usize) {
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(e.target_qualname, ''), t.qualname FROM edges e
               JOIN symbols s ON s.id = e.source_symbol_id
               LEFT JOIN symbols t ON t.id = e.target_symbol_id
              WHERE s.qualname = 'My.App.U.Go' AND e.kind = 'CALLS' ORDER BY 1",
        )
        .unwrap();
    let rows = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let amb: usize = conn
        .query_row(
            "SELECT COUNT(*) FROM unresolved_references WHERE reason = 'ambiguous'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    (rows, amb)
}

#[test]
fn partial_class_split_across_files_is_not_ambiguous() {
    let mut results = Vec::new();
    for user_first in [true, false] {
        let (_tmp, indexer) = index(&partial_files(user_first));
        let (rows, amb) = go_targets(&indexer);
        assert_eq!(amb, 0, "ambiguous rows (user_first={user_first}): {rows:?}");
        for m in ["One", "Two"] {
            assert!(
                rows.iter()
                    .any(|(_, t)| t.as_deref() == Some(&format!("Foo.Part.{m}"))),
                "{m} unresolved (user_first={user_first}): {rows:?}"
            );
        }
        let conn = indexer.db().read_conn().unwrap();
        let base: Vec<(String, Option<String>)> = conn
            .prepare(
                "SELECT t.qualname, tf.path FROM edges e
                   JOIN symbols s ON s.id = e.source_symbol_id
                   JOIN symbols t ON t.id = e.target_symbol_id
                   JOIN files tf ON tf.id = t.file_id
                  WHERE s.qualname = 'My.App.U' AND e.kind IN ('EXTENDS','INHERITS')",
            )
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            base,
            vec![("Foo.Part".to_string(), Some("m/P1.cs".to_string()))],
            "user_first={user_first}"
        );
        results.push(rows);
    }
    assert_eq!(results[0], results[1]);
}

/// Downstream benefit: a bare type name in a file whose `using` of a shared
/// namespace now resolves binds to that namespace's type.
#[test]
fn bare_reference_gains_candidate_via_shared_namespace_using() {
    for user_first in [true, false] {
        let user = (
            if user_first { "0/U.cs" } else { "z/U.cs" },
            "using Foo.Baz;\nnamespace My.App;\npublic class U : Y0 { public void Go() { var y = new Y1(); } }\n",
        );
        let mut files = vec![
            ("m/Baz0.cs", "namespace Foo.Baz;\npublic class Y0 {}\n"),
            ("m/Baz1.cs", "namespace Foo.Baz;\npublic class Y1 {}\n"),
            ("m/Other.cs", "namespace Other;\npublic class Y0 {}\n"),
        ];
        if user_first {
            files.insert(0, user);
        } else {
            files.push(user);
        }
        let (_tmp, indexer) = index(&files);
        let conn = indexer.db().read_conn().unwrap();
        let t: Option<String> = conn
            .query_row(
                "SELECT t.qualname FROM edges e
                   JOIN symbols s ON s.id = e.source_symbol_id
                   JOIN symbols t ON t.id = e.target_symbol_id
                  WHERE s.qualname = 'My.App.U' AND e.kind IN ('EXTENDS','INHERITS')",
                [],
                |r| r.get(0),
            )
            .ok();
        assert_eq!(t.as_deref(), Some("Foo.Baz.Y0"), "user_first={user_first}");
    }
}

fn baz_target_path(indexer: &Indexer) -> Option<String> {
    let conn = indexer.db().read_conn().unwrap();
    conn.query_row(
        "SELECT tf.path FROM edges e
           JOIN files f ON f.id = e.file_id
           JOIN symbols t ON t.id = e.target_symbol_id
           JOIN files tf ON tf.id = t.file_id
          WHERE e.kind = 'IMPORTS' AND e.target_qualname = 'Foo.Baz'
            AND f.path LIKE '%A.cs'
            AND e.graph_version = (SELECT MAX(graph_version) FROM edges)",
        [],
        |r| r.get(0),
    )
    .ok()
}

/// Adding or deleting a declaring file incrementally moves the canonical
/// target exactly as a fresh reindex of the final tree would.
#[test]
fn shared_namespace_target_follows_incremental_sync() {
    let owned = tree(false, 2);
    let files: Vec<(&str, &str)> = owned
        .iter()
        .map(|(p, s)| (p.as_str(), s.as_str()))
        .collect();
    let (tmp, mut indexer) = index(&files);
    assert_eq!(baz_target_path(&indexer).as_deref(), Some("m/Baz0.cs"));

    common::write_files(
        tmp.path(),
        &[("a/First.cs", "namespace Foo.Baz;\npublic class F {}\n")],
    );
    indexer.sync_rel_paths(&["a/First.cs".to_string()]).unwrap();
    assert_eq!(baz_target_path(&indexer).as_deref(), Some("a/First.cs"));
    let gv = indexer.db().current_graph_version().unwrap();
    let snap = lidx_snapshot(&indexer, gv);
    let mut all = files.clone();
    all.push(("a/First.cs", "namespace Foo.Baz;\npublic class F {}\n"));
    let (_t, fresh) = common::index_files(&all);
    common::assert_matches_fresh(&snap, &fresh);

    std::fs::remove_file(tmp.path().join("a/First.cs")).unwrap();
    indexer.sync_rel_paths(&["a/First.cs".to_string()]).unwrap();
    assert_eq!(baz_target_path(&indexer).as_deref(), Some("m/Baz0.cs"));
    let gv = indexer.db().current_graph_version().unwrap();
    let snap = lidx_snapshot(&indexer, gv);
    let (_t, fresh) = common::index_files(&files);
    common::assert_matches_fresh(&snap, &fresh);
}

fn lidx_snapshot(indexer: &Indexer, gv: i64) -> BTreeSet<common::golden::EdgeKey> {
    common::golden::snapshot_edges(indexer.db(), gv).unwrap()
}
