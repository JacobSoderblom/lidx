//! `global using` applies to the whole project (nearest `.csproj` directory),
//! and stays correct incrementally: adding, editing or removing one in a file
//! re-resolves the other files of its project (issue #185).
mod common;

use common::golden;
use lidx::indexer::Indexer;
use std::path::PathBuf;

const INTERFACES: (&str, &str) = (
    "lib/Interfaces.cs",
    "namespace N1 { public interface IA { void Run(); } }\nnamespace N2 { public interface IA { void Run(); } }\n",
);
const CSPROJS: [(&str, &str); 3] = [
    ("lib/Lib.csproj", "<Project />\n"),
    ("p1/P1.csproj", "<Project />\n"),
    ("p2/P2.csproj", "<Project />\n"),
];
const P1_CODE: (&str, &str) = (
    "p1/Code.cs",
    "namespace P1 { public class Impl : IA { public void Run() { } }\n public class Caller { private readonly IA _a; public void Go() { _a.Run(); } } }\n",
);
const P2_CODE: (&str, &str) = (
    "p2/Code.cs",
    "namespace P2 { public class Impl : IA { public void Run() { } }\n public class Caller { private readonly IA _a; public void Go() { _a.Run(); } } }\n",
);
const GLOBALS: &str = "p1/Globals.cs";

fn tree(globals: Option<&str>) -> Vec<(&'static str, String)> {
    let mut files: Vec<(&str, String)> = CSPROJS
        .iter()
        .chain([&INTERFACES, &P1_CODE, &P2_CODE])
        .map(|(p, s)| (*p, s.to_string()))
        .collect();
    if let Some(g) = globals {
        files.push((GLOBALS, g.to_string()));
    }
    files
}

fn write(root: &std::path::Path, files: &[(&str, String)]) {
    let refs: Vec<(&str, &str)> = files.iter().map(|(p, s)| (*p, s.as_str())).collect();
    common::write_files(root, &refs);
}

fn indexed(files: &[(&str, String)]) -> (tempfile::TempDir, PathBuf, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-csglobal-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    write(&root, files);
    let mut indexer = Indexer::new(root.clone(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    (tmp, root, indexer)
}

/// `(implemented interface, called interface method)` of one project's code.
fn resolved(indexer: &Indexer, ns: &str) -> (Option<String>, Option<String>) {
    let conn = indexer.db().read_conn().unwrap();
    let target = |kind: &str, source: &str| -> Option<String> {
        conn.query_row(
            "SELECT t.qualname FROM edges e
               JOIN symbols s ON s.id = e.source_symbol_id
               JOIN symbols t ON t.id = e.target_symbol_id
              WHERE e.kind = ?1 AND s.qualname = ?2",
            rusqlite::params![kind, source],
            |r| r.get(0),
        )
        .ok()
    };
    (
        target("IMPLEMENTS", &format!("{ns}.Impl")),
        target("CALLS", &format!("{ns}.Caller.Go")),
    )
}

fn snapshot(indexer: &Indexer) -> std::collections::BTreeSet<golden::EdgeKey> {
    let gv = indexer.db().current_graph_version().unwrap();
    golden::snapshot_edges(indexer.db(), gv).unwrap()
}

fn fresh(files: &[(&str, String)]) -> std::collections::BTreeSet<golden::EdgeKey> {
    let (_t, _root, indexer) = indexed(files);
    snapshot(&indexer)
}

#[test]
fn global_using_applies_to_its_whole_project_only() {
    let (_t, _r, indexer) = indexed(&tree(Some("global using N1;\n")));
    assert_eq!(
        resolved(&indexer, "P1"),
        (Some("N1.IA".into()), Some("N1.IA.Run".into()))
    );
    // Another project's files do not see it: still ambiguous.
    assert_eq!(resolved(&indexer, "P2"), (None, None));
}

#[test]
fn adding_a_global_using_re_resolves_unchanged_files_of_its_project() {
    let (_t, root, mut indexer) = indexed(&tree(None));
    assert_eq!(resolved(&indexer, "P1"), (None, None));
    let with = tree(Some("global using N1;\n"));
    write(&root, &with);
    indexer.sync_rel_paths(&[GLOBALS.to_string()]).unwrap();
    assert_eq!(
        resolved(&indexer, "P1"),
        (Some("N1.IA".into()), Some("N1.IA.Run".into()))
    );
    assert_eq!(resolved(&indexer, "P2"), (None, None));
    common::assert_no_dangling_edge_targets(indexer.db());
    common::assert_matches_fresh(&snapshot(&indexer), &fresh(&with));
}

#[test]
fn editing_a_global_using_re_resolves_its_project() {
    let (_t, root, mut indexer) = indexed(&tree(Some("global using N1;\n")));
    let edited = tree(Some("global using N2;\n"));
    write(&root, &edited);
    indexer.sync_rel_paths(&[GLOBALS.to_string()]).unwrap();
    assert_eq!(
        resolved(&indexer, "P1"),
        (Some("N2.IA".into()), Some("N2.IA.Run".into()))
    );
    common::assert_matches_fresh(&snapshot(&indexer), &fresh(&edited));
}

#[test]
fn removing_a_global_using_file_re_resolves_its_project() {
    let (_t, root, mut indexer) = indexed(&tree(Some("global using N1;\n")));
    std::fs::remove_file(root.join(GLOBALS)).unwrap();
    indexer.sync_rel_paths(&[GLOBALS.to_string()]).unwrap();
    assert_eq!(resolved(&indexer, "P1"), (None, None));
    common::assert_no_dangling_edge_targets(indexer.db());
    common::assert_matches_fresh(&snapshot(&indexer), &fresh(&tree(None)));
}

#[test]
fn full_reindex_notices_a_changed_global_using() {
    let (_t, root, mut indexer) = indexed(&tree(None));
    write(&root, &tree(Some("global using N1;\n")));
    indexer.reindex().unwrap();
    assert_eq!(
        resolved(&indexer, "P1"),
        (Some("N1.IA".into()), Some("N1.IA.Run".into()))
    );
    common::assert_matches_fresh(
        &snapshot(&indexer),
        &fresh(&tree(Some("global using N1;\n"))),
    );
}

#[test]
fn without_a_csproj_global_usings_apply_to_the_whole_repo() {
    let mut files = vec![
        (INTERFACES.0, INTERFACES.1.to_string()),
        ("Code.cs", P1_CODE.1.replace("P1", "Root")),
        ("Globals.cs", "global using N1;\n".to_string()),
    ];
    let (_t, root, mut indexer) = indexed(&files);
    assert_eq!(
        resolved(&indexer, "Root"),
        (Some("N1.IA".into()), Some("N1.IA.Run".into()))
    );
    files.pop();
    std::fs::remove_file(root.join("Globals.cs")).unwrap();
    indexer.sync_rel_paths(&["Globals.cs".to_string()]).unwrap();
    assert_eq!(resolved(&indexer, "Root"), (None, None));
    common::assert_matches_fresh(&snapshot(&indexer), &fresh(&files));
}
