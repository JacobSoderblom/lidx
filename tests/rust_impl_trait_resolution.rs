//! Issue #244: `impl Trait for Type` resolves its trait the way the compiler
//! would (explicit path, `use`, alias, local declaration, prelude), and no
//! edge ever resolves to its own source symbol.

mod common;

use lidx::indexer::Indexer;
use std::path::Path;

fn index(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-impl-trait-")
        .tempdir()
        .unwrap();
    common::write_files(tmp.path(), files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

/// `(source qualname, stored target_qualname, resolved target qualname)` of
/// every IMPLEMENTS edge whose source qualname is `source`.
fn implements(indexer: &Indexer, source: &str) -> Vec<(String, Option<String>, Option<String>)> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, e.target_qualname, t.qualname
             FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ?1 AND e.kind = 'IMPLEMENTS' AND s.qualname = ?2",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv, source], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

fn unresolved_names(indexer: &Indexer) -> Vec<String> {
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare("SELECT reference_name FROM unresolved_references WHERE edge_kind = 'IMPLEMENTS'")
        .unwrap();
    stmt.query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn assert_no_self_edges(indexer: &Indexer, label: &str) {
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT e.kind, e.target_qualname FROM edges e
             WHERE e.source_symbol_id IS NOT NULL
               AND e.source_symbol_id = e.target_symbol_id
               AND e.kind != 'CALLS'",
        )
        .unwrap();
    let rows: Vec<(String, Option<String>)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(rows.is_empty(), "{label}: self-referential edges: {rows:?}");
}

const FILES: &[(&str, &str)] = &[
    (
        "Cargo.toml",
        "[package]\nname = \"demo\"\nversion = \"0.1.0\"\n",
    ),
    (
        "src/lib.rs",
        "pub mod mytrait;\npub mod mystruct;\npub mod conv;\npub mod watcher;\npub mod local;\npub mod qualified;\npub mod aliased;\npub mod unknown;\n",
    ),
    (
        "src/mytrait.rs",
        "pub trait Greet { fn greet(&self) -> String; }\n",
    ),
    (
        "src/mystruct.rs",
        "use crate::mytrait::Greet;\npub struct Greeter;\nimpl Greet for Greeter { fn greet(&self) -> String { String::new() } }\n",
    ),
    (
        "src/conv.rs",
        "pub struct MyId(u32);\nimpl From<u32> for MyId { fn from(v: u32) -> Self { MyId(v) } }\n",
    ),
    (
        "src/watcher.rs",
        "pub struct WatcherHandle;\nimpl Drop for WatcherHandle { fn drop(&mut self) {} }\n",
    ),
    (
        "src/local.rs",
        "pub trait Shape { fn area(&self) -> u32; }\npub struct Sq;\nimpl Shape for Sq { fn area(&self) -> u32 { 1 } }\n",
    ),
    (
        "src/qualified.rs",
        "pub struct Q;\nimpl crate::mytrait::Greet for Q { fn greet(&self) -> String { String::new() } }\n",
    ),
    (
        "src/aliased.rs",
        "use crate::mytrait::Greet as Hello;\npub struct A;\nimpl Hello for A { fn greet(&self) -> String { String::new() } }\n",
    ),
    (
        "src/unknown.rs",
        "pub struct U;\nimpl Mystery for U { fn go(&self) {} }\n",
    ),
];

#[test]
fn imported_trait_target_is_the_used_path_not_the_impl_module() {
    let (_t, ix) = index(FILES);
    let edges = implements(&ix, "crate::mystruct::Greeter");
    assert_eq!(edges.len(), 1, "{edges:?}");
    assert_eq!(edges[0].1.as_deref(), Some("crate::mytrait::Greet"));
    assert_eq!(edges[0].2.as_deref(), Some("crate::mytrait::Greet"));
}

#[test]
fn method_level_control_still_resolves_to_trait_method() {
    let (_t, ix) = index(FILES);
    let edges = implements(&ix, "crate::mystruct::Greeter::greet");
    assert_eq!(edges.len(), 1, "{edges:?}");
    assert_eq!(edges[0].2.as_deref(), Some("crate::mytrait::Greet::greet"));
}

#[test]
fn prelude_traits_resolve_external_not_module_qualified() {
    let (_t, ix) = index(FILES);
    for (src, name) in [
        ("crate::watcher::WatcherHandle", "Drop"),
        ("crate::conv::MyId", "From"),
    ] {
        let edges = implements(&ix, src);
        assert_eq!(edges.len(), 1, "{src}: {edges:?}");
        let stored = edges[0].1.clone().unwrap();
        assert!(
            !stored.starts_with("crate::"),
            "{src}: prelude trait must not be repo-qualified: {stored}"
        );
        assert!(stored.ends_with(name), "{stored}");
        let resolved = edges[0].2.clone().expect("prelude trait resolves");
        assert!(resolved.starts_with("ext:"), "{resolved}");
    }
    let unresolved = unresolved_names(&ix);
    assert!(
        !unresolved
            .iter()
            .any(|n| n.contains("Drop") || n.contains("From")),
        "{unresolved:?}"
    );
}

#[test]
fn method_level_implements_never_targets_itself() {
    let (_t, ix) = index(FILES);
    for src in [
        "crate::watcher::WatcherHandle::drop",
        "crate::conv::MyId::from",
    ] {
        let edges = implements(&ix, src);
        assert_eq!(edges.len(), 1, "{src}: {edges:?}");
        assert_ne!(edges[0].2.as_deref(), Some(src), "{src} implements itself");
        assert!(
            edges[0].2.as_deref().is_none_or(|t| t.starts_with("ext:")),
            "{edges:?}"
        );
    }
}

#[test]
fn local_qualified_and_aliased_traits_resolve() {
    let (_t, ix) = index(FILES);
    let local = implements(&ix, "crate::local::Sq");
    assert_eq!(local[0].1.as_deref(), Some("crate::local::Shape"));
    assert_eq!(local[0].2.as_deref(), Some("crate::local::Shape"));
    let q = implements(&ix, "crate::qualified::Q");
    assert_eq!(q[0].1.as_deref(), Some("crate::mytrait::Greet"));
    assert_eq!(q[0].2.as_deref(), Some("crate::mytrait::Greet"));
    let a = implements(&ix, "crate::aliased::A");
    assert_eq!(a[0].1.as_deref(), Some("crate::mytrait::Greet"));
    assert_eq!(a[0].2.as_deref(), Some("crate::mytrait::Greet"));
}

#[test]
fn unknown_bare_trait_is_not_module_qualified() {
    let (_t, ix) = index(FILES);
    assert!(implements(&ix, "crate::unknown::U").is_empty());
    let unresolved = unresolved_names(&ix);
    assert!(unresolved.iter().any(|n| n == "Mystery"), "{unresolved:?}");
    assert!(
        !unresolved.iter().any(|n| n.contains("unknown::Mystery")),
        "{unresolved:?}"
    );
}

#[test]
fn no_self_edges_in_synthetic_repo_or_any_fixture() {
    let (_t, ix) = index(FILES);
    assert_no_self_edges(&ix, "synthetic");

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut fixtures = Vec::new();
    for entry in std::fs::read_dir(&root).unwrap() {
        let entry = entry.unwrap();
        if !entry.file_type().unwrap().is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if name == "golden" {
            for sub in std::fs::read_dir(entry.path()).unwrap() {
                let sub = sub.unwrap();
                if sub.file_type().unwrap().is_dir() {
                    fixtures.push(format!("golden/{}", sub.file_name().to_string_lossy()));
                }
            }
        } else {
            fixtures.push(name);
        }
    }
    assert!(!fixtures.is_empty());
    for fixture in fixtures {
        let (_tmp, repo_root, db_path) = common::setup_repo(&fixture);
        let mut indexer = Indexer::new(repo_root, db_path).unwrap();
        indexer.reindex().unwrap();
        assert_no_self_edges(&indexer, &fixture);
    }
}
