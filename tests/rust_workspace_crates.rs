//! Issue #434: in a Cargo workspace, a path that starts with another member's
//! crate name (`a::Foo::new()`, or `Foo::new()` after `use a::Foo;`) binds to
//! that member's item and to nothing else, an external crate of the same name
//! stays external, and an incremental sync after a manifest change matches a
//! fresh reindex.

mod common;

use common::golden;
use lidx::indexer::Indexer;
use std::collections::BTreeSet;
use std::path::PathBuf;

const WORKSPACE: &str = "[workspace]\nmembers = [\"crates/*\"]\n";

const A_MANIFEST: &str = "[package]\nname = \"a\"\nversion = \"0.1.0\"\n";
const A_LIB: &str = "mod shapes;\npub use shapes::Foo;\n\npub fn helper() {}\n";
const A_SHAPES: &str = "pub struct Foo;\nimpl Foo {\n    pub fn new() -> Foo { Foo }\n    pub fn size(&self) -> u32 { 1 }\n}\n";

/// A decoy with the same type and method names, in a package `b` does not use.
const C_MANIFEST: &str = "[package]\nname = \"c\"\nversion = \"0.1.0\"\n";
const C_LIB: &str = "pub struct Foo;\nimpl Foo {\n    pub fn new() -> Foo { Foo }\n    pub fn size(&self) -> u32 { 2 }\n}\npub fn helper() {}\n";

const B_MANIFEST: &str =
    "[package]\nname = \"b\"\nversion = \"0.1.0\"\n[dependencies]\na = { path = \"../a\" }\n";

fn files(b_lib: &str) -> Vec<(&str, &str)> {
    vec![
        ("Cargo.toml", WORKSPACE),
        ("crates/a/Cargo.toml", A_MANIFEST),
        ("crates/a/src/lib.rs", A_LIB),
        ("crates/a/src/shapes.rs", A_SHAPES),
        ("crates/c/Cargo.toml", C_MANIFEST),
        ("crates/c/src/lib.rs", C_LIB),
        ("crates/b/Cargo.toml", B_MANIFEST),
        ("crates/b/src/lib.rs", b_lib),
    ]
}

fn targets(b_lib: &str, caller: &str) -> BTreeSet<String> {
    common::call_targets(&files(b_lib), caller)
        .into_iter()
        .collect()
}

#[test]
fn crate_named_path_binds_to_that_members_item() {
    let got = targets(
        "pub fn go() {\n    let _f = a::Foo::new();\n    a::helper();\n}\n",
        "crate::go",
    );
    assert!(got.contains("crate::shapes::Foo::new"), "{got:#?}");
    assert!(got.contains("crate::helper"), "{got:#?}");
    // `c` declares the same names; none of its items may be bound.
    let all = common::call_targets(
        &files("pub fn go() {\n    let _f = a::Foo::new();\n    a::helper();\n}\n"),
        "crate::go",
    );
    assert_eq!(all.len(), 2, "{all:#?}");
}

#[test]
fn crate_named_path_binds_to_the_right_file() {
    let (_tmp, root, db_path) = common::index_repo(
        "lidx-ws-binds-",
        &files("pub fn go() {\n    let _f = a::Foo::new();\n    a::helper();\n}\n"),
    );
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT f.path, t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             JOIN files f ON f.id = t.file_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS' AND s.qualname = 'crate::go'
             ORDER BY 1",
        )
        .unwrap();
    let rows: Vec<(String, String)> = stmt
        .query_map([gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        rows,
        vec![
            (
                "crates/a/src/lib.rs".to_string(),
                "crate::helper".to_string()
            ),
            (
                "crates/a/src/shapes.rs".to_string(),
                "crate::shapes::Foo::new".to_string()
            ),
        ]
    );
}

#[test]
fn use_imported_crate_item_binds_to_that_members_item() {
    let got = targets(
        "use a::Foo;\npub fn go() {\n    let f = Foo::new();\n    f.size();\n}\n",
        "crate::go",
    );
    assert!(got.contains("crate::shapes::Foo::new"), "{got:#?}");
    assert!(got.contains("crate::shapes::Foo::size"), "{got:#?}");
    assert_eq!(got.len(), 2, "{got:#?}");
}

#[test]
fn parameter_typed_with_an_imported_member_type_binds_its_methods() {
    let got = targets(
        "use a::Foo;\npub fn go(f: &Foo) -> u32 {\n    f.size()\n}\n",
        "crate::go",
    );
    assert_eq!(
        got,
        BTreeSet::from(["crate::shapes::Foo::size".to_string()]),
        "{got:#?}"
    );
}

#[test]
fn use_imported_function_binds_to_that_members_function() {
    let got = targets(
        "use a::helper;\npub fn go() {\n    helper();\n}\n",
        "crate::go",
    );
    assert_eq!(
        got,
        BTreeSet::from(["crate::helper".to_string()]),
        "{got:#?}"
    );
}

#[test]
fn builder_chain_through_a_member_stays_in_that_member() {
    let a_lib = "pub struct Builder { n: u32 }\nimpl Builder {\n    pub fn new() -> Builder { Builder { n: 0 } }\n    pub fn size(&mut self, n: u32) -> &mut Builder { self.n = n; self }\n    pub fn build(&self) -> u32 { self.n }\n}\n";
    let c_lib = "pub struct Builder { n: u32 }\nimpl Builder {\n    pub fn new() -> Builder { Builder { n: 0 } }\n    pub fn size(&mut self, n: u32) -> &mut Builder { self.n = n; self }\n    pub fn build(&self) -> u32 { self.n + 1 }\n}\n";
    let tree = vec![
        ("Cargo.toml", WORKSPACE),
        ("crates/a/Cargo.toml", A_MANIFEST),
        ("crates/a/src/lib.rs", a_lib),
        ("crates/c/Cargo.toml", C_MANIFEST),
        ("crates/c/src/lib.rs", c_lib),
        ("crates/b/Cargo.toml", B_MANIFEST),
        (
            "crates/b/src/lib.rs",
            "use a::Builder;\npub fn go() -> u32 {\n    Builder::new().size(3).build()\n}\n",
        ),
    ];
    let (_tmp, root, db_path) = common::index_repo("lidx-ws-builder-", &tree);
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT f.path FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             JOIN files f ON f.id = t.file_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS' AND s.qualname = 'crate::go'",
        )
        .unwrap();
    let paths: Vec<String> = stmt
        .query_map([gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        paths.len(),
        3,
        "new, size and build must all bind: {paths:?}"
    );
    assert!(
        paths.iter().all(|p| p == "crates/a/src/lib.rs"),
        "every link binds to crate a, never to c: {paths:?}"
    );
}

#[test]
fn facade_reexport_of_a_member_resolves_through_the_alias() {
    let tree = vec![
        ("Cargo.toml", WORKSPACE),
        ("crates/a/Cargo.toml", A_MANIFEST),
        ("crates/a/src/lib.rs", A_LIB),
        ("crates/a/src/shapes.rs", A_SHAPES),
        ("crates/c/Cargo.toml", C_MANIFEST),
        ("crates/c/src/lib.rs", C_LIB),
        (
            "crates/facade/Cargo.toml",
            "[package]\nname = \"facade\"\n[dependencies]\na = { path = \"../a\" }\n",
        ),
        ("crates/facade/src/lib.rs", "pub use a as inner;\n"),
        (
            "crates/b/Cargo.toml",
            "[package]\nname = \"b\"\n[dependencies]\nfacade = { path = \"../facade\" }\n",
        ),
        (
            "crates/b/src/lib.rs",
            "pub fn go() {\n    facade::inner::helper();\n}\n",
        ),
    ];
    let got: BTreeSet<String> = common::call_targets(&tree, "crate::go")
        .into_iter()
        .collect();
    assert_eq!(
        got,
        BTreeSet::from(["crate::helper".to_string()]),
        "{got:#?}"
    );
    let (_tmp, root, db_path) = common::index_repo("lidx-ws-facade-", &tree);
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let path: String = conn
        .query_row(
            "SELECT f.path FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             JOIN files f ON f.id = t.file_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS' AND s.qualname = 'crate::go'",
            [gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(path, "crates/a/src/lib.rs");
}

#[test]
fn registry_dependency_of_a_member_name_stays_external() {
    // `d` depends on the registry crate `a`, not on the workspace member of
    // that name, and on a crate `serde_like` no member declares.
    let tree = vec![
        ("Cargo.toml", WORKSPACE),
        ("crates/a/Cargo.toml", A_MANIFEST),
        ("crates/a/src/lib.rs", A_LIB),
        ("crates/a/src/shapes.rs", A_SHAPES),
        (
            "crates/d/Cargo.toml",
            "[package]\nname = \"d\"\n[dependencies]\na = \"1\"\nserde_like = \"1\"\n",
        ),
        (
            "crates/d/src/lib.rs",
            "pub fn go() {\n    let _x = a::Foo::new();\n    let _y = serde_like::Foo::new();\n    a::helper();\n}\n",
        ),
    ];
    let (_tmp, root, db_path) = common::index_repo("lidx-ws-external-", &tree);
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(t.kind, ''), e.target_qualname, COALESCE(f.path, '') FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             LEFT JOIN files f ON f.id = t.file_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS' AND s.qualname = 'crate::go'",
        )
        .unwrap();
    let rows: Vec<(String, String, String)> = stmt
        .query_map([gv], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(!rows.is_empty(), "{rows:#?}");
    for (kind, target, path) in &rows {
        assert!(
            kind == "external" || path.is_empty(),
            "{target} must not bind to a repo symbol: {rows:#?}"
        );
    }
}

fn indexed(tree: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-ws-sync-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, tree);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, root, indexer)
}

fn snapshot(indexer: &Indexer) -> BTreeSet<golden::EdgeKey> {
    let gv = indexer.db().current_graph_version().unwrap();
    golden::snapshot_edges(indexer.db(), gv).unwrap()
}

fn calls_into_a(snapshot: &BTreeSet<golden::EdgeKey>) -> bool {
    snapshot.iter().any(|e| {
        e.kind == "CALLS"
            && e.source_qualname == "crate::go"
            && e.target_qualname.as_deref() == Some("crate::shapes::Foo::new")
    })
}

const GO_LIB: &str = "pub fn go() {\n    let _f = a::Foo::new();\n}\n";

fn final_tree<'a>(
    root_manifest: &'a str,
    b_manifest: &'a str,
    a_manifest: &'a str,
) -> Vec<(&'a str, &'a str)> {
    vec![
        ("Cargo.toml", root_manifest),
        ("crates/a/Cargo.toml", a_manifest),
        ("crates/a/src/lib.rs", A_LIB),
        ("crates/a/src/shapes.rs", A_SHAPES),
        ("crates/c/Cargo.toml", C_MANIFEST),
        ("crates/c/src/lib.rs", C_LIB),
        ("crates/b/Cargo.toml", b_manifest),
        ("crates/b/src/lib.rs", GO_LIB),
    ]
}

/// A manifest edit alone (no `.rs` file changes) re-points hash-unchanged
/// callers, in both directions, and always lands on a fresh reindex.
#[test]
fn incremental_dependency_change_matches_a_fresh_reindex() {
    let registry_b = "[package]\nname = \"b\"\n[dependencies]\na = \"1\"\n";
    let (_tmp, root, mut indexer) = indexed(&final_tree(WORKSPACE, B_MANIFEST, A_MANIFEST));
    assert!(calls_into_a(&snapshot(&indexer)));

    common::write_files(&root, &[("crates/b/Cargo.toml", registry_b)]);
    indexer
        .sync_rel_paths(&["crates/b/Cargo.toml".to_string()])
        .unwrap();
    let after = snapshot(&indexer);
    assert!(!calls_into_a(&after), "{after:#?}");
    let (_fresh, expected) = common::index_files(&final_tree(WORKSPACE, registry_b, A_MANIFEST));
    common::assert_matches_fresh(&after, &expected);

    common::write_files(&root, &[("crates/b/Cargo.toml", B_MANIFEST)]);
    indexer
        .sync_rel_paths(&["crates/b/Cargo.toml".to_string()])
        .unwrap();
    let back = snapshot(&indexer);
    assert!(calls_into_a(&back), "{back:#?}");
    let (_fresh, expected) = common::index_files(&final_tree(WORKSPACE, B_MANIFEST, A_MANIFEST));
    common::assert_matches_fresh(&back, &expected);
}

/// Adding a package to the workspace `members` makes its name resolvable
/// for the members that did not declare it.
#[test]
fn incremental_members_change_matches_a_fresh_reindex() {
    let narrow = "[workspace]\nmembers = [\"crates/b\", \"crates/c\"]\n";
    let undeclared_b = "[package]\nname = \"b\"\n";
    let (_tmp, root, mut indexer) = indexed(&final_tree(narrow, undeclared_b, A_MANIFEST));
    assert!(!calls_into_a(&snapshot(&indexer)));

    common::write_files(&root, &[("Cargo.toml", WORKSPACE)]);
    indexer.sync_rel_paths(&["Cargo.toml".to_string()]).unwrap();
    let after = snapshot(&indexer);
    assert!(calls_into_a(&after), "{after:#?}");
    let (_fresh, expected) = common::index_files(&final_tree(WORKSPACE, undeclared_b, A_MANIFEST));
    common::assert_matches_fresh(&after, &expected);
}

/// Renaming a member's package stops its old name from resolving, with no
/// edit to any caller.
#[test]
fn incremental_member_rename_matches_a_fresh_reindex() {
    let undeclared_b = "[package]\nname = \"b\"\n";
    let renamed_a = "[package]\nname = \"a_renamed\"\nversion = \"0.1.0\"\n";
    let (_tmp, root, mut indexer) = indexed(&final_tree(WORKSPACE, undeclared_b, A_MANIFEST));
    assert!(calls_into_a(&snapshot(&indexer)));

    common::write_files(&root, &[("crates/a/Cargo.toml", renamed_a)]);
    indexer
        .sync_rel_paths(&["crates/a/Cargo.toml".to_string()])
        .unwrap();
    let after = snapshot(&indexer);
    assert!(!calls_into_a(&after), "{after:#?}");
    let (_fresh, expected) = common::index_files(&final_tree(WORKSPACE, undeclared_b, renamed_a));
    common::assert_matches_fresh(&after, &expected);

    // The same through a full reindex: stale callers are re-extracted too.
    common::write_files(&root, &[("crates/a/Cargo.toml", A_MANIFEST)]);
    indexer.reindex().unwrap();
    let restored = snapshot(&indexer);
    assert!(calls_into_a(&restored), "{restored:#?}");
    let (_fresh, expected) = common::index_files(&final_tree(WORKSPACE, undeclared_b, A_MANIFEST));
    common::assert_matches_fresh(&restored, &expected);
}

/// A library root's `pub use a as inner;` is part of what a path means.
#[test]
fn incremental_reexport_change_matches_a_fresh_reindex() {
    let facade_manifest = "[package]\nname = \"facade\"\n[dependencies]\na = { path = \"../a\" }\n";
    let b_manifest = "[package]\nname = \"b\"\n[dependencies]\nfacade = { path = \"../facade\" }\n";
    let b_lib = "pub fn go() {\n    let _f = facade::inner::Foo::new();\n}\n";
    let tree = |facade_lib: &'static str| {
        vec![
            ("Cargo.toml", WORKSPACE),
            ("crates/a/Cargo.toml", A_MANIFEST),
            ("crates/a/src/lib.rs", A_LIB),
            ("crates/a/src/shapes.rs", A_SHAPES),
            ("crates/c/Cargo.toml", C_MANIFEST),
            ("crates/c/src/lib.rs", C_LIB),
            ("crates/facade/Cargo.toml", facade_manifest),
            ("crates/facade/src/lib.rs", facade_lib),
            ("crates/b/Cargo.toml", b_manifest),
            ("crates/b/src/lib.rs", b_lib),
        ]
    };
    let (_tmp, root, mut indexer) = indexed(&tree("pub use a as other;\n"));
    assert!(!calls_into_a(&snapshot(&indexer)));

    common::write_files(
        &root,
        &[("crates/facade/src/lib.rs", "pub use a as inner;\n")],
    );
    indexer
        .sync_rel_paths(&["crates/facade/src/lib.rs".to_string()])
        .unwrap();
    let after = snapshot(&indexer);
    assert!(calls_into_a(&after), "{after:#?}");
    let (_fresh, expected) = common::index_files(&tree("pub use a as inner;\n"));
    common::assert_matches_fresh(&after, &expected);
}

fn call_paths(tree: &[(&str, &str)], caller: &str) -> Vec<(String, String)> {
    let (_tmp, root, db_path) = common::index_repo("lidx-ws-paths-", tree);
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(f.path, ''), COALESCE(t.qualname, e.target_qualname, '') FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             LEFT JOIN files f ON f.id = t.file_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS' AND s.qualname = ?
             ORDER BY 1, 2",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv, caller], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

/// `pkg::helpers::f` where `f` is declared in a private submodule of
/// `helpers` and re-exported with a glob.
#[test]
fn pinned_path_through_a_reexporting_module_binds_the_package_item() {
    let tree = vec![
        ("Cargo.toml", WORKSPACE),
        ("crates/a/Cargo.toml", A_MANIFEST),
        ("crates/a/src/lib.rs", "pub mod helpers;\n"),
        ("crates/a/src/helpers/mod.rs", "mod imp;\npub use imp::*;\n"),
        (
            "crates/a/src/helpers/imp.rs",
            "pub fn highlight_end() -> u32 { 1 }\n",
        ),
        ("crates/c/Cargo.toml", C_MANIFEST),
        (
            "crates/c/src/lib.rs",
            "pub fn highlight_end() -> u32 { 2 }\n",
        ),
        ("crates/b/Cargo.toml", B_MANIFEST),
        (
            "crates/b/src/lib.rs",
            "pub fn go() -> u32 { a::helpers::highlight_end() }\n",
        ),
    ];
    assert_eq!(
        call_paths(&tree, "crate::go"),
        vec![(
            "crates/a/src/helpers/imp.rs".to_string(),
            "crate::helpers::imp::highlight_end".to_string()
        )]
    );
}

/// A module declared in the calling file wins over a dependency of the same
/// name.
#[test]
fn local_module_shadows_a_dependency_of_the_same_name() {
    let tree = vec![
        (
            "Cargo.toml",
            "[package]\nname = \"app\"\n[dependencies]\nregex = \"1\"\n",
        ),
        (
            "src/lib.rs",
            "mod regex;\npub fn go() { let _r = regex::Regex::new(\"x\"); regex::compile(\"y\"); }\n",
        ),
        (
            "src/regex.rs",
            "pub struct Regex;\nimpl Regex { pub fn new(s: &str) -> Regex { Regex } }\npub fn compile(s: &str) -> Regex { Regex }\n",
        ),
    ];
    let got = call_paths(&tree, "crate::go");
    let want = [
        ("src/regex.rs", "crate::regex::Regex::new"),
        ("src/regex.rs", "crate::regex::compile"),
    ];
    for (path, qualname) in want {
        assert!(
            got.contains(&(path.to_string(), qualname.to_string())),
            "{qualname} missing: {got:#?}"
        );
    }
    assert!(got.iter().all(|(_, q)| !q.starts_with("ext:")), "{got:#?}");
}

/// A path dependency is named by its `[lib] name`, not its package name.
#[test]
fn dependency_is_named_by_its_library_not_its_package() {
    let tree = vec![
        (
            "a/Cargo.toml",
            "[package]\nname = \"a\"\n[lib]\nname = \"a_lib\"\n",
        ),
        ("a/src/lib.rs", "pub fn f() -> u32 { 1 }\n"),
        (
            "b/Cargo.toml",
            "[package]\nname = \"b\"\n[dependencies]\na = { path = \"../a\" }\n",
        ),
        (
            "b/src/lib.rs",
            "pub fn call_a() -> u32 { a_lib::f() }\npub fn call_wrong_name() -> u32 { a::f() }\n",
        ),
        ("c/Cargo.toml", "[package]\nname = \"c\"\n"),
        ("c/src/lib.rs", "pub fn f() -> u32 { 3 }\n"),
    ];
    assert_eq!(
        call_paths(&tree, "crate::call_a"),
        vec![("a/src/lib.rs".to_string(), "crate::f".to_string())]
    );
    // The package spelling names no crate here: nothing binds to `a` or `c`.
    let wrong = call_paths(&tree, "crate::call_wrong_name");
    assert!(
        wrong
            .iter()
            .all(|(path, _)| path.is_empty() || path == "<external>"),
        "{wrong:#?}"
    );
}

/// A facade's re-export may span lines, carry attributes and be `pub(crate)`
/// next to a grouped form.
#[test]
fn facade_reexports_are_read_with_the_rust_parser() {
    let tree = vec![
        ("Cargo.toml", WORKSPACE),
        ("crates/a/Cargo.toml", A_MANIFEST),
        ("crates/a/src/lib.rs", A_LIB),
        ("crates/a/src/shapes.rs", A_SHAPES),
        ("crates/c/Cargo.toml", C_MANIFEST),
        ("crates/c/src/lib.rs", C_LIB),
        (
            "crates/facade/Cargo.toml",
            "[package]\nname = \"facade\"\n[dependencies]\na = { path = \"../a\" }\n",
        ),
        (
            "crates/facade/src/lib.rs",
            "#[doc(hidden)]\npub use a\n    as inner;\npub extern crate a as other;\n",
        ),
        (
            "crates/b/Cargo.toml",
            "[package]\nname = \"b\"\n[dependencies]\nfacade = { path = \"../facade\" }\n",
        ),
        (
            "crates/b/src/lib.rs",
            "pub fn go() { facade::inner::helper(); }\n",
        ),
    ];
    assert_eq!(
        call_paths(&tree, "crate::go"),
        vec![(
            "crates/a/src/lib.rs".to_string(),
            "crate::helper".to_string()
        )]
    );
}

/// `mod job; pub use job::job;`: `exec::job(..)` is the function, never the
/// module `exec::job`.
#[test]
fn call_through_a_module_named_like_its_function_binds_the_function() {
    let tree = vec![
        ("Cargo.toml", "[package]\nname = \"g7\"\n"),
        (
            "src/lib.rs",
            "pub mod exec;\npub fn receive() -> u32 { exec::job(1) }\n",
        ),
        ("src/exec/mod.rs", "mod job;\npub use self::job::job;\n"),
        ("src/exec/job.rs", "pub fn job(n: u32) -> u32 { n }\n"),
    ];
    assert_eq!(
        call_paths(&tree, "crate::receive"),
        vec![(
            "src/exec/job.rs".to_string(),
            "crate::exec::job::job".to_string()
        )]
    );
}

/// The name-only tier for a path through a module must stay inside that
/// module: `helpers` re-exports an outside crate's `f`, and another module of
/// the package happens to declare an `f` too.
#[test]
fn name_only_tier_requires_the_named_module() {
    let tree = vec![
        ("Cargo.toml", "[workspace]\nmembers = [\"crates/*\"]\n"),
        (
            "crates/a/Cargo.toml",
            "[package]\nname = \"a\"\n[dependencies]\nextcrate = \"1\"\n",
        ),
        (
            "crates/a/src/lib.rs",
            "pub mod helpers;\npub mod other;\npub mod deep;\n",
        ),
        ("crates/a/src/helpers/mod.rs", "pub use extcrate::f;\n"),
        ("crates/a/src/other.rs", "pub fn f() -> u32 { 1 }\n"),
        ("crates/a/src/deep/mod.rs", "mod imp;\npub use imp::*;\n"),
        ("crates/a/src/deep/imp.rs", "pub fn g() -> u32 { 2 }\n"),
        (
            "crates/b/Cargo.toml",
            "[package]\nname = \"b\"\n[dependencies]\na = { path = \"../a\" }\n",
        ),
        (
            "crates/b/src/lib.rs",
            "pub fn ext_reexport() -> u32 { a::helpers::f() }\npub fn glob_reexport() -> u32 { a::deep::g() }\npub fn wrong_module() -> u32 { a::other::g() }\n",
        ),
    ];
    let bound_paths = |caller: &str| -> Vec<String> {
        call_paths(&tree, caller)
            .into_iter()
            .map(|(path, _)| path)
            .filter(|path| path.starts_with("crates/"))
            .collect()
    };
    assert!(bound_paths("crate::ext_reexport").is_empty());
    assert_eq!(
        bound_paths("crate::glob_reexport"),
        vec!["crates/a/src/deep/imp.rs".to_string()]
    );
    assert!(bound_paths("crate::wrong_module").is_empty());
}
