//! Issue #457: a type-level `IMPLEMENTS` edge for a Rust `impl Trait for Type`
//! starts at the type's declaration even when the impl is in another module
//! than the type (imported, reached by a glob or a re-export, or written as a
//! path), a trait of another package of the workspace binds to that package's
//! trait, a std or dependency trait never binds a repo type of the same name,
//! and an incremental sync after the impl or the type moves, or after a
//! re-export changes, equals a fresh reindex.

mod common;

use lidx::indexer::Indexer;
use std::collections::BTreeSet;

const MANIFEST: &str = "[package]\nname = \"demo\"\nversion = \"0.1.0\"\n";

/// `(source file, source qualname, target qualname, target kind)`.
type ImplEdges = BTreeSet<(String, String, String, String)>;

/// Every type-level `IMPLEMENTS` edge (its source is not a method).
fn implements(indexer: &Indexer) -> ImplEdges {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT sf.path, s.qualname, t.qualname, t.kind
             FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN files sf ON sf.id = s.file_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ?1 AND e.kind = 'IMPLEMENTS'
               AND s.kind IN ('struct', 'enum', 'union', 'type')",
        )
        .unwrap();
    stmt.query_map([gv], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn index(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::tempdir().unwrap();
    common::write_files(tmp.path(), files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

fn has(set: &ImplEdges, file: &str, source: &str, target: &str) -> bool {
    set.iter()
        .any(|(f, s, t, _)| f == file && s == source && t == target)
}

fn tree_with(node: &'static str, lib: &'static str) -> Vec<(&'static str, &'static str)> {
    vec![
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", lib),
        ("src/ast.rs", "pub struct Ast;\npub struct Item;\n"),
        ("src/node.rs", node),
    ]
}

const LIB_GLOB: &str = "mod ast;\nmod node;\nuse ast::Ast;\nuse std::fmt::Display;\n";
const NODE_GLOB: &str = "\
use super::*;
pub trait Node { fn tree(&self); }
impl Node for Ast { fn tree(&self) {} }
";

#[test]
fn an_impl_for_a_type_reached_by_a_glob_starts_at_the_types_declaration() {
    let (_tmp, indexer) = index(&tree_with(NODE_GLOB, LIB_GLOB));
    let got = implements(&indexer);
    assert!(
        has(&got, "src/ast.rs", "crate::ast::Ast", "crate::node::Node"),
        "{got:#?}"
    );
}

#[test]
fn an_impl_for_an_explicitly_imported_type_starts_at_the_types_declaration() {
    for imports in [
        "use crate::ast::Ast;",
        "use super::ast::Ast;",
        "use crate::ast::{Ast, Item};",
        "use crate::ast::Ast as A;",
    ] {
        let alias = imports.ends_with("as A;");
        let node: &'static str = Box::leak(
            format!(
                "{imports}\npub trait Node {{ fn tree(&self); }}\nimpl Node for {} {{ fn tree(&self) {{}} }}\n",
                if alias { "A" } else { "Ast" }
            )
            .into_boxed_str(),
        );
        let (_tmp, indexer) = index(&tree_with(node, "mod ast;\nmod node;\n"));
        let got = implements(&indexer);
        assert!(
            has(&got, "src/ast.rs", "crate::ast::Ast", "crate::node::Node"),
            "{imports}: {got:#?}"
        );
    }
}

#[test]
fn an_impl_for_a_path_written_type_starts_at_the_types_declaration() {
    for written in ["crate::ast::Ast", "super::ast::Ast", "ast::Ast"] {
        let node: &'static str = Box::leak(
            format!(
                "pub trait Node {{ fn tree(&self); }}\nimpl Node for {written} {{ fn tree(&self) {{}} }}\n"
            )
            .into_boxed_str(),
        );
        let (_tmp, indexer) = index(&tree_with(node, "mod ast;\nmod node;\n"));
        let got = implements(&indexer);
        if written == "ast::Ast" {
            // `ast` is not a module of `node`: nothing is guessed
            assert!(
                !has(&got, "src/ast.rs", "crate::ast::Ast", "crate::node::Node"),
                "{written}: {got:#?}"
            );
        } else {
            assert!(
                has(&got, "src/ast.rs", "crate::ast::Ast", "crate::node::Node"),
                "{written}: {got:#?}"
            );
        }
    }
}

#[test]
fn an_impl_for_a_re_exported_type_follows_the_re_export() {
    let node = "\
use crate::facade::Ast;
pub trait Node { fn tree(&self); }
impl Node for Ast { fn tree(&self) {} }
";
    let files = vec![
        ("Cargo.toml", MANIFEST),
        (
            "src/lib.rs",
            "mod ast;\nmod node;\nmod facade;\nmod deeper;\n",
        ),
        ("src/ast.rs", "pub struct Ast;\n"),
        ("src/node.rs", node),
        ("src/facade.rs", "pub use crate::deeper::*;\n"),
        ("src/deeper.rs", "pub use crate::ast::Ast;\n"),
    ];
    let (_tmp, indexer) = index(&files);
    let got = implements(&indexer);
    assert!(
        has(&got, "src/ast.rs", "crate::ast::Ast", "crate::node::Node"),
        "{got:#?}"
    );
}

#[test]
fn an_impl_in_the_types_own_module_is_unchanged() {
    let files = vec![
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", "mod a;\n"),
        (
            "src/a.rs",
            "pub trait T { fn f(&self); }\npub struct S;\nimpl T for S { fn f(&self) {} }\n",
        ),
    ];
    let (_tmp, indexer) = index(&files);
    assert!(has(
        &implements(&indexer),
        "src/a.rs",
        "crate::a::S",
        "crate::a::T"
    ));
}

#[test]
fn a_glob_imported_std_trait_is_an_external_base_of_the_type() {
    let files = vec![
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", "mod count;\nuse std::fmt::Display;\n"),
        (
            "src/count.rs",
            "use super::*;\npub struct Count;\nimpl Display for Count {\n    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { Ok(()) }\n}\n",
        ),
    ];
    let (_tmp, indexer) = index(&files);
    let got = implements(&indexer);
    assert!(
        got.iter().any(|(f, s, t, k)| f == "src/count.rs"
            && s == "crate::count::Count"
            && t.starts_with("ext:")
            && t.ends_with("Display")
            && k == "external"),
        "{got:#?}"
    );
}

// ---- the trait is another workspace package's ----

const WORKSPACE: &str = "[workspace]\nmembers = [\"crates/*\"]\n";

fn workspace(regex_lib: &'static str) -> Vec<(&'static str, &'static str)> {
    vec![
        ("Cargo.toml", WORKSPACE),
        (
            "crates/matcher/Cargo.toml",
            "[package]\nname = \"grep-matcher\"\nversion = \"0.1.0\"\n",
        ),
        (
            "crates/matcher/src/lib.rs",
            "pub trait Matcher { fn find(&self) -> u32; }\npub struct Error;\n",
        ),
        (
            "crates/decoy/Cargo.toml",
            "[package]\nname = \"decoy\"\nversion = \"0.1.0\"\n",
        ),
        (
            "crates/decoy/src/lib.rs",
            "pub mod error;\npub trait Matcher { fn find(&self) -> u32; }\npub struct Error;\n",
        ),
        ("crates/decoy/src/error.rs", "pub struct Error;\n"),
        (
            "crates/regex/Cargo.toml",
            "[package]\nname = \"grep-regex\"\nversion = \"0.1.0\"\n[dependencies]\ngrep-matcher = { path = \"../matcher\" }\n",
        ),
        ("crates/regex/src/lib.rs", regex_lib),
    ]
}

#[test]
fn a_workspace_crates_trait_binds_to_that_crates_trait() {
    for lib in [
        "use grep_matcher::Matcher;\npub struct RegexMatcher;\nimpl Matcher for RegexMatcher { fn find(&self) -> u32 { 1 } }\n",
        "pub struct RegexMatcher;\nimpl grep_matcher::Matcher for RegexMatcher { fn find(&self) -> u32 { 1 } }\n",
    ] {
        let (_tmp, indexer) = index(&workspace(lib));
        let got = implements(&indexer);
        assert!(
            has(
                &got,
                "crates/regex/src/lib.rs",
                "crate::RegexMatcher",
                "crate::Matcher"
            ),
            "{got:#?}"
        );
        let to_matcher: Vec<_> = got
            .iter()
            .filter(|(f, ..)| f == "crates/regex/src/lib.rs")
            .collect();
        assert_eq!(to_matcher.len(), 1, "{got:#?}");
        // bound to the matcher package's trait, not the decoy's
        let gv = indexer.db().current_graph_version().unwrap();
        let conn = indexer.db().read_conn().unwrap();
        let path: String = conn
            .query_row(
                "SELECT tf.path FROM edges e
                 JOIN symbols t ON t.id = e.target_symbol_id
                 JOIN files tf ON tf.id = t.file_id
                 JOIN symbols s ON s.id = e.source_symbol_id
                 WHERE e.graph_version = ?1 AND e.kind = 'IMPLEMENTS'
                   AND s.qualname = 'crate::RegexMatcher'",
                [gv],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(path, "crates/matcher/src/lib.rs");
    }
}

#[test]
fn a_dependency_trait_never_binds_a_repo_type_of_the_same_name() {
    let lib = "\
use std::error;
pub struct Error;
pub enum Other { A }
impl std::error::Error for Error {}
impl error::Error for Other {}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { Ok(()) }
}
";
    // another package declares `error::Error` as a struct
    let (_tmp, indexer) = index(&workspace(lib));
    let got = implements(&indexer);
    for (file, source, target, kind) in &got {
        if file == "crates/regex/src/lib.rs" {
            assert!(
                !matches!(kind.as_str(), "struct" | "enum"),
                "{source} -> {target} ({kind}) binds a type: {got:#?}"
            );
        }
    }
    let ext: Vec<_> = got
        .iter()
        .filter(|(f, s, t, k)| {
            f == "crates/regex/src/lib.rs"
                && k == "external"
                && t.ends_with("error::Error")
                && (s == "crate::Error" || s == "crate::Other")
        })
        .collect();
    assert_eq!(ext.len(), 2, "{got:#?}");
}

#[test]
fn a_trait_is_never_a_same_named_struct_of_the_repo() {
    // no manifest says where `Error` comes from; a struct must not stand in
    let files = vec![
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", "mod error;\nmod b;\n"),
        ("src/error.rs", "pub struct Error;\n"),
        (
            "src/b.rs",
            "pub struct Mine;\nimpl std::error::Error for Mine {}\n",
        ),
    ];
    let (_tmp, indexer) = index(&files);
    let got = implements(&indexer);
    assert!(
        !got.iter()
            .any(|(_, s, _, k)| s == "crate::b::Mine" && k == "struct"),
        "{got:#?}"
    );
}

// ---- incremental sync == fresh reindex ----

fn snapshot(indexer: &Indexer) -> BTreeSet<common::golden::EdgeKey> {
    let gv = indexer.db().current_graph_version().unwrap();
    common::golden::snapshot_edges(indexer.db(), gv).unwrap()
}

/// Index `before`, rewrite each of `edits`, sync those files, and check the
/// result against a fresh reindex of the final tree; returns the type-level
/// `IMPLEMENTS` edges before and after.
fn edit_matches_fresh(
    before: &[(&'static str, &'static str)],
    edits: &[(&'static str, &'static str)],
) -> (ImplEdges, ImplEdges) {
    let (tmp, mut indexer) = index(before);
    let first = implements(&indexer);
    common::write_files(tmp.path(), edits);
    let rels: Vec<String> = edits.iter().map(|(p, _)| p.to_string()).collect();
    indexer.sync_rel_paths(&rels).unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let incremental = snapshot(&indexer);
    let after: Vec<(&str, &str)> = before
        .iter()
        .map(|&(p, s)| {
            let edited = edits.iter().find(|(e, _)| *e == p).map_or(s, |e| e.1);
            (p, edited)
        })
        .collect();
    let (_fresh_tmp, fresh) = common::index_files(&after);
    common::assert_matches_fresh(&incremental, &fresh);
    let (_t2, fresh_indexer) = index(&after);
    assert_eq!(implements(&indexer), implements(&fresh_indexer));
    (first, implements(&indexer))
}

#[test]
fn moving_the_type_to_another_file_matches_a_fresh_reindex() {
    let before = vec![
        ("Cargo.toml", MANIFEST),
        (
            "src/lib.rs",
            "mod ast;\nmod ast2;\nmod node;\nuse ast::Ast;\n",
        ),
        ("src/ast.rs", "pub struct Ast;\n"),
        ("src/ast2.rs", "pub struct Other;\n"),
        ("src/node.rs", NODE_GLOB),
    ];
    let (first, after) = edit_matches_fresh(
        &before,
        &[
            (
                "src/lib.rs",
                "mod ast;\nmod ast2;\nmod node;\nuse ast2::Ast;\n",
            ),
            ("src/ast.rs", "pub struct Gone;\n"),
            ("src/ast2.rs", "pub struct Ast;\n"),
        ],
    );
    assert!(has(
        &first,
        "src/ast.rs",
        "crate::ast::Ast",
        "crate::node::Node"
    ));
    assert!(has(
        &after,
        "src/ast2.rs",
        "crate::ast2::Ast",
        "crate::node::Node"
    ));
    assert!(!has(
        &after,
        "src/ast.rs",
        "crate::ast::Ast",
        "crate::node::Node"
    ));
}

#[test]
fn moving_the_impl_to_another_file_matches_a_fresh_reindex() {
    let before = vec![
        ("Cargo.toml", MANIFEST),
        (
            "src/lib.rs",
            "mod ast;\nmod node;\nmod other;\nuse ast::Ast;\n",
        ),
        ("src/ast.rs", "pub struct Ast;\n"),
        ("src/node.rs", NODE_GLOB),
        ("src/other.rs", "use super::*;\n"),
    ];
    let moved = "\
use super::*;
use crate::node::Node;
impl Node for Ast { fn tree(&self) {} }
";
    let (first, after) = edit_matches_fresh(
        &before,
        &[
            ("src/node.rs", "pub trait Node { fn tree(&self); }\n"),
            ("src/other.rs", moved),
        ],
    );
    assert!(has(
        &first,
        "src/ast.rs",
        "crate::ast::Ast",
        "crate::node::Node"
    ));
    assert!(has(
        &after,
        "src/ast.rs",
        "crate::ast::Ast",
        "crate::node::Node"
    ));
}

#[test]
fn deleting_the_impl_removes_the_edge_like_a_fresh_reindex() {
    let before = tree_with(NODE_GLOB, LIB_GLOB);
    let (first, after) = edit_matches_fresh(
        &before,
        &[("src/node.rs", "pub trait Node { fn tree(&self); }\n")],
    );
    assert!(has(
        &first,
        "src/ast.rs",
        "crate::ast::Ast",
        "crate::node::Node"
    ));
    assert!(!after.iter().any(|(_, s, ..)| s == "crate::ast::Ast"));
}

#[test]
fn editing_the_re_export_a_trait_comes_through_matches_a_fresh_reindex() {
    let files = |lib: &'static str| {
        vec![
            ("Cargo.toml", MANIFEST),
            ("src/lib.rs", lib),
            (
                "src/count.rs",
                "use super::*;\npub struct Count;\nimpl Display for Count {\n    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { Ok(()) }\n}\n",
            ),
            (
                "src/mine.rs",
                "pub trait Display { fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result; }\n",
            ),
        ]
    };
    let std_lib = "mod count;\nmod mine;\nuse std::fmt::Display;\n";
    let repo_lib = "mod count;\nmod mine;\nuse crate::mine::Display;\n";
    let (first, after) = edit_matches_fresh(&files(std_lib), &[("src/lib.rs", repo_lib)]);
    assert!(first.iter().any(|(_, s, t, k)| s == "crate::count::Count"
        && t.starts_with("ext:")
        && k == "external"));
    assert!(has(
        &after,
        "src/count.rs",
        "crate::count::Count",
        "crate::mine::Display"
    ));
    let (first, after) = edit_matches_fresh(&files(repo_lib), &[("src/lib.rs", std_lib)]);
    assert!(has(
        &first,
        "src/count.rs",
        "crate::count::Count",
        "crate::mine::Display"
    ));
    assert!(after.iter().any(|(_, s, t, k)| s == "crate::count::Count"
        && t.starts_with("ext:")
        && k == "external"));
}

#[test]
fn a_type_reached_through_a_modules_own_glob_of_a_child_is_found() {
    // `config/mod.rs`: `mod config_impl; pub use config_impl::*;`
    let files = vec![
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", "mod config;\nmod list;\n"),
        (
            "src/config/mod.rs",
            "mod config_impl;\npub use config_impl::*;\n",
        ),
        ("src/config/config_impl.rs", "pub struct Profile;\n"),
        (
            "src/list.rs",
            "use crate::config::Profile;\npub trait ListProfile { fn f(&self); }\nimpl ListProfile for Profile { fn f(&self) {} }\n",
        ),
    ];
    let (_tmp, indexer) = index(&files);
    let got = implements(&indexer);
    assert!(
        has(
            &got,
            "src/config/config_impl.rs",
            "crate::config::config_impl::Profile",
            "crate::list::ListProfile"
        ),
        "{got:#?}"
    );
}

#[test]
fn a_type_declared_inside_pin_project_is_a_symbol_with_its_impls() {
    let files = vec![
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", "mod sleep;\n"),
        (
            "src/sleep.rs",
            "use std::future::Future;\nuse pin_project_lite::pin_project;\n\npin_project! {\n    #[derive(Debug)]\n    pub struct Sleep {\n        #[pin]\n        inner: u32,\n    }\n}\n\nimpl Future for Sleep {\n    type Output = ();\n    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> { todo!() }\n}\n",
        ),
    ];
    let (_tmp, indexer) = index(&files);
    let got = implements(&indexer);
    assert!(
        got.iter().any(|(f, s, t, k)| f == "src/sleep.rs"
            && s == "crate::sleep::Sleep"
            && t.ends_with("Future")
            && k == "external"),
        "{got:#?}"
    );
}
