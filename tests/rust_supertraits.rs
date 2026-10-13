//! Issue #468: a Rust trait `EXTENDS` each of its supertraits (`trait A: B +
//! C`, `where Self: B`), and a struct or enum `IMPLEMENTS` each trait it
//! derives. Both name their trait the way an `impl Trait for Type` does: a
//! trait of the repo binds, std or a dependency is an external stub, a package
//! of the workspace pins the trait's own package. A call through a supertrait
//! on a `dyn Sub` binds the supertrait's method and fans out to its impls, and
//! an incremental sync after a bound or a derive changes equals a fresh
//! reindex.

mod common;

use lidx::indexer::Indexer;
use std::collections::BTreeSet;

const MANIFEST: &str = "[package]\nname = \"demo\"\nversion = \"0.1.0\"\n";

/// `(source qualname, target qualname, target kind)`.
type Edges = BTreeSet<(String, String, String)>;

fn index(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::tempdir().unwrap();
    common::write_files(tmp.path(), files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

fn single(lib: &str) -> (tempfile::TempDir, Indexer) {
    let lib: &'static str = Box::leak(lib.to_string().into_boxed_str());
    index(&[("Cargo.toml", MANIFEST), ("src/lib.rs", lib)])
}

/// Type-level edges of `kind` from a trait (`EXTENDS`) or a struct / enum
/// (`IMPLEMENTS`), with the target's kind.
fn edges(indexer: &Indexer, kind: &str) -> Edges {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, t.qualname, t.kind
             FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ?1 AND e.kind = ?2
               AND s.kind IN ('trait', 'struct', 'enum')",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv, kind], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

/// The edge's target spelled as the tests compare it: a repo item by its
/// qualname, an external stub by the qualname after `ext:`.
fn targets_of(edges: &Edges, source: &str) -> BTreeSet<String> {
    edges
        .iter()
        .filter(|(s, ..)| s == source)
        .map(|(_, t, k)| match (k.as_str(), t.strip_prefix("ext:")) {
            ("external", Some(name)) => format!("ext {name}"),
            _ => t.clone(),
        })
        .collect()
}

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| (*s).to_string()).collect()
}

// ---- supertraits ----

#[test]
fn a_supertrait_of_the_repo_is_an_extends_edge_to_its_declaration() {
    let (_tmp, indexer) = single(
        "pub trait Named { fn name(&self) -> String; }
pub trait Aged { fn age(&self) -> u32; }
pub trait Person: Named + Aged {}
",
    );
    let got = edges(&indexer, "EXTENDS");
    assert_eq!(
        targets_of(&got, "crate::Person"),
        set(&["crate::Named", "crate::Aged"]),
        "{got:#?}"
    );
}

#[test]
fn std_and_dependency_supertraits_are_external_stubs() {
    let (_tmp, indexer) = single(
        "use std::fmt::{self, Debug};
use serde::Serialize;
pub trait Shape: Debug + Send + Sync + 'static + ?Sized {}
pub trait Solid: fmt::Display + Serialize {}
pub trait Gen: Into<String> + Iterator<Item = u8> {}
",
    );
    let got = edges(&indexer, "EXTENDS");
    assert_eq!(
        targets_of(&got, "crate::Shape"),
        set(&[
            "ext std::fmt::Debug",
            "ext std::marker::Send",
            "ext std::marker::Sync"
        ]),
        "lifetimes and `?Sized` name no trait: {got:#?}"
    );
    assert_eq!(
        targets_of(&got, "crate::Solid"),
        set(&["ext std::fmt::Display", "ext serde::Serialize"]),
        "{got:#?}"
    );
    assert_eq!(
        targets_of(&got, "crate::Gen"),
        set(&["ext std::convert::Into", "ext std::iter::Iterator"]),
        "a generic bound is its path: {got:#?}"
    );
}

#[test]
fn a_where_self_bound_is_a_supertrait_too() {
    let (_tmp, indexer) = single(
        "pub trait Base { fn base(&self); }
pub trait Other { fn other(&self); }
pub trait Sub: Base where Self: Other + std::fmt::Debug, T: Clone {}
pub trait Plain<T> where T: Clone {}
",
    );
    let got = edges(&indexer, "EXTENDS");
    assert_eq!(
        targets_of(&got, "crate::Sub"),
        set(&["crate::Base", "crate::Other", "ext std::fmt::Debug"]),
        "{got:#?}"
    );
    assert!(targets_of(&got, "crate::Plain").is_empty(), "{got:#?}");
}

#[test]
fn a_supertrait_in_another_module_binds_through_use_glob_and_path() {
    let files = vec![
        ("Cargo.toml", MANIFEST),
        (
            "src/lib.rs",
            "mod base;\nmod a;\nmod b;\nmod c;\nuse base::Base;\n",
        ),
        ("src/base.rs", "pub trait Base { fn base(&self); }\n"),
        ("src/a.rs", "use crate::base::Base;\npub trait A: Base {}\n"),
        ("src/b.rs", "use super::*;\npub trait B: Base {}\n"),
        ("src/c.rs", "pub trait C: crate::base::Base {}\n"),
    ];
    let (_tmp, indexer) = index(&files);
    let got = edges(&indexer, "EXTENDS");
    for sub in ["crate::a::A", "crate::b::B", "crate::c::C"] {
        assert_eq!(
            targets_of(&got, sub),
            set(&["crate::base::Base"]),
            "{sub}: {got:#?}"
        );
    }
}

#[test]
fn a_dependency_supertrait_never_binds_a_repo_trait_of_the_same_name() {
    let files = vec![
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", "mod mine;\nmod user;\n"),
        ("src/mine.rs", "pub trait Display { fn show(&self); }\n"),
        (
            "src/user.rs",
            "use std::fmt::Display;\npub trait Show: Display {}\n",
        ),
    ];
    let (_tmp, indexer) = index(&files);
    let got = edges(&indexer, "EXTENDS");
    assert_eq!(
        targets_of(&got, "crate::user::Show"),
        set(&["ext std::fmt::Display"]),
        "{got:#?}"
    );
}

#[test]
fn a_workspace_crates_trait_is_the_supertrait() {
    let files = vec![
        ("Cargo.toml", "[workspace]\nmembers = [\"crates/*\"]\n"),
        (
            "crates/matcher/Cargo.toml",
            "[package]\nname = \"grep-matcher\"\nversion = \"0.1.0\"\n",
        ),
        (
            "crates/matcher/src/lib.rs",
            "pub trait Matcher { fn find(&self) -> u32; }\n",
        ),
        (
            "crates/decoy/Cargo.toml",
            "[package]\nname = \"decoy\"\nversion = \"0.1.0\"\n",
        ),
        (
            "crates/decoy/src/lib.rs",
            "pub trait Matcher { fn find(&self) -> u32; }\n",
        ),
        (
            "crates/regex/Cargo.toml",
            "[package]\nname = \"grep-regex\"\nversion = \"0.1.0\"\n[dependencies]\ngrep-matcher = { path = \"../matcher\" }\n",
        ),
        (
            "crates/regex/src/lib.rs",
            "use grep_matcher::Matcher;\npub trait Fast: Matcher {}\npub trait Slow: grep_matcher::Matcher {}\n",
        ),
    ];
    let (_tmp, indexer) = index(&files);
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, tf.path FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             JOIN files tf ON tf.id = t.file_id
             WHERE e.graph_version = ?1 AND e.kind = 'EXTENDS'",
        )
        .unwrap();
    let rows: BTreeSet<(String, String)> = stmt
        .query_map([gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let want = |q: &str| (q.to_string(), "crates/matcher/src/lib.rs".to_string());
    assert!(rows.contains(&want("crate::Fast")), "{rows:#?}");
    assert!(rows.contains(&want("crate::Slow")), "{rows:#?}");
}

// ---- derives ----

#[test]
fn a_derive_is_an_implements_edge_to_the_derived_trait() {
    let (_tmp, indexer) = single(
        "use serde::Serialize;
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
pub struct Point { x: i32 }

/// Docs between the derives do not detach them.
#[derive(Copy)]
// and neither do comments
#[derive(Eq, Hash, PartialOrd, Ord, Default, Unknown)]
pub enum Kind { A }
",
    );
    let got = edges(&indexer, "IMPLEMENTS");
    assert_eq!(
        targets_of(&got, "crate::Point"),
        set(&[
            "ext std::fmt::Debug",
            "ext std::clone::Clone",
            "ext std::cmp::PartialEq",
            "ext serde::Serialize",
            "ext serde::Deserialize",
        ]),
        "{got:#?}"
    );
    assert_eq!(
        targets_of(&got, "crate::Kind"),
        set(&[
            "ext std::marker::Copy",
            "ext std::cmp::Eq",
            "ext std::hash::Hash",
            "ext std::cmp::PartialOrd",
            "ext std::cmp::Ord",
            "ext std::default::Default",
            "ext Unknown",
        ]),
        "a derive of a crate nothing here declares is external: {got:#?}"
    );
}

#[test]
fn a_derive_and_an_impl_of_the_same_std_trait_share_one_stub() {
    let (_tmp, indexer) = single(
        "use std::fmt::{self, Debug};
#[derive(Debug)]
pub struct A;
pub struct B;
impl fmt::Debug for B {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { Ok(()) }
}
",
    );
    let got = edges(&indexer, "IMPLEMENTS");
    assert_eq!(targets_of(&got, "crate::A"), set(&["ext std::fmt::Debug"]));
    assert_eq!(targets_of(&got, "crate::B"), set(&["ext std::fmt::Debug"]));
}

#[test]
fn only_a_derive_attribute_of_the_item_counts() {
    let (_tmp, indexer) = single(
        "#[cfg_attr(test, derive(Debug))]
pub struct Conditional;

#[derive(Clone)]
pub struct Derived;

pub struct Next;

#[derive(Clone)]
pub fn not_a_type() {}

#[derive(Debug)]
pub trait NotDerivable {}
",
    );
    let got = edges(&indexer, "IMPLEMENTS");
    assert!(
        targets_of(&got, "crate::Conditional").is_empty(),
        "{got:#?}"
    );
    assert!(targets_of(&got, "crate::Next").is_empty(), "{got:#?}");
    assert_eq!(
        targets_of(&got, "crate::Derived"),
        set(&["ext std::clone::Clone"])
    );
    assert!(
        targets_of(&got, "crate::NotDerivable").is_empty(),
        "{got:#?}"
    );
}

#[test]
fn a_derive_naming_a_trait_of_the_repo_binds_it() {
    let files = vec![
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", "mod describe;\nmod item;\n"),
        (
            "src/describe.rs",
            "pub trait Describe { fn describe(&self) -> String; }\n",
        ),
        (
            "src/item.rs",
            "use crate::describe::Describe;\n#[derive(Describe)]\npub struct Item;\n",
        ),
    ];
    let (_tmp, indexer) = index(&files);
    let got = edges(&indexer, "IMPLEMENTS");
    assert_eq!(
        targets_of(&got, "crate::item::Item"),
        set(&["crate::describe::Describe"]),
        "{got:#?}"
    );
}

#[test]
fn derives_do_not_make_a_type_a_caller_of_its_own_methods_external() {
    // A method that calls itself on a field of its own type is genuine
    // recursion; the type deriving `Clone` must not turn that call external.
    let (_tmp, indexer) = single(
        "#[derive(Debug, Clone)]
pub struct Node { next: Option<Box<Node>> }
impl Node {
    pub fn depth(&self) -> usize {
        match &self.next { Some(n) => 1 + n.depth(), None => 0 }
    }
}
",
    );
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
             WHERE e.graph_version = ?1 AND e.kind = 'CALLS'
               AND s.qualname = 'crate::Node::depth' AND e.target_symbol_id = s.id",
            [gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 1);
}

// ---- dispatch through a supertrait ----

const DYN_LIB: &str = "\
pub trait Named { fn name(&self) -> String; }
pub trait Shape: Named { fn area(&self) -> f64; }
pub trait Solid: Shape { fn volume(&self) -> f64; }

pub struct Sq;
impl Named for Sq { fn name(&self) -> String { String::new() } }
impl Shape for Sq { fn area(&self) -> f64 { 1.0 } }
impl Solid for Sq { fn volume(&self) -> f64 { 1.0 } }

pub fn own(s: &dyn Shape) -> f64 { s.area() }
pub fn one_up(s: &dyn Shape) -> String { s.name() }
pub fn two_up(s: Box<dyn Solid>) -> String { s.name() }
";

fn call_targets(indexer: &Indexer, caller: &str) -> BTreeSet<(String, String)> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT t.qualname, e.resolution_kind FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ?1 AND e.kind = 'CALLS' AND s.qualname = ?2",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv, caller], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

#[test]
fn a_call_on_a_dyn_trait_binds_the_trait_method_or_the_supertraits() {
    let (_tmp, indexer) = single(DYN_LIB);
    let only = |caller: &str| {
        let got = call_targets(&indexer, caller);
        assert_eq!(got.len(), 1, "{caller}: {got:#?}");
        got.into_iter().next().unwrap()
    };
    assert_eq!(only("crate::own").0, "crate::Shape::area");
    assert_eq!(
        only("crate::one_up"),
        ("crate::Named::name".to_string(), "inherited".to_string())
    );
    assert_eq!(
        only("crate::two_up"),
        ("crate::Named::name".to_string(), "inherited".to_string())
    );
}

#[test]
fn dispatch_fans_a_supertrait_method_out_to_the_impls() {
    let (_tmp, indexer) = single(DYN_LIB);
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let id = |qualname: &str| -> i64 {
        conn.query_row(
            "SELECT id FROM symbols WHERE graph_version = ?1 AND qualname = ?2",
            rusqlite::params![gv, qualname],
            |r| r.get(0),
        )
        .unwrap()
    };
    let pairs: BTreeSet<(i64, i64)> = indexer
        .db()
        .all_dispatch_pairs(gv)
        .unwrap()
        .into_iter()
        .collect();
    for (base, imp) in [
        ("crate::Named::name", "crate::Sq::name"),
        ("crate::Shape::area", "crate::Sq::area"),
        ("crate::Solid::volume", "crate::Sq::volume"),
    ] {
        assert!(pairs.contains(&(id(base), id(imp))), "{base} -> {imp}");
    }
    // the sub-trait's chain adds no pair of its own: nothing pairs a trait's
    // method with a method of another trait
    assert_eq!(pairs.len(), 3, "{pairs:?}");
}

// ---- incremental sync == fresh reindex ----

fn snapshot(indexer: &Indexer) -> BTreeSet<common::golden::EdgeKey> {
    let gv = indexer.db().current_graph_version().unwrap();
    common::golden::snapshot_edges(indexer.db(), gv).unwrap()
}

/// Index `before`, rewrite each of `edits`, sync those files and compare with
/// a fresh reindex of the final tree; returns the `EXTENDS` and `IMPLEMENTS`
/// edges before and after.
fn edit_matches_fresh(
    before: &[(&'static str, &'static str)],
    edits: &[(&'static str, &'static str)],
) -> ((Edges, Edges), (Edges, Edges)) {
    let (tmp, mut indexer) = index(before);
    let first = (edges(&indexer, "EXTENDS"), edges(&indexer, "IMPLEMENTS"));
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
    (
        first,
        (edges(&indexer, "EXTENDS"), edges(&indexer, "IMPLEMENTS")),
    )
}

fn tree(
    lib: &'static str,
    base: &'static str,
    sub: &'static str,
) -> Vec<(&'static str, &'static str)> {
    vec![
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", lib),
        ("src/base.rs", base),
        ("src/sub.rs", sub),
    ]
}

#[test]
fn changing_a_supertrait_bound_matches_a_fresh_reindex() {
    let before = tree(
        "mod base;\nmod sub;\n",
        "pub trait Base { fn base(&self); }\npub trait Other { fn other(&self); }\n",
        "use crate::base::Base;\npub trait Sub: Base {}\n",
    );
    let (first, after) = edit_matches_fresh(
        &before,
        &[(
            "src/sub.rs",
            "use crate::base::{Base, Other};\npub trait Sub: Other + std::fmt::Debug {}\n",
        )],
    );
    assert_eq!(
        targets_of(&first.0, "crate::sub::Sub"),
        set(&["crate::base::Base"])
    );
    assert_eq!(
        targets_of(&after.0, "crate::sub::Sub"),
        set(&["crate::base::Other", "ext std::fmt::Debug"])
    );
}

#[test]
fn moving_the_supertrait_to_another_file_matches_a_fresh_reindex() {
    let before = vec![
        ("Cargo.toml", MANIFEST),
        (
            "src/lib.rs",
            "mod base;\nmod base2;\nmod sub;\nuse base::Base;\n",
        ),
        ("src/base.rs", "pub trait Base { fn base(&self); }\n"),
        ("src/base2.rs", "pub struct Unrelated;\n"),
        ("src/sub.rs", "use super::*;\npub trait Sub: Base {}\n"),
    ];
    let (first, after) = edit_matches_fresh(
        &before,
        &[
            (
                "src/lib.rs",
                "mod base;\nmod base2;\nmod sub;\nuse base2::Base;\n",
            ),
            ("src/base.rs", "pub struct Gone;\n"),
            ("src/base2.rs", "pub trait Base { fn base(&self); }\n"),
        ],
    );
    assert_eq!(
        targets_of(&first.0, "crate::sub::Sub"),
        set(&["crate::base::Base"])
    );
    assert_eq!(
        targets_of(&after.0, "crate::sub::Sub"),
        set(&["crate::base2::Base"])
    );
}

#[test]
fn editing_the_re_export_a_supertrait_comes_through_matches_a_fresh_reindex() {
    let files = |lib: &'static str| {
        vec![
            ("Cargo.toml", MANIFEST),
            ("src/lib.rs", lib),
            ("src/sub.rs", "use super::*;\npub trait Sub: Display {}\n"),
            (
                "src/mine.rs",
                "pub trait Display { fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result; }\n",
            ),
        ]
    };
    let std_lib = "mod sub;\nmod mine;\nuse std::fmt::Display;\n";
    let repo_lib = "mod sub;\nmod mine;\nuse crate::mine::Display;\n";
    let (first, after) = edit_matches_fresh(&files(std_lib), &[("src/lib.rs", repo_lib)]);
    // (a glob-imported std trait keeps the written name: `Display`)
    let external = targets_of(&first.0, "crate::sub::Sub");
    assert!(
        external.len() == 1
            && external
                .iter()
                .all(|t| t.starts_with("ext ") && t.ends_with("Display")),
        "{external:?}"
    );
    assert_eq!(
        targets_of(&after.0, "crate::sub::Sub"),
        set(&["crate::mine::Display"])
    );
    let (first, after) = edit_matches_fresh(&files(repo_lib), &[("src/lib.rs", std_lib)]);
    assert_eq!(
        targets_of(&first.0, "crate::sub::Sub"),
        set(&["crate::mine::Display"])
    );
    let external = targets_of(&after.0, "crate::sub::Sub");
    assert!(
        external.len() == 1
            && external
                .iter()
                .all(|t| t.starts_with("ext ") && t.ends_with("Display")),
        "{external:?}"
    );
}

#[test]
fn deleting_the_supertrait_unbinds_the_edge_like_a_fresh_reindex() {
    let before = tree(
        "mod base;\nmod sub;\n",
        "pub trait Base { fn base(&self); }\n",
        "use crate::base::Base;\npub trait Sub: Base {}\n",
    );
    let (first, after) = edit_matches_fresh(&before, &[("src/base.rs", "pub struct NotATrait;\n")]);
    assert_eq!(
        targets_of(&first.0, "crate::sub::Sub"),
        set(&["crate::base::Base"])
    );
    assert!(
        targets_of(&after.0, "crate::sub::Sub").is_empty(),
        "{after:#?}"
    );
}

#[test]
fn adding_and_removing_a_derive_matches_a_fresh_reindex() {
    let before = tree(
        "mod base;\nmod sub;\n",
        "pub trait Base { fn base(&self); }\n",
        "#[derive(Debug)]\npub struct S;\n",
    );
    let (first, after) = edit_matches_fresh(
        &before,
        &[(
            "src/sub.rs",
            "use crate::base::Base;\n#[derive(Clone, Base)]\npub struct S;\n",
        )],
    );
    assert_eq!(
        targets_of(&first.1, "crate::sub::S"),
        set(&["ext std::fmt::Debug"])
    );
    assert_eq!(
        targets_of(&after.1, "crate::sub::S"),
        set(&["ext std::clone::Clone", "crate::base::Base"])
    );
}
