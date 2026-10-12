//! Issue #424: Rust receivers that are not plain locals -- builder chains,
//! call results and field chains -- are typed like a `let` initializer, so a
//! method name shared by several types still binds to the right one.

mod common;

use common::golden;
use lidx::indexer::Indexer;
use lidx::indexer::extract::{LanguageExtractor, ReceiverType};
use lidx::indexer::rust::RustExtractor;
use std::collections::BTreeSet;

const BUILDER_RS: &str = "pub struct Out;\n\
pub struct B;\n\
impl B {\n    pub fn new() -> B { B }\n    pub fn a(&mut self, n: u32) -> &mut Self { self }\n    pub fn b(&mut self) -> &mut Self { self }\n    pub fn build(&self) -> Out { Out }\n}\n";

const OTHER_RS: &str = "pub struct C;\n\
impl C {\n    pub fn new() -> C { C }\n    pub fn a(&mut self, n: u32) -> &mut Self { self }\n    pub fn b(&mut self) -> &mut Self { self }\n    pub fn build(&self) -> u32 { 0 }\n}\n\
pub struct Core2;\nimpl Core2 {\n    pub fn finish(&self) {}\n}\n";

const CORE_RS: &str = "pub struct Core;\nimpl Core {\n    pub fn finish(&self) {}\n}\n";

const CHAIN_USER_RS: &str = "use crate::builder::B;\n\
pub fn go() {\n    B::new().a(1).b().build();\n}\n";

fn targets(files: &[(&str, &str)], caller: &str) -> BTreeSet<String> {
    common::call_targets(files, caller).into_iter().collect()
}

#[test]
fn builder_chain_links_bind_to_the_builder_type() {
    let got = targets(
        &[
            ("src/builder.rs", BUILDER_RS),
            ("src/other.rs", OTHER_RS),
            ("src/user.rs", CHAIN_USER_RS),
        ],
        "crate::user::go",
    );
    for want in ["new", "a", "b", "build"] {
        let want = format!("crate::builder::B::{want}");
        assert!(got.contains(&want), "missing {want}: {got:#?}");
    }
    assert!(
        !got.iter().any(|t| t.starts_with("crate::other::")),
        "{got:#?}"
    );
}

#[test]
fn builder_chain_in_the_same_file_binds_each_link() {
    let src = format!("{BUILDER_RS}{OTHER_RS}pub fn go() {{\n    B::new().a(1).b().build();\n}}\n");
    let got = targets(&[("src/lib.rs", &src)], "crate::go");
    for want in ["new", "a", "b", "build"] {
        let want = format!("crate::B::{want}");
        assert!(got.contains(&want), "missing {want}: {got:#?}");
    }
}

const ENGINE_RS: &str = "use crate::core::Core;\n\
pub struct Engine {\n    core: Core,\n}\n\
impl Engine {\n    pub fn run(&self) {\n        self.core.finish();\n    }\n}\n";

#[test]
fn self_field_chain_uses_a_field_type_declared_in_another_file() {
    let got = targets(
        &[
            ("src/core.rs", CORE_RS),
            ("src/other.rs", OTHER_RS),
            ("src/engine.rs", ENGINE_RS),
        ],
        "crate::engine::Engine::run",
    );
    assert!(
        got.contains("crate::core::Core::finish"),
        "self.core.finish() must bind to Core: {got:#?}"
    );
}

#[test]
fn editing_a_field_type_in_another_file_retargets_like_a_fresh_reindex() {
    let engine = |ty: &str| {
        format!(
            "use crate::core::{ty};\npub struct Engine {{\n    core: {ty},\n}}\n\
impl Engine {{\n    pub fn run(&self) {{\n        self.core.finish();\n    }}\n}}\n"
        )
    };
    let core = "pub struct Core;\nimpl Core {\n    pub fn finish(&self) {}\n}\n\
pub struct Core2;\nimpl Core2 {\n    pub fn finish(&self) {}\n}\n";
    let tmp = tempfile::Builder::new()
        .prefix("lidx-rust-field-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(
        &root,
        &[("src/core.rs", core), ("src/engine.rs", &engine("Core"))],
    );
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    let snapshot = |indexer: &Indexer| {
        common::assert_no_dangling_edge_targets(indexer.db());
        let gv = indexer.db().current_graph_version().unwrap();
        golden::snapshot_edges(indexer.db(), gv).unwrap()
    };
    let finish = |snap: &BTreeSet<golden::EdgeKey>| {
        snap.iter()
            .filter(|e| e.kind == "CALLS" && e.source_qualname == "crate::engine::Engine::run")
            .filter_map(|e| e.target_qualname.clone())
            .find(|t| t.ends_with("::finish"))
    };
    assert_eq!(
        finish(&snapshot(&indexer)).as_deref(),
        Some("crate::core::Core::finish")
    );

    // The field's type changes in the file that declares the struct.
    std::fs::write(root.join("src/engine.rs"), engine("Core2")).unwrap();
    indexer
        .sync_rel_paths(&["src/engine.rs".to_string()])
        .unwrap();
    let snap = snapshot(&indexer);
    assert_eq!(finish(&snap).as_deref(), Some("crate::core::Core2::finish"));
    let (_t, fresh) =
        common::index_files(&[("src/core.rs", core), ("src/engine.rs", &engine("Core2"))]);
    common::assert_matches_fresh(&snap, &fresh);

    // The struct moves to another file: the edge must follow it.
    let moved = "pub struct Core;\nimpl Core {\n    pub fn finish(&self) {}\n}\n";
    let core2 = "pub struct Core2;\nimpl Core2 {\n    pub fn finish(&self) {}\n}\n";
    std::fs::write(root.join("src/core.rs"), moved).unwrap();
    std::fs::write(root.join("src/more.rs"), core2).unwrap();
    indexer
        .sync_rel_paths(&["src/core.rs".to_string(), "src/more.rs".to_string()])
        .unwrap();
    let snap = snapshot(&indexer);
    let (_t, fresh) = common::index_files(&[
        ("src/core.rs", moved),
        ("src/more.rs", core2),
        ("src/engine.rs", &engine("Core2")),
    ]);
    common::assert_matches_fresh(&snap, &fresh);
}

#[test]
fn editing_a_builder_method_in_another_file_retargets_like_a_fresh_reindex() {
    let builder = |ret: &str| {
        format!(
            "pub struct B;\nimpl B {{\n    pub fn new() -> B {{ B }}\n    pub fn a(&self) -> {ret} {{ todo!() }}\n}}\n\
pub struct D;\nimpl D {{\n    pub fn build(&self) {{}}\n}}\n"
        )
    };
    let other = "pub struct C;\nimpl C {\n    pub fn build(&self) {}\n}\n";
    let user = "use crate::builder::B;\npub fn go() {\n    B::new().a().build();\n}\n";
    let tmp = tempfile::Builder::new()
        .prefix("lidx-rust-chain-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(
        &root,
        &[
            ("src/builder.rs", &builder("D")),
            ("src/other.rs", other),
            ("src/user.rs", user),
        ],
    );
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    let build_target = |indexer: &Indexer| {
        let gv = indexer.db().current_graph_version().unwrap();
        golden::snapshot_edges(indexer.db(), gv)
            .unwrap()
            .iter()
            .filter(|e| e.kind == "CALLS" && e.source_qualname == "crate::user::go")
            .filter_map(|e| e.target_qualname.clone())
            .find(|t| t.ends_with("::build"))
    };
    assert_eq!(
        build_target(&indexer).as_deref(),
        Some("crate::builder::D::build")
    );
    // `a` now returns `Self`: `.build()` has no declaration on B, so it must
    // not keep the stale D target.
    std::fs::write(root.join("src/builder.rs"), builder("Self")).unwrap();
    indexer
        .sync_rel_paths(&["src/builder.rs".to_string()])
        .unwrap();
    assert_eq!(build_target(&indexer), None);
    let (_t, fresh) = common::index_files(&[
        ("src/builder.rs", &builder("Self")),
        ("src/other.rs", other),
        ("src/user.rs", user),
    ]);
    let gv = indexer.db().current_graph_version().unwrap();
    let snap = golden::snapshot_edges(indexer.db(), gv).unwrap();
    common::assert_matches_fresh(&snap, &fresh);
}

#[test]
fn cfg_twins_of_one_function_are_one_edge() {
    let util = "pub struct HyperlinkPath;\nimpl HyperlinkPath {\n\
    #[cfg(unix)]\n    pub fn from_path(p: &str) -> HyperlinkPath { HyperlinkPath }\n\
    #[cfg(windows)]\n    pub fn from_path(p: &str) -> HyperlinkPath { HyperlinkPath }\n}\n";
    let user =
        "use crate::util::HyperlinkPath;\npub fn go() {\n    HyperlinkPath::from_path(\"x\");\n}\n";
    let got = common::call_targets(
        &[("src/util.rs", util), ("src/user.rs", user)],
        "crate::user::go",
    );
    let from_path: Vec<_> = got.iter().filter(|t| t.ends_with("from_path")).collect();
    assert_eq!(
        from_path,
        ["crate::util::HyperlinkPath::from_path"],
        "{got:#?}"
    );
}

#[test]
fn workspace_crates_sharing_crate_prefer_the_callers_crate() {
    let lib_a = "pub fn helper() {}\npub fn go() {\n    crate::helper();\n}\n";
    let lib_b = "pub fn helper() {}\n";
    let (_tmp, root, db_path) = common::index_repo(
        "lidx-rust-crates-",
        &[
            ("crates/a/Cargo.toml", "[package]\nname = \"a\"\n"),
            ("crates/b/Cargo.toml", "[package]\nname = \"b\"\n"),
            ("crates/a/src/lib.rs", lib_a),
            ("crates/b/src/lib.rs", lib_b),
        ],
    );
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let files: Vec<String> = conn
        .prepare(
            "SELECT tf.path FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             JOIN files tf ON tf.id = t.file_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS' AND s.qualname = 'crate::go'",
        )
        .unwrap()
        .query_map([gv], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(files, ["crates/a/src/lib.rs"]);
}

fn receivers(src: &str, method: &str) -> Vec<String> {
    let file = RustExtractor::new().unwrap().extract(src, "crate").unwrap();
    file.edges
        .iter()
        .filter(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some(method))
        .map(|e| match &e.receiver_type {
            ReceiverType::Known(t) => t.clone(),
            ReceiverType::RustDeferred(_) => "deferred".to_string(),
            _ => "-".to_string(),
        })
        .collect()
}

#[test]
fn ranges_literals_and_std_values_stay_untyped() {
    let src = "pub struct R;\nimpl R { pub fn map(&self) {} }\n\
pub fn f(v: Vec<u8>) {\n    (0..256).map(|x| x);\n    \"abc\".map();\n    v.len().map();\n}\n";
    assert_eq!(receivers(src, "map"), ["-", "-", "-"]);
}

#[test]
fn chained_same_file_receivers_are_typed() {
    let src = format!("{BUILDER_RS}pub fn go() {{\n    B::new().a(1).b().build();\n}}\n");
    assert_eq!(receivers(&src, "a"), ["B"]);
    assert_eq!(receivers(&src, "b"), ["B"]);
    assert_eq!(receivers(&src, "build"), ["B"]);
}

fn resolved_to(files: &[(&str, &str)], caller: &str, suffix: &str) -> Vec<String> {
    common::call_targets(files, caller)
        .into_iter()
        .filter(|t| t.ends_with(suffix))
        .collect()
}

#[test]
fn cfg_gated_impl_blocks_merge_their_twin_methods() {
    let util = "pub struct H;\n\
#[cfg(unix)]\nimpl H {\n    pub fn open() -> H { H }\n}\n\
#[cfg(windows)]\nimpl H {\n    pub fn open() -> H { H }\n}\n";
    let user = "use crate::util::H;\npub fn go() {\n    H::open();\n}\n";
    let got = resolved_to(
        &[("src/util.rs", util), ("src/user.rs", user)],
        "crate::user::go",
        "::open",
    );
    assert_eq!(got, ["crate::util::H::open"]);
}

#[test]
fn nested_fn_twins_are_not_cfg_variants() {
    let src = "fn first() {\n    fn walk() {}\n    walk();\n}\n\
fn second() {\n    fn walk() {}\n    walk();\n}\n\
pub fn go() {\n    walk();\n}\n";
    let got = resolved_to(&[("src/lib.rs", src)], "crate::go", "walk");
    assert!(got.is_empty(), "ambiguous nested fn must not bind: {got:?}");
}

#[test]
fn from_impls_for_two_types_stay_distinct() {
    let src = "pub struct E;\npub struct A;\npub struct B;\n\
impl From<A> for E {\n    fn from(a: A) -> E { E }\n}\n\
impl From<B> for E {\n    fn from(b: B) -> E { E }\n}\n\
pub fn go(a: A) {\n    E::from(a);\n}\n";
    let got = resolved_to(&[("src/lib.rs", src)], "crate::go", "::from");
    assert!(got.is_empty(), "two From impls must not collapse: {got:?}");
}

#[test]
fn debug_and_display_fmt_stay_distinct() {
    let src = "use std::fmt;\npub struct T;\n\
impl fmt::Debug for T {\n    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { Ok(()) }\n}\n\
impl fmt::Display for T {\n    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { Ok(()) }\n}\n\
pub fn go(t: T, f: &mut fmt::Formatter) {\n    T::fmt(&t, f);\n}\n";
    let got = resolved_to(&[("src/lib.rs", src)], "crate::go", "::fmt");
    assert!(
        got.is_empty(),
        "Debug and Display fmt must not collapse: {got:?}"
    );
}

#[test]
fn associated_type_paths_are_not_read_by_their_last_segment() {
    let src = "pub struct Item;\nimpl Item {\n    pub fn weight(&self) {}\n}\n\
pub struct Other;\nimpl Other {\n    pub fn weight(&self) {}\n}\n\
pub struct L;\nimpl Iterator for L {\n    type Item = u8;\n    fn next(&mut self) -> Option<Self::Item> { None }\n}\n\
pub fn go(mut lx: L) {\n    lx.next().unwrap().weight();\n}\n";
    let got = resolved_to(&[("src/lib.rs", src)], "crate::go", "::weight");
    assert!(
        got.is_empty(),
        "`Self::Item` is not the type `Item`: {got:?}"
    );
    assert_eq!(receivers(src, "weight"), ["-"]);
}

#[test]
fn a_nested_fn_yields_to_the_module_level_fn_of_the_same_name() {
    let src = "pub fn walk() {}\nfn host() {\n    fn walk() {}\n    walk();\n}\n\
pub fn go() {\n    walk();\n}\n";
    let got = resolved_to(&[("src/lib.rs", src)], "crate::go", "walk");
    assert_eq!(got, ["crate::walk"]);
}
