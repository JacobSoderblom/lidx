//! Issues #463 / #464: type mentions, method values and unit structs keep
//! their targets alive for `dead_symbols`, and the edges that do so come out
//! the same from an incremental sync as from a fresh reindex.

mod common;

use common::golden;
use lidx::indexer::Indexer;
use std::path::PathBuf;

const GO_MOD: &str = "module example.com/m\n\ngo 1.22\n";

const GO_USER: &str = r#"package m

type wrapper struct{ inner *model }

type runner struct{}

func (r runner) run() {}

func mk() runner { return runner{} }

func use(f func()) { f() }

func noop() {}

func pick() func() { return noop }

func Main() {
	_ = (*tick)(nil)
	var c = conv{"a"}
	_ = c
	_ = wrapper{}
	use(mk().run)
	_ = pick
}
"#;

const GO_TYPES: &str = r#"package m

type model struct{ n int }

type tick struct{}

type conv []string

type neverUsed struct{}
"#;

fn tree(label: &str, files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix(&format!("lidx-{label}-"))
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, files);
    let mut indexer = Indexer::new(root.clone(), root.join(".lidx").join(".lidx.sqlite")).unwrap();
    indexer.reindex().unwrap();
    (tmp, root, indexer)
}

fn dead(indexer: &Indexer, root: &std::path::Path) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let mut names: Vec<String> = indexer
        .db()
        .dead_symbols(10_000, None, None, gv, root, false)
        .unwrap()
        .dead
        .into_iter()
        .map(|s| s.symbol.qualname)
        .collect();
    names.sort();
    names
}

#[test]
fn go_type_mentions_and_function_values_are_not_dead() {
    let (_tmp, root, indexer) = tree(
        "go-uses",
        &[
            ("go.mod", GO_MOD),
            ("user.go", GO_USER),
            ("types.go", GO_TYPES),
        ],
    );
    let dead = dead(&indexer, &root);
    assert!(
        dead.iter().any(|q| q.ends_with("neverUsed")),
        "an unmentioned type stays dead: {dead:?}"
    );
    for live in [
        "model",
        "tick",
        "conv",
        "wrapper",
        "runner.run",
        "noop",
        "mk",
    ] {
        assert!(
            !dead.iter().any(|q| q.ends_with(&format!(".{live}"))),
            "{live} is used: {dead:?}"
        );
    }
}

#[test]
fn go_type_use_edges_match_fresh_after_the_type_file_arrives_later() {
    let (_tmp, root, mut indexer) =
        tree("go-uses-inc", &[("go.mod", GO_MOD), ("user.go", GO_USER)]);
    assert!(
        dead(&indexer, &root).iter().all(|q| !q.ends_with(".model")),
        "no declaration yet"
    );
    common::write_files(&root, &[("types.go", GO_TYPES)]);
    indexer.sync_rel_paths(&["types.go".to_string()]).unwrap();

    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    assert!(
        snapshot
            .iter()
            .any(|e| e.kind == "USES" && e.target_qualname.as_deref() == Some("types.model")),
        "the type mention binds once its file exists: {snapshot:#?}"
    );
    let (_f, fresh) = common::index_files(&[
        ("go.mod", GO_MOD),
        ("user.go", GO_USER),
        ("types.go", GO_TYPES),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
    assert!(dead(&indexer, &root).iter().all(|q| !q.ends_with(".model")));
}

#[test]
fn go_type_use_edges_match_fresh_after_the_mention_is_removed() {
    let (_tmp, root, mut indexer) = tree(
        "go-uses-rm",
        &[
            ("go.mod", GO_MOD),
            ("user.go", GO_USER),
            ("types.go", GO_TYPES),
        ],
    );
    let edited = GO_USER.replace(
        "type wrapper struct{ inner *model }",
        "type wrapper struct{}",
    );
    common::write_files(&root, &[("user.go", &edited)]);
    indexer.sync_rel_paths(&["user.go".to_string()]).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_f, fresh) = common::index_files(&[
        ("go.mod", GO_MOD),
        ("user.go", &edited),
        ("types.go", GO_TYPES),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
    assert!(
        dead(&indexer, &root).iter().any(|q| q.ends_with(".model")),
        "model lost its only mention"
    );
}

const RUST_LIB: &str = r#"pub trait Flag { fn name(&self) -> &str; }

struct Unrestricted;
impl Flag for Unrestricted { fn name(&self) -> &str { "u" } }

struct Never;

struct Row { n: u32 }

fn helper() -> u32 { 1 }

fn apply(f: fn() -> u32) -> u32 { f() }

pub fn all() -> Vec<&'static dyn Flag> {
    let _ = Row { n: apply(helper) };
    vec![&Unrestricted]
}
"#;

#[test]
fn rust_unit_structs_struct_literals_and_function_values_are_not_dead() {
    let (_tmp, root, indexer) = tree(
        "rs-uses",
        &[
            (
                "Cargo.toml",
                "[package]\nname = \"m\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
            ),
            ("src/lib.rs", RUST_LIB),
        ],
    );
    let dead = dead(&indexer, &root);
    assert!(dead.iter().any(|q| q.ends_with("::Never")), "{dead:?}");
    for live in ["Unrestricted", "Row", "helper"] {
        assert!(
            !dead.iter().any(|q| q.ends_with(&format!("::{live}"))),
            "{live} is used: {dead:?}"
        );
    }
}

#[test]
fn rust_use_edges_match_fresh_after_a_later_file() {
    let manifest = "[package]\nname = \"m\"\nversion = \"0.1.0\"\nedition = \"2021\"\n";
    let lib = "mod defs;\nuse defs::Unit;\npub fn all() -> Vec<&'static dyn defs::Flag> { vec![&Unit] }\n";
    let defs = "pub trait Flag {}\npub struct Unit;\nimpl Flag for Unit {}\n";
    let (_tmp, root, mut indexer) =
        tree("rs-inc", &[("Cargo.toml", manifest), ("src/lib.rs", lib)]);
    common::write_files(&root, &[("src/defs.rs", defs)]);
    indexer
        .sync_rel_paths(&["src/defs.rs".to_string()])
        .unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_f, fresh) = common::index_files(&[
        ("Cargo.toml", manifest),
        ("src/lib.rs", lib),
        ("src/defs.rs", defs),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn rust_ambiguous_method_calls_and_macro_arguments_keep_candidates_alive() {
    let manifest = "[package]\nname = \"m\"\nversion = \"0.1.0\"\nedition = \"2021\"\n";
    let lib = r#"pub struct A;
impl A { fn matches(&self) -> bool { true } fn solo(&self) {} }
pub struct B;
impl B { fn matches(&self) -> bool { false } }

fn in_macro() -> u32 { 1 }

pub fn run(x: &dyn std::any::Any, v: Vec<A>) {
    let s = &v[0];
    let _ = s.matches();
    let _ = x.downcast_ref::<u8>();
    anyhow::bail!(in_macro());
}
"#;
    let (_tmp, root, indexer) = tree("rs-amb", &[("Cargo.toml", manifest), ("src/lib.rs", lib)]);
    let dead = dead(&indexer, &root);
    assert!(dead.iter().any(|q| q.ends_with("A::solo")), "{dead:?}");
    assert!(!dead.iter().any(|q| q.ends_with("::in_macro")), "{dead:?}");
}
