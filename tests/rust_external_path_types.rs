//! Issue #444: a receiver declared with a module-qualified type is typed by
//! where that path points. `ent: &fs::DirEntry` / `&walkdir::DirEntry` are
//! std's and a dependency's, so `ent.path()` never binds the repo's own
//! `DirEntry::path`; `&crate::walk::DirEntry` and an imported repo
//! `DirEntry` still do.

mod common;

use lidx::indexer::Indexer;
use std::collections::BTreeSet;

const MANIFEST: &str = "[package]\nname = \"g\"\n[dependencies]\nwalkdir = \"2\"\n";
const LIB: &str = "pub mod entry;\npub mod walk;\n";
const ENTRY: &str =
    "pub struct DirEntry;\nimpl DirEntry {\n    pub fn path(&self) -> u32 { 1 }\n}\n";
const WALK: &str = "\
use std::fs;
use crate::entry::DirEntry;
use crate::entry as ent_mod;
pub fn std_path(ent: &fs::DirEntry) -> u32 { ent.path() }
pub fn dep_path(ent: &walkdir::DirEntry) -> u32 { ent.path() }
pub fn full_std(ent: &std::fs::DirEntry) -> u32 { ent.path() }
pub fn crate_path(ent: &crate::entry::DirEntry) -> u32 { ent.path() }
pub fn imported(ent: &DirEntry) -> u32 { ent.path() }
pub fn via_module(ent: &ent_mod::DirEntry) -> u32 { ent.path() }
pub fn local_let() -> u32 {
    let ent: &fs::DirEntry = todo!();
    ent.path()
}
";

fn files(walk: &'static str) -> Vec<(&'static str, &'static str)> {
    vec![
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", LIB),
        ("src/entry.rs", ENTRY),
        ("src/walk.rs", walk),
    ]
}

fn binds(tree: &[(&str, &str)], caller: &str) -> bool {
    common::call_targets(tree, caller)
        .iter()
        .any(|t| t == "crate::entry::DirEntry::path")
}

#[test]
fn external_path_types_do_not_bind_the_repo_method() {
    let tree = files(WALK);
    for caller in [
        "crate::walk::std_path",
        "crate::walk::dep_path",
        "crate::walk::full_std",
        "crate::walk::local_let",
    ] {
        assert!(
            !binds(&tree, caller),
            "{caller} must not bind the repo method"
        );
    }
}

#[test]
fn repo_path_types_still_bind() {
    let tree = files(WALK);
    for caller in [
        "crate::walk::crate_path",
        "crate::walk::imported",
        "crate::walk::via_module",
    ] {
        assert!(binds(&tree, caller), "{caller} must bind the repo method");
    }
}

#[test]
fn a_struct_field_of_an_external_path_type_does_not_bind() {
    let walk = "\
use std::fs;
pub struct W { ent: fs::DirEntry }
impl W {
    pub fn go(&self) -> u32 { self.ent.path() }
}
";
    assert!(!binds(&files(walk), "crate::walk::W::go"));
}

/// A method declared in another file returns an external type: the call on
/// its result takes no repo method, bare-name fallback included.
#[test]
fn deferred_receiver_declared_with_an_external_path_does_not_bind() {
    let lib = "pub mod maker;\npub mod user;\npub struct Builder;\nimpl Builder {\n    pub fn env(&mut self) -> u32 { 1 }\n}\n";
    let maker = "pub struct Maker;\nimpl Maker {\n    pub fn command_mut(&mut self) -> &mut std::process::Command { todo!() }\n}\n";
    let user = "use crate::maker::Maker;\npub fn go(m: &mut Maker) {\n    let c = m.command_mut();\n    c.env();\n}\n";
    let tree = [
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", lib),
        ("src/maker.rs", maker),
        ("src/user.rs", user),
    ];
    let targets = common::call_targets(&tree, "crate::user::go");
    assert!(
        !targets.iter().any(|t| t == "crate::Builder::env"),
        "{targets:?}"
    );
}

fn snapshot(indexer: &Indexer) -> BTreeSet<common::golden::EdgeKey> {
    let gv = indexer.db().current_graph_version().unwrap();
    common::golden::snapshot_edges(indexer.db(), gv).unwrap()
}

/// A `use` edit alone changes which `DirEntry` a parameter names; the
/// incremental result must equal a fresh reindex of the final tree.
#[test]
fn use_edit_changing_which_module_a_path_names_matches_a_fresh_reindex() {
    let before = "\
use crate::entry as fs;
pub fn go(ent: &fs::DirEntry) -> u32 { ent.path() }
";
    let after = "\
use std::fs;
pub fn go(ent: &fs::DirEntry) -> u32 { ent.path() }
";
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, &files(before));
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    assert!(snapshot(&indexer).iter().any(|e| e.kind == "CALLS"
        && e.target_qualname.as_deref() == Some("crate::entry::DirEntry::path")));

    common::write_files(&root, &[("src/walk.rs", after)]);
    indexer
        .sync_rel_paths(&["src/walk.rs".to_string()])
        .unwrap();
    let incremental = snapshot(&indexer);
    assert!(!incremental.iter().any(|e| e.kind == "CALLS"
        && e.target_qualname.as_deref() == Some("crate::entry::DirEntry::path")));

    let (_fresh_tmp, fresh) = common::index_files(&files(after));
    common::assert_matches_fresh(&incremental, &fresh);
}
