//! Issue #497: a receiver typed through a type alias, a `Deref` impl or a
//! `#[path]` module depends on a declaration in another file. After an
//! incremental sync the edges must equal a fresh reindex of the same tree.

mod common;

use common::golden;
use lidx::indexer::Indexer;

const MANIFEST: &str = "[package]\nname = \"x\"\n";

const TYPES: &str = "pub struct Entry { p: u32 }\nimpl Entry {\n    pub fn path(&self) -> u32 { self.p }\n}\n\
pub struct Other { p: u32 }\nimpl Other {\n    pub fn path(&self) -> u32 { self.p }\n}\n";

const USER: &str = "use crate::{CliResult, Entry};\n\
pub fn fetch() -> CliResult<Entry> { Ok(Entry { p: 1 }) }\n\
pub fn go() -> CliResult<u32> {\n    let ent = fetch()?;\n    Ok(ent.path())\n}\n";

fn indexed(
    label: &str,
    files: &[(&str, &str)],
) -> (tempfile::TempDir, std::path::PathBuf, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix(&format!("lidx-{label}-"))
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, files);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, root, indexer)
}

/// The alias appears after the code that uses it was indexed, then changes
/// its target: the receiver's type follows each step, as a fresh index has it.
#[test]
fn alias_declaration_added_then_changed_matches_fresh() {
    let lib_without = format!("mod user;\n{TYPES}");
    let alias = "pub type CliResult<T> = Result<T, u8>;\n";
    let lib_with = format!("mod user;\n{alias}{TYPES}");
    let (_tmp, root, mut indexer) = indexed(
        "alias-added",
        &[
            ("Cargo.toml", MANIFEST),
            ("src/lib.rs", &lib_without),
            ("src/user.rs", USER),
        ],
    );

    common::write_files(&root, &[("src/lib.rs", &lib_with)]);
    indexer.sync_rel_paths(&["src/lib.rs".to_string()]).unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    assert!(
        snapshot.iter().any(|e| e.kind == "CALLS"
            && e.source_qualname == "crate::user::go"
            && e.target_qualname.as_deref() == Some("crate::Entry::path")),
        "the alias makes `ent` an Entry: {snapshot:#?}"
    );
    let (_fresh_tmp, fresh) = common::index_files(&[
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", &lib_with),
        ("src/user.rs", USER),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);

    // The alias now stands for something else: no `Entry` behind the `?`.
    let lib_changed = format!("mod user;\npub type CliResult<T> = Vec<T>;\n{TYPES}");
    common::write_files(&root, &[("src/lib.rs", &lib_changed)]);
    indexer.sync_rel_paths(&["src/lib.rs".to_string()]).unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&[
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", &lib_changed),
        ("src/user.rs", USER),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// A `Deref` impl added later makes the wrapper's target's methods its own.
#[test]
fn deref_impl_added_matches_fresh() {
    let lib = format!("mod user;\n{TYPES}pub struct Wrapper {{ e: Entry }}\n");
    let deref = "impl std::ops::Deref for crate::Wrapper {\n    type Target = crate::Entry;\n    fn deref(&self) -> &crate::Entry { &self.e }\n}\n";
    let user = "use crate::Wrapper;\npub fn go(w: Wrapper) -> u32 {\n    w.path()\n}\n";
    let (_tmp, root, mut indexer) = indexed(
        "deref-added",
        &[
            ("Cargo.toml", MANIFEST),
            ("src/lib.rs", &lib),
            ("src/user.rs", user),
        ],
    );
    let lib_with = format!("mod user;\n{TYPES}pub struct Wrapper {{ e: Entry }}\n{deref}");
    common::write_files(&root, &[("src/lib.rs", &lib_with)]);
    indexer.sync_rel_paths(&["src/lib.rs".to_string()]).unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&[
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", &lib_with),
        ("src/user.rs", user),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// The file a `#[path]` module names is added after the declaration.
#[test]
fn path_module_file_added_matches_fresh() {
    let lib = "#[path = \"platform/unix_impl.rs\"]\nmod os;\npub fn go() -> u8 { os::setup() }\n";
    let imp = "pub fn setup() -> u8 { 1 }\n";
    let (_tmp, root, mut indexer) = indexed(
        "path-mod-added",
        &[("Cargo.toml", MANIFEST), ("src/lib.rs", lib)],
    );
    common::write_files(&root, &[("src/platform/unix_impl.rs", imp)]);
    indexer
        .sync_rel_paths(&["src/platform/unix_impl.rs".to_string()])
        .unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&[
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", lib),
        ("src/platform/unix_impl.rs", imp),
    ]);
    common::assert_matches_fresh(&snapshot, &fresh);
}

/// A call through a name a repo module re-exports from a dependency is an
/// external stub; when the module's re-export becomes the repo's own type the
/// call binds it, and back: incremental sync equals a fresh reindex each way.
#[test]
fn re_export_flipping_between_dependency_and_repo_matches_fresh() {
    let manifest = "[package]\nname = \"x\"\n[dependencies]\nwalkdir = \"2\"\n";
    let lib = "mod walk;\nmod entry;\nmod user;\n";
    let entry =
        "pub struct DirEntry;\nimpl DirEntry {\n    pub fn make() -> DirEntry { DirEntry }\n}\n";
    let user = "use crate::walk::DirEntry;\npub fn go() {\n    let _e = DirEntry::make();\n}\n";
    let foreign = "pub use walkdir::DirEntry;\n";
    let repo = "pub use crate::entry::DirEntry;\n";
    for (from, to) in [(foreign, repo), (repo, foreign)] {
        let tree = |walk: &'static str| {
            vec![
                ("Cargo.toml", manifest),
                ("src/lib.rs", lib),
                ("src/entry.rs", entry),
                ("src/user.rs", user),
                ("src/walk.rs", walk),
            ]
        };
        let (_tmp, root, mut indexer) = indexed("reexport-flip", &tree(from));
        common::write_files(&root, &[("src/walk.rs", to)]);
        indexer
            .sync_rel_paths(&["src/walk.rs".to_string()])
            .unwrap();
        common::assert_no_dangling_edge_targets(indexer.db());
        let gv = indexer.db().current_graph_version().unwrap();
        let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
        let (_fresh_tmp, fresh) = common::index_files(&tree(to));
        common::assert_matches_fresh(&snapshot, &fresh);
    }
}
