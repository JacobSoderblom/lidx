//! Issue #448: where a declared type points is settled at extraction, so a
//! signature read back from another file needs no scope. A type from std or a
//! dependency (`-> walkdir::DirEntry`, `use walkdir::DirEntry; e: &DirEntry`,
//! a `pub use walkdir::DirEntry;` read as `crate::walk::DirEntry`) never binds
//! the methods of the repo's own `DirEntry`; the repo-typed equivalents still do.

mod common;

use lidx::indexer::Indexer;
use std::collections::BTreeSet;

const MANIFEST: &str = "[package]\nname = \"g\"\n[dependencies]\nwalkdir = \"2\"\n";
const LIB: &str = "pub mod entry;\npub mod maker;\npub mod user;\npub mod walk;\npub mod facade;\npub mod holder;\npub mod alias;\n";
const ENTRY: &str = "pub struct DirEntry;\nimpl DirEntry {\n    pub fn path(&self) -> u32 { 1 }\n    pub fn make() -> u32 { 1 }\n}\n";
const HOLDER_REPO: &str = "pub struct Wrapper { pub ent: crate::entry::DirEntry }\n";
const HOLDER_FOREIGN: &str = "pub struct Wrapper { pub ent: walkdir::DirEntry }\n";
const ALIAS_REPO: &str = "pub type Entry = crate::entry::DirEntry;\n";
const ALIAS_FOREIGN: &str = "pub type Entry = walkdir::DirEntry;\n";
const REPO_METHOD: &str = "crate::entry::DirEntry::path";

fn tree(
    maker: &'static str,
    user: &'static str,
    walk: &'static str,
) -> Vec<(&'static str, &'static str)> {
    vec![
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", LIB),
        ("src/entry.rs", ENTRY),
        ("src/maker.rs", maker),
        ("src/user.rs", user),
        ("src/walk.rs", walk),
        ("src/facade.rs", "pub use crate::walk::DirEntry;\n"),
        ("src/holder.rs", HOLDER_REPO),
        ("src/alias.rs", ALIAS_REPO),
    ]
}

fn with(
    mut t: Vec<(&'static str, &'static str)>,
    path: &'static str,
    src: &'static str,
) -> Vec<(&'static str, &'static str)> {
    for entry in &mut t {
        if entry.0 == path {
            entry.1 = src;
        }
    }
    t
}

fn binds(tree: &[(&str, &str)], caller: &str) -> bool {
    common::call_targets(tree, caller)
        .iter()
        .any(|t| t == REPO_METHOD)
}

// ---- 1. signatures read from another file ----

const MAKER_EXT: &str = "\
use std::fs;
pub struct Holder { pub ent: walkdir::DirEntry, pub std_ent: fs::DirEntry }
pub fn first() -> walkdir::DirEntry { todo!() }
pub fn maybe() -> Option<walkdir::DirEntry> { todo!() }
pub fn std_first() -> std::fs::DirEntry { todo!() }
";
const MAKER_BARE: &str = "\
use walkdir::DirEntry;
pub struct Holder { pub ent: DirEntry }
pub fn first() -> DirEntry { todo!() }
";
const MAKER_REPO: &str = "\
use crate::entry::DirEntry;
pub struct Holder { pub ent: DirEntry }
pub fn first() -> DirEntry { todo!() }
pub fn maybe() -> Option<DirEntry> { todo!() }
";
const USER_CALLS: &str = "\
use crate::maker::{first, maybe, std_first, Holder};
pub fn ret_call() -> u32 { let e = first(); e.path() }
pub fn ret_option() -> u32 { let e = maybe().unwrap(); e.path() }
pub fn ret_std() -> u32 { let e = std_first(); e.path() }
pub fn field_dep(h: &Holder) -> u32 { h.ent.path() }
pub fn field_std(h: &Holder) -> u32 { h.std_ent.path() }
";

#[test]
fn a_cross_file_external_return_or_field_type_does_not_bind_the_repo_method() {
    let t = tree(MAKER_EXT, USER_CALLS, "");
    for caller in [
        "ret_call",
        "ret_option",
        "ret_std",
        "field_dep",
        "field_std",
    ] {
        assert!(
            !binds(&t, &format!("crate::user::{caller}")),
            "{caller} must not bind the repo method"
        );
    }
}

#[test]
fn a_cross_file_bare_imported_external_type_does_not_bind_the_repo_method() {
    let t = tree(MAKER_BARE, USER_CALLS, "");
    for caller in ["ret_call", "field_dep"] {
        assert!(
            !binds(&t, &format!("crate::user::{caller}")),
            "{caller} must not bind the repo method"
        );
    }
}

#[test]
fn a_cross_file_repo_return_or_field_type_still_binds() {
    let t = tree(MAKER_REPO, USER_CALLS, "");
    for caller in ["ret_call", "ret_option", "field_dep"] {
        assert!(
            binds(&t, &format!("crate::user::{caller}")),
            "{caller} must bind the repo method"
        );
    }
}

// ---- 2. re-exported foreign types ----

const USER_PATHS: &str = "\
pub fn via_walk(e: &crate::walk::DirEntry) -> u32 { e.path() }
pub fn via_facade(e: &crate::facade::DirEntry) -> u32 { e.path() }
pub fn via_local(e: crate::walk::DirEntry) -> u32 { let f = e; f.path() }
pub fn via_option(e: Option<&crate::walk::DirEntry>) -> u32 { e.unwrap().path() }
";

#[test]
fn a_re_exported_foreign_type_does_not_bind_the_repo_method() {
    let t = tree(MAKER_REPO, USER_PATHS, "pub use walkdir::DirEntry;\n");
    for caller in ["via_walk", "via_facade", "via_local", "via_option"] {
        assert!(
            !binds(&t, &format!("crate::user::{caller}")),
            "{caller} must not bind the repo method"
        );
    }
}

#[test]
fn a_re_exported_repo_type_still_binds() {
    let t = tree(MAKER_REPO, USER_PATHS, "pub use crate::entry::DirEntry;\n");
    for caller in ["via_walk", "via_facade", "via_local", "via_option"] {
        assert!(
            binds(&t, &format!("crate::user::{caller}")),
            "{caller} must bind the repo method"
        );
    }
}

#[test]
fn a_foreign_type_aliased_in_the_use_is_still_foreign() {
    let user = "pub fn go(e: &crate::walk::Entry) -> u32 { e.path() }\n";
    let t = tree(MAKER_REPO, user, "pub use walkdir::DirEntry as Entry;\n");
    assert!(!binds(&t, "crate::user::go"));
    let t = tree(
        MAKER_REPO,
        user,
        "pub use crate::entry::DirEntry as Entry;\n",
    );
    assert!(binds(&t, "crate::user::go"));
}

#[test]
fn a_self_path_to_an_imported_foreign_type_does_not_bind() {
    let user = "\
use walkdir::DirEntry;
pub fn go(e: &self::DirEntry) -> u32 { e.path() }
pub fn go_crate(e: &crate::user::DirEntry) -> u32 { e.path() }
";
    let t = tree(MAKER_REPO, user, "");
    assert!(!binds(&t, "crate::user::go"));
    assert!(!binds(&t, "crate::user::go_crate"));
}

#[test]
fn a_foreign_type_re_exported_from_an_inline_module_does_not_bind() {
    let user = "\
pub mod inner { pub use walkdir::DirEntry; }
pub fn go(e: &crate::user::inner::DirEntry) -> u32 { e.path() }
";
    let t = tree(MAKER_REPO, user, "");
    assert!(!binds(&t, "crate::user::go"));
}

// ---- 3. bare imported external types ----

#[test]
fn a_bare_imported_external_type_does_not_bind_the_repo_method() {
    for import in ["use walkdir::DirEntry;", "use std::fs::DirEntry;"] {
        let user = format!(
            "{import}\npub fn go(e: &DirEntry) -> u32 {{ e.path() }}\npub fn local() -> u32 {{ let e: &DirEntry = todo!(); e.path() }}\n"
        );
        let user: &'static str = Box::leak(user.into_boxed_str());
        let t = tree(MAKER_REPO, user, "");
        assert!(!binds(&t, "crate::user::go"), "{import}");
        assert!(!binds(&t, "crate::user::local"), "{import}");
    }
}

#[test]
fn a_bare_imported_repo_type_still_binds() {
    let user = "\
use crate::entry::DirEntry;
pub fn go(e: &DirEntry) -> u32 { e.path() }
";
    let t = tree(MAKER_REPO, user, "");
    assert!(binds(&t, "crate::user::go"));
}

/// Edition 2015 names a crate-root module without `crate::`: an import whose
/// first segment is not a declared dependency is not provably external.
#[test]
fn a_bare_import_rooted_at_an_undeclared_name_still_binds() {
    let user = "\
use entry::DirEntry;
pub fn go(e: &DirEntry) -> u32 { e.path() }
pub fn via_path(e: &entry::DirEntry) -> u32 { e.path() }
";
    let t = tree(MAKER_REPO, user, "");
    assert!(binds(&t, "crate::user::go"));
    assert!(binds(&t, "crate::user::via_path"));
}

// ---- a bare import of a module's re-export ----

const USER_BARE_REEXPORT: &str = "\
use crate::walk::DirEntry;
pub fn go(e: &DirEntry) -> u32 { e.path() }
pub fn local() -> u32 { let e: &DirEntry = todo!(); e.path() }
";

#[test]
fn a_bare_import_of_a_foreign_re_export_does_not_bind() {
    let t = tree(
        MAKER_REPO,
        USER_BARE_REEXPORT,
        "pub use walkdir::DirEntry;\n",
    );
    assert!(!binds(&t, "crate::user::go"));
    assert!(!binds(&t, "crate::user::local"));
}

#[test]
fn a_bare_import_of_a_repo_re_export_still_binds() {
    let t = tree(
        MAKER_REPO,
        USER_BARE_REEXPORT,
        "pub use crate::entry::DirEntry;\n",
    );
    assert!(binds(&t, "crate::user::go"));
    assert!(binds(&t, "crate::user::local"));
}

// ---- a field of a `crate::`-path-typed value ----

const USER_FIELD: &str = "\
pub fn go(h: &crate::holder::Wrapper) -> u32 { h.ent.path() }
pub fn via_facade(h: &crate::reholder::Wrapper) -> u32 { h.ent.path() }
";

#[test]
fn a_field_of_a_path_typed_value_is_read_from_its_declaration() {
    let t = tree(MAKER_REPO, USER_FIELD, "");
    assert!(binds(&t, "crate::user::go"));
    let t = with(t, "src/holder.rs", HOLDER_FOREIGN);
    assert!(!binds(&t, "crate::user::go"));
}

#[test]
fn a_field_of_a_value_typed_through_a_re_export_is_read_from_its_declaration() {
    let mut t = tree(MAKER_REPO, USER_FIELD, "");
    t.push(("src/reholder.rs", "pub use crate::holder::Wrapper;\n"));
    t = with(
        t,
        "src/lib.rs",
        "pub mod entry;\npub mod maker;\npub mod user;\npub mod walk;\npub mod facade;\npub mod holder;\npub mod alias;\npub mod reholder;\n",
    );
    assert!(binds(&t, "crate::user::via_facade"));
    let t = with(t, "src/holder.rs", HOLDER_FOREIGN);
    assert!(!binds(&t, "crate::user::via_facade"));
}

// ---- a type alias of a foreign type ----

const USER_ALIAS: &str = "pub fn go() -> u32 { crate::alias::Entry::make() }\n";

#[test]
fn an_alias_of_a_foreign_type_does_not_bind_the_repo_type() {
    let t = tree(MAKER_REPO, USER_ALIAS, "");
    assert!(
        common::call_targets(&t, "crate::user::go")
            .iter()
            .any(|t| t == "crate::entry::DirEntry::make")
    );
    let t = with(t, "src/alias.rs", ALIAS_FOREIGN);
    assert!(
        !common::call_targets(&t, "crate::user::go")
            .iter()
            .any(|t| t == "crate::entry::DirEntry::make")
    );
}

// ---- incremental sync == fresh reindex ----

fn snapshot(indexer: &Indexer) -> BTreeSet<common::golden::EdgeKey> {
    let gv = indexer.db().current_graph_version().unwrap();
    common::golden::snapshot_edges(indexer.db(), gv).unwrap()
}

fn calls_repo_method(
    edges: &BTreeSet<common::golden::EdgeKey>,
    caller_suffix: &str,
    target: &str,
) -> bool {
    edges.iter().any(|e| {
        e.kind == "CALLS"
            && e.target_qualname.as_deref() == Some(target)
            && e.source_qualname.ends_with(caller_suffix)
    })
}

/// Index `before`, rewrite `rel` to `after_src`, sync only that file, and
/// check the result against a fresh index of the final tree.
fn edit_matches_fresh(
    before: &[(&'static str, &'static str)],
    rel: &'static str,
    after_src: &'static str,
    caller: &str,
    bound_before: bool,
) {
    edit_matches_fresh_calling(before, rel, after_src, caller, REPO_METHOD, bound_before);
}

fn edit_matches_fresh_calling(
    before: &[(&'static str, &'static str)],
    rel: &'static str,
    after_src: &'static str,
    caller: &str,
    target: &str,
    bound_before: bool,
) {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, before);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    assert_eq!(
        calls_repo_method(&snapshot(&indexer), caller, target),
        bound_before,
        "before the edit"
    );

    common::write_files(&root, &[(rel, after_src)]);
    indexer.sync_rel_paths(&[rel.to_string()]).unwrap();
    let incremental = snapshot(&indexer);
    assert_eq!(
        calls_repo_method(&incremental, caller, target),
        !bound_before,
        "after the edit"
    );

    let after: Vec<(&str, &str)> = before
        .iter()
        .map(|&(p, s)| if p == rel { (p, after_src) } else { (p, s) })
        .collect();
    let (_fresh_tmp, fresh) = common::index_files(&after);
    common::assert_matches_fresh(&incremental, &fresh);
}

#[test]
fn editing_a_re_export_matches_a_fresh_reindex() {
    let user = "pub fn go(e: &crate::walk::DirEntry) -> u32 { e.path() }\n";
    let foreign = "pub use walkdir::DirEntry;\n";
    let repo = "pub use crate::entry::DirEntry;\n";
    edit_matches_fresh(
        &tree(MAKER_REPO, user, foreign),
        "src/walk.rs",
        repo,
        "user::go",
        false,
    );
    edit_matches_fresh(
        &tree(MAKER_REPO, user, repo),
        "src/walk.rs",
        foreign,
        "user::go",
        true,
    );
}

#[test]
fn editing_a_use_behind_a_cross_file_signature_matches_a_fresh_reindex() {
    let user = "use crate::maker::first;\npub fn go() -> u32 { let e = first(); e.path() }\n";
    edit_matches_fresh(
        &tree(MAKER_REPO, user, ""),
        "src/maker.rs",
        MAKER_BARE,
        "user::go",
        true,
    );
    edit_matches_fresh(
        &tree(MAKER_BARE, user, ""),
        "src/maker.rs",
        MAKER_REPO,
        "user::go",
        false,
    );
}

#[test]
fn editing_a_use_behind_a_cross_file_field_matches_a_fresh_reindex() {
    let user = "use crate::maker::Holder;\npub fn go(h: &Holder) -> u32 { h.ent.path() }\n";
    edit_matches_fresh(
        &tree(MAKER_REPO, user, ""),
        "src/maker.rs",
        MAKER_BARE,
        "user::go",
        true,
    );
}

#[test]
fn editing_a_bare_use_matches_a_fresh_reindex() {
    let foreign = "use walkdir::DirEntry;\npub fn go(e: &DirEntry) -> u32 { e.path() }\n";
    let repo = "use crate::entry::DirEntry;\npub fn go(e: &DirEntry) -> u32 { e.path() }\n";
    edit_matches_fresh(
        &tree(MAKER_REPO, foreign, ""),
        "src/user.rs",
        repo,
        "user::go",
        false,
    );
}

#[test]
fn editing_the_type_of_a_field_behind_a_path_matches_a_fresh_reindex() {
    let t = tree(MAKER_REPO, USER_FIELD, "");
    edit_matches_fresh(&t, "src/holder.rs", HOLDER_FOREIGN, "user::go", true);
    let t = with(t, "src/holder.rs", HOLDER_FOREIGN);
    edit_matches_fresh(&t, "src/holder.rs", HOLDER_REPO, "user::go", false);
}

#[test]
fn editing_a_re_export_behind_a_bare_import_matches_a_fresh_reindex() {
    let t = tree(
        MAKER_REPO,
        USER_BARE_REEXPORT,
        "pub use walkdir::DirEntry;\n",
    );
    edit_matches_fresh(
        &t,
        "src/walk.rs",
        "pub use crate::entry::DirEntry;\n",
        "user::go",
        false,
    );
}

fn alias_edit_matches_fresh(before: &'static str, after: &'static str, bound_before: bool) {
    let t = with(tree(MAKER_REPO, USER_ALIAS, ""), "src/alias.rs", before);
    edit_matches_fresh_calling(
        &t,
        "src/alias.rs",
        after,
        "user::go",
        "crate::entry::DirEntry::make",
        bound_before,
    );
}

#[test]
fn editing_an_alias_matches_a_fresh_reindex() {
    alias_edit_matches_fresh(ALIAS_REPO, ALIAS_FOREIGN, true);
    alias_edit_matches_fresh(ALIAS_FOREIGN, ALIAS_REPO, false);
}
