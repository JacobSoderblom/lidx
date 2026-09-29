//! Rust receiver types read off a declaration in another file (fn return
//! types, struct/enum field types) hang on that declaration alone, so an edit
//! to it must retarget the caller's edge exactly as a fresh reindex would.

mod common;

use common::golden;
use lidx::indexer::Indexer;

const TYPES_RS: &str = "pub struct Engine;\nimpl Engine {\n    pub fn resolve(&self) {}\n}\n\
pub struct Cache;\nimpl Cache {\n    pub fn resolve(&self) {}\n}\n";

const USER_RS: &str = "use crate::decls::{Pair, make};\n\
pub fn via_fn() {\n    let e = make();\n    e.resolve();\n}\n\
pub fn via_field(p: Pair) {\n    let e = p.item;\n    e.resolve();\n}\n\
pub fn via_pattern(p: Pair) {\n    let Pair { item } = p;\n    item.resolve();\n}\n";

fn decls(ret: &str, field: &str) -> String {
    format!("pub fn make() -> {ret} {{ todo!() }}\npub struct Pair {{ pub item: {field} }}\n")
}

fn snapshot(indexer: &Indexer) -> std::collections::BTreeSet<golden::EdgeKey> {
    common::assert_no_dangling_edge_targets(indexer.db());
    let graph_version = indexer.db().current_graph_version().unwrap();
    golden::snapshot_edges(indexer.db(), graph_version).unwrap()
}

fn assert_targets(snap: &std::collections::BTreeSet<golden::EdgeKey>, want: &str) {
    for fn_name in ["via_fn", "via_field", "via_pattern"] {
        let source = format!("crate::user::{fn_name}");
        let hit = snap.iter().find(|e| {
            e.kind == "CALLS"
                && e.source_qualname == source
                && e.target_qualname
                    .as_deref()
                    .is_some_and(|t| t.ends_with("::resolve"))
        });
        let target = hit.and_then(|e| e.target_qualname.as_deref());
        assert_eq!(
            target,
            Some(want),
            "{fn_name} must resolve to {want}: {snap:#?}"
        );
    }
}

#[test]
fn editing_a_declaration_retargets_deferred_receivers_like_a_fresh_reindex() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-rust-deferred-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(
        &root,
        &[
            ("src/types.rs", TYPES_RS),
            ("src/decls.rs", &decls("Engine", "Engine")),
            ("src/user.rs", USER_RS),
        ],
    );
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    assert_targets(&snapshot(&indexer), "crate::types::Engine::resolve");

    // The declaration file changes; the caller file is untouched.
    std::fs::write(root.join("src/decls.rs"), decls("Cache", "Cache")).unwrap();
    indexer
        .sync_rel_paths(&["src/decls.rs".to_string()])
        .unwrap();
    let snap = snapshot(&indexer);
    assert_targets(&snap, "crate::types::Cache::resolve");
    let (_fresh_tmp, fresh) = common::index_files(&[
        ("src/types.rs", TYPES_RS),
        ("src/decls.rs", &decls("Cache", "Cache")),
        ("src/user.rs", USER_RS),
    ]);
    common::assert_matches_fresh(&snap, &fresh);

    // The declaration stops naming a repo type: the edges must not keep the
    // stale target.
    let untyped = decls("impl Sized", "u32");
    std::fs::write(root.join("src/decls.rs"), &untyped).unwrap();
    indexer
        .sync_rel_paths(&["src/decls.rs".to_string()])
        .unwrap();
    let snap = snapshot(&indexer);
    let (_fresh_tmp, fresh) = common::index_files(&[
        ("src/types.rs", TYPES_RS),
        ("src/decls.rs", &untyped),
        ("src/user.rs", USER_RS),
    ]);
    common::assert_matches_fresh(&snap, &fresh);
}
