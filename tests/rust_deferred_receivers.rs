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

/// The `resolve` method `crate::user::<function>` binds, if any.
fn resolve_target(
    snap: &std::collections::BTreeSet<golden::EdgeKey>,
    function: &str,
) -> Option<String> {
    let source = format!("crate::user::{function}");
    snap.iter()
        .filter(|e| e.kind == "CALLS" && e.source_qualname == source)
        .filter_map(|e| e.target_qualname.clone())
        .find(|t| t.ends_with("::resolve"))
}

/// Indexes `files`, then applies each edit (a rewrite of one file) with an
/// incremental sync, checking `expect` against the snapshot and that it
/// matches a fresh reindex of the same tree after every step.
fn assert_incremental_matches_fresh(
    files: &[(&str, String)],
    edits: &[(&str, String)],
    expect: impl Fn(usize, &std::collections::BTreeSet<golden::EdgeKey>),
) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-rust-deferred-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    let borrowed: Vec<(&str, &str)> = files.iter().map(|(p, c)| (*p, c.as_str())).collect();
    common::write_files(&root, &borrowed);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    expect(0, &snapshot(&indexer));

    let mut tree: Vec<(String, String)> = files
        .iter()
        .map(|(p, c)| (p.to_string(), c.clone()))
        .collect();
    for (step, (path, content)) in edits.iter().enumerate() {
        std::fs::write(root.join(path), content).unwrap();
        indexer.sync_rel_paths(&[path.to_string()]).unwrap();
        for entry in tree.iter_mut().filter(|(p, _)| p == path) {
            entry.1 = content.clone();
        }
        let snap = snapshot(&indexer);
        expect(step + 1, &snap);
        let borrowed: Vec<(&str, &str)> =
            tree.iter().map(|(p, c)| (p.as_str(), c.as_str())).collect();
        let (_fresh_tmp, fresh) = common::index_files(&borrowed);
        common::assert_matches_fresh(&snap, &fresh);
    }
}

#[test]
fn editing_a_method_declaration_retargets_deferred_method_receivers() {
    let decls = |ret: &str| {
        format!(
            "pub struct Maker;\nimpl Maker {{\n    pub fn build(&self) -> {ret} {{ todo!() }}\n}}\n"
        )
    };
    let user = "use crate::decls::Maker;\n\
        pub fn via_method(m: Maker) {\n    let e = m.build();\n    e.resolve();\n}\n"
        .to_string();
    let engine = "crate::types::Engine::resolve";
    let cache = "crate::types::Cache::resolve";
    assert_incremental_matches_fresh(
        &[
            ("src/types.rs", TYPES_RS.to_string()),
            ("src/decls.rs", decls("Engine")),
            ("src/user.rs", user),
        ],
        &[
            ("src/decls.rs", decls("Cache")),
            ("src/decls.rs", decls("Result<Cache, ()>")),
        ],
        |step, snap| {
            let want = match step {
                0 => Some(engine),
                1 => Some(cache),
                _ => None,
            };
            assert_eq!(
                resolve_target(snap, "via_method").as_deref(),
                want,
                "step {step}"
            );
        },
    );
}

#[test]
fn editing_an_async_declaration_retargets_awaited_and_plain_calls() {
    let decls = |sig: &str| format!("pub {sig} {{ todo!() }}\n");
    let user = "use crate::decls::make;\n\
        pub async fn awaited() {\n    let e = make().await;\n    e.resolve();\n}\n\
        pub async fn not_awaited() {\n    let f = make();\n    f.resolve();\n}\n"
        .to_string();
    let engine = "crate::types::Engine::resolve";
    let cache = "crate::types::Cache::resolve";
    assert_incremental_matches_fresh(
        &[
            ("src/types.rs", TYPES_RS.to_string()),
            ("src/decls.rs", decls("async fn make() -> Engine")),
            ("src/user.rs", user),
        ],
        &[
            ("src/decls.rs", decls("async fn make() -> Cache")),
            // No longer async: a plain call yields the type, `.await` on it
            // is not a future.
            ("src/decls.rs", decls("fn make() -> Engine")),
        ],
        |step, snap| {
            let (awaited, plain) = match step {
                0 => (Some(engine), None),
                1 => (Some(cache), None),
                _ => (None, Some(engine)),
            };
            assert_eq!(
                resolve_target(snap, "awaited").as_deref(),
                awaited,
                "awaited, step {step}"
            );
            assert_eq!(
                resolve_target(snap, "not_awaited").as_deref(),
                plain,
                "plain, step {step}"
            );
        },
    );
}
