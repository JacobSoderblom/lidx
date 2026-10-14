//! A `const` / `static` has its declared type: `for flag in FLAGS.iter()`
//! types `flag` as the registry's element type, in the file that declares the
//! constant and in files that import it, and a change of the declared type
//! reaches the importing file (incremental sync equals a fresh reindex).

mod common;

use common::golden;
use lidx::indexer::Indexer;

const CARGO: &str = "[package]\nname = \"t\"\nversion = \"0.1.0\"\nedition = \"2021\"\n";
const TRAITS: &str = "pub trait Flag { fn name_long(&self) -> &'static str; }\n\
                      pub trait Other { fn name_long(&self) -> &'static str; }\n\
                      pub struct A;\nimpl Flag for A { fn name_long(&self) -> &'static str { \"a\" } }\n";
const USER: &str =
    "use crate::FLAGS;\npub fn cross() { for f in FLAGS.iter() { f.name_long(); } }\n";

fn lib(declared: &str) -> String {
    format!(
        "pub mod traits;\npub mod user;\npub use traits::*;\npub {declared} = &[&A];\n\
         pub fn local() {{ for f in FLAGS.iter() {{ f.name_long(); }} }}\n"
    )
}

fn calls_from(indexer: &Indexer, source: &str) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.graph_version = ? AND s.name = ?
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv, source], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn a_constants_declared_type_types_its_loop_variable_in_every_file() {
    let flag_lib = lib("const FLAGS: &[&dyn Flag]");
    let files = [
        ("Cargo.toml", CARGO),
        ("src/lib.rs", flag_lib.as_str()),
        ("src/traits.rs", TRAITS),
        ("src/user.rs", USER),
    ];
    let (_tmp, root, db_path) = common::index_repo("lidx-rust-const-", &files);
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    assert_eq!(
        calls_from(&indexer, "local"),
        ["crate::traits::Flag::name_long"]
    );
    assert_eq!(
        calls_from(&indexer, "cross"),
        ["crate::traits::Flag::name_long"]
    );

    // The declared type changes: the importing file follows.
    let other_lib = lib("const FLAGS: &[&dyn Other]");
    common::write_files(&root, &[("src/lib.rs", other_lib.as_str())]);
    indexer.sync_rel_paths(&["src/lib.rs".to_string()]).unwrap();
    assert_eq!(
        calls_from(&indexer, "local"),
        ["crate::traits::Other::name_long"]
    );
    assert_eq!(
        calls_from(&indexer, "cross"),
        ["crate::traits::Other::name_long"]
    );

    let final_files = [
        ("Cargo.toml", CARGO),
        ("src/lib.rs", other_lib.as_str()),
        ("src/traits.rs", TRAITS),
        ("src/user.rs", USER),
    ];
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&final_files);
    common::assert_matches_fresh(&snapshot, &fresh);
}
