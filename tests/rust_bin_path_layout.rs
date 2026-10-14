//! A crate whose manifest declares its root outside `src/` (`[[bin]] path =
//! "app/main.rs"`) names its modules from that directory: `app/flags/defs.rs`
//! is `crate::flags::defs`, so `use crate::flags::defs::make;` in another
//! file binds the function instead of staying unresolved.

mod common;

use lidx::indexer::Indexer;

#[test]
fn modules_of_a_bin_outside_src_hang_off_the_roots_directory() {
    let files = [
        (
            "Cargo.toml",
            "[package]\nname = \"t\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[[bin]]\nname = \"t\"\npath = \"app/main.rs\"\n",
        ),
        ("app/main.rs", "mod flags;\nfn main() { flags::run(); }\n"),
        (
            "app/flags/mod.rs",
            "pub mod defs;\nuse crate::flags::defs::make;\npub fn run() { make(); }\n",
        ),
        ("app/flags/defs.rs", "pub fn make() {}\n"),
        ("other/make.rs", "pub fn make() {}\n"),
    ];
    let (_tmp, root, db_path) = common::index_repo("lidx-rust-bin-path-", &files);
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let qualnames: Vec<String> = conn
        .prepare(
            "SELECT qualname FROM symbols WHERE kind = 'module' AND graph_version = ? ORDER BY 1",
        )
        .unwrap()
        .query_map([gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(
        qualnames.iter().any(|q| q == "crate::flags::defs"),
        "{qualnames:?}"
    );
    assert!(
        !qualnames.iter().any(|q| q.contains("app")),
        "{qualnames:?}"
    );
    let targets: Vec<String> = conn
        .prepare(
            "SELECT t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.graph_version = ? AND s.name = 'run'",
        )
        .unwrap()
        .query_map([gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(targets, ["crate::flags::defs::make"]);
}
