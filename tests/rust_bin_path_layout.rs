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

/// Declaring the root outside `src/` after the files were indexed renames
/// their modules: an incremental sync of the manifest ends equal to a fresh
/// index.
#[test]
fn declaring_the_bin_path_later_equals_a_fresh_index() {
    use common::golden;
    const PLAIN: &str = "[package]\nname = \"t\"\nversion = \"0.1.0\"\nedition = \"2021\"\n";
    const DECLARED: &str = "[package]\nname = \"t\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[[bin]]\nname = \"t\"\npath = \"app/main.rs\"\n";
    let rest = [
        ("app/main.rs", "mod flags;\nfn main() { flags::run(); }\n"),
        (
            "app/flags/mod.rs",
            "pub mod defs;\nuse crate::flags::defs::make;\npub fn run() { make(); }\n",
        ),
        ("app/flags/defs.rs", "pub fn make() {}\n"),
    ];
    let with = |manifest: &'static str| {
        let mut files = vec![("Cargo.toml", manifest)];
        files.extend(rest);
        files
    };
    let (_tmp, root, db_path) = common::index_repo("lidx-rust-bin-path-inc-", &with(PLAIN));
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    common::write_files(&root, &[("Cargo.toml", DECLARED)]);
    indexer.sync_rel_paths(&["Cargo.toml".to_string()]).unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(&with(DECLARED));
    common::assert_matches_fresh(&snapshot, &fresh);
}
