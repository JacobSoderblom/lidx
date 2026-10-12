//! Go calls bound through packages (`Name()` in a sibling file, `pkg.Name()`
//! through an import) across incremental sync. A call whose package has no
//! such member yet is stored like an unresolved call and retried when the
//! member appears, so every scenario ends equal to a fresh reindex.

mod common;

use common::golden;
use lidx::indexer::Indexer;

const GO_MOD: &str = "module example.com/m\n\ngo 1.22\n";
const MAIN: &str =
    "package main\n\nimport \"example.com/m/lib\"\n\nfunc Run() {\n\tlib.Helper()\n\tlocal()\n}\n";
const LOCAL: &str = "package main\n\nfunc local() {}\n";
const LIB: &str = "package lib\n\nfunc Helper() {}\n";
const LIB_RENAMED: &str = "package lib\n\nfunc Helper2() {}\n";
/// A same-named decoy elsewhere that must never be bound.
const DECOY: &str = "package other\n\nfunc Helper() {}\n\nfunc local() {}\n";

fn calls(indexer: &Indexer) -> Vec<(String, String)> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, COALESCE(t.qualname, '') FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.graph_version = ? AND s.qualname LIKE 'main.%'
             ORDER BY 1, 2",
        )
        .unwrap();
    stmt.query_map([gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn indexed(files: &[(&str, &str)]) -> (tempfile::TempDir, std::path::PathBuf, Indexer) {
    let (tmp, root, db_path) = common::index_repo("lidx-go-call-sync-", files);
    let indexer = Indexer::new(root.clone(), db_path).unwrap();
    (tmp, root, indexer)
}

fn assert_incremental_equals_fresh(indexer: &Indexer, final_files: &[(&str, &str)]) {
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(final_files);
    common::assert_matches_fresh(&snapshot, &fresh);
}

fn run_calls(indexer: &Indexer) -> Vec<String> {
    calls(indexer)
        .into_iter()
        .filter(|(s, _)| s == "main.Run")
        .map(|(_, t)| t)
        .collect()
}

#[test]
fn a_package_member_added_later_is_picked_up_like_a_fresh_index() {
    let base = [("go.mod", GO_MOD), ("main.go", MAIN), ("other/o.go", DECOY)];
    let (_tmp, root, mut indexer) = indexed(&base);
    // Neither call binds to the decoy's same-named functions.
    let before = run_calls(&indexer);
    assert!(
        before.iter().all(|t| !t.starts_with("other/")),
        "{before:?}"
    );
    assert_incremental_equals_fresh(&indexer, &base);

    common::write_files(&root, &[("lib/lib.go", LIB), ("local.go", LOCAL)]);
    indexer
        .sync_rel_paths(&["lib/lib.go".to_string(), "local.go".to_string()])
        .unwrap();
    let after = run_calls(&indexer);
    assert!(after.contains(&"lib/lib.Helper".to_string()), "{after:?}");
    assert!(after.contains(&"local.local".to_string()), "{after:?}");
    let mut files = base.to_vec();
    files.extend([("lib/lib.go", LIB), ("local.go", LOCAL)]);
    assert_incremental_equals_fresh(&indexer, &files);
}

#[test]
fn renaming_a_package_member_away_and_back_matches_a_fresh_index() {
    let base = [
        ("go.mod", GO_MOD),
        ("main.go", MAIN),
        ("local.go", LOCAL),
        ("other/o.go", DECOY),
    ];
    let with = |lib: &'static str| {
        let mut files = base.to_vec();
        files.push(("lib/lib.go", lib));
        files
    };
    let (_tmp, root, mut indexer) = indexed(&with(LIB));
    assert!(run_calls(&indexer).contains(&"lib/lib.Helper".to_string()));

    common::write_files(&root, &[("lib/lib.go", LIB_RENAMED)]);
    indexer.sync_rel_paths(&["lib/lib.go".to_string()]).unwrap();
    let renamed = run_calls(&indexer);
    assert!(
        !renamed.contains(&"lib/lib.Helper".to_string()),
        "{renamed:?}"
    );
    assert!(
        renamed.iter().all(|t| !t.starts_with("other/")),
        "{renamed:?}"
    );
    assert_incremental_equals_fresh(&indexer, &with(LIB_RENAMED));

    common::write_files(&root, &[("lib/lib.go", LIB)]);
    indexer.sync_rel_paths(&["lib/lib.go".to_string()]).unwrap();
    assert!(run_calls(&indexer).contains(&"lib/lib.Helper".to_string()));
    assert_incremental_equals_fresh(&indexer, &with(LIB));
}

#[test]
fn deleting_a_package_member_file_drops_the_edge_like_a_fresh_index() {
    let all = [
        ("go.mod", GO_MOD),
        ("main.go", MAIN),
        ("local.go", LOCAL),
        ("lib/lib.go", LIB),
    ];
    let (_tmp, root, mut indexer) = indexed(&all);
    assert!(run_calls(&indexer).contains(&"local.local".to_string()));

    std::fs::remove_file(root.join("local.go")).unwrap();
    indexer.sync_rel_paths(&["local.go".to_string()]).unwrap();
    assert!(!run_calls(&indexer).contains(&"local.local".to_string()));
    assert_incremental_equals_fresh(
        &indexer,
        &[("go.mod", GO_MOD), ("main.go", MAIN), ("lib/lib.go", LIB)],
    );
}

#[test]
fn a_standard_library_call_stays_external_when_a_same_named_repo_function_appears() {
    let main = "package main\n\nimport \"path\"\n\nfunc Run() {\n\tpath.Join(\"a\")\n}\n";
    let base = [("go.mod", GO_MOD), ("main.go", main)];
    let (_tmp, root, mut indexer) = indexed(&base);
    assert_eq!(run_calls(&indexer), ["ext:path.Join"]);

    let join = "package util\n\nfunc Join(p ...string) string { return \"\" }\n";
    common::write_files(&root, &[("util/join.go", join)]);
    indexer
        .sync_rel_paths(&["util/join.go".to_string()])
        .unwrap();
    assert_eq!(run_calls(&indexer), ["ext:path.Join"]);
    let mut files = base.to_vec();
    files.push(("util/join.go", join));
    assert_incremental_equals_fresh(&indexer, &files);
}

/// Target qualnames of the resolved CALLS edges of `source`, sorted.
fn calls_from(indexer: &Indexer, source: &str) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.graph_version = ? AND s.qualname = ?
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv, source], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn a_package_whose_name_differs_from_its_directory_binds_once_it_exists() {
    let caller = "package main\n\nimport \"example.com/m/dirx\"\n\nfunc Run() {\n\tnamed.Do()\n}\n";
    let pkg = "package named\n\nfunc Do() {}\n";
    let base = [("go.mod", GO_MOD), ("main.go", caller)];
    let (_tmp, root, mut indexer) = indexed(&base);
    assert!(
        !calls_from(&indexer, "main.Run")
            .iter()
            .any(|t| t.starts_with("dirx/")),
        "{:?}",
        calls_from(&indexer, "main.Run")
    );
    assert_incremental_equals_fresh(&indexer, &base);

    common::write_files(&root, &[("dirx/x.go", pkg)]);
    indexer.sync_rel_paths(&["dirx/x.go".to_string()]).unwrap();
    assert_eq!(calls_from(&indexer, "main.Run"), ["dirx/x.Do"]);
    let mut files = base.to_vec();
    files.push(("dirx/x.go", pkg));
    assert_incremental_equals_fresh(&indexer, &files);
}

#[test]
fn a_nested_module_importing_the_root_binds_once_the_package_exists() {
    let nested =
        "package tools\n\nimport \"example.com/m/lib\"\n\nfunc Run() {\n\tlib.Helper()\n}\n";
    let base = [
        ("go.mod", GO_MOD),
        ("tools/go.mod", "module example.com/tools\n"),
        ("tools/t.go", nested),
    ];
    let (_tmp, root, mut indexer) = indexed(&base);
    assert!(calls_from(&indexer, "tools/t.Run").is_empty());
    assert_incremental_equals_fresh(&indexer, &base);

    common::write_files(&root, &[("lib/lib.go", LIB)]);
    indexer.sync_rel_paths(&["lib/lib.go".to_string()]).unwrap();
    assert_eq!(calls_from(&indexer, "tools/t.Run"), ["lib/lib.Helper"]);
    let mut files = base.to_vec();
    files.push(("lib/lib.go", LIB));
    assert_incremental_equals_fresh(&indexer, &files);
}
