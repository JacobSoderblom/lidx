//! Go declarations repeated under mutually exclusive build constraints
//! (`//go:build` lines and `_GOOS`/`_GOARCH` filename suffixes) are twins of
//! one declaration: a call binds once, to the variant that holds on the
//! default host (linux/amd64, no extra tags), else the unconstrained one. A
//! call written in a constrained file means the twin with its own
//! constraint. Every incremental scenario ends equal to a fresh reindex.

mod common;

use common::golden;
use lidx::indexer::Indexer;

const GO_MOD: &str = "module example.com/m\n\ngo 1.22\n";
const CALLER: &str = "package app\n\nfunc Run() {\n\tCompile()\n}\n";
const STDLIB: &str = "//go:build !alt\n\npackage app\n\nfunc Compile() {}\n";
const ALT: &str = "//go:build alt\n\npackage app\n\nfunc Compile() {}\n";
const PLAIN: &str = "package app\n\nfunc Compile() {}\n";
const WINDOWS: &str = "package app\n\nfunc Open() {}\n\nfunc (s *Server) Close() {}\n";
const LINUX: &str = "package app\n\nfunc Open() {}\n\nfunc (s *Server) Close() {}\n";
const SERVER: &str = "package app\n\ntype Server struct{}\n";
const WIN_CALLER: &str = "package app\n\nfunc Boot() {\n\tOpen()\n\tvar s Server\n\ts.Close()\n}\n";

fn indexed(files: &[(&str, &str)]) -> (tempfile::TempDir, std::path::PathBuf, Indexer) {
    let (tmp, root, db_path) = common::index_repo("lidx-go-tags-", files);
    let indexer = Indexer::new(root.clone(), db_path).unwrap();
    (tmp, root, indexer)
}

fn calls_from(indexer: &Indexer, source: &str) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(t.qualname, '') FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.graph_version = ? AND s.qualname = ?
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv, source], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn sync(indexer: &mut Indexer, root: &std::path::Path, files: &[(&str, &str)]) {
    common::write_files(root, files);
    let paths: Vec<String> = files.iter().map(|(p, _)| p.to_string()).collect();
    indexer.sync_rel_paths(&paths).unwrap();
}

fn assert_incremental_equals_fresh(indexer: &Indexer, final_files: &[(&str, &str)]) {
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let snapshot = golden::snapshot_edges(indexer.db(), gv).unwrap();
    let (_fresh_tmp, fresh) = common::index_files(final_files);
    common::assert_matches_fresh(&snapshot, &fresh);
}

#[test]
fn a_call_binds_once_to_the_twin_that_holds_on_the_default_host() {
    let files = [
        ("go.mod", GO_MOD),
        ("run.go", CALLER),
        ("stdlib.go", STDLIB),
        ("alt.go", ALT),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(calls_from(&indexer, "run.Run"), vec!["stdlib.Compile"]);
}

#[test]
fn an_unconstrained_twin_wins_over_a_constrained_one_that_also_holds() {
    let both_hold = "//go:build linux\n\npackage app\n\nfunc Compile() {}\n";
    let files = [
        ("go.mod", GO_MOD),
        ("run.go", CALLER),
        ("plain.go", PLAIN),
        ("tagged.go", both_hold),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(calls_from(&indexer, "run.Run"), vec!["plain.Compile"]);
}

#[test]
fn twins_that_all_hold_stay_ambiguous() {
    let one = "//go:build linux\n\npackage app\n\nfunc Compile() {}\n";
    let two = "//go:build amd64\n\npackage app\n\nfunc Compile() {}\n";
    let files = [
        ("go.mod", GO_MOD),
        ("run.go", CALLER),
        ("a.go", one),
        ("b.go", two),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    assert!(calls_from(&indexer, "run.Run").is_empty());
}

#[test]
fn filename_suffixes_constrain_functions_and_methods() {
    let files = [
        ("go.mod", GO_MOD),
        ("boot.go", WIN_CALLER),
        ("server.go", SERVER),
        ("os_linux.go", LINUX),
        ("os_windows.go", WINDOWS),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(
        calls_from(&indexer, "boot.Boot"),
        vec!["os_linux.Open", "os_linux.Server.Close"]
    );
}

#[test]
fn a_call_inside_a_constrained_file_means_the_twin_with_its_constraint() {
    let win_boot = "package app\n\nfunc Boot() {\n\tOpen()\n}\n";
    let files = [
        ("go.mod", GO_MOD),
        ("boot_windows.go", win_boot),
        ("os_linux.go", "package app\n\nfunc Open() {}\n"),
        ("os_windows.go", "package app\n\nfunc Open() {}\n"),
    ];
    let (_tmp, _root, indexer) = indexed(&files);
    assert_eq!(
        calls_from(&indexer, "boot_windows.Boot"),
        vec!["os_windows.Open"]
    );
}

#[test]
fn editing_a_build_constraint_matches_a_fresh_reindex() {
    let files = [
        ("go.mod", GO_MOD),
        ("run.go", CALLER),
        ("stdlib.go", STDLIB),
        ("alt.go", ALT),
    ];
    let (_tmp, root, mut indexer) = indexed(&files);
    assert_eq!(calls_from(&indexer, "run.Run"), vec!["stdlib.Compile"]);
    assert_incremental_equals_fresh(&indexer, &files);

    // The constraints swap: the other file now holds on the host.
    let swapped = [
        (
            "stdlib.go",
            "//go:build alt\n\npackage app\n\nfunc Compile() {}\n",
        ),
        (
            "alt.go",
            "//go:build !alt\n\npackage app\n\nfunc Compile() {}\n",
        ),
    ];
    sync(&mut indexer, &root, &swapped);
    assert_eq!(calls_from(&indexer, "run.Run"), vec!["alt.Compile"]);
    let after = [
        ("go.mod", GO_MOD),
        ("run.go", CALLER),
        swapped[0],
        swapped[1],
    ];
    assert_incremental_equals_fresh(&indexer, &after);

    // Both end up holding: ambiguous again.
    let both = [
        (
            "stdlib.go",
            "//go:build linux\n\npackage app\n\nfunc Compile() {}\n",
        ),
        (
            "alt.go",
            "//go:build amd64\n\npackage app\n\nfunc Compile() {}\n",
        ),
    ];
    sync(&mut indexer, &root, &both);
    assert!(calls_from(&indexer, "run.Run").is_empty());
    let after = [("go.mod", GO_MOD), ("run.go", CALLER), both[0], both[1]];
    assert_incremental_equals_fresh(&indexer, &after);

    // And back to a told-apart pair: the stored ambiguous call resolves.
    sync(&mut indexer, &root, &swapped);
    assert_eq!(calls_from(&indexer, "run.Run"), vec!["alt.Compile"]);
    let after = [
        ("go.mod", GO_MOD),
        ("run.go", CALLER),
        swapped[0],
        swapped[1],
    ];
    assert_incremental_equals_fresh(&indexer, &after);
}

#[test]
fn a_twin_added_later_is_picked_up_like_a_fresh_index() {
    let files = [
        ("go.mod", GO_MOD),
        ("run.go", CALLER),
        ("stdlib.go", STDLIB),
    ];
    let (_tmp, root, mut indexer) = indexed(&files);
    assert_eq!(calls_from(&indexer, "run.Run"), vec!["stdlib.Compile"]);
    sync(&mut indexer, &root, &[("alt.go", ALT)]);
    assert_eq!(calls_from(&indexer, "run.Run"), vec!["stdlib.Compile"]);
    let after = [
        ("go.mod", GO_MOD),
        ("run.go", CALLER),
        ("stdlib.go", STDLIB),
        ("alt.go", ALT),
    ];
    assert_incremental_equals_fresh(&indexer, &after);
}
