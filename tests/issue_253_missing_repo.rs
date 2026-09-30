//! Issue #253: a missing or non-directory `--repo` must be a hard input error,
//! and `lidx reindex` must refuse a scan that finds zero files unless
//! `--allow-empty` is given. All tests drive the real `lidx` binary.
mod common;

use common::setup_repo;
use rusqlite::Connection;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

fn lidx(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_lidx"))
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("run lidx")
}

fn s(path: &Path) -> &str {
    path.to_str().unwrap()
}

/// Row counts that must not change when an operation is refused.
#[derive(Debug, PartialEq, Eq)]
struct DbState {
    files: i64,
    symbols: i64,
    edges: i64,
    graph_versions: i64,
    current_graph_version: i64,
}

fn db_state(db_path: &Path) -> DbState {
    let conn = Connection::open(db_path).unwrap();
    let count = |table: &str| -> i64 {
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    };
    DbState {
        files: count("files"),
        symbols: count("symbols"),
        edges: count("edges"),
        graph_versions: count("graph_versions"),
        current_graph_version: conn
            .query_row("SELECT COALESCE(MAX(id), 0) FROM graph_versions", [], |r| {
                r.get(0)
            })
            .unwrap(),
    }
}

/// Files in the current graph version, as the indexer sees them.
fn live_files(db_path: &Path) -> usize {
    let db = lidx::db::Db::new(db_path).unwrap();
    db.list_files(db.current_graph_version().unwrap())
        .unwrap()
        .len()
}

fn dir_listing(dir: &Path) -> BTreeSet<String> {
    fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// Indexes the python fixture through the CLI and returns (guard, repo, db).
fn indexed_repo() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let (tmp, repo, db) = setup_repo("golden/python");
    let out = lidx(&["reindex", "--repo", s(&repo)]);
    assert!(out.status.success(), "{}", stderr(&out));
    let state = db_state(&db);
    assert!(state.files > 0 && state.symbols > 0 && state.edges > 0);
    (tmp, repo, db)
}

fn remove_all_indexed_files(repo: &Path) {
    for entry in fs::read_dir(repo).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "py") {
            fs::remove_file(path).unwrap();
        }
    }
}

#[test]
fn missing_repo_with_existing_db_fails_and_leaves_db_untouched() {
    let (tmp, repo, db) = indexed_repo();
    let before = db_state(&db);
    let missing = tmp.path().join("nonexistent");

    let out = lidx(&["reindex", "--repo", s(&missing), "--db", s(&db)]);

    assert!(!out.status.success());
    assert!(stderr(&out).contains(s(&missing)), "{}", stderr(&out));
    assert_eq!(db_state(&db), before);
    assert!(!missing.exists());
    drop(repo);
}

#[test]
fn missing_repo_without_db_creates_no_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("nonexistent");
    let before = dir_listing(tmp.path());

    let out = lidx(&["reindex", "--repo", s(&missing)]);

    assert!(!out.status.success());
    assert!(stderr(&out).contains(s(&missing)), "{}", stderr(&out));
    assert!(!missing.exists());
    assert_eq!(dir_listing(tmp.path()), before);
}

#[test]
fn every_subcommand_rejects_missing_repo_without_creating_directories() {
    // (subcommand, extra args after --repo <path>)
    let table: &[(&str, &[&str])] = &[
        ("serve", &[]),
        ("reindex", &[]),
        ("changed-files", &[]),
        ("overview", &[]),
        ("request", &["--method", "orient"]),
        ("mcp-serve", &[]),
        ("context", &["src/x.py"]),
        ("init", &[]),
    ];

    // The table must cover every subcommand the binary exposes.
    let help = lidx(&["--help"]);
    let help = String::from_utf8_lossy(&help.stdout).into_owned();
    let advertised: BTreeSet<String> = help
        .lines()
        .skip_while(|l| !l.starts_with("Commands:"))
        .skip(1)
        .take_while(|l| !l.trim().is_empty())
        .filter_map(|l| l.split_whitespace().next())
        .filter(|name| *name != "help")
        .map(str::to_string)
        .collect();
    let covered: BTreeSet<String> = table.iter().map(|(n, _)| n.to_string()).collect();
    assert_eq!(advertised, covered, "test table drifted from the CLI");

    for (name, extra) in table {
        for with_db in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let missing = tmp.path().join("nonexistent");
            let db = tmp.path().join("elsewhere").join("db.sqlite");
            let mut args = vec![*name, "--repo", s(&missing)];
            if with_db {
                args.extend(["--db", s(&db)]);
            }
            args.extend(extra.iter().copied());

            let out = lidx(&args);

            assert!(!out.status.success(), "{name} exited 0: {args:?}");
            assert!(
                stderr(&out).contains(s(&missing)),
                "{name}: {}",
                stderr(&out)
            );
            assert_eq!(
                dir_listing(tmp.path()),
                BTreeSet::new(),
                "{name} created something: {args:?}"
            );
        }
    }
}

#[test]
fn repo_that_is_a_file_fails_and_names_path() {
    let tmp = tempfile::tempdir().unwrap();
    let file = tmp.path().join("file_not_dir");
    fs::write(&file, "x").unwrap();

    let out = lidx(&["reindex", "--repo", s(&file)]);

    assert!(!out.status.success());
    assert!(stderr(&out).contains(s(&file)), "{}", stderr(&out));
    assert_eq!(dir_listing(tmp.path()).len(), 1);
}

#[test]
fn emptied_repo_is_refused_and_changes_nothing() {
    let (_tmp, repo, db) = indexed_repo();
    let before = db_state(&db);
    let previous = live_files(&db);
    remove_all_indexed_files(&repo);

    let out = lidx(&["reindex", "--repo", s(&repo)]);

    assert!(!out.status.success());
    let err = stderr(&out);
    assert!(
        err.contains(&format!(
            "scan found 0 file(s) but the previous index has {previous}"
        )),
        "{err}"
    );
    assert_eq!(db_state(&db), before);
}

#[test]
fn emptied_repo_with_allow_empty_empties_the_index() {
    let (_tmp, repo, db) = indexed_repo();
    remove_all_indexed_files(&repo);

    let out = lidx(&["reindex", "--repo", s(&repo), "--allow-empty"]);

    assert!(out.status.success(), "{}", stderr(&out));
    let live = live_files(&db);
    assert_eq!(live, 0, "index should be legitimately emptied");
}

#[test]
fn genuinely_empty_directory_needs_allow_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("empty");
    fs::create_dir(&repo).unwrap();

    let refused = lidx(&["reindex", "--repo", s(&repo)]);
    assert!(!refused.status.success());
    assert!(
        stderr(&refused).contains("scan found 0 file(s) but the previous index has 0"),
        "{}",
        stderr(&refused)
    );

    let allowed = lidx(&["reindex", "--repo", s(&repo), "--allow-empty"]);
    assert!(allowed.status.success(), "{}", stderr(&allowed));
}

#[test]
fn valid_repo_reindex_is_unaffected() {
    let (_tmp, repo, _db) = setup_repo("golden/python");

    let out = lidx(&["reindex", "--repo", s(&repo)]);

    assert!(out.status.success(), "{}", stderr(&out));
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(json["indexed"].as_u64().unwrap() > 0);
    assert!(json.get("scanned").is_some() && json.get("deleted").is_some());
}

#[test]
fn scanning_a_vanished_root_is_an_error_not_an_empty_scan() {
    let tmp = tempfile::tempdir().unwrap();
    let gone = tmp.path().join("gone");

    let err = lidx::indexer::scan::scan_repo(&gone).unwrap_err();

    assert!(format!("{err:#}").contains("gone"), "{err:#}");
}
