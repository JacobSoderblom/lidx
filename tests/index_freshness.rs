//! Issue #411: index freshness (`indexed_commit`, age, `stale`) for every
//! response's `_meta`.

use lidx::freshness;
use lidx::indexer::Indexer;
use std::path::Path;
use std::process::Command;

fn git(root: &Path, args: &[&str]) {
    let status = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["-c", "user.name=t", "-c", "user.email=t@t"])
        .args(args)
        .status()
        .unwrap();
    assert!(status.success(), "git {args:?}");
}

fn indexed(git_repo: bool) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    std::fs::write(root.join("app.py"), "def needle():\n    pass\n").unwrap();
    if git_repo {
        git(&root, &["init", "-q"]);
        git(&root, &["add", "."]);
        git(&root, &["commit", "-q", "-m", "init"]);
    }
    let db = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root, db).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

fn check(indexer: &Indexer) -> freshness::Freshness {
    freshness::compute(
        indexer.db(),
        indexer.repo_root(),
        indexer.db().current_graph_version().unwrap(),
        lidx::indexer::scan::ScanOptions::default(),
    )
    .unwrap()
}

#[test]
fn fresh_index_has_no_stale_object() {
    let (_tmp, indexer) = indexed(true);
    let fresh = check(&indexer);
    assert!(fresh.stale.is_none(), "{fresh:?}");
    assert!(fresh.stale_meta().is_none());
    assert!(fresh.warning_line().is_none());
    assert_eq!(fresh.indexed_commit.as_deref().map(str::len), Some(40));
    assert!(fresh.indexed_at.is_some());
    assert!(fresh.index_age_s.unwrap() < 60);
}

#[test]
fn commit_after_index_reports_head_moved() {
    let (tmp, indexer) = indexed(true);
    let before = check(&indexer).indexed_commit.unwrap();
    git(tmp.path(), &["commit", "-q", "--allow-empty", "-m", "next"]);
    let fresh = check(&indexer);
    let stale = fresh.stale.clone().expect("stale after commit");
    assert!(stale.head_moved);
    assert_eq!(stale.indexed_commit.as_deref(), Some(before.as_str()));
    assert_ne!(stale.current_head.as_deref(), Some(before.as_str()));
    assert_eq!(stale.changed_files, 0);
    let line = fresh.warning_line().unwrap();
    assert!(
        line.starts_with("\u{26a0} index stale: HEAD moved "),
        "{line}"
    );
    assert!(line.ends_with("run reindex"), "{line}");
}

#[test]
fn edit_without_sync_counts_changed_files() {
    let (tmp, indexer) = indexed(true);
    std::fs::write(
        tmp.path().join("app.py"),
        "def needle():\n    return 1  # edited\n",
    )
    .unwrap();
    std::fs::write(tmp.path().join("new.py"), "def added():\n    pass\n").unwrap();
    let stale = check(&indexer).stale.expect("stale after edit");
    assert!(!stale.head_moved);
    assert_eq!(stale.changed_files, 2);
}

#[test]
fn touched_but_identical_file_is_not_stale() {
    let (tmp, indexer) = indexed(true);
    let path = tmp.path().join("app.py");
    let content = std::fs::read(&path).unwrap();
    // Same size, new mtime: the content hash decides.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    std::fs::write(&path, content).unwrap();
    assert!(check(&indexer).stale.is_none());
}

#[test]
fn sync_clears_staleness_and_reindex_records_head() {
    let (tmp, mut indexer) = indexed(true);
    std::fs::write(tmp.path().join("app.py"), "def needle():\n    return 2\n").unwrap();
    git(tmp.path(), &["commit", "-q", "-am", "edit"]);
    assert!(check(&indexer).stale.is_some());
    indexer.sync_rel_paths(&["app.py".to_string()]).unwrap();
    assert!(check(&indexer).stale.is_none());
}

#[test]
fn non_git_directory_has_null_commit_and_no_crash() {
    let (tmp, indexer) = indexed(false);
    let fresh = check(&indexer);
    assert_eq!(fresh.indexed_commit, None);
    assert!(fresh.stale.is_none());
    std::fs::write(tmp.path().join("app.py"), "def needle():\n    return 3\n").unwrap();
    let stale = check(&indexer).stale.expect("edit detected without git");
    assert!(!stale.head_moved);
    assert_eq!(stale.changed_files, 1);
    let json = serde_json::to_value(check(&indexer)).unwrap();
    assert!(json["indexed_commit"].is_null());
}

#[test]
fn rpc_request_meta_only_appears_when_stale() {
    let (tmp, indexer) = indexed(true);
    let root = tmp.path().to_path_buf();
    let db = root.join(".lidx").join(".lidx.sqlite");
    let run = || {
        let out = lidx::rpc::call(
            root.clone(),
            db.clone(),
            "search".into(),
            r#"{"query":"needle"}"#,
            "1",
        )
        .unwrap();
        serde_json::from_str::<serde_json::Value>(&out).unwrap()
    };
    assert!(run().get("_meta").is_none());
    std::fs::write(root.join("app.py"), "def needle():\n    return 4\n").unwrap();
    let meta = run()["_meta"].clone();
    assert_eq!(meta["stale"]["changed_files"], 1, "{meta}");
    assert_eq!(meta["stale"]["head_moved"], false);
    assert_eq!(meta["indexed_commit"].as_str().unwrap().len(), 40);
    assert!(meta["indexed_at"].is_i64() && meta["index_age_s"].is_i64());
    drop(indexer);
}

#[test]
fn new_files_in_new_directories_count_but_ignored_ones_do_not() {
    let (tmp, indexer) = indexed(true);
    let root = tmp.path();
    std::fs::write(root.join(".gitignore"), "ignored/\n").unwrap();
    std::fs::create_dir_all(root.join("ignored")).unwrap();
    std::fs::write(root.join("ignored/skip.py"), "x = 1\n").unwrap();
    std::fs::write(root.join("notes.txt"), "not indexable\n").unwrap();
    assert!(check(&indexer).stale.is_none());
    std::fs::create_dir_all(root.join("pkg/inner")).unwrap();
    std::fs::write(root.join("pkg/inner/deep.py"), "x = 1\n").unwrap();
    assert_eq!(check(&indexer).stale.unwrap().changed_files, 1);
}

#[test]
fn deleted_file_counts_as_changed() {
    let (tmp, indexer) = indexed(true);
    std::fs::remove_file(tmp.path().join("app.py")).unwrap();
    assert_eq!(check(&indexer).stale.unwrap().changed_files, 1);
}
