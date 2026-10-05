use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::Value;
use std::path::PathBuf;

fn rg_available() -> bool {
    std::process::Command::new("rg")
        .arg("--version")
        .output()
        .is_ok()
}

fn git(root: &std::path::Path, args: &[&str]) {
    let out = std::process::Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .expect("git failed to start");
    assert!(out.status.success(), "git {args:?} failed");
}

/// Writes `files`, makes the dir a git repo with one commit, and reindexes it.
/// Returns `(guard, repo_root, db_path)`; the guard removes the dir on drop.
fn indexed_repo(files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf, PathBuf) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-search-hidden-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    for (rel, text) in files {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }
    git(&root, &["init", "-q"]);
    git(&root, &["config", "user.email", "test@example.com"]);
    git(&root, &["config", "user.name", "Test User"]);
    git(&root, &["add", "-A"]);
    git(&root, &["commit", "-q", "-m", "initial"]);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    Indexer::new(root.clone(), db_path.clone())
        .unwrap()
        .reindex()
        .unwrap();
    (tmp, root, db_path)
}

/// Runs `search` and returns the hit paths.
fn search_paths(root: &std::path::Path, db_path: &std::path::Path, params: &str) -> Vec<String> {
    let response = rpc::call(
        root.to_path_buf(),
        db_path.to_path_buf(),
        "search".to_string(),
        params,
        "1",
    )
    .unwrap();
    let value: Value = serde_json::from_str(&response).unwrap();
    value["result"]["results"]
        .as_array()
        .unwrap_or_else(|| panic!("no results array: {value}"))
        .iter()
        .map(|hit| hit["path"].as_str().unwrap_or("?").to_string())
        .collect()
}

/// A repo with a `.migrations` dir, a normal dir, and `.sql` content planted in `.git/`.
fn migrations_repo() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let (tmp, root, db) = indexed_repo(&[
        (".migrations/001.sql", "CREATE TABLE dpb.audit (id int);"),
        ("db/002.sql", "CREATE TABLE dpb.other (id int);"),
    ]);
    // Not tracked or indexed; only rg could ever find it.
    std::fs::write(
        root.join(".git/planted.sql"),
        "CREATE TABLE dpb.secret (id int);",
    )
    .unwrap();
    (tmp, root, db)
}

#[test]
fn default_search_finds_indexed_file_in_hidden_dir() {
    if !rg_available() {
        return;
    }
    let (_tmp, root, db) = migrations_repo();
    let paths = search_paths(&root, &db, r#"{"query":"CREATE TABLE","limit":500}"#);
    assert!(
        paths.contains(&".migrations/001.sql".to_string()),
        "{paths:?}"
    );
}

#[test]
fn default_search_still_finds_normal_dir() {
    if !rg_available() {
        return;
    }
    let (_tmp, root, db) = migrations_repo();
    let paths = search_paths(&root, &db, r#"{"query":"CREATE TABLE","limit":500}"#);
    assert!(paths.contains(&"db/002.sql".to_string()), "{paths:?}");
}

#[test]
fn default_search_excludes_git_dir() {
    if !rg_available() {
        return;
    }
    let (_tmp, root, db) = migrations_repo();
    let paths = search_paths(&root, &db, r#"{"query":"dpb.secret","limit":500}"#);
    assert!(!paths.iter().any(|p| p.starts_with(".git/")), "{paths:?}");
}

#[test]
fn git_dir_excluded_with_user_glob() {
    if !rg_available() {
        return;
    }
    let (_tmp, root, db) = migrations_repo();
    let paths = search_paths(
        &root,
        &db,
        r#"{"query":"dpb.secret","globs":["*.sql"],"hidden":true,"limit":500}"#,
    );
    assert!(!paths.iter().any(|p| p.starts_with(".git/")), "{paths:?}");
}

#[test]
fn git_dir_excluded_with_explicit_path() {
    if !rg_available() {
        return;
    }
    let (_tmp, root, db) = migrations_repo();
    let paths = search_paths(
        &root,
        &db,
        r#"{"query":"dpb.secret","path":".git","hidden":true,"limit":500}"#,
    );
    assert!(!paths.iter().any(|p| p.starts_with(".git/")), "{paths:?}");
}

#[test]
fn git_dir_excluded_with_explicit_hidden_true() {
    if !rg_available() {
        return;
    }
    let (_tmp, root, db) = migrations_repo();
    let paths = search_paths(
        &root,
        &db,
        r#"{"query":"dpb.secret","hidden":true,"limit":500}"#,
    );
    assert!(!paths.iter().any(|p| p.starts_with(".git/")), "{paths:?}");
}

#[test]
fn explicit_hidden_false_skips_hidden_dir() {
    if !rg_available() {
        return;
    }
    let (_tmp, root, db) = migrations_repo();
    let paths = search_paths(
        &root,
        &db,
        r#"{"query":"CREATE TABLE","hidden":false,"limit":500}"#,
    );
    assert!(
        !paths.contains(&".migrations/001.sql".to_string()),
        "{paths:?}"
    );
}

fn env_repo() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let (tmp, root, db) =
        indexed_repo(&[(".migrations/001.sql", "CREATE TABLE dpb.audit (id int);")]);
    // Written after indexing, and `.env` is not an indexed language anyway.
    std::fs::write(root.join(".env"), "API_TOKEN=hunter2 CREATE TABLE").unwrap();
    (tmp, root, db)
}

#[test]
fn default_search_omits_unindexed_dotenv() {
    if !rg_available() {
        return;
    }
    let (_tmp, root, db) = env_repo();
    let paths = search_paths(&root, &db, r#"{"query":"hunter2","limit":500}"#);
    assert!(paths.is_empty(), "{paths:?}");
}

#[test]
fn explicit_hidden_true_finds_unindexed_dotenv() {
    if !rg_available() {
        return;
    }
    let (_tmp, root, db) = env_repo();
    let paths = search_paths(
        &root,
        &db,
        r#"{"query":"hunter2","hidden":true,"limit":500}"#,
    );
    assert_eq!(paths, vec![".env".to_string()]);
}

#[test]
fn default_limit_counts_only_indexed_dot_hits() {
    if !rg_available() {
        return;
    }
    let (_tmp, root, db) = indexed_repo(&[
        (".migrations/001.sql", "SELECT needle1;"),
        (".migrations/002.sql", "SELECT needle1;"),
        (".migrations/003.sql", "SELECT needle1;"),
    ]);
    // Unindexed (non-source) dot files that also match; they must not eat the limit.
    for i in 0..20 {
        std::fs::write(root.join(format!(".noise{i}.env")), "needle1").unwrap();
    }
    let response = rpc::call(
        root.clone(),
        db.clone(),
        "search".to_string(),
        r#"{"query":"needle1","limit":2}"#,
        "1",
    )
    .unwrap();
    let value: Value = serde_json::from_str(&response).unwrap();
    let result = &value["result"];
    assert_eq!(result["results"].as_array().unwrap().len(), 2, "{result}");
    assert_eq!(result["truncated"], Value::Bool(true), "{result}");
}

#[test]
fn default_limit_not_capped_when_all_indexed_hits_fit() {
    if !rg_available() {
        return;
    }
    let (_tmp, root, db) = indexed_repo(&[(".migrations/001.sql", "SELECT needle1;")]);
    for i in 0..20 {
        std::fs::write(root.join(format!(".noise{i}.env")), "needle1").unwrap();
    }
    let response = rpc::call(
        root.clone(),
        db.clone(),
        "search".to_string(),
        r#"{"query":"needle1","limit":2}"#,
        "1",
    )
    .unwrap();
    let value: Value = serde_json::from_str(&response).unwrap();
    let result = &value["result"];
    assert_eq!(result["results"].as_array().unwrap().len(), 1, "{result}");
    assert_ne!(result["truncated"], Value::Bool(true), "{result}");
}

#[test]
fn default_path_inside_dot_dir_returns_only_indexed_files() {
    if !rg_available() {
        return;
    }
    let (_tmp, root, db) = indexed_repo(&[(".migrations/001.sql", "SELECT needle1;")]);
    std::fs::write(root.join(".migrations/notes.env"), "needle1").unwrap();
    let paths = search_paths(&root, &db, r#"{"query":"needle1","path":".migrations"}"#);
    assert_eq!(paths, vec![".migrations/001.sql".to_string()]);
}
