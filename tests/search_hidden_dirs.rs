use lidx::indexer::Indexer;
use lidx::rpc;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_repo_dir(label: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("lidx-search-hidden-{label}-{nanos}-{counter}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn search_finds_matches_in_hidden_directories() {
    if std::process::Command::new("rg")
        .arg("--version")
        .output()
        .is_err()
    {
        return;
    }

    let repo_root = temp_repo_dir("hidden");

    // Create .migrations/001.sql and db/002.sql
    std::fs::create_dir_all(repo_root.join(".migrations")).unwrap();
    std::fs::create_dir_all(repo_root.join("db")).unwrap();
    std::fs::write(
        repo_root.join(".migrations/001.sql"),
        "CREATE TABLE dpb.audit (id int);",
    )
    .unwrap();
    std::fs::write(
        repo_root.join("db/002.sql"),
        "CREATE TABLE dpb.other (id int);",
    )
    .unwrap();

    // Initialize git repo
    std::process::Command::new("git")
        .arg("init")
        .current_dir(&repo_root)
        .output()
        .expect("git init failed");
    std::process::Command::new("git")
        .arg("config")
        .arg("user.email")
        .arg("test@example.com")
        .current_dir(&repo_root)
        .output()
        .expect("git config email failed");
    std::process::Command::new("git")
        .arg("config")
        .arg("user.name")
        .arg("Test User")
        .current_dir(&repo_root)
        .output()
        .expect("git config name failed");
    std::process::Command::new("git")
        .arg("add")
        .arg("-A")
        .current_dir(&repo_root)
        .output()
        .expect("git add failed");
    std::process::Command::new("git")
        .arg("commit")
        .arg("-m")
        .arg("initial")
        .current_dir(&repo_root)
        .output()
        .expect("git commit failed");

    // Reindex
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Search without hidden:true should find both files
    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "search".to_string(),
        r#"{"query":"CREATE TABLE","languages":["sql"],"limit":500}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let hits = value["result"]["results"].as_array().unwrap();

    // Both files should be found
    let found_001 = hits.iter().any(|hit| {
        hit.get("path")
            .and_then(|v| v.as_str())
            .map(|p| p.contains(".migrations/001.sql"))
            .unwrap_or(false)
    });
    let found_002 = hits.iter().any(|hit| {
        hit.get("path")
            .and_then(|v| v.as_str())
            .map(|p| p.contains("db/002.sql"))
            .unwrap_or(false)
    });

    assert!(
        found_001,
        "Should find .migrations/001.sql. Found {} hits: {:?}",
        hits.len(),
        hits.iter()
            .map(|h| h.get("path").and_then(|v| v.as_str()).unwrap_or("?"))
            .collect::<Vec<_>>()
    );
    assert!(
        found_002,
        "Should find db/002.sql. Found {} hits: {:?}",
        hits.len(),
        hits.iter()
            .map(|h| h.get("path").and_then(|v| v.as_str()).unwrap_or("?"))
            .collect::<Vec<_>>()
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn search_excludes_git_directory() {
    if std::process::Command::new("rg")
        .arg("--version")
        .output()
        .is_err()
    {
        return;
    }

    let repo_root = temp_repo_dir("git-exclude");

    // Create .git-like file and a normal file
    std::fs::create_dir_all(repo_root.join(".git")).unwrap();
    std::fs::write(
        repo_root.join(".git/secrets.txt"),
        "CREATE TABLE dpb.secret (id int);",
    )
    .unwrap();
    std::fs::write(
        repo_root.join("file.sql"),
        "CREATE TABLE dpb.public (id int);",
    )
    .unwrap();

    // Initialize git repo
    std::process::Command::new("git")
        .arg("init")
        .current_dir(&repo_root)
        .output()
        .expect("git init failed");
    std::process::Command::new("git")
        .arg("config")
        .arg("user.email")
        .arg("test@example.com")
        .current_dir(&repo_root)
        .output()
        .expect("git config email failed");
    std::process::Command::new("git")
        .arg("config")
        .arg("user.name")
        .arg("Test User")
        .current_dir(&repo_root)
        .output()
        .expect("git config name failed");
    std::process::Command::new("git")
        .arg("add")
        .arg("-A")
        .current_dir(&repo_root)
        .output()
        .expect("git add failed");
    std::process::Command::new("git")
        .arg("commit")
        .arg("-m")
        .arg("initial")
        .current_dir(&repo_root)
        .output()
        .expect("git commit failed");

    // Reindex
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Search for the table name should find only the public file, not .git/secrets.txt
    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "search".to_string(),
        r#"{"query":"CREATE TABLE","languages":["sql"],"limit":500}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let hits = value["result"]["results"].as_array().unwrap();

    // Should find file.sql but not .git/secrets.txt
    let found_public = hits.iter().any(|hit| {
        hit.get("path")
            .and_then(|v| v.as_str())
            .map(|p| p.contains("file.sql"))
            .unwrap_or(false)
    });
    let found_secret = hits.iter().any(|hit| {
        hit.get("path")
            .and_then(|v| v.as_str())
            .map(|p| p.contains(".git"))
            .unwrap_or(false)
    });

    assert!(
        found_public,
        "Should find file.sql. Found {} hits",
        hits.len()
    );
    assert!(
        !found_secret,
        ".git directory should be excluded. Found secret hit"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}
