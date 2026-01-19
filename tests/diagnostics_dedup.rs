use lidx::db::Db;
use lidx::diagnostics::DiagnosticInput;
use std::path::PathBuf;

fn temp_db_path() -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = std::process::id();
    path.push(format!("lidx_diag_dedup_{pid}_{nanos}.sqlite"));
    path
}

#[test]
fn diagnostics_import_is_deduped() {
    let db_path = temp_db_path();
    let mut db = Db::new(&db_path).unwrap();
    let diagnostic = DiagnosticInput {
        path: Some("src/main.rs".to_string()),
        line: Some(10),
        column: Some(5),
        end_line: Some(10),
        end_column: Some(12),
        severity: Some("error".to_string()),
        message: "Duplicated".to_string(),
        rule_id: Some("R1".to_string()),
        tool: Some("demo".to_string()),
        snippet: Some("bad".to_string()),
    };

    db.insert_diagnostics(&[diagnostic.clone()]).unwrap();
    db.insert_diagnostics(&[diagnostic]).unwrap();
    let summary = db
        .diagnostics_summary(None, None, None, None, None)
        .unwrap();
    assert_eq!(summary.total, 1);

    drop(db);
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(PathBuf::from(format!("{}-wal", db_path.display())));
    let _ = std::fs::remove_file(PathBuf::from(format!("{}-shm", db_path.display())));
}
