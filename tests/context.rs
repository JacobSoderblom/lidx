use lidx::context;
use lidx::db::Db;
use lidx::indexer::Indexer;
use lidx::rpc;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn temp_repo_dir(label: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("lidx-ctx-{label}-{nanos}-{counter}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&path, &target);
        } else {
            std::fs::copy(&path, &target).unwrap();
        }
    }
}

struct TempRepo {
    pub repo_root: PathBuf,
    pub db_path: PathBuf,
}

impl Drop for TempRepo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.repo_root);
    }
}

impl TempRepo {
    fn new(fixture: &str) -> Self {
        let src = fixture_path(fixture);
        let repo_root = temp_repo_dir(fixture);
        copy_dir(&src, &repo_root);
        let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
        Self { repo_root, db_path }
    }

    fn index(&self) -> Indexer {
        let mut indexer = Indexer::new(self.repo_root.clone(), self.db_path.clone()).unwrap();
        indexer.reindex().unwrap();
        indexer
    }
}

#[test]
fn context_symbols_for_file() {
    let temp = TempRepo::new("py_mvp");
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    let ctx = context::build_file_context(db, &temp.repo_root, "pkg/core.py", gv).unwrap();
    assert_eq!(ctx.path, "pkg/core.py");
    assert!(
        ctx.symbol_summary.contains("symbols"),
        "Expected symbol summary, got: {}",
        ctx.symbol_summary
    );
    // core.py has: module, Base class, Greeter class, greet method, make_greeter function, imports
    assert!(
        !ctx.symbol_summary.starts_with("0 symbols"),
        "Expected non-zero symbols"
    );
}

#[test]
fn context_cross_file_callers() {
    let temp = TempRepo::new("py_mvp");
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    // pkg/b.py defines helper() which is called by pkg/a.py
    let ctx = context::build_file_context(db, &temp.repo_root, "pkg/b.py", gv).unwrap();

    // Should show a.py as a caller of helper()
    let caller_files: Vec<&str> = ctx
        .cross_file_callers
        .iter()
        .map(|c| c.file_path.as_str())
        .collect();
    assert!(
        caller_files.iter().any(|f| f.contains("a.py")),
        "Expected pkg/a.py as caller. Callers: {:?}",
        ctx.cross_file_callers
    );
}

#[test]
fn context_cross_file_callees() {
    let temp = TempRepo::new("py_mvp");
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    // app.py calls make_greeter() and Greeter.greet() from pkg/core.py
    let ctx = context::build_file_context(db, &temp.repo_root, "app.py", gv).unwrap();

    // Should show callees from core.py
    // app.py imports from pkg.core and calls make_greeter/greet
    assert!(
        !ctx.cross_file_callees.is_empty(),
        "Expected callees from app.py. Got none."
    );
}

#[test]
fn context_missing_db() {
    let dir = temp_repo_dir("nodb");
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    // DB doesn't exist — Db::new will create it but no symbols
    let db = Db::new(&db_path).unwrap();
    let gv = db.current_graph_version().unwrap();

    let ctx = context::build_file_context(&db, &dir, "nonexistent.py", gv).unwrap();
    assert_eq!(ctx.symbol_summary, "0 symbols");
    assert!(ctx.cross_file_callers.is_empty());
    assert!(ctx.cross_file_callees.is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn context_text_format() {
    let temp = TempRepo::new("py_mvp");
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    let ctx = context::build_file_context(db, &temp.repo_root, "pkg/core.py", gv).unwrap();
    let text = context::format_text(&ctx);

    // Should start with file header
    assert!(
        text.starts_with("# pkg/core.py"),
        "Expected header, got: {}",
        text
    );
    assert!(text.contains("symbols"));
}

#[test]
fn context_text_format_empty() {
    let dir = temp_repo_dir("empty-text");
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    let db = Db::new(&db_path).unwrap();
    let gv = db.current_graph_version().unwrap();

    let ctx = context::build_file_context(&db, &dir, "nonexistent.py", gv).unwrap();
    let text = context::format_text(&ctx);
    assert!(text.is_empty(), "Expected empty text for no-symbol file");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn context_rpc_method() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "context",
        serde_json::json!({"path": "pkg/core.py"}),
    )
    .unwrap();

    // Default format is text
    assert!(
        result.get("context").is_some(),
        "Expected 'context' key in response: {:?}",
        result
    );
    let text = result["context"].as_str().unwrap();
    assert!(text.contains("pkg/core.py"));
}

#[test]
fn context_rpc_method_json_format() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let result = rpc::handle_method(
        &mut indexer,
        "context",
        serde_json::json!({"path": "pkg/core.py", "format": "json"}),
    )
    .unwrap();

    // JSON format returns the full struct
    assert!(
        result.get("path").is_some(),
        "Expected 'path' key in JSON response: {:?}",
        result
    );
    assert_eq!(result["path"].as_str().unwrap(), "pkg/core.py");
    assert!(result.get("symbol_summary").is_some());
}
