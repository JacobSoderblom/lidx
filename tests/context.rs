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

#[test]
fn context_rpc_method_graph_version_aliases() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();

    let baseline = rpc::handle_method(
        &mut indexer,
        "context",
        serde_json::json!({"path": "pkg/core.py"}),
    )
    .unwrap();

    // "as_of" and "version" are accepted aliases for "graph_version". Serde
    // silently drops unknown keys, so equality with the baseline alone would
    // not prove an alias is wired — also query a version with no symbols and
    // require the output to change, which shows the value was consumed.
    for alias in ["graph_version", "as_of", "version"] {
        let result = rpc::handle_method(
            &mut indexer,
            "context",
            serde_json::json!({"path": "pkg/core.py", alias: gv}),
        )
        .unwrap();
        assert_eq!(
            result, baseline,
            "context with {alias}={gv} should match the default-version output"
        );

        let empty = rpc::handle_method(
            &mut indexer,
            "context",
            serde_json::json!({"path": "pkg/core.py", alias: gv + 1}),
        )
        .unwrap();
        assert_ne!(
            empty,
            baseline,
            "context with {alias}={} (no symbols) should differ from the current-version output",
            gv + 1
        );
    }
}

#[test]
fn context_rpc_method_missing_path_errors() {
    let temp = TempRepo::new("py_mvp");
    let mut indexer = Indexer::new(temp.repo_root.clone(), temp.db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let err = rpc::handle_method(&mut indexer, "context", serde_json::json!({})).unwrap_err();
    assert!(
        err.to_string().contains("path"),
        "missing-path error should mention 'path', got: {err}"
    );
}

#[test]
fn context_caller_names_are_qualnames_not_ids() {
    let temp = TempRepo::new("py_mvp");
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    // pkg/b.py defines helper() which is called by pkg/a.py
    // pkg/a.py's call() function is the caller
    let ctx = context::build_file_context(db, &temp.repo_root, "pkg/b.py", gv).unwrap();

    // Verify callers are not printed as id:N
    for caller in &ctx.cross_file_callers {
        assert!(
            !caller.symbol_name.starts_with("id:"),
            "Caller symbol_name should not be an id, got: {}",
            caller.symbol_name
        );
    }

    // Verify the caller has a qualname (should contain a function name like call or pkg.a.call)
    if !ctx.cross_file_callers.is_empty() {
        let caller_name = &ctx.cross_file_callers[0].symbol_name;
        assert!(
            !caller_name.is_empty() && caller_name != "?" && !caller_name.starts_with("id:"),
            "First caller should have a real qualname, got: {}",
            caller_name
        );
    }
}

#[test]
fn context_callee_names_match_resolved_symbols() {
    let temp = TempRepo::new("py_mvp");
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    // app.py calls make_greeter() and greet() from pkg/core.py
    let ctx = context::build_file_context(db, &temp.repo_root, "app.py", gv).unwrap();

    // Verify callees are not just the raw guess "?"
    for callee in &ctx.cross_file_callees {
        assert!(
            callee.symbol_name != "?",
            "Callee should have a resolved qualname, got '?'"
        );
        // The qualname should be something like "make_greeter" or "Greeter.greet" or the full module path
        assert!(
            !callee.symbol_name.is_empty(),
            "Callee symbol_name should not be empty"
        );
    }
}

#[test]
fn context_qualnames_resolve_through_db_query() {
    let temp = TempRepo::new("py_mvp");
    let indexer = temp.index();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();

    // Get context for a file with cross-file references
    let ctx = context::build_file_context(db, &temp.repo_root, "pkg/b.py", gv).unwrap();

    // Verify all caller names can be resolved
    for caller in &ctx.cross_file_callers {
        let symbol_result = db.get_symbol_by_qualname(&caller.symbol_name, gv);
        assert!(
            symbol_result.is_ok() && symbol_result.unwrap().is_some(),
            "Caller qualname should resolve: {}",
            caller.symbol_name
        );
    }

    // Verify all callee names can be resolved
    let ctx2 = context::build_file_context(db, &temp.repo_root, "app.py", gv).unwrap();
    for callee in &ctx2.cross_file_callees {
        let symbol_result = db.get_symbol_by_qualname(&callee.symbol_name, gv);
        assert!(
            symbol_result.is_ok() && symbol_result.unwrap().is_some(),
            "Callee qualname should resolve: {}",
            callee.symbol_name
        );
    }
}
