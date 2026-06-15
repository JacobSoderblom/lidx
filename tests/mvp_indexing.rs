use lidx::indexer::Indexer;
use lidx::{rpc, subgraph};
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
    dir.push(format!("lidx-{label}-{nanos}-{counter}"));
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

fn setup_repo(fixture: &str) -> (PathBuf, PathBuf) {
    let src = fixture_path(fixture);
    let repo_root = temp_repo_dir(fixture);
    copy_dir(&src, &repo_root);
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    (repo_root, db_path)
}

#[test]
fn python_indexing_is_deterministic() {
    let (repo_root_a, db_path_a) = setup_repo("py_mvp");
    let mut indexer_a = Indexer::new(repo_root_a.clone(), db_path_a.clone()).unwrap();
    indexer_a.reindex().unwrap();
    let digest_a = indexer_a.db().digest().unwrap();

    let (repo_root_b, db_path_b) = setup_repo("py_mvp");
    let mut indexer_b = Indexer::new(repo_root_b.clone(), db_path_b.clone()).unwrap();
    indexer_b.reindex().unwrap();
    let digest_b = indexer_b.db().digest().unwrap();

    assert_eq!(digest_a, digest_b);

    let _ = std::fs::remove_dir_all(&repo_root_a);
    let _ = std::fs::remove_dir_all(&repo_root_b);
}

#[test]
fn rust_indexing_is_deterministic() {
    let (repo_root_a, db_path_a) = setup_repo("rust_mvp");
    let mut indexer_a = Indexer::new(repo_root_a.clone(), db_path_a.clone()).unwrap();
    indexer_a.reindex().unwrap();
    let digest_a = indexer_a.db().digest().unwrap();

    let (repo_root_b, db_path_b) = setup_repo("rust_mvp");
    let mut indexer_b = Indexer::new(repo_root_b.clone(), db_path_b.clone()).unwrap();
    indexer_b.reindex().unwrap();
    let digest_b = indexer_b.db().digest().unwrap();

    assert_eq!(digest_a, digest_b);

    let _ = std::fs::remove_dir_all(&repo_root_a);
    let _ = std::fs::remove_dir_all(&repo_root_b);
}

#[test]
fn python_import_edges_capture_evidence_snippet() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let db = indexer.db();
    let graph_version = db.current_graph_version().unwrap();

    let app = db
        .get_symbol_by_qualname("app", graph_version)
        .unwrap()
        .unwrap();
    let edges = db.edges_for_symbol(app.id, None, graph_version).unwrap();
    let import_edge = edges
        .iter()
        .find(|edge| {
            edge.kind == "IMPORTS" && edge.target_qualname.as_deref() == Some("pkg.core.Greeter")
        })
        .unwrap();
    assert_eq!(
        import_edge.evidence_snippet.as_deref(),
        Some("from pkg.core import Greeter, make_greeter")
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn rust_module_file_edges_capture_evidence_snippet() {
    let (repo_root, db_path) = setup_repo("rust_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let db = indexer.db();
    let graph_version = db.current_graph_version().unwrap();

    let root = db
        .get_symbol_by_qualname("crate", graph_version)
        .unwrap()
        .unwrap();
    let edges = db.edges_for_symbol(root.id, None, graph_version).unwrap();
    let missing_edge = edges
        .iter()
        .find(|edge| {
            edge.kind == "MODULE_FILE" && edge.target_qualname.as_deref() == Some("crate::missing")
        })
        .unwrap();
    assert_eq!(
        missing_edge.evidence_snippet.as_deref(),
        Some("mod missing;")
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn python_imports_resolve_to_files_and_subgraph_traverses() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let db = indexer.db();
    let graph_version = db.current_graph_version().unwrap();

    let module = db
        .get_symbol_by_qualname("pkg.a", graph_version)
        .unwrap()
        .unwrap();
    let edges = db.edges_for_symbol(module.id, None, graph_version).unwrap();
    let import_edge = edges
        .iter()
        .find(|edge| {
            edge.kind == "IMPORTS_FILE" && edge.target_qualname.as_deref() == Some("pkg.b")
        })
        .unwrap();
    let detail: serde_json::Value =
        serde_json::from_str(import_edge.detail.as_ref().unwrap()).unwrap();
    assert_eq!(detail["src_path"].as_str().unwrap(), "pkg/a.py");
    assert_eq!(detail["dst_path"].as_str().unwrap(), "pkg/b.py");
    assert_eq!(detail["confidence"].as_f64().unwrap(), 1.0);

    let graph = subgraph::build_subgraph(db, &[module.id], 2, 10, None, graph_version).unwrap();
    let qualnames: Vec<_> = graph.nodes.iter().map(|s| s.qualname.as_str()).collect();
    assert!(qualnames.contains(&"pkg.a"));
    assert!(qualnames.contains(&"pkg.b"));
    assert!(
        graph
            .edges
            .iter()
            .any(|edge| edge.kind == "IMPORTS_FILE" && edge.target_symbol_id.is_some())
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn python_incremental_updates_are_detected() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let digest_before = indexer.db().digest().unwrap();

    let changed = indexer.changed_files(None).unwrap();
    assert!(changed.added.is_empty());
    assert!(changed.modified.is_empty());
    assert!(changed.deleted.is_empty());

    let utils_path = repo_root.join("pkg").join("utils.py");
    let mut content = std::fs::read_to_string(&utils_path).unwrap();
    content.push_str("\n\n");
    content.push_str("def added():\n");
    content.push_str("    return 42\n");
    std::fs::write(&utils_path, content).unwrap();

    let changed = indexer.changed_files(None).unwrap();
    assert!(changed.modified.contains(&"pkg/utils.py".to_string()));

    indexer.reindex().unwrap();
    let digest_after = indexer.db().digest().unwrap();

    assert_ne!(digest_before.files, digest_after.files);
    assert_ne!(digest_before.symbols, digest_after.symbols);

    let changed = indexer.changed_files(None).unwrap();
    assert!(changed.added.is_empty());
    assert!(changed.modified.is_empty());
    assert!(changed.deleted.is_empty());

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn rpc_search_rg_returns_regex_matches() {
    if std::process::Command::new("rg")
        .arg("--version")
        .output()
        .is_err()
    {
        return;
    }
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "search".to_string(),
        r#"{"query":"def\\s+greet","limit":5}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let hits = value["result"].as_array().unwrap();
    assert!(!hits.is_empty());
    assert!(
        hits.iter()
            .any(|hit| { hit.get("path").and_then(|v| v.as_str()) == Some("pkg/core.py") })
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn rpc_top_complexity_respects_paths() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "top_complexity".to_string(),
        r#"{"limit":50,"min_complexity":1,"paths":["pkg"]}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let results = value["result"].as_array().unwrap();
    assert!(!results.is_empty());
    assert!(results.iter().all(|entry| {
        entry["symbol"]["file_path"]
            .as_str()
            .map(|path| path.starts_with("pkg/"))
            .unwrap_or(false)
    }));

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn xref_links_cross_language_strings() {
    let (repo_root, db_path) = setup_repo("poly_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let db = indexer.db();
    let graph_version = db.current_graph_version().unwrap();

    let source = db
        .get_symbol_by_qualname("Service.UserRepo.GetUser", graph_version)
        .unwrap()
        .unwrap();
    let target = db
        .get_symbol_by_qualname("dbo.get_user", graph_version)
        .unwrap()
        .unwrap();
    let edges = db.edges_for_symbol(source.id, None, graph_version).unwrap();
    assert!(
        edges
            .iter()
            .any(|edge| { edge.kind == "XREF" && edge.target_symbol_id == Some(target.id) })
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn rpc_summary_and_fields_filter_results() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "orient".to_string(),
        r#"{"view":"overview"}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();
    // orient with view=overview returns overview section
    assert!(result.contains_key("overview"));

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "reindex".to_string(),
        r#"{"summary":true}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();
    assert!(result.contains_key("scanned"));
    assert!(result.contains_key("indexed"));
    assert!(result.contains_key("skipped"));
    assert!(result.contains_key("deleted"));
    assert!(!result.contains_key("symbols"));
    assert!(!result.contains_key("edges"));
    assert!(!result.contains_key("duration_ms"));

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "reindex".to_string(),
        r#"{"fields":["indexed","deleted"]}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();
    assert!(result.contains_key("indexed"));
    assert!(result.contains_key("deleted"));
    assert_eq!(result.len(), 2);

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn rust_module_linking_and_subgraph_are_consistent() {
    let (repo_root, db_path) = setup_repo("rust_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let db = indexer.db();
    let graph_version = db.current_graph_version().unwrap();

    let root = db
        .get_symbol_by_qualname("crate", graph_version)
        .unwrap()
        .unwrap();
    let edges = db.edges_for_symbol(root.id, None, graph_version).unwrap();

    let api_edge = edges
        .iter()
        .find(|edge| {
            edge.kind == "MODULE_FILE" && edge.target_qualname.as_deref() == Some("crate::api")
        })
        .unwrap();
    let api_detail: serde_json::Value =
        serde_json::from_str(api_edge.detail.as_ref().unwrap()).unwrap();
    assert_eq!(api_detail["dst_path"].as_str().unwrap(), "src/api.rs");
    assert_eq!(api_detail["dst_name"].as_str().unwrap(), "api");
    assert_eq!(api_detail["confidence"].as_f64().unwrap(), 1.0);

    let missing_edge = edges
        .iter()
        .find(|edge| {
            edge.kind == "MODULE_FILE" && edge.target_qualname.as_deref() == Some("crate::missing")
        })
        .unwrap();
    let missing_detail: serde_json::Value =
        serde_json::from_str(missing_edge.detail.as_ref().unwrap()).unwrap();
    assert!(missing_detail["dst_path"].is_null());
    assert_eq!(missing_detail["dst_name"].as_str().unwrap(), "missing");
    assert_eq!(missing_detail["confidence"].as_f64().unwrap(), 0.4);

    let graph = subgraph::build_subgraph(db, &[root.id], 2, 10, None, graph_version).unwrap();
    let qualnames: Vec<_> = graph.nodes.iter().map(|s| s.qualname.as_str()).collect();
    assert!(qualnames.contains(&"crate"));
    assert!(qualnames.contains(&"crate::api"));
    assert!(qualnames.contains(&"crate::api::inner"));
    assert!(
        graph
            .edges
            .iter()
            .any(|edge| edge.kind == "MODULE_FILE" && edge.target_symbol_id.is_some())
    );

    let limited = subgraph::build_subgraph(db, &[root.id], 1, 2, None, graph_version).unwrap();
    let limited_names: Vec<_> = limited.nodes.iter().map(|s| s.qualname.as_str()).collect();
    assert!(limited_names.contains(&"crate"));
    assert!(limited_names.contains(&"crate::api"));
    assert!(!limited_names.contains(&"crate::missing"));

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn find_symbols_multi_word_query() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let db = indexer.db();
    let graph_version = db.current_graph_version().unwrap();

    // Multi-word query: both tokens appear in qualname "pkg.core.Greeter" and "pkg.core.Greeter.greet"
    let results = db
        .find_symbols("Greeter greet", 5, None, graph_version)
        .unwrap();
    assert!(!results.is_empty(), "multi-word query should match");
    // Both "Greeter" and "greet" must appear as substrings in the qualname
    assert!(
        results
            .iter()
            .any(|s| s.qualname.contains("Greeter") && s.qualname.contains("greet")),
        "should find symbol matching both tokens, got: {:?}",
        results.iter().map(|s| &s.qualname).collect::<Vec<_>>()
    );

    // Multi-word query: "core make_greeter" matches "pkg.core.make_greeter"
    let results = db
        .find_symbols("core make_greeter", 5, None, graph_version)
        .unwrap();
    assert!(!results.is_empty(), "multi-word query should match");
    assert!(results[0].qualname.contains("make_greeter"));

    // Multi-word query with nonexistent token returns empty
    let results = db
        .find_symbols("Greeter nonexistent", 5, None, graph_version)
        .unwrap();
    assert!(
        results.is_empty(),
        "query with nonexistent token should return empty"
    );

    // Single-word query still works
    let results = db.find_symbols("Greeter", 5, None, graph_version).unwrap();
    assert!(!results.is_empty(), "single-word query should still work");

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn explain_symbol_multi_word_query() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Multi-word query through explain_symbol — should find a symbol matching both tokens
    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "explain_symbol".to_string(),
        r#"{"query":"Greeter greet"}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let qn = value["result"]["symbol"]["qualname"].as_str().unwrap();
    assert!(
        qn.contains("Greeter"),
        "explain_symbol should resolve multi-word query, got: {}",
        qn
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn explain_symbol_bad_query_shows_suggestions() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Query that partially matches — should show "did you mean" suggestions
    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "explain_symbol".to_string(),
        r#"{"query":"Greeter nonexistent"}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let error_msg = value["error"]["message"].as_str().unwrap_or("");
    assert!(
        error_msg.contains("not found") || error_msg.contains("no symbol"),
        "bad query should return error, got: {}",
        error_msg
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

/// After an incremental rename sync, no edge should reference a symbol rowid
/// that no longer exists in the current graph version.
///
/// Scenario:
/// - helper.py defines `greet()`; caller.py calls it → edge with target_symbol_id = rowid of greet
/// - We rename `greet` → `greet_v2` in helper.py and sync incrementally
/// - Old rowid is freed; new symbol gets a new rowid
/// - Without repair, the edge in caller.py still points at the old rowid (dangling)
/// - After repair, the edge's target_symbol_id must be NULL (not dangling)
#[test]
fn incremental_rename_repairs_dangling_target_symbol_id() {
    let (repo_root, db_path) = setup_repo("py_rename");

    // Full reindex
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();

    // Verify helper.greet exists after initial index (the symbol we'll rename)
    let greet = indexer
        .db()
        .get_symbol_by_qualname("helper.greet", graph_version)
        .unwrap()
        .expect("helper.greet must exist after initial index");

    // Precondition: caller.py's edges actually resolved to greet's rowid; without
    // this the dangling/mis-pointing assertions below would pass vacuously.
    {
        let conn = indexer.db().read_conn().unwrap();
        let resolved_to_greet: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE target_symbol_id = ? AND graph_version = ?",
                rusqlite::params![greet.id, graph_version],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            resolved_to_greet > 0,
            "Precondition: at least one edge must resolve to helper.greet's rowid"
        );
    }

    // Rename greet → greet_v2 in helper.py; caller.py is unchanged (still calls greet)
    let helper_path = repo_root.join("helper.py");
    std::fs::write(
        &helper_path,
        "def greet_v2(name: str) -> str:\n    return f\"Hello, {name}\"\n",
    )
    .unwrap();
    // Canonicalize to resolve symlinks (e.g. /tmp → /private/tmp on macOS) so that
    // normalize_rel_path can strip the repo_root prefix correctly.
    let helper_path = std::fs::canonicalize(&helper_path).unwrap();

    // Incremental sync of just helper.py
    indexer.sync_abs_paths(&[helper_path]).unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();

    // After the sync, no edge should have a non-NULL source_symbol_id or target_symbol_id
    // that points at a rowid absent from the symbols table. If the old greet rowid was
    // reused (SQLite reuses freed rowids without AUTOINCREMENT), edges with the old rowid
    // now point at greet_v2, which is also wrong — but that case is tested differently.
    // Here we check the core invariant: all non-NULL symbol ids in edges are valid.
    let conn = indexer.db().read_conn().unwrap();
    let dangling_source_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges
             WHERE source_symbol_id IS NOT NULL
               AND graph_version = ?
               AND source_symbol_id NOT IN (SELECT id FROM symbols WHERE graph_version = ?)",
            rusqlite::params![graph_version, graph_version],
            |row| row.get(0),
        )
        .unwrap();
    let dangling_target_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges
             WHERE target_symbol_id IS NOT NULL
               AND graph_version = ?
               AND target_symbol_id NOT IN (SELECT id FROM symbols WHERE graph_version = ?)",
            rusqlite::params![graph_version, graph_version],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(
        dangling_source_count, 0,
        "No edge should have a source_symbol_id pointing at a non-existent symbol rowid"
    );
    assert_eq!(
        dangling_target_count, 0,
        "No edge should have a target_symbol_id pointing at a non-existent symbol rowid"
    );

    // The old greet rowid must not appear as a dangling pointer in edges from caller.py
    // (which originally called greet). If rowid was reused for greet_v2, those edges
    // should have been NULLed during repair (not silently re-pointed at greet_v2).
    let mispointed_greet_edges: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges
             WHERE target_qualname IN ('helper.greet', 'caller.greet', 'greet')
               AND target_symbol_id IS NOT NULL
               AND graph_version = ?",
            rusqlite::params![graph_version],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        mispointed_greet_edges, 0,
        "Edges targeting 'helper.greet' must have target_symbol_id = NULL after rename (greet no longer exists)"
    );

    // The new symbol greet_v2 must exist; old greet must be gone
    let new_symbol = indexer
        .db()
        .get_symbol_by_qualname("helper.greet_v2", graph_version)
        .unwrap();
    assert!(
        new_symbol.is_some(),
        "helper.greet_v2 must exist after rename sync"
    );
    let old_symbol = indexer
        .db()
        .get_symbol_by_qualname("helper.greet", graph_version)
        .unwrap();
    assert!(
        old_symbol.is_none(),
        "helper.greet must not exist after rename sync"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

/// After both the renamed file and its caller are synced incrementally,
/// null-target resolution should re-resolve the edge to the new symbol.
#[test]
fn incremental_rename_re_resolves_edges_to_new_symbol() {
    let (repo_root, db_path) = setup_repo("py_rename");

    // Full reindex
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Rename greet → greet_v2 AND update caller to use greet_v2
    let helper_path = repo_root.join("helper.py");
    let caller_path = repo_root.join("caller.py");
    std::fs::write(
        &helper_path,
        "def greet_v2(name: str) -> str:\n    return f\"Hello, {name}\"\n",
    )
    .unwrap();
    std::fs::write(
        &caller_path,
        "from helper import greet_v2\n\ndef run():\n    return greet_v2(\"world\")\n",
    )
    .unwrap();
    // Canonicalize to resolve symlinks (e.g. /tmp → /private/tmp on macOS)
    let helper_path = std::fs::canonicalize(&helper_path).unwrap();
    let caller_path = std::fs::canonicalize(&caller_path).unwrap();

    // Incremental sync of both files
    indexer.sync_abs_paths(&[helper_path, caller_path]).unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();

    // Edge from caller.run → helper.greet_v2 should exist and be fully resolved
    let new_symbol = indexer
        .db()
        .get_symbol_by_qualname("helper.greet_v2", graph_version)
        .unwrap()
        .expect("helper.greet_v2 must exist");

    let edges = indexer
        .db()
        .edges_for_symbol(new_symbol.id, None, graph_version)
        .unwrap();

    // There should be at least one CALLS edge with target resolved to greet_v2's rowid
    let resolved_calls: Vec<_> = edges
        .iter()
        .filter(|e| e.kind == "CALLS" && e.target_symbol_id == Some(new_symbol.id))
        .collect();

    assert!(
        !resolved_calls.is_empty(),
        "After rename+caller update, there should be a CALLS edge resolved to helper.greet_v2"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

/// Deleting a file in an incremental sync frees its symbol rowids. If another
/// file indexed in the same sync reuses a freed rowid (SQLite reuses rowids
/// without AUTOINCREMENT), edges in unchanged files must not silently re-point
/// at the new, unrelated symbol — they must be NULLed instead.
#[test]
fn incremental_file_delete_does_not_repoint_edges_at_reused_rowids() {
    let (repo_root, db_path) = setup_repo("py_rename");
    // Canonicalize the root so joined paths match the Indexer's canonicalized
    // repo_root even for files that no longer exist on disk.
    let repo_root = std::fs::canonicalize(&repo_root).unwrap();

    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();

    // Precondition: caller.py's edges actually resolved to helper.py's symbols,
    // otherwise this test would pass vacuously.
    let greet = indexer
        .db()
        .get_symbol_by_qualname("helper.greet", graph_version)
        .unwrap()
        .expect("helper.greet must exist after initial index");
    {
        let conn = indexer.db().read_conn().unwrap();
        let resolved_to_greet: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE target_symbol_id = ? AND graph_version = ?",
                rusqlite::params![greet.id, graph_version],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            resolved_to_greet > 0,
            "Precondition: at least one edge must resolve to helper.greet's rowid"
        );
    }

    // Delete helper.py and add zebra.py in the SAME sync batch. zebra's new
    // symbols are prime candidates to reuse helper's freed rowids.
    let helper_path = repo_root.join("helper.py");
    std::fs::remove_file(&helper_path).unwrap();
    let zebra_path = repo_root.join("zebra.py");
    std::fs::write(
        &zebra_path,
        "def zulu(name: str) -> str:\n    return name\n",
    )
    .unwrap();

    indexer.sync_abs_paths(&[helper_path, zebra_path]).unwrap();

    let graph_version = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();

    // No edge that still names a helper symbol may carry a resolved id: helper's
    // symbols are gone, so a non-NULL id either dangles or points at a reused
    // rowid belonging to zebra.
    let mispointed: Vec<(String, String)> = {
        let mut stmt = conn
            .prepare(
                "SELECT e.target_qualname, s.qualname
                 FROM edges e JOIN symbols s ON e.target_symbol_id = s.id
                 WHERE e.target_qualname IN ('helper.greet', 'caller.greet', 'greet', 'helper')
                   AND e.graph_version = ?",
            )
            .unwrap();
        let rows = stmt
            .query_map(rusqlite::params![graph_version], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();
        rows.collect::<Result<Vec<_>, _>>().unwrap()
    };
    assert!(
        mispointed.is_empty(),
        "Edges naming deleted helper symbols must have NULL target_symbol_id, \
         but some resolve to other symbols (target_qualname -> actual symbol): {mispointed:?}"
    );

    // And the general invariant: no dangling ids either.
    for column in ["source_symbol_id", "target_symbol_id"] {
        let dangling: i64 = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM edges
                     WHERE {column} IS NOT NULL
                       AND graph_version = ?
                       AND {column} NOT IN (SELECT id FROM symbols WHERE graph_version = ?)"
                ),
                rusqlite::params![graph_version, graph_version],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            dangling, 0,
            "No edge {column} may dangle after file delete sync"
        );
    }

    let _ = std::fs::remove_dir_all(&repo_root);
}
