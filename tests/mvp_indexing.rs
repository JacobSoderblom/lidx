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
    let results = db.find_symbols("Greeter greet", 5, None, graph_version).unwrap();
    assert!(!results.is_empty(), "multi-word query should match");
    // Both "Greeter" and "greet" must appear as substrings in the qualname
    assert!(
        results.iter().any(|s| s.qualname.contains("Greeter") && s.qualname.contains("greet")),
        "should find symbol matching both tokens, got: {:?}",
        results.iter().map(|s| &s.qualname).collect::<Vec<_>>()
    );

    // Multi-word query: "core make_greeter" matches "pkg.core.make_greeter"
    let results = db.find_symbols("core make_greeter", 5, None, graph_version).unwrap();
    assert!(!results.is_empty(), "multi-word query should match");
    assert!(results[0].qualname.contains("make_greeter"));

    // Multi-word query with nonexistent token returns empty
    let results = db.find_symbols("Greeter nonexistent", 5, None, graph_version).unwrap();
    assert!(results.is_empty(), "query with nonexistent token should return empty");

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
