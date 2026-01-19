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
fn python_open_symbol_returns_exact_text() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let db = indexer.db();
    let graph_version = db.current_graph_version().unwrap();
    let symbol = db
        .get_symbol_by_qualname("pkg.core.Greeter.greet", graph_version)
        .unwrap()
        .unwrap();
    assert_eq!(symbol.kind, "method");
    assert_eq!(symbol.docstring.as_deref(), Some("Greets someone."));
    assert_eq!(
        symbol.signature.as_deref(),
        Some("(self, name: str) -> str")
    );

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "open_symbol".to_string(),
        r#"{"qualname":"pkg.core.Greeter.greet"}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let snippet = value["result"]["snippet"].as_str().unwrap();

    let content = std::fs::read_to_string(repo_root.join("pkg/core.py")).unwrap();
    let expected = content
        .get(symbol.start_byte as usize..symbol.end_byte as usize)
        .unwrap();

    assert_eq!(snippet, expected);
    assert!(snippet.starts_with("def greet"));
    assert!(snippet.contains("return f\"Hi {name}\""));

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn python_open_symbol_respects_limits() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "open_symbol".to_string(),
        r#"{"qualname":"pkg.core.Greeter.greet","include_snippet":false}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    assert!(value["result"].get("snippet").is_none());

    let max_bytes = 16usize;
    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "open_symbol".to_string(),
        &format!("{{\"qualname\":\"pkg.core.Greeter.greet\",\"max_snippet_bytes\":{max_bytes}}}"),
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let snippet = value["result"]["snippet"].as_str().unwrap();
    assert!(snippet.len() <= max_bytes);

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "open_symbol".to_string(),
        r#"{"qualname":"pkg.core.Greeter.greet","include_symbol":false}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    assert!(value["result"].get("symbol").is_none());
    assert!(value["result"]["snippet"].as_str().is_some());

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "open_symbol".to_string(),
        r#"{"qualname":"pkg.core.Greeter.greet","snippet_only":true}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    assert!(value["result"].get("symbol").is_none());
    assert!(value["result"]["snippet"].as_str().is_some());

    let db = indexer.db();
    let graph_version = db.current_graph_version().unwrap();
    let symbol = db
        .get_symbol_by_qualname("pkg.core.Greeter.greet", graph_version)
        .unwrap()
        .unwrap();
    let content = std::fs::read_to_string(repo_root.join("pkg/core.py")).unwrap();
    let full_snippet = content
        .get(symbol.start_byte as usize..symbol.end_byte as usize)
        .unwrap();
    assert!(full_snippet.starts_with(snippet));

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn rpc_open_file_reads_ranges_and_limits() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "open_file".to_string(),
        r#"{"path":"pkg/core.py","start_line":1,"end_line":4}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();
    assert_eq!(result["path"].as_str().unwrap(), "pkg/core.py");
    let text = result["text"].as_str().unwrap();
    assert!(text.contains("Core module doc."));
    assert!(text.contains("from . import utils"));
    assert!(!text.contains("class Greeter"));

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "open_file".to_string(),
        r#"{"path":"pkg/core.py","max_bytes":12}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let text = value["result"]["text"].as_str().unwrap();
    assert!(text.len() <= 12);

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn rpc_open_file_reports_missing_path() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "open_file".to_string(),
        r#"{"path":"missing.py"}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let message = value["error"]["message"].as_str().unwrap();
    assert_eq!(message, "open_file path not found: missing.py");

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn rpc_query_aliases_and_path_filters_work() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "find_symbol".to_string(),
        r#"{"symbol":"Greeter","limit":5}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let results = value["result"].as_array().unwrap();
    assert!(!results.is_empty());

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "grep".to_string(),
        r#"{"pattern":"Greeter","path":"pkg/core.py","limit":10}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let hits = value["result"]["results"].as_array().unwrap();
    assert!(!hits.is_empty());
    assert!(
        hits.iter()
            .all(|hit| hit.get("path").and_then(|v| v.as_str()) == Some("pkg/core.py"))
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn rpc_references_returns_call_edges() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "references".to_string(),
        r#"{"qualname":"pkg.core.make_greeter","direction":"out"}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();
    assert_eq!(
        result["symbol"]["qualname"].as_str().unwrap(),
        "pkg.core.make_greeter"
    );

    let outgoing = result["outgoing"].as_array().unwrap();
    assert!(!outgoing.is_empty());
    let edge = outgoing.iter().find(|edge| {
        edge["edge"]["kind"].as_str() == Some("CALLS")
            && edge["edge"]["target_qualname"].as_str() == Some("pkg.core.Greeter")
    });
    assert!(edge.is_some());
    let target = edge.unwrap()["target"]["qualname"].as_str();
    assert_eq!(target, Some("pkg.core.Greeter"));

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
        "search_rg".to_string(),
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
fn rpc_list_xrefs_includes_line_numbers() {
    let (repo_root, db_path) = setup_repo("poly_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "list_xrefs".to_string(),
        r#"{"paths":["Service.cs"],"min_confidence":0.7,"limit":20}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let edges = value["result"].as_array().unwrap();
    assert!(!edges.is_empty());
    let entry = edges.iter().find(|entry| {
        entry["edge"]["kind"].as_str() == Some("XREF")
            && entry["edge"]["target_qualname"].as_str() == Some("dbo.get_user")
    });
    let entry = entry.unwrap();
    assert_eq!(entry["edge"]["evidence_start_line"].as_i64(), Some(5));
    assert_eq!(entry["edge"]["evidence_end_line"].as_i64(), Some(5));
    assert!(entry["edge"]["confidence"].as_f64().unwrap_or(0.0) >= 0.7);

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn rpc_subgraph_accepts_roots_and_kind_filters() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "subgraph".to_string(),
        r#"{"roots":["pkg.a.call"],"kinds":["CALLS"],"depth":2,"max_nodes":5}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let nodes = value["result"]["nodes"].as_array().unwrap();
    assert!(
        nodes
            .iter()
            .any(|node| node["qualname"].as_str() == Some("pkg.a.call"))
    );
    let edges = value["result"]["edges"].as_array().unwrap();
    assert!(!edges.is_empty());
    assert!(
        edges
            .iter()
            .all(|edge| edge["kind"].as_str() == Some("CALLS"))
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
        "repo_overview".to_string(),
        r#"{"summary":true}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();
    assert!(result.contains_key("files"));
    assert!(result.contains_key("symbols"));
    assert!(result.contains_key("edges"));
    assert!(!result.contains_key("repo_root"));
    assert!(!result.contains_key("last_indexed"));

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "repo_overview".to_string(),
        r#"{"fields":["files"]}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let result = value["result"].as_object().unwrap();
    assert!(result.contains_key("files"));
    assert_eq!(result.len(), 1);

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
fn rpc_list_graph_versions_returns_current() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let current = indexer.db().current_graph_version().unwrap();

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "list_graph_versions".to_string(),
        r#"{"limit":5}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let versions = value["result"].as_array().unwrap();
    assert!(!versions.is_empty());
    assert!(
        versions
            .iter()
            .any(|version| version["id"].as_i64() == Some(current))
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn rpc_grep_returns_compact_hits() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "grep".to_string(),
        r#"{"query":"Greeter","limit":5}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let hits = value["result"]["results"].as_array().unwrap();
    assert!(!hits.is_empty());
    let first = hits[0].as_object().unwrap();
    assert!(first.contains_key("path"));
    assert!(first.contains_key("line"));
    assert!(first.contains_key("column"));
    assert!(!first.contains_key("line_text"));

    let response = rpc::call(
        repo_root.clone(),
        db_path.clone(),
        "grep".to_string(),
        r#"{"query":"Greeter","limit":1,"include_text":true}"#,
        "1",
    )
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&response).unwrap();
    let hits = value["result"]["results"].as_array().unwrap();
    let first = hits[0].as_object().unwrap();
    assert!(first.get("line_text").and_then(|v| v.as_str()).is_some());

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
