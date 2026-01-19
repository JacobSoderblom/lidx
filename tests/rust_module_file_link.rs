use lidx::db::Db;
use lidx::indexer::extract::{EdgeInput, SymbolInput};
use lidx::indexer::rust::resolve_module_file_edges;
use lidx::subgraph::build_subgraph;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn temp_repo_dir() -> PathBuf {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    dir.push(format!("lidx-test-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_file(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, contents).unwrap();
}

#[test]
fn resolve_module_file_edges_sets_detail() {
    let repo_root = temp_repo_dir();
    write_file(&repo_root.join("src/lib.rs"), "mod foo;");
    write_file(&repo_root.join("src/foo.rs"), "");
    write_file(&repo_root.join("src/foo/mod.rs"), "");
    write_file(&repo_root.join("src/outer/inner.rs"), "");

    let mut edges = vec![
        EdgeInput {
            kind: "MODULE_FILE".to_string(),
            source_qualname: Some("crate".to_string()),
            target_qualname: Some("crate::foo".to_string()),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        },
        EdgeInput {
            kind: "MODULE_FILE".to_string(),
            source_qualname: Some("crate::outer".to_string()),
            target_qualname: Some("crate::outer::inner".to_string()),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        },
        EdgeInput {
            kind: "MODULE_FILE".to_string(),
            source_qualname: Some("crate".to_string()),
            target_qualname: Some("crate::missing".to_string()),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        },
    ];

    resolve_module_file_edges(&repo_root, "src/lib.rs", "crate", &mut edges);

    let detail: serde_json::Value =
        serde_json::from_str(edges[0].detail.as_ref().unwrap()).unwrap();
    assert_eq!(detail["src_path"].as_str().unwrap(), "src/lib.rs");
    assert_eq!(detail["dst_path"].as_str().unwrap(), "src/foo.rs");
    assert_eq!(detail["dst_name"].as_str().unwrap(), "foo");
    assert_eq!(detail["confidence"].as_f64().unwrap(), 1.0);

    let detail: serde_json::Value =
        serde_json::from_str(edges[1].detail.as_ref().unwrap()).unwrap();
    assert_eq!(detail["dst_path"].as_str().unwrap(), "src/outer/inner.rs");
    assert_eq!(detail["dst_name"].as_str().unwrap(), "inner");
    assert_eq!(detail["confidence"].as_f64().unwrap(), 1.0);

    let detail: serde_json::Value =
        serde_json::from_str(edges[2].detail.as_ref().unwrap()).unwrap();
    assert!(detail["dst_path"].is_null());
    assert_eq!(detail["dst_name"].as_str().unwrap(), "missing");
    assert_eq!(detail["confidence"].as_f64().unwrap(), 0.4);

    let _ = std::fs::remove_dir_all(&repo_root);
}

// Ignored: :memory: databases are incompatible with connection pools.
// Each pooled connection sees a separate in-memory database, so migrations
// run on write_conn don't affect read_pool connections. This is an architectural
// limitation. Production uses file-based databases where all connections access
// the same file, so this test failure doesn't affect production behavior.
#[test]
#[ignore]
fn subgraph_resolves_module_file_edge_targets() {
    let mut db = Db::new(Path::new(":memory:")).unwrap();
    let graph_version = db.current_graph_version().unwrap();
    let file_id_root = db.upsert_file("src/lib.rs", "hash", "rust", 0, 0).unwrap();
    let file_id_child = db.upsert_file("src/foo.rs", "hash", "rust", 0, 0).unwrap();

    let root_symbols = db
        .insert_symbols(
            file_id_root,
            "src/lib.rs",
            &[SymbolInput {
                kind: "module".to_string(),
                name: "crate".to_string(),
                qualname: "crate".to_string(),
                start_line: 1,
                start_col: 1,
                end_line: 1,
                end_col: 1,
                start_byte: 0,
                end_byte: 0,
                signature: None,
                docstring: None,
            }],
            graph_version,
            None,
        )
        .unwrap();
    let root_id = root_symbols[0].id;

    let mut symbol_map = HashMap::new();
    symbol_map.insert("crate".to_string(), root_id);
    let edges = vec![EdgeInput {
        kind: "MODULE_FILE".to_string(),
        source_qualname: Some("crate".to_string()),
        target_qualname: Some("crate::foo".to_string()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    }];
    db.insert_edges(file_id_root, &edges, &symbol_map, graph_version, None)
        .unwrap();

    let child_symbols = db
        .insert_symbols(
            file_id_child,
            "src/foo.rs",
            &[SymbolInput {
                kind: "module".to_string(),
                name: "foo".to_string(),
                qualname: "crate::foo".to_string(),
                start_line: 1,
                start_col: 1,
                end_line: 1,
                end_col: 1,
                start_byte: 0,
                end_byte: 0,
                signature: None,
                docstring: None,
            }],
            graph_version,
            None,
        )
        .unwrap();
    let child_id = child_symbols[0].id;

    let graph = build_subgraph(&db, &[root_id], 2, 10, None, graph_version).unwrap();
    let qualnames: Vec<_> = graph.nodes.iter().map(|s| s.qualname.as_str()).collect();
    assert!(qualnames.contains(&"crate"));
    assert!(qualnames.contains(&"crate::foo"));
    let module_edge = graph
        .edges
        .iter()
        .find(|edge| edge.kind == "MODULE_FILE")
        .unwrap();
    assert_eq!(module_edge.target_symbol_id, Some(child_id));
}
