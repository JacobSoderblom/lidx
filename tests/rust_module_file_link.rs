use lidx::indexer::extract::EdgeInput;
use lidx::indexer::rust::resolve_module_file_edges;
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
