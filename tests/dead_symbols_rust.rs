use lidx::indexer::Indexer;
use lidx::rpc;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// Indexes the `dead_symbols_rust` fixture and returns the dead qualnames.
fn rust_dead_qualnames() -> Vec<String> {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("dead_symbols_rust");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    let repo_root = std::env::temp_dir().join(format!("lidx-deadrs-{nanos}-{counter}"));
    copy_dir(&src, &repo_root);
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    let result = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"include_unused_imports": false, "include_orphan_tests": false}),
    )
    .unwrap();
    let names = result["dead_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["qualname"].as_str().map(str::to_string))
        .collect();
    let _ = std::fs::remove_dir_all(&repo_root);
    names
}

#[test]
fn still_reports_truly_dead() {
    let names = rust_dead_qualnames();
    assert!(
        names.iter().any(|q| q.ends_with("truly_dead_function")),
        "{names:?}"
    );
    assert!(
        names.iter().any(|q| q.ends_with("TrulyDeadStruct")),
        "{names:?}"
    );
}

#[test]
fn excludes_test_functions() {
    let names = rust_dead_qualnames();
    assert!(
        !names.iter().any(|q| q.contains("test_default_config")),
        "{names:?}"
    );
    assert!(
        !names.iter().any(|q| q.contains("test_async_thing")),
        "{names:?}"
    );
}

#[test]
fn excludes_trait_impl_methods() {
    let names = rust_dead_qualnames();
    assert!(
        !names.iter().any(|q| q.ends_with("on_acquire")),
        "{names:?}"
    );
}

#[test]
fn counts_type_references_as_uses() {
    let names = rust_dead_qualnames();
    assert!(!names.iter().any(|q| q.ends_with("CrossRef")), "{names:?}");
    assert!(
        !names.iter().any(|q| q.ends_with("FileContext")),
        "{names:?}"
    );
}

#[test]
fn counts_fn_as_value_as_use() {
    let names = rust_dead_qualnames();
    assert!(!names.iter().any(|q| q.ends_with("from_env")), "{names:?}");
}
