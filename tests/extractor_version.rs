//! Issue #109: extractor changes must reach existing indexes. The version
//! stored in `meta` forces re-extraction on mismatch, and a source hash guard
//! makes forgetting to bump `EXTRACTOR_VERSION` a test failure.

mod common;

use lidx::indexer::{EXTRACTOR_VERSION, Indexer};
use std::path::Path;

/// Recorded alongside `EXTRACTOR_VERSION`; update both together.
const RECORDED_HASH: &str = "6a87515b96bda968aa54c2e8ae2042b190ed37671b3ce83edf91faf0a70f9587";

fn collect(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn extractor_sources_match_recorded_hash() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/indexer");
    let mut files = Vec::new();
    collect(&root, &mut files);
    files.sort();
    let mut hasher = blake3::Hasher::new();
    for f in &files {
        hasher.update(f.strip_prefix(&root).unwrap().to_string_lossy().as_bytes());
        // Normalize line endings so the hash is stable across platforms.
        let text = std::fs::read_to_string(f).unwrap().replace("\r\n", "\n");
        hasher.update(text.as_bytes());
    }
    let actual = hasher.finalize().to_hex().to_string();
    assert_eq!(
        actual, RECORDED_HASH,
        "src/indexer/** changed. Bump EXTRACTOR_VERSION (currently {EXTRACTOR_VERSION}) in \
         src/indexer/mod.rs if extractor output changed, then set RECORDED_HASH in \
         tests/extractor_version.rs to {actual}"
    );
}

#[test]
fn stale_extractor_version_forces_reextraction() {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-extractor-version-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, &[("a.py", "def foo():\n    pass\n")]);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root, db_path).unwrap();

    indexer.reindex().unwrap();
    let stats = indexer.reindex().unwrap();
    assert_eq!(
        (stats.indexed, stats.skipped),
        (0, 1),
        "unchanged is skipped"
    );

    indexer.db().set_meta_i64("extractor_version", 0).unwrap();
    let stats = indexer.reindex().unwrap();
    assert_eq!(
        (stats.indexed, stats.skipped),
        (1, 0),
        "stale version re-extracts"
    );
    assert_eq!(
        indexer.db().get_meta_i64("extractor_version").unwrap(),
        Some(EXTRACTOR_VERSION)
    );

    let stats = indexer.reindex().unwrap();
    assert_eq!(
        (stats.indexed, stats.skipped),
        (0, 1),
        "version now current"
    );
}
