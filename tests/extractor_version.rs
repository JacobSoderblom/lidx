//! Issue #109: extractor changes must reach existing indexes. The version
//! stored in `meta` forces re-extraction on mismatch, and a source hash guard
//! makes forgetting to bump `EXTRACTOR_VERSION` a test failure.

mod common;

use lidx::indexer::{EXTRACTOR_VERSION, Indexer};
use std::path::Path;

/// Recorded alongside `EXTRACTOR_VERSION`; update both together.
const RECORDED_HASH: &str = "12cfc9b54ba3f0bbf8b8c4ea46ffcb32ecc09c8df3bd65bc551ab751015de587";

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
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let root = manifest.clone();
    let mut files = Vec::new();
    collect(&manifest.join("indexer"), &mut files);
    files.push(manifest.join("db/resolver.rs"));
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
        "src/indexer/** or src/db/resolver.rs changed. Bump EXTRACTOR_VERSION (currently {EXTRACTOR_VERSION}) in \
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

    // Seed a bogus edge; a forced re-extraction must replace the file's edges.
    let file = indexer.db().get_file_by_path("a.py").unwrap().unwrap();
    let bogus = |gv: i64| {
        indexer
            .db()
            .read_conn()
            .unwrap()
            .execute(
                "INSERT INTO edges (file_id, kind, target_qualname, graph_version) \
                 VALUES (?, 'CALLS', 'bogus.stale', ?)",
                rusqlite::params![file.id, gv],
            )
            .unwrap();
    };
    bogus(indexer.graph_version());
    let count = |ix: &Indexer| -> i64 {
        ix.db()
            .read_conn()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE target_qualname = 'bogus.stale' AND graph_version = ?",
                [ix.graph_version()],
                |r| r.get(0),
            )
            .unwrap()
    };
    assert_eq!(count(&indexer), 1);
    indexer.db().set_meta_i64("extractor_version", 0).unwrap();
    assert!(indexer.extractor_version_stale().unwrap());
    let stats = indexer.reindex().unwrap();
    assert_eq!(
        (stats.indexed, stats.skipped),
        (1, 0),
        "stale version re-extracts"
    );
    assert_eq!(count(&indexer), 0, "stale edge replaced");
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
