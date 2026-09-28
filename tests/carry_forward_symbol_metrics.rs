use lidx::indexer::Indexer;
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

fn metrics_count_at_version(db: &lidx::db::Db, graph_version: i64) -> i64 {
    db.read_conn()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM symbol_metrics m
             JOIN symbols s ON s.id = m.symbol_id
             WHERE s.graph_version = ?",
            rusqlite::params![graph_version],
            |row| row.get(0),
        )
        .unwrap()
}

/// Regression test for the carry-forward metrics-loss bug: `carry_forward_files`
/// copies an unchanged file's `symbols`/`edges` into the new graph version with
/// fresh symbol ids, but `symbol_metrics` is keyed by `symbol_id` with
/// `ON DELETE CASCADE`. Left uncopied, a warm reindex (every file carried
/// forward unchanged) leaves the *current* graph version with zero metrics --
/// `top_complexity`, `dead_symbols`, and every other metrics-backed query go
/// empty on a repo that plainly has complexity to report.
#[test]
fn warm_reindex_carries_symbol_metrics_to_new_version() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();

    // Cold reindex: every file is freshly parsed, so `insert_symbol_metrics`
    // attaches metrics to this version's symbol ids directly (the path that
    // already worked before the carry-forward feature existed).
    indexer.reindex().unwrap();
    let first_version = indexer.graph_version();

    // Warm reindex: no file content changed, so every file takes the
    // carry-forward path instead of being re-parsed.
    indexer.reindex().unwrap();
    let second_version = indexer.graph_version();

    assert!(
        second_version > first_version,
        "reindex must always advance the graph version, even with no file changes"
    );

    let db = indexer.db();

    let first_version_metrics = metrics_count_at_version(db, first_version);
    assert!(
        first_version_metrics > 0,
        "sanity: the cold-indexed version must have metrics (fixture has functions to measure)"
    );

    // This is the assertion that fails without the fix: it targets the NEW
    // (current) version specifically, not just "metrics exist somewhere in the
    // database" -- carried-forward symbols get new ids in `second_version`, and
    // uncopied `symbol_metrics` rows stay pinned to `first_version`'s now-stale
    // symbol ids, which `prune_old_graph_versions` will eventually delete outright.
    let second_version_metrics = metrics_count_at_version(db, second_version);
    assert!(
        second_version_metrics > 0,
        "carried-forward files' symbols in the new graph version ({second_version}) must \
         have symbol_metrics attached, not just the old version ({first_version}); found \
         {first_version_metrics} metric row(s) at the old version and {second_version_metrics} \
         at the new one"
    );
    assert_eq!(
        second_version_metrics, first_version_metrics,
        "carry-forward must preserve every metric row, not just make the count non-zero"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}
