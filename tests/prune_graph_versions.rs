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

fn symbol_count_at_version(db: &lidx::db::Db, graph_version: i64) -> i64 {
    db.read_conn()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM symbols WHERE graph_version = ?",
            rusqlite::params![graph_version],
            |row| row.get(0),
        )
        .unwrap()
}

/// Repeated `reindex()` with no file changes still creates a new graph version
/// every time (matching a real repo that's reindexed on every commit) and, per
/// perf/carry-forward-unchanged-files, carries every unchanged file's symbols
/// and edges forward into it -- duplicating the full symbol/edge set each run.
/// That's exactly the unbounded-growth bug this test guards against: the
/// automatic prune wired into `reindex()` must keep history bounded without
/// ever changing what a query against the current version returns, and without
/// destroying the history its own retention window promises to keep.
#[test]
fn reindex_auto_prune_bounds_history_without_disturbing_current_version() {
    let (repo_root, db_path) = setup_repo("py_mvp");
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();

    // 5 reindexes, no file edits in between: every run's carry-forward path
    // copies every file's rows into a fresh graph version. Each of these
    // reindex() calls also runs the automatic prune at its own tail end.
    for _ in 0..5 {
        indexer.reindex().unwrap();
    }

    let db = indexer.db();

    let versions_with_data: Vec<i64> = {
        let conn = db.read_conn().unwrap();
        let mut stmt = conn
            .prepare("SELECT DISTINCT graph_version FROM symbols ORDER BY graph_version DESC")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };

    // Automatic pruning already ran after each of the 5 reindexes above
    // (default retention: lidx::db::DEFAULT_GRAPH_VERSION_RETENTION), so the
    // 5 reindexes must not have left 5 (or even 4) versions' worth of
    // duplicated symbol rows sitting in the database.
    assert!(
        versions_with_data.len() as i64 <= lidx::db::DEFAULT_GRAPH_VERSION_RETENTION,
        "reindex's automatic prune should bound history to {} version(s), found data in {:?}",
        lidx::db::DEFAULT_GRAPH_VERSION_RETENTION,
        versions_with_data
    );
    assert!(
        versions_with_data.len() >= 2,
        "fixture must have produced more than one version of history to prune: {versions_with_data:?}"
    );

    let current = db.current_graph_version().unwrap();
    assert_eq!(
        versions_with_data[0], current,
        "the current version must always have symbol rows"
    );

    let digest_before = db.digest().unwrap();

    // Tighten retention further (as if the user re-ran maintenance with a
    // stricter window) and check the three things a prune must guarantee.
    let (_symbols_deleted, _edges_deleted, versions_pruned) =
        db.prune_old_graph_versions(2).unwrap();
    assert!(
        versions_pruned > 0,
        "at least one version older than the 2 being kept must be prunable"
    );

    // 1. Current-version query results are unchanged by pruning older versions.
    let digest_after = db.digest().unwrap();
    assert_eq!(
        digest_before, digest_after,
        "pruning older graph versions must not change current-version query results"
    );

    // 2. The history retention promises to keep (here: the version right
    // before current) is still readable.
    let kept_history_version = versions_with_data[1];
    assert!(
        symbol_count_at_version(db, kept_history_version) > 0,
        "version {kept_history_version} is within retention=2 and must still have symbol rows"
    );

    // 3. History older than the retention window is actually gone.
    let pruned_version = versions_with_data[versions_with_data.len() - 1];
    assert!(
        pruned_version < kept_history_version,
        "sanity: oldest observed version must be older than the kept one"
    );
    assert_eq!(
        symbol_count_at_version(db, pruned_version),
        0,
        "version {pruned_version} is older than retention=2 and must be pruned"
    );

    // 4. graph_versions metadata (id/created/commit_sha) is never pruned by
    // symbol/edge retention: it's not duplicated per reindex like
    // symbols/edges are, and co-change/git-mining history doesn't read it
    // (src/db/co_change.rs, src/git_mining.rs read git and the
    // version-independent co_changes table instead).
    let metadata_rows: i64 = db
        .read_conn()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM graph_versions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        metadata_rows, current,
        "graph_versions metadata must be retained for every version ever created \
         (ids are contiguous from the pre-reindex bootstrap version), even though \
         only the last 2 versions still have symbol/edge rows"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}
