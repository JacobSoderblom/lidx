//! Issue #250: concurrent reindexes must serialise, and a graph version must
//! only become current once it is fully populated.

use lidx::db::Db;
use lidx::indexer::Indexer;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

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

/// A repo of `files` small Python modules, each calling into the next.
fn setup_big_repo(files: usize) -> (PathBuf, PathBuf) {
    let repo_root = temp_repo_dir("concurrent");
    for i in 0..files {
        let body = format!(
            "def func_{i}(x):\n    return func_{next}(x) + 1\n\n\nclass Thing{i}:\n    def method(self):\n        return func_{i}(1)\n",
            next = (i + 1) % files
        );
        std::fs::write(repo_root.join(format!("mod_{i}.py")), body).unwrap();
    }
    let db_path = repo_root.join(".lidx").join(".lidx.sqlite");
    (repo_root, db_path)
}

fn count_at(db: &Db, table: &str, gv: i64) -> i64 {
    db.read_conn()
        .unwrap()
        .query_row(
            &format!("SELECT COUNT(*) FROM {table} WHERE graph_version = ?"),
            rusqlite::params![gv],
            |row| row.get(0),
        )
        .unwrap()
}

fn file_count(db: &Db) -> i64 {
    db.read_conn()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM files WHERE deleted_version IS NULL",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

/// (files, symbols, edges) of the current version.
fn counts(db: &Db) -> (i64, i64, i64) {
    let gv = db.current_graph_version().unwrap();
    (
        file_count(db),
        count_at(db, "symbols", gv),
        count_at(db, "edges", gv),
    )
}

#[test]
fn readers_never_see_a_partial_version_as_current() {
    let (repo_root, db_path) = setup_big_repo(400);
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let baseline = count_at(
        indexer.db(),
        "symbols",
        indexer.db().current_graph_version().unwrap(),
    );
    assert!(
        baseline >= 800,
        "fixture should index many symbols: {baseline}"
    );

    // Touch one file so the reindex is not a pure carry-forward.
    std::fs::write(repo_root.join("mod_0.py"), "def func_0(x):\n    return x\n").unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let poller = {
        let stop = stop.clone();
        let db_path = db_path.clone();
        std::thread::spawn(move || {
            let db = Db::new(&db_path).unwrap();
            let mut worst = i64::MAX;
            let mut polls = 0;
            while !stop.load(Ordering::SeqCst) {
                let gv = db.current_graph_version().unwrap();
                let n = count_at(&db, "symbols", gv);
                worst = worst.min(n);
                polls += 1;
            }
            (worst, polls)
        })
    };
    // Give the poller time to start, then reindex.
    std::thread::sleep(std::time::Duration::from_millis(200));
    indexer.reindex().unwrap();
    stop.store(true, Ordering::SeqCst);
    let (worst, polls) = poller.join().unwrap();
    assert!(polls > 10);
    // mod_0 lost 2 symbols (class + method); everything else is unchanged.
    assert!(
        worst >= baseline - 4,
        "a reader observed a current version with {worst} symbols; the pre-reindex version had {baseline}"
    );
}

fn fresh_counts(repo_root: &Path) -> (i64, i64, i64) {
    let db_path = repo_root.join(".lidx-fresh").join(".lidx.sqlite");
    let mut indexer = Indexer::new(repo_root.to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    counts(indexer.db())
}

#[test]
fn concurrent_reindexes_serialise_and_leave_a_complete_index() {
    let (repo_root, db_path) = setup_big_repo(120);
    Db::new(&db_path).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let handles: Vec<_> = (0..2)
        .map(|_| {
            let (repo_root, db_path, barrier) =
                (repo_root.clone(), db_path.clone(), barrier.clone());
            std::thread::spawn(move || {
                let mut indexer = Indexer::new(repo_root, db_path).unwrap();
                barrier.wait();
                indexer.reindex().map(|_| ())
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert!(
        results.iter().any(|r| r.is_ok()),
        "one reindex must succeed"
    );
    for result in &results {
        if let Err(err) = result {
            assert!(
                err.downcast_ref::<lidx::db::ReindexBusy>().is_some(),
                "a losing reindex must refuse with ReindexBusy, got: {err:#}"
            );
            assert!(!format!("{err:#}").contains("database is locked"));
        }
    }
    let db = Db::new(&db_path).unwrap();
    assert_eq!(counts(&db), fresh_counts(&repo_root));
}

#[test]
fn held_lock_makes_reindex_refuse_without_modifying_anything() {
    let (repo_root, db_path) = setup_big_repo(20);
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let before = counts(indexer.db());
    let versions_before = indexer.db().list_graph_versions(100, 0).unwrap().len();
    let gv_before = indexer.db().current_graph_version().unwrap();

    let lock = indexer.db().try_lock_reindex().unwrap();
    let err = indexer.reindex().unwrap_err();
    let busy = err
        .downcast_ref::<lidx::db::ReindexBusy>()
        .expect("ReindexBusy");
    assert_eq!(busy.holder_pid, Some(std::process::id()));
    assert_eq!(indexer.db().current_graph_version().unwrap(), gv_before);
    assert_eq!(
        indexer.db().list_graph_versions(100, 0).unwrap().len(),
        versions_before
    );
    assert_eq!(counts(indexer.db()), before);

    // The CLI refuses with its own exit code and names the holder.
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_lidx"))
        .args(["reindex", "--repo"])
        .arg(&repo_root)
        .arg("--db")
        .arg(&db_path)
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(75),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains(&std::process::id().to_string()), "{stderr}");

    drop(lock);
    indexer.reindex().unwrap();
    assert_eq!(counts(indexer.db()), before);
}

#[test]
fn stale_lock_file_from_a_dead_process_does_not_block() {
    let (repo_root, db_path) = setup_big_repo(10);
    std::fs::create_dir_all(db_path.parent().unwrap()).unwrap();
    let mut lock_path = db_path.clone().into_os_string();
    lock_path.push(".reindex.lock");
    // A killed process leaves its pid behind, but the OS dropped its lock.
    std::fs::write(&lock_path, "999999").unwrap();
    let mut indexer = Indexer::new(repo_root, db_path).unwrap();
    indexer.reindex().unwrap();
    assert!(counts(indexer.db()).1 > 0);
}

#[test]
fn allocated_version_is_not_current_until_promoted() {
    let (repo_root, db_path) = setup_big_repo(5);
    let mut indexer = Indexer::new(repo_root, db_path).unwrap();
    indexer.reindex().unwrap();
    let db = indexer.db();
    let done = db.current_graph_version().unwrap();
    let building = db.allocate_graph_version(None).unwrap();
    assert!(building > done);
    assert_eq!(db.current_graph_version().unwrap(), done);
    assert!(
        db.list_graph_versions(100, 0)
            .unwrap()
            .iter()
            .all(|v| v.id != building)
    );
    db.promote_graph_version(building).unwrap();
    assert_eq!(db.current_graph_version().unwrap(), building);
}

/// Mimics a reindex killed partway: a `building` version holding some rows and
/// a `files` row already rewritten for an edited file.
#[test]
fn killed_reindex_leaves_previous_version_queryable_and_is_reclaimed() {
    let (repo_root, db_path) = setup_big_repo(30);
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let db = indexer.db();
    let done = db.current_graph_version().unwrap();
    let before = counts(db);
    let overview_before = db.repo_overview(repo_root.clone(), None, done).unwrap();

    // The edit, and the dying reindex's partial work on it.
    let edited = "def brand_new(x):\n    return x\n";
    std::fs::write(repo_root.join("mod_3.py"), edited).unwrap();
    let hash = blake3::hash(edited.as_bytes()).to_hex().to_string();
    let building = db.allocate_graph_version(None).unwrap();
    db.upsert_file("mod_3.py", &hash, "python", edited.len() as i64, 0)
        .unwrap();
    let _ = db.carry_forward_symbols(&[1, 2], done, building).unwrap();
    db.mark_file_deleted("mod_4.py", building).unwrap();
    drop(indexer);

    // A reader (new process) still sees the completed version.
    let reader = Db::new(&db_path).unwrap();
    assert_eq!(reader.current_graph_version().unwrap(), done);
    assert_eq!(counts(&reader).1, before.1);
    let overview = reader.repo_overview(repo_root.clone(), None, done).unwrap();
    assert_eq!(
        serde_json::to_value(&overview).unwrap(),
        serde_json::to_value(&overview_before).unwrap()
    );
    drop(reader);

    // The next reindex needs no manual cleanup, sees the edit, and reclaims.
    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    let db = indexer.db();
    let now = db.current_graph_version().unwrap();
    // The abandoned partial rows are gone (its id may be reused by the new run).
    assert!(now >= building);
    let abandoned: i64 = db
        .read_conn()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM graph_versions WHERE status != 'complete'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(abandoned, 0);
    let names: Vec<String> = {
        let conn = db.read_conn().unwrap();
        let mut stmt = conn
            .prepare("SELECT name FROM symbols WHERE graph_version = ? AND name = 'brand_new'")
            .unwrap();
        stmt.query_map([now], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    };
    assert_eq!(
        names,
        vec!["brand_new".to_string()],
        "edit must not be carried over stale"
    );
    assert_eq!(counts(db), fresh_counts(&repo_root));
}

#[test]
fn v0_6_0_database_upgrades_with_current_version_complete() {
    let (repo_root, db_path) = setup_big_repo(10);
    {
        let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
        indexer.reindex().unwrap();
        // Rewind to the v0.6.0 schema: no `status` column.
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "ALTER TABLE graph_versions DROP COLUMN status;
             UPDATE meta SET value = '24' WHERE key = 'schema_version';",
        )
        .unwrap();
    }
    let mut indexer = Indexer::new(repo_root.clone(), db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let status: String = indexer
        .db()
        .read_conn()
        .unwrap()
        .query_row(
            "SELECT status FROM graph_versions WHERE id = ?",
            [gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(status, "complete");
    let before = counts(indexer.db());
    indexer.reindex().unwrap();
    assert_eq!(counts(indexer.db()), before);
}
