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

/// A degraded index (edges whose `target_symbol_id` is NULL but resolvable against the
/// current version's symbols) must be healed by a subsequent `reindex()` even when no file
/// content changed — i.e. even on a pure carry-forward run.
///
/// Before this fix, `reindex()` only ran the dangling-id / NULL-target repair pass when
/// `stats.indexed > 0 || stats.deleted > 0`. A run where every file is carried forward
/// unchanged reports `indexed: 0, deleted: 0`, so repair was skipped unconditionally — a
/// hollow/degraded index stayed hollow no matter how many times `reindex` ran, and the only
/// remedy was a full cold rebuild (`rm -rf .lidx`).
///
/// Scenario:
/// - caller.py calls `greet` in helper.py. A first full reindex builds graph version 1 with
///   both files freshly parsed, so the CALLS edge resolves immediately.
/// - A second `reindex()` with zero file changes carries both files forward unchanged
///   (`indexed: 0`) into graph version 2. The edge is still resolved at this point.
/// - The test then directly NULLs out that edge's `target_symbol_id` under graph version 2,
///   simulating degradation from any external cause (corruption, a bug in an older build,
///   manual DB surgery) — not something `reindex` itself would do, but exactly the shape of
///   state a degraded index is in.
/// - A third `reindex()`, again with zero file changes, carries the degraded edge forward
///   into graph version 3. `carry_forward_files` re-links each edge by stable_id; when the
///   old target_symbol_id is already NULL there is nothing to look up, so the NULL — and the
///   edge's stale `target_qualname` — propagate into version 3 untouched.
/// - Without this fix, that NULL is now permanent: `stats.indexed`/`deleted` are `0` on every
///   future reindex, so repair never runs again. With this fix, the cheap COUNT of
///   unresolved edges in the current version detects the outstanding work and runs the
///   existing repair pass, which re-resolves the edge by qualname — without re-parsing
///   either file.
#[test]
fn reindex_heals_degraded_index_with_no_file_changes() {
    let (repo_root, db_path) = setup_repo("py_rename");

    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Second reindex, zero file changes: pure carry-forward into graph version 2.
    let carried = indexer.reindex().unwrap();
    assert_eq!(
        carried.indexed, 0,
        "no file content changed; nothing should be (re)parsed"
    );
    assert!(
        carried.skipped > 0,
        "unchanged files must be carried forward, not re-parsed"
    );

    let degraded_version = indexer.db().current_graph_version().unwrap();

    // Simulate a degraded/hollow index: NULL out the resolvable CALLS edge's
    // target_symbol_id directly, the way corruption from any cause would leave it.
    {
        let conn = indexer.db().read_conn().unwrap();
        let degraded = conn
            .execute(
                "UPDATE edges SET target_symbol_id = NULL
                 WHERE target_qualname = 'caller.greet' AND kind = 'CALLS' AND graph_version = ?",
                rusqlite::params![degraded_version],
            )
            .unwrap();
        assert!(
            degraded > 0,
            "precondition: there must be a resolved edge to degrade"
        );
    }

    // Third reindex, again with zero file changes. Must still heal the NULL target without
    // re-parsing any file.
    let healing = indexer.reindex().unwrap();
    assert_eq!(
        healing.indexed, 0,
        "healing reindex must not re-parse any file"
    );
    assert!(
        healing.skipped > 0,
        "unchanged files must still be carried forward on the healing run"
    );

    let healed_version = indexer.db().current_graph_version().unwrap();
    let greet = indexer
        .db()
        .get_symbol_by_qualname("helper.greet", healed_version)
        .unwrap()
        .expect("helper.greet must exist under the current graph version");

    let conn = indexer.db().read_conn().unwrap();
    let (total, resolved): (i64, i64) = conn
        .query_row(
            "SELECT COUNT(*), COUNT(target_symbol_id) FROM edges
             WHERE target_qualname = 'caller.greet' AND kind = 'CALLS' AND graph_version = ?",
            rusqlite::params![healed_version],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(
        total > 0,
        "precondition: the degraded edge must survive carry-forward into this version"
    );
    assert_eq!(
        resolved, total,
        "a degraded index (resolvable NULL target) must be healed by a subsequent reindex \
         even when no file content changed"
    );

    let resolved_to_greet_id: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges
             WHERE target_qualname = 'caller.greet' AND kind = 'CALLS' AND graph_version = ?
               AND target_symbol_id = ?",
            rusqlite::params![healed_version, greet.id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        resolved_to_greet_id > 0,
        "the healed edge must resolve to helper.greet's actual current-version rowid ({}), not \
         merely some non-NULL id",
        greet.id
    );

    // No edge in the current version may target a symbol row from a different graph_version.
    let leaked: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges e
             JOIN symbols s ON s.id = e.target_symbol_id
             WHERE e.graph_version = (SELECT MAX(graph_version) FROM edges)
               AND s.graph_version != e.graph_version",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        leaked, 0,
        "no edge may target a symbol row from a different graph_version"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}
