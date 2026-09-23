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

/// A full `reindex()` must repair NULL edge targets, including ones its own
/// carry-forward step creates.
///
/// Scenario (matches how the gap actually manifests, not a synthetic one):
/// - caller.py calls `greet` in helper.py. A first full reindex builds graph
///   version 1 with both files freshly parsed, so the CALLS edge resolves
///   immediately (both symbol rows exist before edges are processed).
/// - caller.py is edited (its hash changes) while helper.py is untouched. A
///   second `reindex()` re-parses caller.py but carries helper.py's rows
///   forward via `carry_forward_files`, which — per `reindex`'s own ordering
///   comment — runs *after* the fresh-file edge loop. At the moment caller's
///   new CALLS edge is inserted, helper.greet does not yet have a row under
///   the new graph version, so the edge is written with target_symbol_id
///   NULL.
/// - Without the repair pass wired into `reindex()`, that NULL is permanent:
///   nothing else re-resolves it. With the repair pass, `resolve_null_target_edges`
///   (run after carry-forward) finds helper.greet's now-present current-version
///   row and re-links the edge.
#[test]
fn reindex_resolves_edge_into_carried_forward_file_after_repair() {
    let (repo_root, db_path) = setup_repo("py_rename");

    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Edit caller.py only; helper.py's content (and hash) stays identical, so
    // the second reindex carries it forward instead of re-parsing it.
    let caller_path = repo_root.join("caller.py");
    std::fs::write(
        &caller_path,
        "from helper import greet\n\n# a harmless comment to change caller's hash\ndef run():\n    return greet(\"world\")\n",
    )
    .unwrap();

    indexer.reindex().unwrap();
    let graph_version = indexer.db().current_graph_version().unwrap();

    let greet = indexer
        .db()
        .get_symbol_by_qualname("helper.greet", graph_version)
        .unwrap()
        .expect("helper.greet must exist under the current graph version after carry-forward");

    // The python extractor records the CALLS edge's target_qualname relative to
    // caller's own import binding ("caller.greet", from `from helper import greet`),
    // not helper's qualname; resolving it to helper.greet's actual symbol row is
    // exactly the fuzzy/exact qualname resolution this repair pass is responsible for.
    let conn = indexer.db().read_conn().unwrap();
    let (total_calls_to_greet, resolved_calls_to_greet): (i64, i64) = conn
        .query_row(
            "SELECT COUNT(*), COUNT(target_symbol_id) FROM edges
             WHERE target_qualname = 'caller.greet'
               AND kind = 'CALLS'
               AND graph_version = ?",
            rusqlite::params![graph_version],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(
        total_calls_to_greet > 0,
        "precondition: caller.run's CALLS edge to greet must exist in the current version"
    );
    assert_eq!(
        resolved_calls_to_greet, total_calls_to_greet,
        "every CALLS edge targeting greet must have a non-NULL target_symbol_id \
         after reindex's repair pass runs"
    );

    let resolved_to_greet_id: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges
             WHERE target_qualname = 'caller.greet'
               AND kind = 'CALLS'
               AND graph_version = ?
               AND target_symbol_id = ?",
            rusqlite::params![graph_version, greet.id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        resolved_to_greet_id > 0,
        "the CALLS edge must resolve to helper.greet's actual current-version rowid ({}), not \
         merely some non-NULL id",
        greet.id
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}

/// A dotted, import-aliased call (`h.greet(...)` after `import helper as h`)
/// whose target file is carried forward on an incremental reindex must
/// still resolve through the import-candidate tier, not just sit stuck
/// unresolved.
///
/// This is a different failure mode than
/// `reindex_resolves_edge_into_carried_forward_file_after_repair` above: a
/// bare `greet("world")` call (no import aliasing) leaves a target_qualname
/// text ("caller.greet") that the *fuzzy suffix* tier can still stumble
/// onto after carry-forward, because the literal text happens to end in
/// ".greet". A dotted `h.greet()` call through an aliased import produces a
/// target_qualname of "h.greet" (the literal receiver text — see
/// `python::resolve_call_target`), which never fuzzy-suffix-matches
/// "helper.greet" (the alias "h" isn't a suffix of "helper"). The *only*
/// path back to the real symbol is the import-candidate tier
/// (`db::resolver::Resolver::resolve_import`, fed `import_candidates = ["helper.greet"]`
/// from the file's own `import helper as h`), and prior to persisting
/// `import_candidates` as a DB column, `resolve_null_target_edges` had no
/// way to retry that tier once helper.py's symbol row was carried forward
/// into the new graph version — the edge was permanently stuck at
/// `target_symbol_id = NULL, receiver_type = ''`.
///
/// Scenario (mirrors the sibling test's structure):
/// - A first full reindex builds graph version 1 with both files freshly
///   parsed: the import tier resolves immediately since helper.py's symbols
///   already exist when caller.py's edges are inserted (both files' symbols
///   are inserted before either file's edges, see `Indexer::reindex`).
/// - caller.py is edited (its hash changes); helper.py is untouched, so the
///   second reindex carries it forward. At the moment caller's new CALLS
///   edge is inserted, helper.greet has no row yet under the new graph
///   version, so both the exact-qualname tier ("h.greet") and the import
///   tier ("helper.greet") miss, and the edge is persisted with
///   `target_symbol_id = NULL, receiver_type = ''` (see the guard in
///   `Db::insert_edges`).
/// - `resolve_null_target_edges` must retry the import tier after
///   carry-forward gives helper.greet its new-version row, using the
///   `import_candidates` this edge's row now carries.
#[test]
fn reindex_resolves_import_candidate_into_carried_forward_file_after_repair() {
    let (repo_root, db_path) = setup_repo("py_import_candidate_carry_forward");

    let mut indexer = Indexer::new(repo_root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();

    // Edit caller.py only; helper.py's content (and hash) stays identical, so
    // the second reindex carries it forward instead of re-parsing it.
    let caller_path = repo_root.join("caller.py");
    std::fs::write(
        &caller_path,
        "import helper as h\n\n# a harmless comment to change caller's hash\ndef run():\n    return h.greet(\"world\")\n",
    )
    .unwrap();

    indexer.reindex().unwrap();
    let graph_version = indexer.db().current_graph_version().unwrap();

    let greet = indexer
        .db()
        .get_symbol_by_qualname("helper.greet", graph_version)
        .unwrap()
        .expect("helper.greet must exist under the current graph version after carry-forward");

    let conn = indexer.db().read_conn().unwrap();
    let (total_calls_to_h_greet, resolved_calls_to_h_greet): (i64, i64) = conn
        .query_row(
            "SELECT COUNT(*), COUNT(target_symbol_id) FROM edges
             WHERE target_qualname = 'h.greet'
               AND kind = 'CALLS'
               AND graph_version = ?",
            rusqlite::params![graph_version],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(
        total_calls_to_h_greet > 0,
        "precondition: caller.run's CALLS edge to h.greet must exist in the current version"
    );
    assert_eq!(
        resolved_calls_to_h_greet, total_calls_to_h_greet,
        "every CALLS edge targeting h.greet must have a non-NULL target_symbol_id after \
         reindex's repair pass retries the import-candidate tier"
    );

    let (resolved_to_greet_id, resolution_kind): (i64, Option<String>) = conn
        .query_row(
            "SELECT COUNT(*), MAX(resolution_kind) FROM edges
             WHERE target_qualname = 'h.greet'
               AND kind = 'CALLS'
               AND graph_version = ?
               AND target_symbol_id = ?",
            rusqlite::params![graph_version, greet.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(
        resolved_to_greet_id > 0,
        "the CALLS edge must resolve to helper.greet's actual current-version rowid ({}), not \
         merely some non-NULL id",
        greet.id
    );
    assert_eq!(
        resolution_kind.as_deref(),
        Some("import"),
        "the edge must resolve via the import-candidate tier specifically (not a coincidental \
         fuzzy match), proving the repair pass actually retried import_candidates"
    );

    let _ = std::fs::remove_dir_all(&repo_root);
}
