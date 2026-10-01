//! Issue #248 follow-up: the Python-only "locally bound bare call" resolver
//! gate must not change JS/TS. A genuine module-level `class Error {}` still
//! binds a bare `Error()` / `new Error()` in the same file through the exact
//! tier, even though the extractor marks unshadowed globals `Unresolved`.

mod common;

use lidx::indexer::Indexer;
use rusqlite::params;

#[test]
fn local_class_named_like_a_global_still_binds_in_js() {
    let tmp = tempfile::tempdir().unwrap();
    common::write_files(
        tmp.path(),
        &[(
            "m.js",
            "class Error {}\nfunction make() {\n  const a = new Error();\n  return Error();\n}\n",
        )],
    );
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let targets: Vec<String> = conn
        .prepare(
            "SELECT t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND s.name = 'make' AND e.graph_version = ?
               AND t.kind != 'external'",
        )
        .unwrap()
        .query_map(params![gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(
        !targets.is_empty() && targets.iter().all(|q| q.ends_with("Error")),
        "{targets:?}"
    );
}
