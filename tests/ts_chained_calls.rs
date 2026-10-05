//! Issue #320: TypeScript calls whose receiver is a call expression
//! (`make().stage(1).storage()`) record a CALLS edge per link.

use lidx::indexer::Indexer;
use rusqlite::params;

const SOURCE: &str = r#"export class Builder {
  stage(x: number): Builder { return this; }
  storage(): Builder { return this; }
}
export function make(): Builder { return new Builder(); }
export function use() { return make().stage(1).storage(); }
export function cb() { return run(() => make().stage(2)); }
export function untyped() { return fetchIt().go(); }
export function run(f: () => Builder) { return f(); }
"#;

fn calls_from(indexer: &Indexer, src: &str) -> Vec<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(t.qualname, e.target_qualname, '') FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.graph_version = ? AND s.qualname = ?
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map(params![gv, src], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn chained_calls_on_call_receivers_are_indexed_and_resolved() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("b.ts"), SOURCE).unwrap();
    let mut indexer = Indexer::new(
        tmp.path().to_path_buf(),
        tmp.path().join(".lidx").join(".lidx.sqlite"),
    )
    .unwrap();
    indexer.reindex().unwrap();

    let use_t = calls_from(&indexer, "b.use");
    for want in ["b.make", "b.Builder.stage", "b.Builder.storage"] {
        assert!(use_t.iter().any(|t| t == want), "{want} missing: {use_t:?}");
    }
    let cb_t = calls_from(&indexer, "b.cb");
    for want in ["b.make", "b.Builder.stage"] {
        assert!(cb_t.iter().any(|t| t == want), "{want} missing: {cb_t:?}");
    }
}

#[test]
fn chained_call_with_unknown_receiver_type_is_recorded() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("b.ts"), SOURCE).unwrap();
    let mut indexer = Indexer::new(
        tmp.path().to_path_buf(),
        tmp.path().join(".lidx").join(".lidx.sqlite"),
    )
    .unwrap();
    indexer.reindex().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let edges: i64 = conn
        .query_row(
            "SELECT count(*) FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
             WHERE e.kind = 'CALLS' AND s.qualname = 'b.untyped'
               AND e.target_qualname LIKE '%go'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let refs: i64 = conn
        .query_row(
            "SELECT count(*) FROM unresolved_references WHERE reference_name LIKE '%go'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(edges + refs > 0, "chained call on untyped receiver dropped");
}
