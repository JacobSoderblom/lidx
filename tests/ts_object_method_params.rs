//! The parameters of an object-literal method inside a function are that
//! method's own locals: an untyped one (`peer`) binds no same-named method of
//! some class by bare name, a typed one binds its type's method.

mod common;

use lidx::indexer::Indexer;

#[test]
fn object_literal_method_parameters_are_locals() {
    let files = [(
        "a.ts",
        "export class ES { send(x: string) {} }\n\
         export function untyped() { return { message(peer, m) { peer.send(m); } }; }\n\
         export function typed() { return { message(peer: ES, m: string) { peer.send(m); } }; }\n",
    )];
    let (_tmp, root, db_path) = common::index_repo("lidx-ts-object-method-", &files);
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let targets = |source: &str| -> Vec<String> {
        conn.prepare(
            "SELECT t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.graph_version = ? AND s.name = ?",
        )
        .unwrap()
        .query_map(rusqlite::params![gv, source], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
    };
    assert!(targets("untyped").is_empty());
    assert_eq!(targets("typed"), ["a.ES.send"]);
}
