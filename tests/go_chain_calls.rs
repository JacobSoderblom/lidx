//! Go calls on a call result (`NewB().C().D(1)`) are typed through the declared
//! result of each link. A link whose type cannot be told (a call of a function
//! no file declares) is stored as an unresolved reference carrying the method
//! name and the receiver text, so `trace_flow` can report it.

mod common;

use lidx::indexer::Indexer;

const SRC: &str = "package p\n\nfunc NewB() *B { return nil }\n\ntype B struct{}\n\nfunc (b *B) C() *B { return b }\n\nfunc (b *B) D(n int) int { return n }\n\nfunc Run() int {\n\treturn NewB().C().D(1)\n}\n\nfunc Opaque() int {\n\treturn Missing().C().D(1)\n}\n\nfunc Other() *B {\n\treturn D()\n}\n\nfunc D() *B { return nil }\n";

#[test]
fn chain_links_are_typed_through_declared_results() {
    let (_tmp, root, db_path) = common::index_repo("lidx-go-chain-", &[("p.go", SRC)]);
    let indexer = Indexer::new(root, db_path).unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let bound: Vec<(String, String)> = conn
        .prepare(
            "SELECT s.qualname, t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND s.qualname IN ('p.Run', 'p.Other') ORDER BY 1, 2",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        bound,
        [
            ("p.Other".into(), "p.D".into()),
            ("p.Run".into(), "p.B.C".into()),
            ("p.Run".into(), "p.B.D".into()),
            ("p.Run".into(), "p.NewB".into()),
        ]
    );
}

#[test]
fn chain_links_without_a_declared_type_are_stored_as_unresolved_rows_with_their_method_names() {
    let (_tmp, root, db_path) = common::index_repo("lidx-go-chain-", &[("p.go", SRC)]);
    let indexer = Indexer::new(root, db_path).unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut rows: Vec<(String, String, String)> = conn
        .prepare(
            "SELECT s.qualname, u.reference_name, COALESCE(u.detail, '')
             FROM unresolved_references u JOIN symbols s ON s.id = u.source_symbol_id
             WHERE u.edge_kind = 'CALLS' ORDER BY 1, 2",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    rows.retain(|(src, ..)| src == "p.Opaque");
    assert_eq!(
        rows,
        [
            ("p.Opaque".into(), "C".into(), "Missing().C".into()),
            ("p.Opaque".into(), "D".into(), "Missing().C().D".into()),
            // The head of the chain is a plain call of an undeclared function.
            ("p.Opaque".into(), "p.Missing".into(), "".into()),
        ]
    );
    // The method call on a call result never binds the package function `D`.
    let bound: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND s.qualname = 'p.Opaque' AND t.name IN ('C', 'D')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(bound, 0);
}
