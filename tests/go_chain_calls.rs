//! Go calls on a call result (`NewB().C().D(1)`) are stored as unresolved
//! references carrying the method name and the receiver text, so `trace_flow`
//! can report them and a receiver-typing pass can finish them later.

mod common;

use lidx::indexer::Indexer;

const SRC: &str = "package p\n\nfunc NewB() *B { return nil }\n\ntype B struct{}\n\nfunc (b *B) C() *B { return b }\n\nfunc (b *B) D(n int) int { return n }\n\nfunc Run() int {\n\treturn NewB().C().D(1)\n}\n\nfunc Other() *B {\n\treturn D()\n}\n\nfunc D() *B { return nil }\n";

#[test]
fn chain_links_are_stored_as_unresolved_rows_with_their_method_names() {
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
    rows.retain(|(src, ..)| src == "p.Run");
    assert_eq!(
        rows,
        [
            ("p.Run".into(), "C".into(), "NewB().C".into()),
            ("p.Run".into(), "D".into(), "NewB().C().D".into()),
        ]
    );
    // The method call on a call result never binds the package function `D`.
    let bound: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND s.qualname = 'p.Run' AND t.name IN ('C', 'D')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(bound, 0);
}
