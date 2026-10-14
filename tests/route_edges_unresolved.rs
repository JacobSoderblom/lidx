//! A `ROUTE` edge names the route string of a literal, never a symbol: the
//! tail of `"/index.html"` must not bind to a function called `html` (a false
//! dependent of that function in every impact answer).

mod common;

use lidx::indexer::Indexer;

#[test]
fn a_route_literal_binds_no_symbol_by_its_tail() {
    let files = [
        (
            "html.ts",
            "export function html(): string { return \"\"; }\n",
        ),
        (
            "static.ts",
            "export function serve(names: string[] = [\"/index.html\"]) { return names; }\n",
        ),
    ];
    let (_tmp, root, db_path) = common::index_repo("lidx-route-", &files);
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let bound: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges
             WHERE kind = 'ROUTE' AND target_symbol_id IS NOT NULL AND graph_version = ?",
            [gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(bound, 0, "no ROUTE edge binds a symbol");
    let into_html: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM edges e JOIN symbols t ON t.id = e.target_symbol_id
             WHERE t.name = 'html' AND e.kind = 'ROUTE' AND e.graph_version = ?",
            [gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(into_html, 0, "nothing depends on `html` through the route");
}
