//! `lidx dispatch-pairs` dumps every (base member id, implementing member id) pair of the dispatch query.
mod common;

use rusqlite::Connection;

fn id_of(db: &std::path::Path, qualname_suffix: &str) -> i64 {
    let con = Connection::open(db).unwrap();
    con.query_row(
        "SELECT id FROM symbols WHERE qualname LIKE ?1 AND kind = 'method'",
        [format!("%{qualname_suffix}")],
        |r| r.get(0),
    )
    .unwrap_or_else(|_| panic!("no method symbol ending {qualname_suffix}"))
}

#[test]
fn dispatch_pairs_prints_sorted_interface_implementation_pairs() {
    let src = "package shapes\n\
        type Shape interface {\n\tArea() float64\n}\n\
        type Square struct{}\n\
        func (Square) Area() float64 { return 1 }\n\
        type Circle struct{}\n\
        func (Circle) Area() float64 { return 2 }\n";
    let (_guard, root, db) = common::index_repo("lidx-dispatch-pairs-", &[("go.mod", "module x\n\ngo 1.21\n"), ("shapes.go", src)]);

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_lidx"))
        .args(["dispatch-pairs", "--repo"])
        .arg(&root)
        .arg("--db")
        .arg(&db)
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let pairs: Vec<(i64, i64)> = serde_json::from_slice(&out.stdout).expect("stdout is a JSON array of [base, impl] pairs");

    let base = id_of(&db, "Shape.Area");
    let square = id_of(&db, "Square.Area");
    let circle = id_of(&db, "Circle.Area");
    assert!(!pairs.is_empty());
    assert!(pairs.contains(&(base, square)), "{pairs:?}");
    assert!(pairs.contains(&(base, circle)), "{pairs:?}");
    let mut sorted = pairs.clone();
    sorted.sort_unstable();
    assert_eq!(pairs, sorted, "pairs are printed in a deterministic order");
}
