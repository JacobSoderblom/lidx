//! `impl Trait for Vec<u8>` (a type that is no symbol of the repository) is
//! still an implementation of the trait's methods: the dispatch overlay pairs
//! its method with the trait method through the member-level `IMPLEMENTS`
//! edge, so the callers of the trait method are the impl method's callers.

mod common;

use lidx::indexer::Indexer;

#[test]
fn an_impl_for_a_foreign_type_is_paired_with_the_trait_method() {
    let files = [
        (
            "Cargo.toml",
            "[package]\nname = \"t\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        ),
        (
            "src/lib.rs",
            "pub trait Shape { fn area(&self) -> u32; }\n\
             pub struct Sq;\n\
             impl Shape for Sq { fn area(&self) -> u32 { 1 } }\n\
             impl Shape for Vec<u8> { fn area(&self) -> u32 { 2 } }\n\
             pub fn total(s: &dyn Shape) -> u32 { s.area() }\n",
        ),
    ];
    let (_tmp, root, db_path) = common::index_repo("lidx-rust-foreign-impl-", &files);
    let indexer = Indexer::new(root, db_path).unwrap();
    let db = indexer.db();
    let gv = db.current_graph_version().unwrap();
    let conn = db.read_conn().unwrap();
    let id = |qual: &str| -> i64 {
        conn.query_row(
            "SELECT id FROM symbols WHERE qualname = ? AND graph_version = ?",
            rusqlite::params![qual, gv],
            |r| r.get(0),
        )
        .unwrap_or_else(|_| panic!("no symbol {qual}"))
    };
    let trait_area = id("crate::Shape::area");
    let sq_area = id("crate::Sq::area");
    let vec_area: i64 = conn
        .query_row(
            "SELECT s.id FROM symbols s WHERE s.name = 'area' AND s.graph_version = ?
               AND s.id NOT IN (?, ?) AND s.kind = 'method' ORDER BY s.id LIMIT 1",
            rusqlite::params![gv, trait_area, sq_area],
            |r| r.get(0),
        )
        .unwrap();
    let pairs = db.dispatch_pairs(&[vec_area], gv).unwrap();
    assert!(
        pairs.contains(&(trait_area, vec_area)),
        "the foreign impl is paired with the trait method: {pairs:?}"
    );
}
