//! A `use` at the top of a function body binds its names over the module's:
//! `use dep::color_scheme;` makes `color_scheme(..)` in that body the
//! dependency's function, not the module's own function of the same name
//! (which would also make the method a false caller of a function that calls
//! it back).

mod common;

use lidx::indexer::Indexer;

#[test]
fn a_body_scoped_use_shadows_the_modules_function() {
    let files = [
        (
            "Cargo.toml",
            "[package]\nname = \"t\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        ),
        (
            "src/lib.rs",
            "pub fn color_scheme() -> u8 { 0 }\n\
             pub fn plain() -> u8 { color_scheme() }\n\
             pub struct D;\n\
             impl D {\n\
             \x20   pub fn detect(&self) -> u8 {\n\
             \x20       use dep::{color_scheme, Opts};\n\
             \x20       color_scheme(Opts::default())\n\
             \x20   }\n\
             }\n",
        ),
    ];
    let (_tmp, root, db_path) = common::index_repo("lidx-rust-body-use-", &files);
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let targets = |source: &str| -> Vec<(String, String)> {
        conn.prepare(
            "SELECT t.qualname, t.kind FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND e.graph_version = ? AND s.name = ?",
        )
        .unwrap()
        .query_map(rusqlite::params![gv, source], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
    };
    assert_eq!(
        targets("plain"),
        [("crate::color_scheme".to_string(), "function".to_string())]
    );
    let detect = targets("detect");
    assert!(
        detect.iter().all(|(q, _)| q != "crate::color_scheme"),
        "the body's `use` shadows the module function: {detect:?}"
    );
}
