//! Issue #475: a `Type::method` or `Trait::method` path in value position
//! (`.map(Node::tree)`, `.is_none_or(AttributeSet::is_enabled)`) is a
//! `REFERENCES` edge from the function that writes it, with its source line,
//! also when the type comes in through a `use super::*` glob or a re-export.
//! No lineless module-level `USES` row repeats it, a path through a generic
//! parameter (`S::Error::from_io`) records nothing it cannot bind, and an
//! incremental sync after a re-export changes equals a fresh reindex.

mod common;

use lidx::indexer::Indexer;
use std::collections::BTreeSet;

const MANIFEST: &str = "[package]\nname = \"demo\"\nversion = \"0.1.0\"\n";

fn index(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let tmp = tempfile::tempdir().unwrap();
    common::write_files(tmp.path(), files);
    let db_path = tmp.path().join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(tmp.path().to_path_buf(), db_path).unwrap();
    indexer.reindex().unwrap();
    (tmp, indexer)
}

/// `(source qualname, target qualname, resolution kind, evidence line)` of the
/// `REFERENCES` edges whose target is a function or method.
fn references(indexer: &Indexer) -> BTreeSet<(String, String, String, Option<i64>)> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, t.qualname, e.resolution_kind, e.evidence_start_line
             FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ?1 AND e.kind = 'REFERENCES'
               AND t.kind IN ('function', 'method')",
        )
        .unwrap();
    stmt.query_map([gv], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

/// `USES` edges with a function or method target that start at a module and
/// carry no source line: the rows `Type::f` paths used to leave.
fn lineless_function_uses(indexer: &Indexer) -> Vec<(String, String)> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, t.qualname
             FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ?1 AND e.kind = 'USES'
               AND t.kind IN ('function', 'method')
               AND e.evidence_start_line IS NULL",
        )
        .unwrap();
    stmt.query_map([gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn uses_targets(indexer: &Indexer, source: &str) -> BTreeSet<String> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ?1 AND e.kind = 'USES' AND s.qualname = ?2",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv, source], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn has(
    got: &BTreeSet<(String, String, String, Option<i64>)>,
    source: &str,
    target: &str,
) -> Option<(String, Option<i64>)> {
    got.iter()
        .find(|(s, t, ..)| s == source && t == target)
        .map(|(_, _, kind, line)| (kind.clone(), *line))
}

const GLOB_FILES: &[(&str, &str)] = &[
    ("Cargo.toml", MANIFEST),
    (
        "src/lib.rs",
        "mod attribute_set;\nmod item;\nmod node;\npub use attribute_set::AttributeSet;\n",
    ),
    (
        "src/attribute_set.rs",
        "pub struct AttributeSet;\nimpl AttributeSet {\n    pub fn is_enabled(&self) -> bool { true }\n}\n",
    ),
    (
        "src/item.rs",
        "use super::*;\n\npub struct Item { a: Option<AttributeSet> }\nimpl Item {\n    pub fn is_enabled(&self) -> bool {\n        self.a.as_ref().is_none_or(AttributeSet::is_enabled)\n    }\n}\n",
    ),
    (
        "src/node.rs",
        "pub struct Node;\nimpl Node {\n    pub fn tree(&self) -> u32 { 1 }\n}\npub trait Describe { fn describe(&self) -> String; }\npub struct Body(Vec<Node>);\nimpl Body {\n    pub fn trees(&self) -> Vec<u32> {\n        self.0.iter().map(Node::tree).collect()\n    }\n    pub fn texts<T: Describe>(&self, items: &[T]) -> Vec<String> {\n        items.iter().map(Describe::describe).collect()\n    }\n}\n",
    ),
];

#[test]
fn a_type_path_in_value_position_references_from_its_function_with_a_line() {
    let (_tmp, indexer) = index(GLOB_FILES);
    let got = references(&indexer);
    let (kind, line) = has(&got, "crate::node::Body::trees", "crate::node::Node::tree")
        .unwrap_or_else(|| panic!("{got:#?}"));
    assert_eq!(kind, "exact");
    assert_eq!(line, Some(9));
    let (_, line) = has(
        &got,
        "crate::node::Body::texts",
        "crate::node::Describe::describe",
    )
    .unwrap_or_else(|| panic!("{got:#?}"));
    assert_eq!(line, Some(12));
}

#[test]
fn a_type_reached_by_a_glob_and_a_re_export_binds() {
    let (_tmp, indexer) = index(GLOB_FILES);
    let got = references(&indexer);
    let (kind, line) = has(
        &got,
        "crate::item::Item::is_enabled",
        "crate::attribute_set::AttributeSet::is_enabled",
    )
    .unwrap_or_else(|| panic!("{got:#?}"));
    assert_eq!(kind, "import");
    assert_eq!(line, Some(6));
}

#[test]
fn no_lineless_module_level_uses_row_repeats_the_path() {
    let (_tmp, indexer) = index(GLOB_FILES);
    let rows = lineless_function_uses(&indexer);
    assert!(rows.is_empty(), "{rows:#?}");
    // the type itself is still used by the module
    assert!(
        uses_targets(&indexer, "crate::item").contains("crate::attribute_set::AttributeSet"),
        "{:#?}",
        uses_targets(&indexer, "crate::item")
    );
}

#[test]
fn a_path_through_a_generic_parameter_records_no_function_use() {
    let files = [
        ("Cargo.toml", MANIFEST),
        (
            "src/lib.rs",
            "pub trait SinkError: Sized { fn from_io(e: std::io::Error) -> Self; }\npub trait Sink { type Error: SinkError; }\npub struct Mine;\nimpl SinkError for Mine { fn from_io(_: std::io::Error) -> Self { Mine } }\npub fn open<S: Sink>(r: std::io::Result<()>) -> Result<(), S::Error> {\n    r.map_err(S::Error::from_io)\n}\n",
        ),
    ];
    let (_tmp, indexer) = index(&files);
    let rows = lineless_function_uses(&indexer);
    assert!(rows.is_empty(), "{rows:#?}");
    let got = references(&indexer);
    assert!(
        !got.iter().any(|(s, ..)| s == "crate::open"),
        "a guess at `Mine::from_io` would be wrong: {got:#?}"
    );
}

#[test]
fn a_type_declared_in_the_same_module_still_binds() {
    let files = [
        ("Cargo.toml", MANIFEST),
        (
            "src/lib.rs",
            "pub struct Node;\nimpl Node { pub fn tree(&self) -> u32 { 1 } }\npub fn all(v: &[Node]) -> Vec<u32> { v.iter().map(Node::tree).collect() }\n",
        ),
    ];
    let (_tmp, indexer) = index(&files);
    let got = references(&indexer);
    assert!(
        has(&got, "crate::all", "crate::Node::tree").is_some(),
        "{got:#?}"
    );
    assert!(lineless_function_uses(&indexer).is_empty());
}

// ---- incremental sync == fresh reindex ----

#[test]
fn editing_the_re_export_a_type_path_comes_through_matches_a_fresh_reindex() {
    let files = |lib: &'static str| -> Vec<(&'static str, &'static str)> {
        vec![
            ("Cargo.toml", MANIFEST),
            ("src/lib.rs", lib),
            (
                "src/a.rs",
                "pub struct Kind;\nimpl Kind { pub fn name(&self) -> u32 { 1 } }\n",
            ),
            (
                "src/b.rs",
                "pub struct Kind;\nimpl Kind { pub fn name(&self) -> u32 { 2 } }\n",
            ),
            (
                "src/user.rs",
                "use super::*;\npub fn all(v: &[Kind]) -> Vec<u32> {\n    v.iter().map(Kind::name).collect()\n}\n",
            ),
        ]
    };
    let before = files("mod a;\nmod b;\nmod user;\npub use a::Kind;\n");
    let edit = [(
        "src/lib.rs",
        "mod a;\nmod b;\nmod user;\npub use b::Kind;\n",
    )];
    let (tmp, mut indexer) = index(&before);
    let first = references(&indexer);
    assert!(
        has(&first, "crate::user::all", "crate::a::Kind::name").is_some(),
        "{first:#?}"
    );
    common::write_files(tmp.path(), &edit);
    indexer.sync_rel_paths(&["src/lib.rs".to_string()]).unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let gv = indexer.db().current_graph_version().unwrap();
    let incremental = common::golden::snapshot_edges(indexer.db(), gv).unwrap();
    let after = files("mod a;\nmod b;\nmod user;\npub use b::Kind;\n");
    let (_fresh_tmp, fresh) = common::index_files(&after);
    common::assert_matches_fresh(&incremental, &fresh);
    let now = references(&indexer);
    assert!(
        has(&now, "crate::user::all", "crate::b::Kind::name").is_some()
            && has(&now, "crate::user::all", "crate::a::Kind::name").is_none(),
        "{now:#?}"
    );
}

#[test]
fn a_method_used_only_as_a_type_path_value_is_not_dead() {
    let (_tmp, mut indexer) = index(GLOB_FILES);
    let result = lidx::rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"include_unused_imports": false, "include_orphan_tests": false, "include_public_api": true}),
    )
    .unwrap();
    let names: Vec<String> = result["dead_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .chain(result["public_api"].as_array().unwrap())
        .filter_map(|s| s["qualname"].as_str().map(str::to_string))
        .collect();
    for live in [
        "crate::node::Node::tree",
        "crate::attribute_set::AttributeSet::is_enabled",
        "crate::node::Describe::describe",
    ] {
        assert!(
            !names.iter().any(|q| q == live),
            "{live} is dead: {names:?}"
        );
    }
}
