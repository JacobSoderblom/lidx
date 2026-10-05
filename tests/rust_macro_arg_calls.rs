//! Calls written inside the arguments of known std macros (`format!`,
//! `println!`, ...) are opaque `token_tree`s to tree-sitter; the extractor
//! re-parses them so those calls still get CALLS edges (issue #362).

mod common;

use lidx::indexer::Indexer;

const LIB_RS: &str = r#"
pub fn plain() {}
pub fn in_format() -> u32 { 1 }
pub fn in_println() -> u32 { 2 }
pub fn in_vec() -> u32 { 3 }
pub fn in_assert() -> bool { true }
pub fn in_matches() -> Option<u32> { None }
pub fn in_write() -> u32 { 4 }
pub fn in_nested() -> u32 { 5 }
pub fn on_later_line() -> u32 { 6 }
pub fn in_custom() -> u32 { 7 }
pub fn in_pattern() -> u32 { 8 }
pub fn in_path() -> u32 { 9 }
pub struct Bag;
impl Bag {
    pub fn size(&self) -> usize { 0 }
}
pub mod m {
    pub fn f() -> u32 { 10 }
}
macro_rules! custom { ($e:expr) => { $e }; }
pub fn run(bag: Bag) {
    plain();
    let _a = format!("{}", in_format());
    println!("{} {}", in_println(), bag.size());
    let _v = vec![in_vec(); 2];
    assert!(in_assert());
    let _m = matches!(in_matches(), Some(_));
    let mut s = String::new();
    let _ = write!(s, "{}", in_write());
    let _n = format!("{}", format!("{}", in_nested()));
    let _l = format!(
        "{}\n{}",
        1,
        on_later_line()
    );
    custom!(in_custom());
    let _p = matches!(1u32, 1 | 2 if in_pattern() > 0);
    let _q = format!("{}", m::f());
}
"#;

const FILES: [(&str, &str); 2] = [
    ("Cargo.toml", "[package]\nname = \"x\"\n"),
    ("src/lib.rs", LIB_RS),
];

fn call_edges() -> Vec<(String, Option<i64>)> {
    let (_tmp, root, db_path) = common::index_repo("lidx-macro-args-", &FILES);
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(t.qualname, e.target_qualname, ''), e.evidence_start_line
             FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS' AND s.qualname = 'crate::run'",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn known_macro_arguments_yield_resolved_calls() {
    let edges = call_edges();
    let targets: Vec<&str> = edges.iter().map(|(t, _)| t.as_str()).collect();
    for want in [
        "crate::plain",
        "crate::in_format",
        "crate::in_println",
        "crate::in_vec",
        "crate::in_assert",
        "crate::in_matches",
        "crate::in_write",
        "crate::in_nested",
        "crate::on_later_line",
        "crate::m::f",
    ] {
        assert!(targets.contains(&want), "missing {want}: {targets:?}");
    }
    assert!(
        targets.iter().any(|t| t.ends_with("size")),
        "method call in println! missing: {targets:?}"
    );
}

#[test]
fn macro_names_yield_no_edges() {
    let edges = call_edges();
    let targets: Vec<&str> = edges.iter().map(|(t, _)| t.as_str()).collect();
    for bad in [
        "format", "println", "vec", "assert", "matches", "write", "custom", "Some",
    ] {
        assert!(
            !targets.iter().any(|t| t.rsplit("::").next() == Some(bad)),
            "bogus edge to {bad}: {targets:?}"
        );
    }
}

#[test]
fn unknown_macro_arguments_are_skipped() {
    let edges = call_edges();
    assert!(
        !edges.iter().any(|(t, _)| t == "crate::in_custom"),
        "unknown macro args must be skipped: {edges:?}"
    );
}

#[test]
fn matches_pattern_and_guard_are_skipped() {
    let edges = call_edges();
    assert!(
        !edges.iter().any(|(t, _)| t == "crate::in_pattern"),
        "matches! pattern/guard must be skipped: {edges:?}"
    );
}

#[test]
fn multi_line_macro_call_records_its_own_line() {
    let edges = call_edges();
    let want = LIB_RS
        .lines()
        .position(|l| l.trim() == "on_later_line()")
        .unwrap() as i64
        + 1;
    let got = edges
        .iter()
        .find(|(t, _)| t == "crate::on_later_line")
        .and_then(|(_, l)| *l);
    assert_eq!(got, Some(want));
}

#[test]
fn incremental_sync_matches_fresh_reindex() {
    let (_t, fresh) = common::index_files(&FILES);
    let (_tmp, root, db_path) = common::index_repo(
        "lidx-macro-args-inc-",
        &[FILES[0], ("src/lib.rs", "pub fn x() {}\n")],
    );
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    std::fs::write(root.join("src/lib.rs"), LIB_RS).unwrap();
    indexer.sync_rel_paths(&["src/lib.rs".to_string()]).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let snap = common::golden::snapshot_edges(indexer.db(), gv).unwrap();
    common::assert_matches_fresh(&snap, &fresh);
}
