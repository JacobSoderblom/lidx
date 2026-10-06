//! Function paths passed as values inside the arguments of known std macros
//! (`assert_eq!(xs.iter().map(parse).count(), 2)`, `vec![f, g]`) are recorded
//! as calls, with the same skip rules as outside a macro: lowercase last
//! segment, not a local binding or parameter.

mod common;

use lidx::indexer::Indexer;

const LIB_RS: &str = r#"
pub mod m {
    pub fn conv(x: u8) -> u8 { x }
}
pub fn parse(x: &str) -> u8 { x.len() as u8 }
pub fn fa() -> u8 { 1 }
pub fn fb() -> u8 { 2 }
pub fn local() -> u8 { 3 }
pub fn param() -> u8 { 4 }
pub fn in_custom() -> u8 { 5 }
pub fn in_matches(x: &str) -> u8 { x.len() as u8 }
pub fn in_pattern() -> u8 { 6 }
pub struct Bag;
impl Bag {
    pub fn make(x: u8) -> u8 { x }
}
macro_rules! custom { ($e:expr) => { $e }; }
pub fn run(xs: Vec<&str>, param: u8) {
    assert_eq!(xs.iter().map(|s| *s).map(parse).count(), 2);
    let _v: Vec<fn() -> u8> = vec![fa, fb];
    let local = 1u8;
    println!("{} {}", local, param);
    let _a = format!("{:?}", [1u8].iter().map(m::conv).count());
    let _b = format!("{:?}", [1u8].iter().map(Bag::make).count());
    let _c = matches!(xs.iter().map(|s| in_matches(s)).next(), Some(_));
    let _n = format!("{}", xs.iter().map(|x| format!("{}", x)).count());
    let _d = format!("{:?}", Some(None::<u8>));
    assert_eq!(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst);
    custom!(xs.iter().map(in_custom).count());
    let _m = matches!(1u8, 1 | 2 if [1u8].iter().map(in_pattern).count() > 0);
}
"#;

const FILES: [(&str, &str); 2] = [
    ("Cargo.toml", "[package]\nname = \"x\"\n"),
    ("src/lib.rs", LIB_RS),
];

fn call_targets() -> Vec<String> {
    let (_tmp, root, db_path) = common::index_repo("lidx-macro-fnref-", &FILES);
    let indexer = Indexer::new(root, db_path).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(t.qualname, e.target_qualname, '')
             FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.kind = 'CALLS' AND s.qualname = 'crate::run'",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

#[test]
fn fn_paths_in_known_macro_arguments_are_calls() {
    let targets = call_targets();
    for want in [
        "crate::parse",
        "crate::fa",
        "crate::fb",
        "crate::m::conv",
        "crate::Bag::make",
    ] {
        assert!(
            targets.iter().any(|t| t == want),
            "missing {want}: {targets:?}"
        );
    }
}

#[test]
fn locals_params_and_constructors_in_macro_arguments_are_not_calls() {
    let targets = call_targets();
    for bad in ["crate::local", "crate::param", "local", "param", "s", "x"] {
        assert!(
            !targets.iter().any(|t| t == bad),
            "bogus {bad}: {targets:?}"
        );
    }
    for t in &targets {
        let last = t.rsplit("::").next().unwrap();
        assert!(
            !["None", "SeqCst", "Some"].contains(&last),
            "value recorded as a call: {t}"
        );
    }
}

#[test]
fn unknown_macros_and_matches_patterns_still_skip_fn_refs() {
    let targets = call_targets();
    assert!(
        !targets.iter().any(|t| t == "crate::in_custom"),
        "{targets:?}"
    );
    assert!(
        !targets.iter().any(|t| t == "crate::in_pattern"),
        "{targets:?}"
    );
}

#[test]
fn incremental_sync_matches_fresh_reindex() {
    let (_t, fresh) = common::index_files(&FILES);
    let (_tmp, root, db_path) = common::index_repo(
        "lidx-macro-fnref-inc-",
        &[FILES[0], ("src/lib.rs", "pub fn x() {}\n")],
    );
    let mut indexer = Indexer::new(root.clone(), db_path).unwrap();
    std::fs::write(root.join("src/lib.rs"), LIB_RS).unwrap();
    indexer.sync_rel_paths(&["src/lib.rs".to_string()]).unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    let snap = common::golden::snapshot_edges(indexer.db(), gv).unwrap();
    common::assert_matches_fresh(&snap, &fresh);
}
