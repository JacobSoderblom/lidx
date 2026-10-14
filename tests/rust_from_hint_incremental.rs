//! A `From` impl picked by the type a combinator hands a path
//! (`.map_err(T::from)`) depends on other files: the callee's declared return
//! type, a type alias and the impls themselves. After an incremental sync the
//! picked impl must be the one a fresh index of the final tree picks.

mod common;

use lidx::indexer::Indexer;
use std::collections::BTreeSet;

/// `(source qualname, target qualname, target signature)` of every
/// `REFERENCES` edge of the current graph version.
fn references(indexer: &Indexer) -> BTreeSet<(String, String, String)> {
    let gv = indexer.db().current_graph_version().unwrap();
    let conn = indexer.db().read_conn().unwrap();
    conn.prepare(
        "SELECT s.qualname, t.qualname, COALESCE(t.signature, '')
         FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
         JOIN symbols t ON t.id = e.target_symbol_id
         WHERE e.graph_version = ? AND e.kind = 'REFERENCES'",
    )
    .unwrap()
    .query_map([gv], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

const MANIFEST: (&str, &str) = (
    "Cargo.toml",
    "[package]\nname = \"conv\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
);

const TYPES: &str = "pub struct Failure;\npub struct E1;\npub struct E2;\n";
const IMPLS: &str = "impl From<E1> for Failure { fn from(_e: E1) -> Self { Failure } }\nimpl From<E2> for Failure { fn from(_e: E2) -> Self { Failure } }\n";

fn check(
    before: &[(&str, &str)],
    after: &[(&str, &str)],
    changed: &[&str],
    caller: &str,
    expect: &str,
) {
    let (_tmp, root, mut indexer) = common::live_indexer("lidx-from-hint-", before);
    let first = references(&indexer);
    assert!(
        first.iter().any(|(s, ..)| s == caller),
        "the caller binds before the edit: {first:#?}"
    );
    common::write_files(&root, after);
    let rels: Vec<String> = changed.iter().map(|c| c.to_string()).collect();
    indexer.sync_rel_paths(&rels).unwrap();
    common::assert_no_dangling_edge_targets(indexer.db());
    let synced = references(&indexer);
    let (_fresh_tmp, fresh_root, fresh_db) = common::index_repo("lidx-from-hint-fresh-", after);
    let fresh = references(&Indexer::new(fresh_root, fresh_db).unwrap());
    assert_eq!(synced, fresh, "incremental sync must equal a fresh index");
    if expect.is_empty() {
        assert!(!fresh.iter().any(|(s, ..)| s == caller), "{fresh:#?}");
    } else {
        assert!(
            fresh
                .iter()
                .any(|(s, _, sig)| s == caller && sig.contains(expect)),
            "{fresh:#?}"
        );
    }
}

#[test]
fn editing_the_impls_parameter_moves_the_binding() {
    let lib_before = format!("mod load;\n{TYPES}{IMPLS}");
    let lib_after = format!(
        "mod load;\n{TYPES}impl From<E1> for Failure {{ fn from(_e: E1) -> Self {{ Failure }} }}\nimpl From<()> for Failure {{ fn from(_e: ()) -> Self {{ Failure }} }}\n"
    );
    let load = "use crate::{E2, Failure};\npub fn run(r: Result<u8, E2>) -> Result<u8, Failure> {\n    r.map_err(Failure::from)\n}\n";
    let before = [MANIFEST, ("src/lib.rs", &lib_before), ("src/load.rs", load)];
    let after = [MANIFEST, ("src/lib.rs", &lib_after), ("src/load.rs", load)];
    check(&before, &after, &["src/lib.rs"], "crate::load::run", "");
}

#[test]
fn editing_a_type_alias_moves_the_binding() {
    let lib_before = format!("mod load;\n{TYPES}{IMPLS}pub type Res = Result<u8, E1>;\n");
    let lib_after = format!("mod load;\n{TYPES}{IMPLS}pub type Res = Result<u8, E2>;\n");
    let load = "use crate::{Failure, Res};\npub fn parse() -> Res {\n    Ok(1)\n}\npub fn run() -> Result<u8, Failure> {\n    parse().map_err(Failure::from)\n}\n";
    let before = [MANIFEST, ("src/lib.rs", &lib_before), ("src/load.rs", load)];
    let after = [MANIFEST, ("src/lib.rs", &lib_after), ("src/load.rs", load)];
    check(
        &before,
        &after,
        &["src/lib.rs"],
        "crate::load::run",
        "(_e: E2)",
    );
}

#[test]
fn editing_a_callees_return_type_in_another_file_moves_the_binding() {
    let lib = format!("mod build;\nmod load;\n{TYPES}{IMPLS}");
    let build_before = "use crate::E1;\npub fn build(_a: u8) -> Result<u8, E1> {\n    Ok(1)\n}\n";
    let build_after = "use crate::E2;\npub fn build(_a: u8) -> Result<u8, E2> {\n    Ok(1)\n}\n";
    let load = "use crate::Failure;\nuse crate::build::build;\npub fn run() -> Result<u8, Failure> {\n    build(1).map_err(Failure::from)\n}\n";
    let before = [
        MANIFEST,
        ("src/lib.rs", &lib),
        ("src/build.rs", build_before),
        ("src/load.rs", load),
    ];
    let after = [
        MANIFEST,
        ("src/lib.rs", &lib),
        ("src/build.rs", build_after),
        ("src/load.rs", load),
    ];
    check(
        &before,
        &after,
        &["src/build.rs"],
        "crate::load::run",
        "(_e: E2)",
    );
}

#[test]
fn editing_the_alias_a_bare_from_is_expected_to_return_moves_the_binding() {
    let types = "pub struct AErr;\npub struct BErr;\npub struct E1;\n\
         impl From<E1> for AErr { fn from(_e: E1) -> Self { AErr } }\n\
         impl From<E1> for BErr { fn from(_e: E1) -> Self { BErr } }\n";
    let lib_before = format!("mod load;\n{types}pub type Res<T> = Result<T, AErr>;\n");
    let lib_after = format!("mod load;\n{types}pub type Res<T> = Result<T, BErr>;\n");
    let load = "use crate::{E1, Res};\npub fn run(r: Result<u8, E1>) -> Res<u8> {\n    r.map_err(From::from)\n}\n";
    let before = [MANIFEST, ("src/lib.rs", &lib_before), ("src/load.rs", load)];
    let after = [MANIFEST, ("src/lib.rs", &lib_after), ("src/load.rs", load)];
    let (_tmp, root, mut indexer) = common::live_indexer("lidx-from-expected-", &before);
    let owner = |indexer: &Indexer| -> BTreeSet<String> {
        references(indexer)
            .into_iter()
            .filter(|(s, ..)| s == "crate::load::run")
            .map(|(_, t, _)| t)
            .collect()
    };
    assert_eq!(
        owner(&indexer),
        BTreeSet::from(["crate::AErr::from".to_string()])
    );
    common::write_files(&root, &after);
    indexer.sync_rel_paths(&["src/lib.rs".to_string()]).unwrap();
    let (_f, froot, fdb) = common::index_repo("lidx-from-expected-fresh-", &after);
    let fresh = Indexer::new(froot, fdb).unwrap();
    assert_eq!(references(&indexer), references(&fresh));
    assert_eq!(
        owner(&fresh),
        BTreeSet::from(["crate::BErr::from".to_string()])
    );
}
