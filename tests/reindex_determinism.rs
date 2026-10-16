//! Issue #420: two fresh reindexes of an unchanged tree must produce the
//! same symbols, edges and unresolved references (ids and timestamps aside).
//!
//! The root cause was symbol insertion order: the symbol differ walked a
//! `HashMap`, so each fresh index inserted a file's symbols in a different
//! order, and every "lowest id wins" tie-break between same-qualname symbols
//! (merged TS declarations, `#[cfg]` twins in Rust) then picked a different
//! symbol. The fixtures below put many such twins in one file so a hash-order
//! regression shows up in a handful of runs.

mod common;

use rusqlite::Connection;
use std::path::Path;

const RUNS: usize = 4;

/// Everything an index records, minus autoincrement ids and timestamps,
/// with symbol endpoints identified by (file, qualname, start line) so two
/// same-qualname symbols are told apart.
fn normalized_output(db_path: &Path) -> Vec<String> {
    let conn = Connection::open(db_path).unwrap();
    let queries = [
        (
            "symbol",
            "SELECT f.path || '|' || s.qualname || '|' || s.kind || '|' || COALESCE(s.stable_id, '') \
                 || '|' || s.start_line || '|' || s.start_col \
             FROM symbols s JOIN files f ON f.id = s.file_id",
        ),
        (
            "edge",
            "SELECT f.path || '|' || COALESCE(a.qualname, '') || '@' || COALESCE(a.start_line, '') \
                 || '|' || e.kind || '|' || COALESCE(e.target_qualname, '') \
                 || '|' || COALESCE(b.qualname, '') || '@' || COALESCE(b.start_line, '') \
                 || '|' || COALESCE(e.resolution_kind, '') \
                 || '|' || COALESCE(CAST(ROUND(e.confidence * 1000) AS TEXT), '') \
                 || '|' || COALESCE(e.evidence_start_line, '') || '-' || COALESCE(e.evidence_end_line, '') \
             FROM edges e JOIN files f ON f.id = e.file_id \
             LEFT JOIN symbols a ON a.id = e.source_symbol_id \
             LEFT JOIN symbols b ON b.id = e.target_symbol_id",
        ),
        (
            "unresolved",
            "SELECT f.path || '|' || COALESCE(a.qualname, '') || '@' || COALESCE(a.start_line, '') \
                 || '|' || u.edge_kind || '|' || COALESCE(u.reference_name, '') || '|' || u.reason \
                 || '|' || COALESCE(u.evidence_start_line, '') \
             FROM unresolved_references u JOIN files f ON f.id = u.file_id \
             LEFT JOIN symbols a ON a.id = u.source_symbol_id",
        ),
    ];
    let mut out = Vec::new();
    for (label, sql) in queries {
        let mut stmt = conn.prepare(sql).unwrap();
        let rows: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        out.extend(rows.into_iter().map(|r| format!("{label}: {r}")));
    }
    out.sort();
    out
}

/// Symbol ids by insertion order: the tie-break the resolver reads back.
fn symbol_order(db_path: &Path) -> Vec<String> {
    let conn = Connection::open(db_path).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT f.path || '|' || s.qualname || '@' || s.start_line \
             FROM symbols s JOIN files f ON f.id = s.file_id ORDER BY s.id",
        )
        .unwrap();
    stmt.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn assert_fresh_indexes_identical(label: &str, files: &[(&str, &str)]) {
    let mut first: Option<(Vec<String>, Vec<String>)> = None;
    for run in 0..RUNS {
        let (_tmp, _root, db_path) = common::index_repo(&format!("lidx-det-{label}-"), files);
        let output = normalized_output(&db_path);
        let order = symbol_order(&db_path);
        assert!(!output.is_empty());
        match &first {
            None => first = Some((output, order)),
            Some((expected, expected_order)) => {
                assert_eq!(
                    &order, expected_order,
                    "{label}: symbol insertion order differs on run {run}"
                );
                assert_eq!(
                    &output, expected,
                    "{label}: normalized output differs on run {run}"
                );
            }
        }
    }
}

/// TS interface + const of the same name (declaration merging) across a file
/// with enough other symbols for hash order to differ run to run.
#[test]
fn ts_merged_declarations_index_identically() {
    let mut lib = String::new();
    let mut user = String::from("import * as lib from './lib';\n");
    for i in 0..30 {
        lib.push_str(&format!(
            "export interface Schema{i} {{ parse(): number; }}\n\
             export const Schema{i}: Schema{i} = {{ parse() {{ return {i}; }} }};\n"
        ));
        user.push_str(&format!(
            "export function use{i}(x: lib.Schema{i}) {{ return lib.Schema{i}.parse() + x.parse(); }}\n"
        ));
    }
    assert_fresh_indexes_identical("ts", &[("src/lib.ts", &lib), ("src/user.ts", &user)]);
}

/// Rust `#[cfg]` twins: two functions with one qualname in one file, called
/// and imported from elsewhere (the ripgrep `device_num` / `normalize_path`
/// shape).
#[test]
fn rust_cfg_twins_index_identically() {
    let mut lib = String::new();
    let mut user = String::from("mod lib;\n");
    for i in 0..30 {
        lib.push_str(&format!(
            "#[cfg(unix)]\npub fn twin{i}() -> u32 {{ 1 }}\n\
             #[cfg(not(unix))]\npub fn twin{i}() -> u32 {{ 2 }}\n"
        ));
        user.push_str(&format!(
            "pub fn call{i}() -> u32 {{ crate::lib::twin{i}() }}\n"
        ));
    }
    assert_fresh_indexes_identical(
        "rust",
        &[
            (
                "Cargo.toml",
                "[package]\nname = \"det\"\nversion = \"0.1.0\"\n",
            ),
            ("src/lib.rs", &user),
            ("src/lib/mod.rs", &lib),
        ],
    );
}
