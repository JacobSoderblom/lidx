//! Issue #212: two declarations in one file that share (qualname, signature,
//! kind) used to collapse into one symbol in the diff's `HashMap` collect,
//! on a fresh index as well as an incremental one. Each shape below must
//! keep every declaration, with its own span and its own outgoing edges,
//! and stable ids must not churn across no-op reindexes, blank-line
//! insertions or declaration moves.

mod common;

use lidx::indexer::Indexer;
use lidx::indexer::extract::SymbolInput;
use lidx::indexer::stable_id::compute_stable_symbol_id;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const CSHARP: &str = r#"namespace N
{
    public class Box<T>
    {
        public bool IsEmpty { get { return false; } }
        public void Put(T a) { }
        public void Put(T a, int b) { }
    }

    public class Box<T, U>
    {
        public bool IsEmpty => true;
    }

    public class Box
    {
        public bool IsEmpty { get; set; }
    }
}
"#;

const RUST: &str = r#"fn helper() {}

fn other_helper() {}

#[cfg(target_os = "macos")]
pub fn fix_path_env() {
    helper();
}

#[cfg(not(target_os = "macos"))]
pub fn fix_path_env() {}

pub struct S;

impl S {
    pub fn new() -> S {
        other_helper();
        S
    }
}

pub trait T {
    fn make() -> Self;
}

impl T for S {
    fn make() -> S {
        S
    }
}

pub trait U {
    fn new() -> S;
}

impl U for S {
    fn new() -> S {
        helper();
        S
    }
}
"#;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Sym {
    kind: String,
    qualname: String,
    stable_id: String,
    start_line: i64,
    end_line: i64,
}

fn open(db_path: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(db_path).unwrap()
}

fn version(indexer: &Indexer) -> i64 {
    indexer.db().current_graph_version().unwrap()
}

fn symbols(db_path: &Path, gv: i64, file: &str) -> Vec<Sym> {
    let conn = open(db_path);
    let mut stmt = conn
        .prepare(
            "SELECT s.kind, s.qualname, s.stable_id, s.start_line, s.end_line
             FROM symbols s JOIN files f ON f.id = s.file_id
             WHERE f.path = ? AND s.graph_version = ?
             ORDER BY s.start_line, s.qualname",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![file, gv], |r| {
        Ok(Sym {
            kind: r.get(0)?,
            qualname: r.get(1)?,
            stable_id: r.get(2)?,
            start_line: r.get(3)?,
            end_line: r.get(4)?,
        })
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

fn named<'a>(syms: &'a [Sym], kind: &str, qualname: &str) -> Vec<&'a Sym> {
    syms.iter()
        .filter(|s| s.kind == kind && s.qualname == qualname)
        .collect()
}

/// Target qualnames of `kind` edges whose source is the symbol at `line`.
fn edges_from(
    db_path: &Path,
    gv: i64,
    file: &str,
    qualname: &str,
    line: i64,
    kind: &str,
) -> Vec<String> {
    let conn = open(db_path);
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(t.qualname, e.target_qualname)
             FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN files f ON f.id = s.file_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE f.path = ? AND s.qualname = ? AND s.start_line = ?
               AND e.kind = ? AND e.graph_version = ?
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![file, qualname, line, kind, gv], |r| {
        r.get::<_, String>(0)
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

/// Start lines of the symbols `kind`-`CONTAINS`-targeted by the symbol at `line`.
fn contains_lines(db_path: &Path, gv: i64, file: &str, qualname: &str, line: i64) -> Vec<i64> {
    let conn = open(db_path);
    let mut stmt = conn
        .prepare(
            "SELECT t.start_line
             FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             JOIN files f ON f.id = s.file_id
             WHERE f.path = ? AND s.qualname = ? AND s.start_line = ?
               AND e.kind = 'CONTAINS' AND e.graph_version = ?
             ORDER BY 1",
        )
        .unwrap();
    stmt.query_map(rusqlite::params![file, qualname, line, gv], |r| {
        r.get::<_, i64>(0)
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

fn indexed(files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf, PathBuf, Indexer) {
    let tmp = tempfile::Builder::new()
        .prefix("lidx-dupid-")
        .tempdir()
        .unwrap();
    let root = tmp.path().to_path_buf();
    common::write_files(&root, files);
    let db_path = root.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(root.clone(), db_path.clone()).unwrap();
    indexer.reindex().unwrap();
    (tmp, root, db_path, indexer)
}

fn assert_distinct_ids(syms: &[Sym]) {
    let mut by_id: BTreeMap<&str, &Sym> = BTreeMap::new();
    for s in syms {
        if let Some(prev) = by_id.insert(&s.stable_id, s) {
            panic!("stable id shared by {prev:?} and {s:?}");
        }
    }
}

fn check_csharp(db_path: &Path, gv: i64) {
    let syms = symbols(db_path, gv, "Box.cs");
    assert_distinct_ids(&syms);
    let boxes = named(&syms, "class", "N.Box");
    assert_eq!(boxes.len(), 3, "three Box declarations: {syms:#?}");
    let props = named(&syms, "property", "N.Box.IsEmpty");
    assert_eq!(props.len(), 3, "three IsEmpty properties: {syms:#?}");
    assert_eq!(
        props.iter().map(|p| p.start_line).collect::<Vec<_>>(),
        vec![5, 12, 17]
    );
    assert_eq!(
        boxes
            .iter()
            .map(|b| (b.start_line, b.end_line))
            .collect::<Vec<_>>(),
        vec![(3, 8), (10, 13), (15, 18)]
    );
    // Each class contains exactly its own IsEmpty (and only the generic
    // one contains the Put overloads).
    assert_eq!(
        contains_lines(db_path, gv, "Box.cs", "N.Box", 3),
        vec![5, 6, 7]
    );
    assert_eq!(contains_lines(db_path, gv, "Box.cs", "N.Box", 10), vec![12]);
    assert_eq!(contains_lines(db_path, gv, "Box.cs", "N.Box", 15), vec![17]);
    // Put overloads unaffected.
    assert_eq!(named(&syms, "method", "N.Box.Put").len(), 2);
}

fn check_rust(db_path: &Path, gv: i64) {
    let syms = symbols(db_path, gv, "src/lib.rs");
    assert_distinct_ids(&syms);
    let twins = named(&syms, "function", "crate::fix_path_env");
    assert_eq!(twins.len(), 2, "both cfg variants stored: {syms:#?}");
    let macos = twins
        .iter()
        .find(|s| s.start_line == 6)
        .expect("macos variant");
    assert_eq!((macos.start_line, macos.end_line), (6, 8));
    assert_eq!(
        edges_from(db_path, gv, "src/lib.rs", "crate::fix_path_env", 6, "CALLS"),
        vec!["crate::helper".to_string()],
        "the real body keeps its outgoing edge"
    );
    assert!(
        edges_from(
            db_path,
            gv,
            "src/lib.rs",
            "crate::fix_path_env",
            11,
            "CALLS"
        )
        .is_empty()
    );
    // Inherent and trait-impl `S::new`.
    let news = named(&syms, "method", "crate::S::new");
    assert_eq!(news.len(), 2, "inherent + trait `new`: {syms:#?}");
    let inherent = news.iter().find(|s| s.start_line == 16).unwrap();
    let traity = news.iter().find(|s| s.start_line == 37).unwrap();
    assert_ne!(inherent.stable_id, traity.stable_id);
    assert_eq!(
        edges_from(db_path, gv, "src/lib.rs", "crate::S::new", 16, "CALLS"),
        vec!["crate::other_helper".to_string()]
    );
    assert_eq!(
        edges_from(db_path, gv, "src/lib.rs", "crate::S::new", 37, "CALLS"),
        vec!["crate::helper".to_string()]
    );
}

#[test]
fn csharp_generic_arity_types_are_distinct_on_fresh_index() {
    let (_t, _r, db, ix) = indexed(&[("Box.cs", CSHARP)]);
    check_csharp(&db, version(&ix));
}

#[test]
fn rust_cfg_variants_and_impl_blocks_are_distinct_on_fresh_index() {
    let (_t, _r, db, ix) = indexed(&[("src/lib.rs", RUST)]);
    check_rust(&db, version(&ix));
}

#[test]
fn incremental_sync_adds_twins_and_matches_fresh_index() {
    // Start with a single variant of each, then add the twins by sync.
    let csharp_v1 = "namespace N\n{\n    public class Box\n    {\n        public bool IsEmpty { get; set; }\n    }\n}\n";
    let rust_v1 = "fn helper() {}\n";
    let (_t, root, db, mut ix) = indexed(&[("Box.cs", csharp_v1), ("src/lib.rs", rust_v1)]);
    common::write_files(&root, &[("Box.cs", CSHARP), ("src/lib.rs", RUST)]);
    ix.sync_rel_paths(&["Box.cs".to_string(), "src/lib.rs".to_string()])
        .unwrap();
    let gv = version(&ix);
    check_csharp(&db, gv);
    check_rust(&db, gv);
    common::assert_no_dangling_edge_targets(ix.db());

    let (_t2, _r2, fresh_db, fresh_ix) = indexed(&[("Box.cs", CSHARP), ("src/lib.rs", RUST)]);
    let fgv = version(&fresh_ix);
    for file in ["Box.cs", "src/lib.rs"] {
        assert_eq!(
            symbols(&db, gv, file),
            symbols(&fresh_db, fgv, file),
            "incremental == fresh for {file}"
        );
    }
}

#[test]
fn incremental_removal_of_a_twin_keeps_the_other() {
    let (_t, root, db, mut ix) = indexed(&[("Box.cs", CSHARP), ("src/lib.rs", RUST)]);
    let rust_one = RUST.replace(
        "#[cfg(not(target_os = \"macos\"))]\npub fn fix_path_env() {}\n",
        "",
    );
    common::write_files(&root, &[("src/lib.rs", &rust_one)]);
    ix.sync_rel_paths(&["src/lib.rs".to_string()]).unwrap();
    let syms = symbols(&db, version(&ix), "src/lib.rs");
    let twins = named(&syms, "function", "crate::fix_path_env");
    assert_eq!(twins.len(), 1);
    assert_eq!(
        edges_from(
            &db,
            version(&ix),
            "src/lib.rs",
            "crate::fix_path_env",
            6,
            "CALLS"
        ),
        vec!["crate::helper".to_string()]
    );
}

#[test]
fn stable_ids_do_not_churn() {
    let (_t, root, db, mut ix) = indexed(&[("Box.cs", CSHARP), ("src/lib.rs", RUST)]);
    let ids = |ix: &Indexer| -> Vec<BTreeMap<(String, String, String), i64>> {
        ["Box.cs", "src/lib.rs"]
            .iter()
            .map(|f| {
                // (kind, qualname, stable_id) -> count; line numbers excluded.
                let mut m = BTreeMap::new();
                for s in symbols(&db, version(ix), f) {
                    *m.entry((s.kind, s.qualname, s.stable_id)).or_insert(0) += 1;
                }
                m
            })
            .collect()
    };
    let before = ids(&ix);

    // No-op reindex.
    ix.reindex().unwrap();
    assert_eq!(before, ids(&ix), "no-op reindex");

    // Blank lines above every declaration.
    let padded_cs = CSHARP.replace("    public", "\n\n    public");
    let padded_rs = format!("\n\n\n{}", RUST.replace("pub fn", "\n\npub fn"));
    common::write_files(&root, &[("Box.cs", &padded_cs), ("src/lib.rs", &padded_rs)]);
    ix.sync_rel_paths(&["Box.cs".to_string(), "src/lib.rs".to_string()])
        .unwrap();
    assert_eq!(before, ids(&ix), "blank lines inserted");

    // Move declarations: swap the cfg twins and reverse the C# classes.
    let swapped_rs = RUST.replace(
        "#[cfg(target_os = \"macos\")]\npub fn fix_path_env() {\n    helper();\n}\n\n#[cfg(not(target_os = \"macos\"))]\npub fn fix_path_env() {}\n",
        "#[cfg(not(target_os = \"macos\"))]\npub fn fix_path_env() {}\n\n#[cfg(target_os = \"macos\")]\npub fn fix_path_env() {\n    helper();\n}\n",
    );
    assert_ne!(swapped_rs, RUST);
    let reordered_cs = "namespace N\n{\n    public class Box\n    {\n        public bool IsEmpty { get; set; }\n    }\n\n    public class Box<T, U>\n    {\n        public bool IsEmpty => true;\n    }\n\n    public class Box<T>\n    {\n        public bool IsEmpty { get { return false; } }\n        public void Put(T a) { }\n        public void Put(T a, int b) { }\n    }\n}\n";
    common::write_files(
        &root,
        &[("Box.cs", reordered_cs), ("src/lib.rs", &swapped_rs)],
    );
    ix.sync_rel_paths(&["Box.cs".to_string(), "src/lib.rs".to_string()])
        .unwrap();
    assert_eq!(before, ids(&ix), "declarations moved");
}

/// A database written before the identity change holds one row per
/// collapsed group under the old id; reindexing must replace it with the
/// full set and leave edges that targeted the group resolved.
#[test]
fn database_indexed_with_old_ids_reindexes_cleanly() {
    let user = "pub fn go() { crate::fix_path_env(); }\n";
    let (_t, _root, db, mut ix) = indexed(&[("src/lib.rs", RUST), ("src/user.rs", user)]);
    let gv = version(&ix);
    // Rewind to the pre-fix shape: only the last twin, under the old hash.
    {
        let conn = open(&db);
        conn.execute(
            "DELETE FROM symbols WHERE qualname = 'crate::fix_path_env' AND start_line = 6
               AND graph_version = ?",
            [gv],
        )
        .unwrap();
        let signature: Option<String> = conn
            .query_row(
                "SELECT signature FROM symbols WHERE qualname = 'crate::fix_path_env'
                   AND graph_version = ?",
                [gv],
                |r| r.get(0),
            )
            .unwrap();
        let old_id = compute_stable_symbol_id(&SymbolInput {
            kind: "function".into(),
            name: "fix_path_env".into(),
            qualname: "crate::fix_path_env".into(),
            start_line: 0,
            start_col: 0,
            end_line: 0,
            end_col: 0,
            start_byte: 0,
            end_byte: 0,
            signature,
            docstring: None,
            identity: None,
        });
        conn.execute(
            "UPDATE symbols SET stable_id = ?
             WHERE qualname = 'crate::fix_path_env' AND graph_version = ?",
            rusqlite::params![old_id, gv],
        )
        .unwrap();
    }
    // Force re-extraction of every file, as an extractor-version bump does.
    ix.db().set_meta_i64("extractor_version", -1).unwrap();
    ix.reindex().unwrap();
    let gv = version(&ix);
    let syms = symbols(&db, gv, "src/lib.rs");
    assert_eq!(named(&syms, "function", "crate::fix_path_env").len(), 2);
    assert_distinct_ids(&syms);
    common::assert_no_dangling_edge_targets(ix.db());
    let resolved: i64 = open(&db)
        .query_row(
            "SELECT COUNT(*) FROM edges e JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND t.qualname = 'crate::fix_path_env'
               AND e.graph_version = ?",
            [gv],
            |r| r.get(0),
        )
        .unwrap();
    assert!(resolved >= 1, "caller edge must stay resolved");
}

fn rpc(ix: &mut Indexer, method: &str, params: serde_json::Value) -> serde_json::Value {
    lidx::rpc::handle_method(ix, method, params).unwrap()
}

#[test]
fn outline_and_read_symbol_return_every_twin() {
    let (_t, _r, _db, mut ix) = indexed(&[("Box.cs", CSHARP), ("src/lib.rs", RUST)]);

    // outline lists each declaration with its own span.
    let outline = rpc(&mut ix, "outline", serde_json::json!({"path": "Box.cs"}));
    let boxes: Vec<(i64, i64)> = outline["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["qualname"] == "N.Box" && e["kind"] == "class")
        .map(|e| {
            (
                e["start_line"].as_i64().unwrap(),
                e["end_line"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(boxes, vec![(3, 8), (10, 13), (15, 18)], "{outline}");
    let outline = rpc(
        &mut ix,
        "outline",
        serde_json::json!({"path": "src/lib.rs"}),
    );
    let fixes = outline["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["qualname"] == "crate::fix_path_env")
        .count();
    assert_eq!(fixes, 2, "{outline}");

    // read_symbol by qualname returns all of them, each with span and source.
    let read = rpc(
        &mut ix,
        "read_symbol",
        serde_json::json!({"qualname": "N.Box"}),
    );
    assert_eq!(read["overloaded"], true, "{read}");
    assert_eq!(read["count"], 3);
    let lines: Vec<i64> = read["overloads"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["start_line"].as_i64().unwrap())
        .collect();
    assert_eq!(lines, vec![3, 10, 15]);

    let read = rpc(
        &mut ix,
        "read_symbol",
        serde_json::json!({"qualname": "crate::fix_path_env"}),
    );
    assert_eq!(read["overloaded"], true, "{read}");
    assert_eq!(read["count"], 2);
    let sources: Vec<String> = read["overloads"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["source"].as_str().unwrap().to_string())
        .collect();
    assert!(
        sources.iter().any(|s| s.contains("helper();")),
        "the real macos body is readable: {read}"
    );

    let read = rpc(
        &mut ix,
        "read_symbol",
        serde_json::json!({"qualname": "crate::S::new"}),
    );
    assert_eq!(read["count"], 2, "{read}");
}

#[test]
fn metrics_belong_to_their_own_twin() {
    let (_t, _r, db, ix) = indexed(&[("Box.cs", CSHARP), ("src/lib.rs", RUST)]);
    let gv = version(&ix);
    let conn = open(&db);
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname, s.start_line, m.loc
             FROM symbol_metrics m JOIN symbols s ON s.id = m.symbol_id
             WHERE s.graph_version = ?
               AND s.qualname IN ('crate::fix_path_env', 'N.Box.Put', 'crate::S::new')
             ORDER BY s.qualname, s.start_line",
        )
        .unwrap();
    let rows: Vec<(String, i64, i64)> = stmt
        .query_map([gv], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        rows,
        vec![
            ("N.Box.Put".to_string(), 6, 1),
            ("N.Box.Put".to_string(), 7, 1),
            ("crate::S::new".to_string(), 16, 4),
            ("crate::S::new".to_string(), 37, 4),
            ("crate::fix_path_env".to_string(), 6, 3),
            ("crate::fix_path_env".to_string(), 11, 1),
        ],
        "each twin/overload has its own metric row"
    );
}

/// Extracts `source` and returns each `m` method's identity.
fn method_identities(source: &str) -> Vec<lidx::indexer::extract::SymbolInput> {
    use lidx::indexer::extract::LanguageExtractor;
    let mut extractor = lidx::indexer::rust::RustExtractor::new().unwrap();
    let extracted = extractor.extract(source, "crate").unwrap();
    extracted
        .symbols
        .into_iter()
        .filter(|s| s.name == "m")
        .collect()
}

#[test]
fn impl_blocks_differing_in_generics_or_where_do_not_fall_to_dup() {
    let src = r#"
pub struct S<T>(T);
pub trait Tr { fn m(&self); }

impl<T> Tr for S<T> { fn m(&self) {} }
impl Tr for S<u8> { fn m(&self) {} }
impl<T> Tr for S<T> where T: Copy { fn m(&self) {} }
impl<T> Tr for S<T> where T: Clone { fn m(&self) {} }
impl<T> S<T> { pub fn m(&self) {} }
impl S<u16> { pub fn m(&self) {} }
"#;
    let methods = method_identities(src);
    // Six impl methods plus the trait's own declaration.
    assert_eq!(methods.len(), 7);
    assert!(
        methods
            .iter()
            .all(|s| s.identity.as_ref().is_none_or(|i| i.dup.is_none())),
        "identity alone tells these apart: {methods:#?}"
    );
    let ids: std::collections::BTreeSet<String> =
        methods.iter().map(compute_stable_symbol_id).collect();
    assert_eq!(ids.len(), 7, "distinct stable ids");

    // And they survive indexing as six rows with no collision fallback.
    let (_t, _r, db, ix) = indexed(&[("src/lib.rs", src)]);
    let syms = symbols(&db, version(&ix), "src/lib.rs");
    assert_eq!(
        syms.iter().filter(|s| s.qualname == "crate::S::m").count(),
        6
    );
    assert_distinct_ids(&syms);
}

#[test]
fn a_pipe_inside_a_cfg_predicate_cannot_alias_another_identity() {
    use lidx::indexer::extract::DeclIdentity;
    let base = SymbolInput {
        kind: "function".into(),
        name: "f".into(),
        qualname: "crate::f".into(),
        start_line: 1,
        start_col: 0,
        end_line: 1,
        end_col: 0,
        start_byte: 0,
        end_byte: 0,
        signature: None,
        docstring: None,
        identity: None,
    };
    let with = |cfg: &[&str]| SymbolInput {
        identity: Some(DeclIdentity {
            cfg: cfg.iter().map(|s| s.to_string()).collect(),
            ..DeclIdentity::default()
        }),
        ..base.clone()
    };
    assert_ne!(
        compute_stable_symbol_id(&with(&["a|b"])),
        compute_stable_symbol_id(&with(&["a", "b"]))
    );
    assert_ne!(
        compute_stable_symbol_id(&with(&["a&b"])),
        compute_stable_symbol_id(&with(&["a", "b"]))
    );
    // An ordinary symbol's id is what origin/main computed: hash of
    // qualname, NUL, signature (none), NUL, kind.
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"crate::f\0\0function");
    assert_eq!(
        compute_stable_symbol_id(&base),
        format!("sym_{}", &hasher.finalize().to_hex()[..16])
    );
}

#[test]
fn twin_route_handlers_each_keep_their_own_route_edge() {
    let src = r#"use actix_web::get;

#[cfg(unix)]
#[get("/unix")]
pub async fn handler() {}

#[cfg(not(unix))]
#[get("/other")]
pub async fn handler() {}
"#;
    let (_t, _r, db, ix) = indexed(&[("src/lib.rs", src)]);
    let gv = version(&ix);
    let conn = open(&db);
    let mut stmt = conn
        .prepare(
            "SELECT s.start_line, e.target_qualname
             FROM edges e JOIN symbols s ON s.id = e.source_symbol_id
             WHERE e.kind = 'HTTP_ROUTE' AND s.qualname = 'crate::handler'
               AND e.graph_version = ? ORDER BY s.start_line",
        )
        .unwrap();
    let rows: Vec<(i64, String)> = stmt
        .query_map([gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_ne!(rows[0].0, rows[1].0, "routes belong to different twins");
    assert!(
        rows[0].1.contains("unix") && rows[1].1.contains("other"),
        "{rows:?}"
    );
}
