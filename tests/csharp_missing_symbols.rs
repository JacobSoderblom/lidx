//! Issue #247: enum members, positional record properties, indexers,
//! operators (including conversion operators), finalizers and delegates
//! produced no C# symbols.

use lidx::indexer::Indexer;
use lidx::rpc;
use rusqlite::params;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

const SVC: &str = r#"namespace N
{
    public enum Color { Red, Green = 2 }

    public record P(string Name, int Age);

    public readonly record struct Pt(int X, int Y);

    public delegate void Handler(int x);

    public class Svc
    {
        public event System.EventHandler Changed;
        public void Use() { }
        public int this[int i] => i;
        public static Svc operator +(Svc a, Svc b) => a;
        public static Svc operator +(Svc a, int b) => a;
        public static implicit operator int(Svc s) => 0;
        ~Svc() { }
    }
}
"#;

const USER: &str = r#"using N;
namespace M
{
    public class Painter
    {
        public Color Pick() { return Color.Red; }
    }
}
"#;

const EXTRA: &str = r#"using N;
using System;
namespace X
{
    public delegate void H<T>(T a);
    public delegate void H<T, U>(T a, U b);

    public class Conv
    {
        public static explicit operator System.Int32(Conv c) => 0;
        public static implicit operator List<System.Int32>(Conv c) => null;

        public void Go()
        {
            var a = nameof(Color.Red);
            var t = System.Console.Out;
            var n = System.Threading.CancellationToken.None;
            var d = DateTime.UtcNow;
            var c = Color.Red.ToString();
        }
    }
}
"#;

const PLAIN: &str = r#"namespace Q
{
    public class A
    {
        public int F;
        public void M() { }
        public int P { get; set; }
        public event System.EventHandler E;
    }
}
"#;

struct Fixture {
    dir: PathBuf,
    indexer: Indexer,
    gv: i64,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn setup() -> Fixture {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("lidx-csharp-missing-{nanos}-{n}"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("Svc.cs"), SVC).unwrap();
    std::fs::write(dir.join("Painter.cs"), USER).unwrap();
    std::fs::write(dir.join("Extra.cs"), EXTRA).unwrap();
    std::fs::write(dir.join("Plain.cs"), PLAIN).unwrap();
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(dir.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    Fixture { dir, indexer, gv }
}

/// `(kind, signature, source text)` of every symbol with this qualname.
fn symbols(fx: &Fixture, qualname: &str) -> Vec<(String, Option<String>, String)> {
    let conn = fx.indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.kind, s.signature, s.start_byte, s.end_byte, f.path FROM symbols s
             JOIN files f ON f.id = s.file_id
             WHERE s.qualname = ? AND s.graph_version = ? ORDER BY s.start_byte",
        )
        .unwrap();
    let rows: Vec<(String, Option<String>, usize, usize, String)> = stmt
        .query_map(params![qualname, fx.gv], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    rows.into_iter()
        .map(|(kind, sig, start, end, path)| {
            let text = std::fs::read_to_string(fx.dir.join(path)).unwrap();
            (kind, sig, text[start..end].to_string())
        })
        .collect()
}

fn only(fx: &Fixture, qualname: &str) -> (String, Option<String>, String) {
    let mut found = symbols(fx, qualname);
    assert_eq!(found.len(), 1, "{qualname}: {found:?}");
    found.remove(0)
}

fn contains(fx: &Fixture, source: &str, target: &str, count: usize) {
    let conn = fx.indexer.db().read_conn().unwrap();
    let n: usize = conn
        .query_row(
            "SELECT COUNT(*) FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CONTAINS' AND s.qualname = ? AND t.qualname = ?
               AND e.graph_version = ?",
            params![source, target, fx.gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, count, "CONTAINS {source} -> {target}");
}

fn call(fx: &Fixture, method: &str, params: serde_json::Value) -> serde_json::Value {
    let raw = rpc::call(
        fx.dir.clone(),
        fx.dir.join(".lidx").join(".lidx.sqlite"),
        method.to_string(),
        &params.to_string(),
        "1",
    )
    .unwrap();
    let envelope: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "{method}: {envelope}"
    );
    envelope["result"].clone()
}

#[test]
fn enum_members_are_constants_of_the_enum() {
    let fx = setup();
    let (kind, _, text) = only(&fx, "N.Color.Red");
    assert_eq!(kind, "const");
    assert_eq!(text, "Red");
    let (_, _, text) = only(&fx, "N.Color.Green");
    assert_eq!(text, "Green = 2");
    contains(&fx, "N.Color", "N.Color.Red", 1);
    contains(&fx, "N.Color", "N.Color.Green", 1);
}

#[test]
fn positional_record_parameters_are_properties() {
    let fx = setup();
    for (ty, member, text) in [
        ("N.P", "Name", "string Name"),
        ("N.P", "Age", "int Age"),
        ("N.Pt", "X", "int X"),
        ("N.Pt", "Y", "int Y"),
    ] {
        let qn = format!("{ty}.{member}");
        let (kind, _, src) = only(&fx, &qn);
        assert_eq!(kind, "property", "{qn}");
        assert_eq!(src, text, "{qn}");
        contains(&fx, ty, &qn, 1);
    }
}

#[test]
fn delegate_is_a_namespace_level_type() {
    let fx = setup();
    let (kind, _, text) = only(&fx, "N.Handler");
    assert_eq!(kind, "delegate");
    assert_eq!(text, "public delegate void Handler(int x);");
    contains(&fx, "N", "N.Handler", 1);
}

#[test]
fn indexer_operators_and_finalizer_are_members_of_the_type() {
    let fx = setup();
    let (kind, _, text) = only(&fx, "N.Svc.this[]");
    assert_eq!(kind, "property");
    assert_eq!(text, "public int this[int i] => i;");
    contains(&fx, "N.Svc", "N.Svc.this[]", 1);

    let (kind, _, text) = only(&fx, "N.Svc.~Svc");
    assert_eq!(kind, "method");
    assert_eq!(text, "~Svc() { }");
    contains(&fx, "N.Svc", "N.Svc.~Svc", 1);

    let (kind, _, text) = only(&fx, "N.Svc.implicit operator int");
    assert_eq!(kind, "method");
    assert_eq!(text, "public static implicit operator int(Svc s) => 0;");
    contains(&fx, "N.Svc", "N.Svc.implicit operator int", 1);
}

#[test]
fn operators_differing_only_in_operand_types_stay_distinct() {
    let fx = setup();
    let plus = symbols(&fx, "N.Svc.operator +");
    assert_eq!(plus.len(), 2, "{plus:?}");
    assert!(plus.iter().all(|(k, _, _)| k == "method"));
    assert_ne!(plus[0].1, plus[1].1, "signatures must differ");
    assert_eq!(
        plus[0].2,
        "public static Svc operator +(Svc a, Svc b) => a;"
    );
    assert_eq!(
        plus[1].2,
        "public static Svc operator +(Svc a, int b) => a;"
    );
    let conn = fx.indexer.db().read_conn().unwrap();
    let ids: usize = conn
        .query_row(
            "SELECT COUNT(DISTINCT stable_id) FROM symbols
             WHERE qualname = 'N.Svc.operator +' AND graph_version = ?",
            params![fx.gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(ids, 2);
    contains(&fx, "N.Svc", "N.Svc.operator +", 2);
}

#[test]
fn existing_members_are_unaffected() {
    let fx = setup();
    only(&fx, "N.Svc.Changed");
    only(&fx, "N.Svc.Use");
    only(&fx, "N.Color");
    only(&fx, "N.P");
}

#[test]
fn outline_lists_every_new_symbol() {
    let fx = setup();
    let result = call(&fx, "outline", serde_json::json!({"path": "Svc.cs"}));
    let text = result.to_string();
    for qn in [
        "N.Color.Red",
        "N.Color.Green",
        "N.P.Name",
        "N.P.Age",
        "N.Pt.X",
        "N.Pt.Y",
        "N.Handler",
        "N.Svc.this[]",
        "N.Svc.operator +",
        "N.Svc.implicit operator int",
        "N.Svc.~Svc",
    ] {
        assert!(text.contains(&format!("\"{qn}\"")), "{qn} missing: {text}");
    }
}

#[test]
fn read_symbol_returns_the_declaration_source() {
    let fx = setup();
    for (qn, expected) in [
        ("N.Color.Red", "Red"),
        ("N.P.Name", "string Name"),
        ("N.Handler", "public delegate void Handler(int x);"),
        ("N.Svc.this[]", "public int this[int i] => i;"),
        ("N.Svc.~Svc", "~Svc() { }"),
        (
            "N.Svc.implicit operator int",
            "public static implicit operator int(Svc s) => 0;",
        ),
    ] {
        let result = call(&fx, "read_symbol", serde_json::json!({"qualname": qn}));
        let text = result.to_string();
        let escaped = serde_json::to_string(expected).unwrap();
        let escaped = &escaped[1..escaped.len() - 1];
        assert!(text.contains(escaped), "{qn}: {text}");
    }
}

#[test]
fn enum_member_reference_from_another_file_resolves_to_the_member() {
    let fx = setup();
    let conn = fx.indexer.db().read_conn().unwrap();
    let n: usize = conn
        .query_row(
            "SELECT COUNT(*) FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE s.qualname = 'M.Painter.Pick' AND t.qualname = 'N.Color.Red'
               AND e.graph_version = ?",
            params![fx.gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 1);
}

#[test]
fn generic_delegates_of_different_arity_stay_distinct() {
    let fx = setup();
    assert_eq!(symbols(&fx, "X.H").len(), 2);
    let conn = fx.indexer.db().read_conn().unwrap();
    let ids: usize = conn
        .query_row(
            "SELECT COUNT(DISTINCT stable_id) FROM symbols
             WHERE qualname = 'X.H' AND graph_version = ?",
            params![fx.gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(ids, 2);
}

#[test]
fn conversion_operator_names_contain_no_dot() {
    let fx = setup();
    only(&fx, "X.Conv.explicit operator System_Int32");
    only(&fx, "X.Conv.implicit operator List<System_Int32>");
}

#[test]
fn member_reads_skip_nameof_and_qualified_names_and_never_bind_external_stubs() {
    let fx = setup();
    let conn = fx.indexer.db().read_conn().unwrap();
    // Only the `Color.Red.ToString()` read survives, resolved; the `nameof`
    // one, `System.Console.Out` and `DateTime.UtcNow` emit nothing.
    let mut stmt = conn
        .prepare(
            "SELECT t.qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             LEFT JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'USES' AND s.qualname = 'X.Conv.Go' AND e.graph_version = ?",
        )
        .unwrap();
    let targets: Vec<Option<String>> = stmt
        .query_map(params![fx.gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(targets, vec![Some("N.Color.Red".to_string())]);
    // Unresolved reads stay retryable but never bind to `ext:` stubs.
    let stubs: usize = conn
        .query_row(
            "SELECT COUNT(*) FROM symbols WHERE kind = 'external' AND qualname LIKE 'ext:%UtcNow'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stubs, 0);
}

#[test]
fn operators_and_finalizers_are_not_dead_symbols() {
    let mut fx = setup();
    let result = rpc::handle_method(
        &mut fx.indexer,
        "dead_symbols",
        serde_json::json!({"include_unused_imports": false, "include_orphan_tests": false}),
    )
    .unwrap();
    let names: Vec<String> = result["dead_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["qualname"].as_str().map(str::to_string))
        .collect();
    assert!(
        !names
            .iter()
            .any(|q| q.contains("operator ") || q.contains("~Svc")),
        "{names:?}"
    );
}

#[test]
fn ordinary_file_symbols_are_unchanged() {
    let fx = setup();
    let conn = fx.indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT s.qualname FROM symbols s JOIN files f ON f.id = s.file_id
             WHERE f.path = 'Plain.cs' AND s.graph_version = ? ORDER BY s.qualname",
        )
        .unwrap();
    let names: Vec<String> = stmt
        .query_map(params![fx.gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        names,
        ["Plain", "Q", "Q.A", "Q.A.E", "Q.A.F", "Q.A.M", "Q.A.P"]
    );
}
