//! Issues #123 and #124: C# overloads share one qualname, so a call must
//! bind by arity (or stay unresolved), and `new T(...)` must bind to the
//! matching `T..ctor` rather than the class.

use lidx::indexer::Indexer;
use lidx::rpc;
use rusqlite::params;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

const SOURCE: &str = r#"
namespace App
{
    public enum Kind { A, B }

    public static class KindExt
    {
        public static string ToDb(this Kind kind) { return "x"; }
        public static string ToDb(this Kind kind, int pad) { return "y"; }
    }

    public class Svc
    {
        public int Add(int a) { return a; }
        public int Add(int a, int b) { return a + b; }
        public int Amb(int a) { return a; }
        public int Amb(string a) { return 0; }
    }

    public class Widget
    {
        public Widget() { }
        public Widget(int size) { }
    }

    public class Plain { }

    public class Caller
    {
        public void CallOne(Svc svc) { svc.Add(1); }
        public void CallTwo(Svc svc) { svc.Add(1, 2); }
        public void CallAmbiguous(Svc svc) { svc.Amb(1); }
        public void CallExt(Kind kind) { kind.ToDb(); }
        public void CallExtPad(Kind kind) { kind.ToDb(5); }
        public void MakeWidget() { var w = new Widget(1); }
        public void MakePlain() { var p = new Plain(); }
        public void MakeDefault() { var w = new Widget(); }
    }
}
"#;

fn setup() -> (PathBuf, Indexer, i64) {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("lidx-csharp-overloads-{nanos}-{n}"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("App.cs"), SOURCE).unwrap();
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(dir.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    (dir, indexer, gv)
}

/// `(target qualname, target signature)` of every resolved CALLS edge whose
/// caller is `caller`.
fn resolved_targets(indexer: &Indexer, gv: i64, caller: &str) -> Vec<(String, Option<String>)> {
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT t.qualname, t.signature FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND s.qualname = ? AND e.graph_version = ?",
        )
        .unwrap();
    stmt.query_map(params![caller, gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn only_target(indexer: &Indexer, gv: i64, caller: &str) -> (String, Option<String>) {
    let mut targets = resolved_targets(indexer, gv, caller);
    assert_eq!(targets.len(), 1, "{caller}: {targets:?}");
    targets.remove(0)
}

#[test]
fn overloads_of_different_arity_each_get_their_own_caller() {
    let (dir, indexer, gv) = setup();
    let (qn, sig) = only_target(&indexer, gv, "App.Caller.CallOne");
    assert_eq!(qn, "App.Svc.Add");
    assert!(
        !sig.unwrap().contains("int b"),
        "1-arg call bound to 2-arg overload"
    );
    let (qn, sig) = only_target(&indexer, gv, "App.Caller.CallTwo");
    assert_eq!(qn, "App.Svc.Add");
    assert!(
        sig.unwrap().contains("int b"),
        "2-arg call bound to 1-arg overload"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn extension_method_overload_binds_by_arity_including_receiver() {
    let (dir, indexer, gv) = setup();
    let (qn, sig) = only_target(&indexer, gv, "App.Caller.CallExt");
    assert_eq!(qn, "App.KindExt.ToDb");
    assert!(
        !sig.unwrap().contains("pad"),
        "kind.ToDb() is the 1-param overload"
    );
    let (_, sig) = only_target(&indexer, gv, "App.Caller.CallExtPad");
    assert!(
        sig.unwrap().contains("pad"),
        "kind.ToDb(5) is the 2-param overload"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn same_arity_overloads_stay_unresolved() {
    let (dir, indexer, gv) = setup();
    assert!(resolved_targets(&indexer, gv, "App.Caller.CallAmbiguous").is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn read_symbol_on_overloaded_qualname_returns_every_overload() {
    let (dir, mut indexer, _) = setup();
    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualname": "App.Svc.Add"}),
    )
    .unwrap();
    let text = result.to_string();
    assert!(text.contains("int a, int b"), "{text}");
    assert!(text.contains("Add(int a)"), "{text}");
    let _ = std::fs::remove_dir_all(&dir);
}
