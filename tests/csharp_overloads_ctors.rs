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
        static Widget() { }
        public Widget() { }
        public Widget(int size) { }
    }

    public class Plain { }

    public class Holder
    {
        private Widget _field = new(7);
        public Widget Prop { get; } = new(8);
        public void Local() { Widget w = new(1); }
        public void Param(Widget p) { p = new(); }
        public Widget Ret() { return new(2, 3); }
        public Widget Arrow() => new(4);
        public void Untyped() { var v = new(); }
        public void Default(Widget w = new(7)) { }
        public void Cond(bool c) { Widget w = c ? new(1) : new(2, 3); }
        public void Coalesce(Widget a) { Widget w = a ?? new(3); }
        public void Thrower() { throw new(); }
        public void Lam() { Func<Widget> f = () => new(5); }
        public async Task<Widget> Async() { return new(6); }
        private Widget _explicit = new Widget(6);
        public Widget Made { get; } = new Widget(6);
        private int _n = Helper();
        private System.Func<int> _fn = () => Helper();
        public int Calc => Helper();
        public static int Helper() { return 1; }
    }

    public class Sink
    {
        public void Take(Widget w) { }
        public void Take(Widget w, int n) { }
        public void Named(int n, Widget w) { }
        public void Pick(Widget w) { }
        public void Pick(Sized s) { }
        public static void Stat(Sized s) { }
    }

    public class Base0
    {
        public Base0() { }
        public Base0(int a) { }
    }

    public class Derived : Base0
    {
        public Derived() : base(1) { }
        public Derived(int a, int b) : this() { }
    }

    public class Acc
    {
        private int _v;
        public int Val { get { return Helper(); } set { _v = Helper(); } }
        public event System.EventHandler Changed { add { Helper(); } remove { Helper(); } }
        public Widget W { get { return new(4); } }
        public static int Helper() { return 1; }
    }

    public class Two
    {
        public Two(int a) : this() { H1(); }
        public Two() { H2(); }
        public Two(string s) : this(1, 2) { H3(); }
        public Two(int a, int b) { }
        public void Over(int a) { H1(); }
        public void Over(string s) { H2(); }
        static void H1() { }
        static void H2() { }
        static void H3() { }
    }

    public class Wrap { public Wrap(Widget w) { } }

    public class Args
    {
        void Bare(Widget w) { }
        void ArgBare() { Bare(new(1)); }
        void ArgMember(Sink sink) { sink.Take(new(1)); }
        void ArgSecond(Sink sink) { sink.Named(1, new(2)); }
        void ArgNamed(Sink sink) { sink.Named(w: new(3), n: 1); }
        void ArgStatic() { Sink.Stat(new(1, 2)); }
        void ArgAmbiguous(Sink sink) { sink.Pick(new(1)); }
        void ArgUnknown(Nope nope) { nope.Foo(new(1)); }
        void ArgCtor() { var x = new Wrap(new(1)); }
    }

    public class Sized
    {
        public Sized(int a) { }
        public Sized(int a, int b) { }
    }
    public class Only1 { public Only1(int a) { } }

    public enum SpecificationKind { S }
    public enum PublicationStatus { P }

    public static class DbExt
    {
        public static string ToDatabaseValue(this SpecificationKind kind) { return "s"; }
        public static string ToDatabaseValue(this PublicationStatus status) { return "p"; }
    }

    public record Rec(int A)
    {
        public Rec(string s) : this(0) { }
    }

    public class Opt
    {
        public string Fmt() { return ""; }
        public string Fmt(string f, params object[] args) { return f; }
        public int Pad() { return 0; }
        public int Pad(int a, int b = 0) { return a; }
    }

    public class Caller
    {
        public void FromField(Sized s) { Sized t = new(1, 2); }
        public void CallOne(Svc svc) { svc.Add(1); }
        public void CallTwo(Svc svc) { svc.Add(1, 2); }
        public void CallAmbiguous(Svc svc) { svc.Amb(1); }
        public void CallExt(Kind kind) { kind.ToDb(); }
        public void CallExtPad(Kind kind) { kind.ToDb(5); }
        public void MakeWidget() { var w = new Widget(1); }
        public void ExtSpec(SpecificationKind kind) { kind.ToDatabaseValue(); }
        public void ExtStatus(PublicationStatus status) { status.ToDatabaseValue(); }
        public void MakeRec() { var r = new Rec(5); }
        public void FmtNone(Opt o) { o.Fmt(); }
        public void FmtParams(Opt o) { o.Fmt("x", 1, 2); }
        public void PadOptional(Opt o) { o.Pad(1); }
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
fn new_binds_to_matching_ctor() {
    let (dir, indexer, gv) = setup();
    let (qn, sig) = only_target(&indexer, gv, "App.Caller.MakeWidget");
    assert_eq!(qn, "App.Widget..ctor");
    assert!(sig.unwrap().contains("int size"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn new_without_declared_ctor_binds_to_class() {
    let (dir, indexer, gv) = setup();
    let (qn, _) = only_target(&indexer, gv, "App.Caller.MakePlain");
    assert_eq!(qn, "App.Plain");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn dead_symbols_does_not_list_a_called_ctor() {
    let (dir, mut indexer, _) = setup();
    let result = rpc::handle_method(&mut indexer, "dead_symbols", serde_json::json!({})).unwrap();
    let dead: Vec<String> = result["dead_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["qualname"].as_str().map(String::from))
        .collect();
    assert!(
        !dead.iter().any(|q| q == "App.Widget..ctor"),
        "called ctor listed as dead: {dead:?}"
    );
    // The runtime runs a static constructor; nothing ever calls it.
    let cctors: i64 = indexer
        .db()
        .read_conn()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM symbols WHERE qualname = 'App.Widget..cctor' AND kind = 'method'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(cctors, 1, "the static constructor symbol must exist");
    assert!(
        !dead.iter().any(|q| q == "App.Widget..cctor"),
        "static ctor listed as dead: {dead:?}"
    );
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

#[test]
fn same_arity_extension_overloads_are_told_apart_by_receiver_type() {
    let (dir, indexer, gv) = setup();
    let (_, sig) = only_target(&indexer, gv, "App.Caller.ExtSpec");
    assert!(sig.unwrap().contains("SpecificationKind"));
    let (_, sig) = only_target(&indexer, gv, "App.Caller.ExtStatus");
    assert!(sig.unwrap().contains("PublicationStatus"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn record_with_primary_constructor_binds_new_to_the_type() {
    let (dir, indexer, gv) = setup();
    let (qn, _) = only_target(&indexer, gv, "App.Caller.MakeRec");
    assert_eq!(qn, "App.Rec");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn params_and_optional_parameters_widen_the_admitted_arity() {
    let (dir, indexer, gv) = setup();
    let (_, sig) = only_target(&indexer, gv, "App.Caller.FmtNone");
    assert!(!sig.unwrap().contains("params"));
    let (_, sig) = only_target(&indexer, gv, "App.Caller.FmtParams");
    assert!(sig.unwrap().contains("params"));
    let (_, sig) = only_target(&indexer, gv, "App.Caller.PadOptional");
    assert!(sig.unwrap().contains("int b = 0"));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Signature of the one constructor `caller` binds (other edges, such as the
/// call to the method taking the argument, are ignored).
fn ctor_sig(indexer: &Indexer, gv: i64, caller: &str) -> String {
    let ctors: Vec<_> = resolved_targets(indexer, gv, caller)
        .into_iter()
        .filter(|(qn, _)| qn.ends_with("..ctor"))
        .collect();
    assert_eq!(ctors.len(), 1, "{caller}: {ctors:?}");
    ctors[0].1.clone().unwrap_or_default()
}

#[test]
fn static_constructor_has_its_own_identity_and_never_absorbs_new() {
    let (dir, indexer, gv) = setup();
    let conn = indexer.db().read_conn().unwrap();
    let names: Vec<String> = conn
        .prepare("SELECT qualname FROM symbols WHERE qualname LIKE 'App.Widget..%' AND graph_version = ?")
        .unwrap()
        .query_map(params![gv], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        names.iter().filter(|n| *n == "App.Widget..cctor").count(),
        1,
        "{names:?}"
    );
    assert_eq!(
        names.iter().filter(|n| *n == "App.Widget..ctor").count(),
        2,
        "{names:?}"
    );
    // `new Widget()` (0 args) binds to the instance ctor, not the static one.
    let sig = ctor_sig(&indexer, gv, "App.Caller.MakeDefault");
    assert_eq!(sig, "()");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn target_typed_new_binds_to_the_declared_types_ctor() {
    let (dir, indexer, gv) = setup();
    assert!(ctor_sig(&indexer, gv, "App.Holder.Local").contains("int size"));
    assert_eq!(ctor_sig(&indexer, gv, "App.Holder.Param"), "()");
    // Two ctors of `Widget` admit no 2-arg call: stays on the class.
    let (qn, _) = only_target(&indexer, gv, "App.Holder.Ret");
    assert_eq!(qn, "App.Widget");
    assert!(ctor_sig(&indexer, gv, "App.Holder.Arrow").contains("int size"));
    let (qn, sig) = only_target(&indexer, gv, "App.Caller.FromField");
    assert_eq!(qn, "App.Sized..ctor");
    assert!(sig.unwrap().contains("int b"));
    assert!(resolved_targets(&indexer, gv, "App.Holder.Untyped").is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn target_typed_new_in_field_and_property_initialisers_binds() {
    let (dir, indexer, gv) = setup();
    assert!(ctor_sig(&indexer, gv, "App.Holder._field").contains("int size"));
    assert!(ctor_sig(&indexer, gv, "App.Holder.Prop").contains("int size"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn initialisers_walk_every_expression_attributed_to_the_member() {
    let (dir, indexer, gv) = setup();
    assert!(ctor_sig(&indexer, gv, "App.Holder._explicit").contains("int size"));
    assert!(ctor_sig(&indexer, gv, "App.Holder.Made").contains("int size"));
    for member in ["_n", "_fn", "Calc"] {
        let (qn, _) = only_target(&indexer, gv, &format!("App.Holder.{member}"));
        assert_eq!(qn, "App.Holder.Helper", "{member}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn target_typed_new_as_argument_binds_via_the_callee_parameter_type() {
    let (dir, indexer, gv) = setup();
    for caller in ["ArgBare", "ArgMember", "ArgSecond", "ArgNamed"] {
        let sig = ctor_sig(&indexer, gv, &format!("App.Args.{caller}"));
        assert!(sig.contains("int size"), "{caller}: {sig}");
    }
    assert!(ctor_sig(&indexer, gv, "App.Args.ArgStatic").contains("int b"));
    // Overloads whose parameter types differ, or an unknown callee: no ctor.
    for caller in ["ArgAmbiguous", "ArgUnknown"] {
        let targets = resolved_targets(&indexer, gv, &format!("App.Args.{caller}"));
        assert!(
            targets
                .iter()
                .all(|(qn, _)| !qn.starts_with("App.Widget") && !qn.starts_with("App.Sized")),
            "{caller}: {targets:?}"
        );
    }
    let targets = resolved_targets(&indexer, gv, "App.Args.ArgCtor");
    assert!(
        targets
            .iter()
            .any(|(q, s)| q == "App.Widget..ctor" && s.as_deref().unwrap().contains("int size")),
        "{targets:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn read_symbol_qualnames_and_query_modes_return_every_overload() {
    let (dir, mut indexer, _) = setup();
    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"qualnames": ["App.Svc.Add", "App.Plain"]}),
    )
    .unwrap();
    // Each overload is a normal entry (path, source, ...); `App.Plain` too.
    let symbols = result["symbols"].as_array().unwrap();
    assert_eq!(symbols.len(), 3, "{result}");
    let adds: Vec<_> = symbols
        .iter()
        .filter(|s| s["qualname"] == "App.Svc.Add")
        .collect();
    assert_eq!(adds.len(), 2, "{result}");
    assert!(
        adds.iter()
            .all(|s| s["path"] == "App.cs" && s["source"].is_string())
    );
    assert!(
        adds.iter()
            .any(|s| s["source"].as_str().unwrap().contains("int a, int b"))
    );
    assert_eq!(symbols[2]["qualname"], "App.Plain");
    assert!(result["omitted"].as_array().unwrap().is_empty());

    let result = rpc::handle_method(
        &mut indexer,
        "read_symbol",
        serde_json::json!({"query": "App.Svc.Add"}),
    )
    .unwrap();
    assert_eq!(result["overloaded"], true, "{result}");
    assert_eq!(result["count"], 2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn accessor_bodies_attribute_calls_to_the_property_or_event() {
    let (dir, indexer, gv) = setup();
    let vals = resolved_targets(&indexer, gv, "App.Acc.Val");
    assert_eq!(vals.len(), 2, "{vals:?}");
    assert!(vals.iter().all(|(q, _)| q == "App.Acc.Helper"));
    let events = resolved_targets(&indexer, gv, "App.Acc.Changed");
    assert_eq!(events.len(), 2, "{events:?}");
    assert!(ctor_sig(&indexer, gv, "App.Acc.W").contains("int size"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn constructor_initializers_call_the_base_or_sibling_ctor_by_arity() {
    let (dir, indexer, gv) = setup();
    let targets = resolved_targets(&indexer, gv, "App.Derived..ctor");
    let has = |q: &str, sig: &str| {
        targets
            .iter()
            .any(|(t, s)| t == q && s.as_deref() == Some(sig))
    };
    assert!(has("App.Base0..ctor", "(int a)"), "{targets:?}");
    assert!(has("App.Derived..ctor", "()"), "{targets:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn bound_edges_never_carry_a_placeholder_target_qualname() {
    let (dir, indexer, gv) = setup();
    let conn = indexer.db().read_conn().unwrap();
    // No edge row, bound or not, and no stored unresolved reference carries a
    // placeholder qualname.
    let leaked: i64 = conn
        .query_row(
            "SELECT (SELECT COUNT(*) FROM edges WHERE target_qualname LIKE '@%')
                  + (SELECT COUNT(*) FROM unresolved_references WHERE reference_name LIKE '@%')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(leaked, 0);
    drop(conn);
    let mut indexer = indexer;
    for (method, params) in [
        (
            "analyze_impact",
            serde_json::json!({"qualname": "App.Widget..ctor", "direction": "upstream"}),
        ),
        (
            "trace_flow",
            serde_json::json!({"start_qualname": "App.Args.ArgMember"}),
        ),
        (
            "explain_symbol",
            serde_json::json!({"qualname": "App.Args.ArgMember"}),
        ),
    ] {
        let out = rpc::handle_method(&mut indexer, method, params).unwrap();
        assert!(!out.to_string().contains("@arg"), "{method}: {out}");
    }
    let conn = indexer.db().read_conn().unwrap();
    let bound: String = conn
        .query_row(
            "SELECT e.target_qualname FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE s.qualname = 'App.Args.ArgMember' AND t.kind = 'method'
               AND t.qualname LIKE '%..ctor' AND e.graph_version = ?",
            params![gv],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(bound, "App.Widget..ctor");
    let _ = std::fs::remove_dir_all(&dir);
}

/// `(target qualname, target signature)` of every resolved CALLS edge from
/// the overload of `qualname` whose signature is `sig`.
fn from_overload(
    indexer: &Indexer,
    gv: i64,
    qualname: &str,
    sig: &str,
) -> Vec<(String, Option<String>)> {
    let conn = indexer.db().read_conn().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT t.qualname, t.signature FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CALLS' AND s.qualname = ? AND s.signature = ?
               AND e.graph_version = ? ORDER BY t.qualname",
        )
        .unwrap();
    stmt.query_map(params![qualname, sig, gv], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn names(v: Vec<(String, Option<String>)>) -> Vec<String> {
    v.into_iter()
        .map(|(q, s)| format!("{q}{}", s.map(|s| format!(" {s}")).unwrap_or_default()))
        .collect()
}

#[test]
fn edges_from_overloads_attach_to_the_right_overload() {
    let (dir, indexer, gv) = setup();
    let from = |qn: &str, sig: &str| names(from_overload(&indexer, gv, qn, sig));
    assert_eq!(
        from("App.Two..ctor", "(int a)"),
        ["App.Two..ctor ()", "App.Two.H1 () -> void"]
    );
    assert_eq!(from("App.Two..ctor", "()"), ["App.Two.H2 () -> void"]);
    assert_eq!(
        from("App.Two..ctor", "(string s)"),
        ["App.Two..ctor (int a, int b)", "App.Two.H3 () -> void"]
    );
    assert_eq!(
        from("App.Two.Over", "(int a) -> void"),
        ["App.Two.H1 () -> void"]
    );
    assert_eq!(
        from("App.Two.Over", "(string s) -> void"),
        ["App.Two.H2 () -> void"]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn contains_edges_reach_every_overload() {
    let (dir, indexer, gv) = setup();
    let conn = indexer.db().read_conn().unwrap();
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(DISTINCT e.target_symbol_id) FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN symbols t ON t.id = e.target_symbol_id
             WHERE e.kind = 'CONTAINS' AND s.qualname = 'App.Two' AND t.kind = 'method'
               AND e.graph_version = ?",
            params![gv],
            |r| r.get(0),
        )
        .unwrap();
    // 4 constructors, 2 `Over` overloads, 3 helpers.
    assert_eq!(n, 9);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn target_typed_new_resolves_through_defaults_conditionals_lambdas_and_async() {
    let (dir, indexer, gv) = setup();
    for caller in ["Default", "Coalesce", "Lam", "Async"] {
        let sig = ctor_sig(&indexer, gv, &format!("App.Holder.{caller}"));
        assert!(sig.contains("int size"), "{caller}: {sig}");
    }
    // Both `?:` branches get the declared type: 1 arg -> ctor, 2 args -> class.
    let cond = resolved_targets(&indexer, gv, "App.Holder.Cond");
    assert!(
        cond.iter()
            .any(|(q, s)| q == "App.Widget..ctor" && s.as_deref() == Some("(int size)")),
        "{cond:?}"
    );
    assert!(cond.iter().any(|(q, _)| q == "App.Widget"), "{cond:?}");
    // `throw new()`: the exception type isn't knowable, so nothing binds.
    assert!(resolved_targets(&indexer, gv, "App.Holder.Thrower").is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

fn setup_files(files: &[(&str, &str)]) -> (PathBuf, Indexer, i64) {
    let mut dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!("lidx-csharp-files-{nanos}-{n}"));
    std::fs::create_dir_all(&dir).unwrap();
    for (name, src) in files {
        std::fs::write(dir.join(name), src).unwrap();
    }
    let db_path = dir.join(".lidx").join(".lidx.sqlite");
    let mut indexer = Indexer::new(dir.clone(), db_path).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();
    (dir, indexer, gv)
}

#[test]
fn base_initializer_keeps_i_prefixed_classes_but_refuses_interfaces() {
    let src = "namespace App {
        public class IPAddressHelper { public IPAddressHelper(int a) { } }
        public class D : IPAddressHelper { public D() : base(1) { } }
        public interface IFoo { }
        public class E : IFoo { public E() : base() { } }
    }";
    let (dir, indexer, gv) = setup_files(&[("A.cs", src)]);
    let d = resolved_targets(&indexer, gv, "App.D..ctor");
    assert_eq!(d.len(), 1, "{d:?}");
    assert_eq!(d[0].0, "App.IPAddressHelper..ctor");
    let e = resolved_targets(&indexer, gv, "App.E..ctor");
    assert!(e.is_empty(), "an interface is never constructed: {e:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn base_initializer_qualifies_the_base_through_usings() {
    let a = "namespace A { public class Base { public Base(int x) { } } }";
    let b = "namespace B { public class Base { public Base(int x, int y) { } } }";
    let d = "using A;\nnamespace C { public class D : Base { public D() : base(1) { } } }";
    let (dir, indexer, gv) = setup_files(&[("A.cs", a), ("B.cs", b), ("D.cs", d)]);
    let got = resolved_targets(&indexer, gv, "C.D..ctor");
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(got[0].0, "A.Base..ctor");
    let _ = std::fs::remove_dir_all(&dir);

    // Both namespaces imported: the same simple name is ambiguous, so refuse.
    let d =
        "using A;\nusing B;\nnamespace C { public class D : Base { public D() : base(1) { } } }";
    let (dir, indexer, gv) = setup_files(&[("A.cs", a), ("B.cs", b), ("D.cs", d)]);
    let got = resolved_targets(&indexer, gv, "C.D..ctor");
    assert!(got.is_empty(), "two repo candidates is ambiguity: {got:?}");
    let conn = indexer.db().read_conn().unwrap();
    let stubs: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM symbols WHERE kind = 'external' AND qualname LIKE '%Base%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stubs, 0, "no external stub for an ambiguous repo type");
    let reason: String = conn
        .query_row(
            "SELECT ur.reason FROM unresolved_references ur
             JOIN symbols s ON s.id = ur.source_symbol_id
             WHERE s.qualname = 'C.D..ctor' AND ur.edge_kind = 'CALLS'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(reason, "ambiguous");
    drop(conn);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn new_of_a_type_named_in_two_imported_namespaces_is_ambiguous_not_external() {
    let a = "namespace A { public class Widget { public Widget(int x) { } } }";
    let b = "namespace B { public class Widget { public Widget(int x) { } } }";
    let c = "using A;\nusing B;\nnamespace C { public class U { public void M() { var w = new Widget(1); } } }";
    let (dir, indexer, gv) = setup_files(&[("A.cs", a), ("B.cs", b), ("C.cs", c)]);
    assert!(resolved_targets(&indexer, gv, "C.U.M").is_empty());
    let conn = indexer.db().read_conn().unwrap();
    let reason: String = conn
        .query_row(
            "SELECT ur.reason FROM unresolved_references ur
             JOIN symbols s ON s.id = ur.source_symbol_id WHERE s.qualname = 'C.U.M'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(reason, "ambiguous");
    let _ = std::fs::remove_dir_all(&dir);
}
