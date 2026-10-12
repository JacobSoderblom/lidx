//! A C# method group -- a method named, not called, as a delegate -- is a
//! `REFERENCES` edge from the enclosing method (issue #323). Locals,
//! parameters, properties, `nameof` and plain calls are not.

mod common;

use common::{edge_targets, ref_targets, unresolved_accounting};
use lidx::indexer::Indexer;
use lidx::rpc;

const FILES: &[(&str, &str)] = &[
    (
        "src/Util.cs",
        r#"namespace App;
public static class Util
{
    public static int Twice(int x) => x * 2;
    public static int Limit = 3;
}
"#,
    ),
    (
        "src/P.cs",
        r#"using System;
using System.Linq;
namespace App;
public class P
{
    public event Action Changed;
    public Func<string, int> Parser { get; set; }
    public int Count { get; set; }

    static int Parse(string s) => s.Length;
    int Compare(string a, string b) => 0;
    void OnChanged() { }
    void Spare() { }

    public int[] Select(string[] xs) => xs.Select(Parse).ToArray();

    public string[] Order(string[] xs) => xs.OrderByDescending(Parse, Comparer<int>.Default).ToArray();

    public void Assign()
    {
        Func<string, int> f = Parse;
        Func<string, int> g;
        g = Parse;
    }

    public void Subscribe()
    {
        Changed += OnChanged;
    }

    public Func<string, int> Wrap() => new Func<string, int>(Parse);

    public void ThisAndType(string[] xs)
    {
        var a = xs.Select(this.Parse2);
        var b = xs.Select(P.Parse);
        var c = xs.Select(Util.Twice2);
    }

    int Parse2(string s) => s.Length;

    public void Imported(int[] xs)
    {
        var a = xs.Select(Util.Twice);
    }

    public void Negatives(Func<string, int> param, string[] xs)
    {
        Func<string, int> local = s => s.Length;
        var a = xs.Select(param);
        var b = xs.Select(local);
        var c = Use(Count);
        var d = Use(Parser);
        var e = nameof(Spare);
        int Parse(string s) => 0;
        var f = xs.Select(Parse);
        var g = Use(Util.Limit);
    }

    public int Called(string s)
    {
        var r = Parse(s);
        return xs2(Parse);
    }

    int xs2(Func<string, int> f) => f("a");
    static int Use(object o) => 0;
}
"#,
    ),
];

#[test]
fn argument_method_groups() {
    assert_eq!(ref_targets(FILES, "App.P.Select"), ["App.P.Parse"]);
    assert_eq!(ref_targets(FILES, "App.P.Order"), ["App.P.Parse"]);
}

#[test]
fn assignment_event_and_constructor_shapes() {
    assert_eq!(
        ref_targets(FILES, "App.P.Assign"),
        ["App.P.Parse", "App.P.Parse"]
    );
    assert_eq!(ref_targets(FILES, "App.P.Subscribe"), ["App.P.OnChanged"]);
    assert_eq!(ref_targets(FILES, "App.P.Wrap"), ["App.P.Parse"]);
}

#[test]
fn qualified_method_groups() {
    // `this.Parse2` and `P.Parse` bind; `Util.Twice2` names nothing.
    assert_eq!(
        ref_targets(FILES, "App.P.ThisAndType"),
        ["App.P.Parse", "App.P.Parse2"]
    );
    assert_eq!(ref_targets(FILES, "App.P.Imported"), ["App.Util.Twice"]);
}

#[test]
fn locals_properties_nameof_and_calls_are_not_references() {
    let t = ref_targets(FILES, "App.P.Negatives");
    assert!(t.is_empty(), "{t:?}");
    // `Parse(s)` is a call, so `xs2(Parse)`'s method group, naming the same
    // pair, keeps only the CALLS edge.
    assert!(ref_targets(FILES, "App.P.Called").is_empty());
    assert!(edge_targets(FILES, "App.P.Called", "CALLS").contains(&"App.P.Parse".to_string()));
}

#[test]
fn unresolved_value_references_are_not_counted() {
    unresolved_accounting(FILES).assert_value_references_not_counted();
}

const ISSUE_323: &[(&str, &str)] = &[(
    "P.cs",
    r#"namespace App;
public static class P {
  static int Parse(string s) => s.Length;
  public static int[] Run(string[] xs) => xs.Select(Parse).ToArray();
}
"#,
)];

#[test]
fn issue_323_fixture_is_referenced_by_run_and_not_dead() {
    let (_tmp, root, db_path) = common::index_repo("lidx-cs-valref-", ISSUE_323);
    let mut indexer = Indexer::new(root, db_path).unwrap();
    let explained = rpc::handle_method(
        &mut indexer,
        "explain_symbol",
        serde_json::json!({"query": "App.P.Parse", "sections": ["callers", "other_edges"]}),
    )
    .unwrap();
    // A method group is not a caller: it is reported under `other_edges`.
    assert_eq!(explained["callers_total"], 0, "{explained}");
    let group = &explained["other_edges"]["REFERENCES"];
    assert_eq!(group["total"], 1, "{explained}");
    assert_eq!(group["items"][0]["symbol"]["qualname"], "App.P.Run");

    let dead = rpc::handle_method(
        &mut indexer,
        "dead_symbols",
        serde_json::json!({"include_unused_imports": false, "include_orphan_tests": false}),
    )
    .unwrap();
    let names: Vec<&str> = dead["dead_symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["qualname"].as_str())
        .collect();
    assert!(!names.contains(&"App.P.Parse"), "{names:?}");
}

const PATTERN_LOCALS: &[(&str, &str)] = &[(
    "src/Neg.cs",
    r#"using System;
namespace App;
public class N
{
    static int Parse(string s) => s.Length;
    static int Use(object o) => 0;

    public void Pattern(object o) { if (o is string Parse) Use(Parse); }
    public void OutVar(string s) { int.TryParse(s, out var Parse); Use(Parse); }
    public void OutTyped(string s) { int.TryParse(s, out int Parse); Use(Parse); }
    public void Switch(object o) { switch (o) { case int Parse: Use(Parse); break; } }
    public void Recursive(object o) { if (o is N { } Parse) Use(Parse); }
    public void Real() { Use(Parse); }
}
"#,
)];

#[test]
fn pattern_and_out_variables_are_locals() {
    for caller in ["Pattern", "OutVar", "OutTyped", "Switch", "Recursive"] {
        let t = ref_targets(PATTERN_LOCALS, &format!("App.N.{caller}"));
        assert!(t.is_empty(), "{caller}: {t:?}");
    }
    // The same name with no local binding still resolves.
    assert_eq!(ref_targets(PATTERN_LOCALS, "App.N.Real"), ["App.N.Parse"]);
}
