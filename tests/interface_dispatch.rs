/// Issue #122: a call through an interface-typed field binds only to the
/// interface method. The implementing method must still show that caller
/// (explain_symbol, dead_symbols), trace_flow / analyze_impact must cross
/// interface <-> impl, and `implements` must list an interface's implementors.
mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};

fn call(repo: &std::path::Path, db: &std::path::Path, method: &str, params: Value) -> Value {
    let raw = rpc::call(
        repo.to_path_buf(),
        db.to_path_buf(),
        method.to_string(),
        &params.to_string(),
        "1",
    )
    .unwrap();
    let envelope: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        envelope.get("error").is_none_or(|e| e.is_null()),
        "{method} errored: {envelope}"
    );
    let result = envelope["result"].clone();
    if result.get("truncated").is_some() && result.get("data").is_some() {
        return result["data"].clone();
    }
    result
}

fn names(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| {
                    x["symbol"]["qualname"]
                        .as_str()
                        .or_else(|| x["qualname"].as_str())
                        .map(String::from)
                })
                .collect()
        })
        .unwrap_or_default()
}

const IFACE_M: &str = "Shop.IPublisher.PublishDeleted";
const IMPL_M: &str = "Shop.Publisher.PublishDeleted";
const CALLER: &str = "Shop.Coordinator.Delete";

#[test]
fn interface_dispatch_links_impl_and_interface_methods() {
    let (_tmp, repo, db) = common::setup_repo("cs_interface_dispatch");
    let mut indexer = Indexer::new(repo.clone(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    // 1. Callers of the impl method include the interface-typed caller.
    let r = call(&repo, &db, "explain_symbol", json!({"qualname": IMPL_M}));
    let callers = names(&r["callers"]);
    assert!(callers.contains(&CALLER.to_string()), "callers: {r}");

    // ... and the impl is no longer dead, while the truly unused method is.
    let r = call(&repo, &db, "dead_symbols", json!({"limit": 100}));
    let dead = names(&r["dead_symbols"]);
    assert!(!dead.contains(&IMPL_M.to_string()), "dead: {dead:?}");
    assert!(
        dead.contains(&"Shop.Publisher.Unused".to_string()),
        "dead: {dead:?}"
    );

    // 2. trace_flow downstream: interface method -> impl method.
    let r = call(
        &repo,
        &db,
        "trace_flow",
        json!({"start_qualname": IFACE_M, "direction": "downstream"}),
    );
    let hops = names(&r["trace"]);
    assert!(hops.contains(&IMPL_M.to_string()), "trace: {r}");

    // trace_flow upstream from the impl reaches the caller.
    let r = call(
        &repo,
        &db,
        "trace_flow",
        json!({"start_qualname": IMPL_M, "direction": "upstream"}),
    );
    assert!(
        names(&r["trace"]).contains(&CALLER.to_string()),
        "trace: {r}"
    );

    // analyze_impact both ways.
    let r = call(
        &repo,
        &db,
        "analyze_impact",
        json!({"qualname": IFACE_M, "direction": "downstream"}),
    );
    assert!(r.to_string().contains(IMPL_M), "impact down: {r}");
    let r = call(
        &repo,
        &db,
        "analyze_impact",
        json!({"qualname": IMPL_M, "direction": "upstream"}),
    );
    assert!(r.to_string().contains(CALLER), "impact up: {r}");

    // 3. explain_symbol on the interface lists implementors.
    let r = call(
        &repo,
        &db,
        "explain_symbol",
        json!({"qualname": "Shop.IPublisher"}),
    );
    assert!(
        names(&r["implements"]).contains(&"Shop.Publisher".to_string()),
        "implements: {r}"
    );
}

/// Same-named interfaces in different namespaces must not be cross-linked.
#[test]
fn interface_dispatch_does_not_link_same_named_interfaces_across_namespaces() {
    let (_tmp, repo, db) = common::setup_repo("cs_interface_dispatch_ns");
    let mut indexer = Indexer::new(repo.clone(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    let r = call(
        &repo,
        &db,
        "explain_symbol",
        json!({"qualname": "Shop.IPublisher"}),
    );
    let implementors = names(&r["implements"]);
    assert_eq!(implementors, vec!["Shop.Publisher".to_string()], "{r}");

    let r = call(
        &repo,
        &db,
        "trace_flow",
        json!({"start_qualname": IFACE_M, "direction": "downstream"}),
    );
    assert!(
        !names(&r["trace"]).contains(&"Other.OtherImpl.PublishDeleted".to_string()),
        "trace: {r}"
    );
}

/// Issue #173: chains, generic interfaces, explicit implementations.
#[test]
fn interface_dispatch_covers_chains_generics_and_explicit_impls() {
    let (_tmp, repo, db) = common::setup_repo("cs_interface_dispatch_gaps");
    let mut indexer = Indexer::new(repo.clone(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    for (imp, caller, iface) in [
        ("Shop.Worker.Run", "Shop.ChainCaller.Go", "Shop.IB.Run"),
        ("Shop.Repo.Save", "Shop.RepoCaller.Store", "Shop.IRepo.Save"),
        (
            "Shop.Publisher.IPublisher.Publish",
            "Shop.PubCaller.Fire",
            "Shop.IPublisher.Publish",
        ),
    ] {
        let r = call(&repo, &db, "explain_symbol", json!({"qualname": imp}));
        assert!(
            names(&r["callers"]).contains(&caller.to_string()),
            "{imp} callers: {r}"
        );
        let r = call(
            &repo,
            &db,
            "trace_flow",
            json!({"start_qualname": iface, "direction": "downstream"}),
        );
        assert!(
            names(&r["trace"]).contains(&imp.to_string()),
            "{iface} trace: {r}"
        );
    }
}

/// Issue #181: an explicit interface implementation is a distinct symbol
/// (`C.IA.Run`) and pairs only with the interface it names; the implicit
/// method pairs with the remaining interfaces; direct calls bind implicit.
#[test]
fn explicit_impl_has_distinct_identity_and_pairs_only_with_named_interface() {
    let (_tmp, repo, db) = common::setup_repo("cs_explicit_impl");
    let mut indexer = Indexer::new(repo.clone(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    let implicit = "Shop.C.Run";
    let explicit = "Shop.C.IA.Run";

    let r = call(&repo, &db, "explain_symbol", json!({"qualname": explicit}));
    assert_eq!(r["symbol"]["name"], "Run", "{r}");
    assert!(
        names(&r["callers"]).contains(&"Shop.ViaA.Go".to_string()),
        "explicit callers: {r}"
    );
    assert!(
        !names(&r["callers"]).contains(&"Shop.Direct.Go".to_string()),
        "explicit must not get direct callers: {r}"
    );

    let r = call(&repo, &db, "explain_symbol", json!({"qualname": implicit}));
    let callers = names(&r["callers"]);
    assert!(callers.contains(&"Shop.Direct.Go".to_string()), "{r}");
    assert!(
        !callers.contains(&"Shop.ViaA.Go".to_string()),
        "implicit must not pair with IA.Run: {r}"
    );

    let down = |iface: &str| {
        let r = call(
            &repo,
            &db,
            "trace_flow",
            json!({"start_qualname": iface, "direction": "downstream"}),
        );
        names(&r["trace"])
    };
    let a = down("Shop.IA.Run");
    assert!(a.contains(&explicit.to_string()), "IA: {a:?}");
    assert!(!a.contains(&implicit.to_string()), "IA: {a:?}");
    let b = down("Shop.IB.Run");
    assert!(b.contains(&implicit.to_string()), "IB: {b:?}");
    assert!(!b.contains(&explicit.to_string()), "IB: {b:?}");
}

/// Issue #181: explicit impl for IA plus one implicit `Run` serving IB and IC.
#[test]
fn implicit_impl_pairs_with_remaining_interfaces_only() {
    let (_tmp, repo, db) = common::setup_repo("cs_explicit_impl_three");
    let mut indexer = Indexer::new(repo.clone(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    let down = |iface: &str| {
        let r = call(
            &repo,
            &db,
            "trace_flow",
            json!({"start_qualname": iface, "direction": "downstream"}),
        );
        names(&r["trace"])
    };
    let explicit = "Shop.D.IA.Run".to_string();
    let implicit = "Shop.D.Run".to_string();
    let a = down("Shop.IA.Run");
    assert!(a.contains(&explicit) && !a.contains(&implicit), "{a:?}");
    for iface in ["Shop.IB.Run", "Shop.IC.Run"] {
        let t = down(iface);
        assert!(
            t.contains(&implicit) && !t.contains(&explicit),
            "{iface}: {t:?}"
        );
    }
}

/// Issue #188: analyze_diff and gather_context follow interface dispatch.
#[test]
fn analyze_diff_and_gather_context_follow_interface_dispatch() {
    let (_tmp, repo, db) = common::setup_repo("cs_interface_dispatch");
    let mut indexer = Indexer::new(repo.clone(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    // Changing the impl file: the interface-typed caller is reachable
    // through the interface method (depth 2).
    let r = call(
        &repo,
        &db,
        "analyze_diff",
        json!({"paths": ["Publisher.cs"], "max_depth": 2}),
    );
    let upstream = names(&r["upstream"]);
    assert!(upstream.contains(&IFACE_M.to_string()), "upstream: {r}");
    assert!(upstream.contains(&CALLER.to_string()), "upstream: {r}");

    // gather_context on the impl method pulls in the interface-typed caller.
    let r = call(
        &repo,
        &db,
        "gather_context",
        json!({"seeds": [{"type": "symbol", "qualname": IMPL_M}], "max_bytes": 80000, "depth": 2}),
    );
    let qualnames: Vec<&str> = r["items"]
        .as_array()
        .expect("items")
        .iter()
        .filter_map(|i| i["symbol"]["qualname"].as_str())
        .collect();
    assert!(qualnames.contains(&CALLER), "gather: {qualnames:?}");
}

const SECOND_IMPL: &str = "namespace Shop
{
    public class AuditPublisher : IPublisher
    {
        public void PublishDeleted(int id)
        {
        }
    }
}
";

const IFACE_TEST: &str = "using Xunit;

namespace Shop.Tests
{
    public class CoordinatorTests
    {
        private readonly IPublisher _pub;

        [Fact]
        public void DeleteCallsPublisher()
        {
            _pub.PublishDeleted(1);
        }
    }
}
";

const TEST_QN: &str = "Shop.Tests.CoordinatorTests.DeleteCallsPublisher";
const SECOND_IMPL_M: &str = "Shop.AuditPublisher.PublishDeleted";

/// A test calling `IPublisher.PublishDeleted` reaches every implementor only
/// via the interface: it is reported as such, never as direct coverage, in
/// analyze_diff, explain_symbol and the impact test layer (issue #188).
#[test]
fn tests_through_interface_are_marked_via_interface() {
    let (_tmp, repo, db) = common::setup_repo("cs_interface_dispatch");
    common::write_files(
        &repo,
        &[
            ("AuditPublisher.cs", SECOND_IMPL),
            ("tests/Shop.Tests/CoordinatorTests.cs", IFACE_TEST),
        ],
    );
    let mut indexer = Indexer::new(repo.clone(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    let gv = indexer.db().current_graph_version().unwrap();

    // analyze_diff coverage, for both implementors.
    let r = call(
        &repo,
        &db,
        "analyze_diff",
        json!({"paths": ["Publisher.cs", "AuditPublisher.cs"]}),
    );
    let coverage = r["test_coverage"].as_array().expect("test_coverage");
    for qn in [IMPL_M, SECOND_IMPL_M] {
        let entry = coverage
            .iter()
            .find(|c| c["symbol_qualname"] == qn)
            .unwrap_or_else(|| panic!("no coverage entry for {qn}: {r}"));
        assert_eq!(entry["status"], "covered_via_interface", "{qn}: {entry}");
        let tests = entry["tests"].as_array().unwrap();
        assert_eq!(tests.len(), 1, "{qn}: {entry}");
        assert_eq!(tests[0]["test_qualname"], TEST_QN);
        assert_eq!(tests[0]["coverage_type"], "via_interface");
    }

    // explain_symbol tests section carries the marker.
    let r = call(
        &repo,
        &db,
        "explain_symbol",
        json!({"qualname": IMPL_M, "sections": ["tests"]}),
    );
    let tests = r["tests"].as_array().expect("tests");
    let hit = tests
        .iter()
        .find(|t| t["symbol"]["qualname"] == TEST_QN)
        .unwrap_or_else(|| panic!("test missing: {r}"));
    assert_eq!(hit["via_interface"], true, "{hit}");

    // Impact test layer: strategy is call_via_interface, not call.
    let seed = indexer
        .db()
        .get_symbol_by_qualname(IMPL_M, gv)
        .unwrap()
        .unwrap();
    let layer = lidx::impact::layers::test::TestImpactLayer::new(indexer.db());
    let result = layer.analyze(&[seed.id], &[], gv).unwrap();
    let test_sym = indexer
        .db()
        .get_symbol_by_qualname(TEST_QN, gv)
        .unwrap()
        .unwrap();
    let evidence = &result.evidence[&test_sym.id];
    use lidx::impact::types::TestStrategy;
    let has = |wanted: TestStrategy| {
        evidence.iter().any(|e| {
            matches!(e, lidx::impact::types::ImpactSource::TestLink { strategy, .. } if *strategy == wanted)
        })
    };
    assert!(has(TestStrategy::CallViaInterface), "{evidence:?}");
    assert!(!has(TestStrategy::Call), "{evidence:?}");

    // The reported test carries the path that reaches the implementation.
    let r = call(
        &repo,
        &db,
        "analyze_impact",
        json!({"qualname": IMPL_M, "direction": "upstream", "max_depth": 3}),
    );
    let entry = r["affected"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["symbol"]["qualname"] == TEST_QN)
        .unwrap_or_else(|| panic!("test missing from analyze_impact: {r}"));
    let steps = entry["path"]["steps"].as_array().expect("path.steps");
    assert!(!steps.is_empty(), "{entry}");
    // Steps run root-to-leaf: the seed-side step first, the test's own step
    // last.
    assert_eq!(steps[0]["to_symbol"], IMPL_M, "{entry}");
    assert_eq!(steps.last().unwrap()["from_symbol"], TEST_QN, "{entry}");
}

/// explain_symbol marks refs that exist only through interface dispatch:
/// the interface-typed caller of an implementation, and the implementations
/// an interface method dispatches to (issue #188).
#[test]
fn explain_symbol_marks_dispatch_callers_and_callees_via_interface() {
    let (_tmp, repo, db) = common::setup_repo("cs_interface_dispatch");
    let mut indexer = Indexer::new(repo.clone(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);

    let find = |refs: &Value, qn: &str| -> Value {
        refs.as_array()
            .unwrap()
            .iter()
            .find(|r| r["symbol"]["qualname"] == qn)
            .cloned()
            .unwrap_or_else(|| panic!("{qn} missing in {refs}"))
    };
    let via = |r: &Value| r["via_interface"].as_bool().unwrap_or(false);

    // Caller of the implementation: reached only through the interface.
    let r = call(
        &repo,
        &db,
        "explain_symbol",
        json!({"qualname": IMPL_M, "sections": ["callers"]}),
    );
    assert!(via(&find(&r["callers"], CALLER)), "callers: {r}");

    // Callee of the interface method: the implementation.
    let r = call(
        &repo,
        &db,
        "explain_symbol",
        json!({"qualname": IFACE_M, "sections": ["callees"]}),
    );
    assert!(via(&find(&r["callees"], IMPL_M)), "callees: {r}");

    // A direct caller is not marked: Coordinator.Delete calls the interface
    // method itself.
    let r = call(
        &repo,
        &db,
        "explain_symbol",
        json!({"qualname": IFACE_M, "sections": ["callers"]}),
    );
    assert!(!via(&find(&r["callers"], CALLER)), "callers: {r}");
}

fn downstream(repo: &std::path::Path, db: &std::path::Path, start: &str) -> Vec<String> {
    let r = call(
        repo,
        db,
        "trace_flow",
        json!({"start_qualname": start, "direction": "downstream"}),
    );
    names(&r["trace"])
}

fn index(fixture: &str) -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let (tmp, repo, db) = common::setup_repo(fixture);
    let mut indexer = Indexer::new(repo.clone(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    (tmp, repo, db)
}

/// Issue #185: `IA<int>.Run` and `IA<string>.Run` are distinct symbols and
/// each dispatches from the interface method (and only from its own one).
#[test]
fn explicit_impls_of_closed_generics_are_distinct_and_dispatch() {
    let (_tmp, repo, db) = index("cs_dispatch_generic");
    let int_run = "Shop.C.IA<int>.Run";
    let str_run = "Shop.C.IA<string>.Run";
    for q in [int_run, str_run] {
        let r = call(&repo, &db, "explain_symbol", json!({"qualname": q}));
        assert_eq!(r["symbol"]["qualname"], q, "{r}");
    }
    // Callers are matched by the receiver's closed type arguments; an open
    // `IA<T>` receiver reaches every closure.
    let callers = |q: &str| {
        let r = call(&repo, &db, "explain_symbol", json!({"qualname": q}));
        let mut v = names(&r["callers"]);
        v.sort();
        v
    };
    assert_eq!(
        callers(int_run),
        [
            "Shop.Caller.Go",
            "Shop.CastCaller.Go",
            "Shop.OpenCaller.Go",
            "Shop.TwoArgs.Go",
            "Shop.TwoArgsRev.Go"
        ]
    );
    assert_eq!(
        callers(str_run),
        [
            "Shop.OpenCaller.Go",
            "Shop.StrCaller.Go",
            "Shop.TwoArgs.Go",
            "Shop.TwoArgsRev.Go"
        ]
    );
    // One method calling through two closed receivers reaches both impls,
    // whichever call comes first.
    for go in ["Shop.TwoArgs.Go", "Shop.TwoArgsRev.Go"] {
        let d = downstream(&repo, &db, go);
        assert!(d.contains(&int_run.to_string()), "{go}: {d:?}");
        assert!(d.contains(&str_run.to_string()), "{go}: {d:?}");
        let impact = call(
            &repo,
            &db,
            "analyze_impact",
            json!({"qualname": go, "direction": "downstream"}),
        )
        .to_string();
        assert!(
            impact.contains(int_run) && impact.contains(str_run),
            "{go}: {impact}"
        );
    }
    let d = downstream(&repo, &db, "Shop.StrCaller.Go");
    assert!(d.contains(&str_run.to_string()), "{d:?}");
    assert!(!d.contains(&int_run.to_string()), "{d:?}");
    let d = downstream(&repo, &db, "Shop.Caller.Go");
    assert!(d.contains(&int_run.to_string()), "{d:?}");
    assert!(!d.contains(&str_run.to_string()), "{d:?}");
    let up = |q: &str| {
        let r = call(
            &repo,
            &db,
            "trace_flow",
            json!({"start_qualname": q, "direction": "upstream"}),
        );
        names(&r["trace"])
    };
    let u = up(str_run);
    assert!(u.contains(&"Shop.StrCaller.Go".to_string()), "{u:?}");
    assert!(!u.contains(&"Shop.Caller.Go".to_string()), "{u:?}");
    let impact = call(
        &repo,
        &db,
        "analyze_impact",
        json!({"qualname": "Shop.StrCaller.Go", "direction": "downstream"}),
    )
    .to_string();
    assert!(
        impact.contains(str_run) && !impact.contains(int_run),
        "{impact}"
    );
    // A closed impl no matching call reaches is dead.
    let r = call(&repo, &db, "dead_symbols", json!({"limit": 200}));
    let dead = names(&r["dead_symbols"]);
    assert!(
        dead.contains(&"Shop.Z.IB<string>.Run".to_string()),
        "{dead:?}"
    );
    assert!(!dead.contains(&int_run.to_string()), "{dead:?}");
    assert!(!dead.contains(&str_run.to_string()), "{dead:?}");

    let a = downstream(&repo, &db, "Shop.IA.Run");
    for q in [int_run, str_run, "Shop.E.IA<int>.Run"] {
        assert!(a.contains(&q.to_string()), "IA: {a:?}");
    }
    assert!(!a.contains(&"Shop.E.Run".to_string()), "IA: {a:?}");
    let b = downstream(&repo, &db, "Shop.IB.Run");
    assert!(b.contains(&"Shop.E.Run".to_string()), "IB: {b:?}");
    assert!(!b.contains(&"Shop.E.IA<int>.Run".to_string()), "IB: {b:?}");
    assert!(!b.contains(&int_run.to_string()), "IB: {b:?}");
}

/// Issue #185: same-named interfaces in two namespaces resolve through the
/// class's namespace/usings, and explicit impls pair with the named one.
#[test]
fn same_named_interfaces_resolve_by_scope_and_pair_explicit_impls() {
    let (_tmp, repo, db) = index("cs_dispatch_ns_scope");
    let implementors = |iface: &str| {
        let r = call(&repo, &db, "explain_symbol", json!({"qualname": iface}));
        let mut v = names(&r["implements"]);
        v.sort();
        v
    };
    assert_eq!(
        implementors("N1.IA"),
        ["App.C", "App.H", "App.OnlyN1"],
        "N1.IA"
    );
    // `N2.Local : IA` binds to its own namespace's IA, not the `using N1`.
    assert_eq!(
        implementors("N2.IA"),
        ["App.C", "App.H", "N2.Local"],
        "N2.IA"
    );

    let n1 = downstream(&repo, &db, "N1.IA.Run");
    let n2 = downstream(&repo, &db, "N2.IA.Run");
    // C: implicit Run serves N1.IA (its explicit twin serves N2.IA).
    assert!(n1.contains(&"App.C.Run".to_string()), "N1: {n1:?}");
    assert!(!n1.contains(&"App.C.N2.IA.Run".to_string()), "N1: {n1:?}");
    assert!(n2.contains(&"App.C.N2.IA.Run".to_string()), "N2: {n2:?}");
    assert!(!n2.contains(&"App.C.Run".to_string()), "N2: {n2:?}");
    // H: a short `IA.Run` (via using) is N1's, `N2.IA.Run` is N2's.
    assert!(n1.contains(&"App.H.IA.Run".to_string()), "N1: {n1:?}");
    assert!(!n1.contains(&"App.H.N2.IA.Run".to_string()), "N1: {n1:?}");
    assert!(n2.contains(&"App.H.N2.IA.Run".to_string()), "N2: {n2:?}");
    assert!(!n2.contains(&"App.H.IA.Run".to_string()), "N2: {n2:?}");
    let callers = |q: &str| {
        let r = call(&repo, &db, "explain_symbol", json!({"qualname": q}));
        names(&r["callers"])
    };
    assert_eq!(callers("N1.IA.Run"), ["App.ViaN1.Go"]);
    assert_eq!(callers("N2.IA.Run"), ["App.ViaN2.Go"]);
    let only = callers("App.OnlyN1.Run");
    assert!(only.contains(&"App.ViaN1.Go".to_string()), "{only:?}");
    assert!(!only.contains(&"App.ViaN2.Go".to_string()), "{only:?}");
    assert!(n1.contains(&"App.OnlyN1.Run".to_string()), "N1: {n1:?}");
    assert!(!n2.contains(&"App.OnlyN1.Run".to_string()), "N2: {n2:?}");
    assert!(n2.contains(&"N2.Local.Run".to_string()), "N2: {n2:?}");
    assert!(!n1.contains(&"N2.Local.Run".to_string()), "N1: {n1:?}");
}

/// Issue #185: explicit property/event impls are distinct symbols and
/// dispatch like methods.
#[test]
fn explicit_property_and_event_impls_dispatch() {
    let (_tmp, repo, db) = index("cs_dispatch_members");
    let a_p = downstream(&repo, &db, "Shop.IA.P");
    assert!(a_p.contains(&"Shop.C.IA.P".to_string()), "IA.P: {a_p:?}");
    assert!(a_p.contains(&"Shop.D.P".to_string()), "IA.P: {a_p:?}");
    assert!(!a_p.contains(&"Shop.C.P".to_string()), "IA.P: {a_p:?}");
    let b_p = downstream(&repo, &db, "Shop.IB.P");
    assert!(b_p.contains(&"Shop.C.P".to_string()), "IB.P: {b_p:?}");
    assert!(!b_p.contains(&"Shop.C.IA.P".to_string()), "IB.P: {b_p:?}");
    let ev = downstream(&repo, &db, "Shop.IA.Changed");
    assert!(ev.contains(&"Shop.C.IA.Changed".to_string()), "{ev:?}");
    assert!(ev.contains(&"Shop.D.Changed".to_string()), "{ev:?}");
}

/// Issue #185: a call through a base-class member reaches every override
/// down the EXTENDS chain, but not a `new` hiding member.
#[test]
fn base_class_dispatch_reaches_override_chain() {
    let (_tmp, repo, db) = index("cs_dispatch_override");
    let m = downstream(&repo, &db, "Shop.Base.M");
    for q in ["Shop.Derived.M", "Shop.Mid.M", "Shop.Leaf.M"] {
        assert!(m.contains(&q.to_string()), "M: {m:?}");
    }
    let v = downstream(&repo, &db, "Shop.Base.V");
    assert!(v.contains(&"Shop.Derived.V".to_string()), "V: {v:?}");
    let n = downstream(&repo, &db, "Shop.Base.N");
    assert!(!n.contains(&"Shop.Derived.N".to_string()), "N: {n:?}");

    let r = call(
        &repo,
        &db,
        "explain_symbol",
        json!({"qualname": "Shop.Leaf.M"}),
    );
    assert!(
        names(&r["callers"]).contains(&"Shop.Caller.Go".to_string()),
        "{r}"
    );
    // Properties and events override too.
    for (base, derived) in [
        ("Shop.Base.P", "Shop.Derived.P"),
        ("Shop.Base.E", "Shop.Derived.E"),
    ] {
        let t = downstream(&repo, &db, base);
        assert!(t.contains(&derived.to_string()), "{base}: {t:?}");
    }
    let r = call(&repo, &db, "dead_symbols", json!({"limit": 100}));
    let dead = names(&r["dead_symbols"]);
    // Override is tracked per overload: only the `new` overload is dead.
    let dead_ov = dead.iter().filter(|q| *q == "Shop.Derived.Ov").count();
    assert_eq!(dead_ov, 1, "{dead:?}");
    assert!(!dead.contains(&"Shop.Derived.V".to_string()), "{dead:?}");
    assert!(dead.contains(&"Shop.Derived.N".to_string()), "{dead:?}");
}

/// Issue #185: a bare `IA` receiver resolves like C# does -- enclosing
/// namespace first, then the single imported one; two imports stay ambiguous.
#[test]
fn bare_interface_receiver_resolves_by_enclosing_namespace_then_usings() {
    let (_tmp, repo, db) = index("cs_dispatch_bare_scope");
    let callers = |q: &str| {
        let r = call(&repo, &db, "explain_symbol", json!({"qualname": q}));
        names(&r["callers"])
    };
    // `using N1` only -> N1; enclosing namespace N2 beats `using N1` -> N2.
    assert_eq!(callers("N1.IA.Run"), ["App.A.Go"]);
    assert_eq!(callers("N2.IA.Run"), ["N2.B.Go"]);
    // Both imported is a C# compile error: never guessed.
    for q in ["N1.IA.Run", "N2.IA.Run"] {
        assert!(!callers(q).contains(&"App2.D.Go".to_string()), "{q}");
    }
}

/// Issue #185: type arguments compare canonically -- built-in aliases,
/// `System.` prefixes, nullable annotations, spacing and nested generics.
#[test]
fn closed_generic_args_compare_after_normalisation() {
    let (_tmp, repo, db) = index("cs_dispatch_normalized");
    let callers = |q: &str| {
        let r = call(&repo, &db, "explain_symbol", json!({"qualname": q}));
        assert_eq!(r["symbol"]["qualname"], q, "{r}");
        names(&r["callers"])
    };
    assert_eq!(callers("Shop.C.IA<int>.Run"), ["Shop.CInt.Go"]);
    assert_eq!(callers("Shop.C.IA<List<int>>.Run"), ["Shop.CList.Go"]);
    let mut strings = callers("Shop.C.IA<string>.Run");
    strings.sort();
    assert_eq!(
        strings,
        ["Shop.CNullableString.Go", "Shop.CSystemString.Go"]
    );
}

/// Issue #185: an implicit impl is only excluded for the closures an
/// explicit twin covers.
#[test]
fn implicit_impl_still_serves_closures_without_an_explicit_twin() {
    let (_tmp, repo, db) = index("cs_dispatch_twin");
    let a = downstream(&repo, &db, "Shop.IA.Run");
    // C: implicit Run serves IA<int>; IA<string> has its explicit twin.
    assert!(a.contains(&"Shop.C.Run".to_string()), "{a:?}");
    assert!(a.contains(&"Shop.C.IA<string>.Run".to_string()), "{a:?}");
    // D: both closures explicit, so its implicit Run serves nothing.
    assert!(a.contains(&"Shop.D.IA<int>.Run".to_string()), "{a:?}");
    assert!(!a.contains(&"Shop.D.Run".to_string()), "{a:?}");
}

/// Issue #185: C# scope forms -- dotted and file-scoped namespaces, `global
/// using`, `using` aliases and nested types.
#[test]
fn scope_forms_resolve_same_named_interfaces() {
    let (_tmp, repo, db) = index("cs_dispatch_scope_forms");
    let explain = |q: &str, key: &str| {
        let r = call(&repo, &db, "explain_symbol", json!({"qualname": q}));
        let mut v = names(&r[key]);
        v.sort();
        v
    };
    assert_eq!(explain("A.B.IX", "implements"), ["A.B.C.DotImpl"]);
    assert_eq!(explain("F1.IX", "implements"), ["F1.FImpl"]);
    assert_eq!(explain("Z.IX", "implements"), ["G.GImpl"]);
    assert_eq!(explain("K.Outer.IX", "implements"), ["K.Outer.Impl"]);
    assert_eq!(explain("F1.IX.Run", "callers"), ["F1.FCaller.Go"]);
    assert_eq!(
        explain("Z.IX.Run", "callers"),
        ["G.GCaller.Go", "H.HCaller.Go"]
    );
    assert_eq!(explain("K.Outer.IX.Run", "callers"), ["K.Outer.Go"]);
}
