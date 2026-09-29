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
    assert!(r.to_string().contains(CALLER), "gather: {r}");
}
