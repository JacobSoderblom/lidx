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
