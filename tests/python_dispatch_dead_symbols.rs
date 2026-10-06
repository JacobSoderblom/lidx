//! Python has no `override` keyword: a subclass method of the same name
//! overrides, so a call through the base type reaches it. dead_symbols must
//! not report such an override (or its class) dead, and traversal must
//! include the base-typed call site.
mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};

const FILES: &[(&str, &str)] = &[
    ("pkg/__init__.py", ""),
    (
        "pkg/base.py",
        "class Base:
    def run(self):
        return 0
",
    ),
    (
        "pkg/impls.py",
        "from pkg.base import Base


class Impl(Base):
    def run(self):
        return 1


class Lonely(Base):
    def extra(self):
        return 2
",
    ),
    (
        "pkg/app.py",
        "from pkg.base import Base


def drive(b: Base):
    return b.run()
",
    ),
];

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
    envelope["result"].clone()
}

fn qualnames(v: &Value) -> Vec<String> {
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

fn indexed() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let (tmp, repo, db) = common::index_repo("lidx-py-dispatch-", FILES);
    let mut indexer = Indexer::new(repo.clone(), db.clone()).unwrap();
    indexer.reindex().unwrap();
    drop(indexer);
    (tmp, repo, db)
}

#[test]
fn override_reached_only_through_a_base_typed_call_is_not_dead() {
    let (_tmp, repo, db) = indexed();
    let r = call(&repo, &db, "dead_symbols", json!({"limit": 100}));
    let dead = qualnames(&r["dead_symbols"]);
    assert!(
        !dead.contains(&"pkg.impls.Impl.run".to_string()),
        "{dead:?}"
    );
    // The class holding the reached override is live too.
    assert!(!dead.contains(&"pkg.impls.Impl".to_string()), "{dead:?}");
}

#[test]
fn subclass_method_without_a_base_declaration_and_callers_is_dead() {
    let (_tmp, repo, db) = indexed();
    let r = call(&repo, &db, "dead_symbols", json!({"limit": 100}));
    let dead = qualnames(&r["dead_symbols"]);
    assert!(
        dead.contains(&"pkg.impls.Lonely.extra".to_string()),
        "{dead:?}"
    );
    assert!(dead.contains(&"pkg.impls.Lonely".to_string()), "{dead:?}");
}

#[test]
fn callers_of_the_override_include_the_base_typed_call_site() {
    let (_tmp, repo, db) = indexed();
    let r = call(
        &repo,
        &db,
        "explain_symbol",
        json!({"qualname": "pkg.impls.Impl.run"}),
    );
    assert!(
        qualnames(&r["callers"]).contains(&"pkg.app.drive".to_string()),
        "{r}"
    );
    let r = call(
        &repo,
        &db,
        "trace_flow",
        json!({"start_qualname": "pkg.impls.Impl.run", "direction": "upstream"}),
    );
    assert!(
        qualnames(&r["trace"]).contains(&"pkg.app.drive".to_string()),
        "{r}"
    );
}
