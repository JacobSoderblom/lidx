//! Issue #529: a const, static or type alias is used, not called, so its
//! users are `USES` / `REFERENCES` edges. `explain_symbol` lists them under a
//! labelled `used_by` section (not `callers`, which stays CALLS-only), and the
//! other "who uses this" views (`analyze_diff`, `gather_context`,
//! `trace_flow`'s empty-trace hints) read them too. Every case indexes real
//! source through the real indexer.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};

const TS: &[(&str, &str)] = &[
    (
        "src/cfg.ts",
        "export const LIMIT = 5;\nexport type Id = string;\n",
    ),
    (
        "src/use.ts",
        "import { LIMIT, Id } from './cfg';\nexport function run(x: Id): number {\n  register(LIMIT);\n  return LIMIT + 1;\n}\nfunction register(v: number) { return v; }\n",
    ),
];

// Rust attributes a type use to the enclosing module, so a type alias's user
// is the module that mentions it (a module's own declaration edge is not one).
const RUST: &[(&str, &str)] = &[
    (
        "src/lib.rs",
        "pub mod other;\n\npub const LIMIT: u32 = 5;\npub static COUNTER: u32 = 0;\npub type Id = u64;\n\npub fn run() -> u32 {\n    let _ = COUNTER;\n    register(LIMIT);\n    LIMIT + 1\n}\n\nfn register(v: u32) -> u32 {\n    v\n}\n",
    ),
    (
        "src/other.rs",
        "use crate::Id;\n\npub struct Holder {\n    pub id: Id,\n}\n",
    ),
];

const PYTHON: &[(&str, &str)] = &[
    ("pkg/__init__.py", ""),
    (
        "pkg/cfg.py",
        "from typing import TypeAlias\n\nLIMIT = 5\nId: TypeAlias = int\n",
    ),
    (
        "pkg/use.py",
        "from pkg.cfg import LIMIT, Id\n\n\ndef run(x):\n    register(LIMIT)\n    register(Id)\n    return LIMIT + 1\n\n\ndef register(v):\n    return v\n",
    ),
    (
        "tests/test_cfg.py",
        "from pkg.cfg import LIMIT\nfrom pkg.use import register\n\n\ndef test_limit():\n    assert register(LIMIT) == 5\n",
    ),
];

fn index(files: &[(&str, &str)]) -> (tempfile::TempDir, Indexer) {
    let (tmp, _root, indexer) = common::live_indexer("lidx-value-users-", files);
    (tmp, indexer)
}

fn call(indexer: &mut Indexer, method: &str, params: Value) -> Value {
    rpc::handle_method(indexer, method, params).unwrap()
}

fn used_by(explained: &Value) -> Vec<(String, String)> {
    explained["used_by"]
        .as_array()
        .unwrap_or_else(|| panic!("no used_by: {explained}"))
        .iter()
        .map(|r| {
            (
                r["symbol"]["qualname"].as_str().unwrap().to_string(),
                r["edge_kind"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

/// `target` lists `user` under `used_by` with a USES or REFERENCES edge, the
/// count is honest, and nothing leaks into `callers`.
fn assert_used_by(indexer: &mut Indexer, target: &str, user: &str) -> Value {
    let explained = call(indexer, "explain_symbol", json!({"qualname": target}));
    let users = used_by(&explained);
    let hit = users.iter().find(|(q, _)| q == user);
    assert!(hit.is_some(), "{target}: {user} missing from {users:?}");
    assert!(
        matches!(hit.unwrap().1.as_str(), "USES" | "REFERENCES"),
        "{users:?}"
    );
    assert_eq!(explained["used_by_total"], users.len(), "{explained}");
    assert_eq!(explained["callers_total"], 0, "{explained}");
    assert!(
        explained["callers"].as_array().unwrap().is_empty(),
        "callers stays CALLS-only: {explained}"
    );
    assert!(
        explained
            .get("other_edges")
            .is_none_or(|o| o.get("USES").is_none() && o.get("REFERENCES").is_none()),
        "users are not repeated under other_edges: {explained}"
    );
    explained
}

#[test]
fn ts_const_and_type_alias_list_their_users() {
    let (_tmp, mut indexer) = index(TS);
    assert_used_by(&mut indexer, "src/cfg.LIMIT", "src/use.run");
    assert_used_by(&mut indexer, "src/cfg.Id", "src/use.run");
}

#[test]
fn rust_const_static_and_type_alias_list_their_users() {
    let (_tmp, mut indexer) = index(RUST);
    let limit = assert_used_by(&mut indexer, "crate::LIMIT", "crate::run");
    assert_used_by(&mut indexer, "crate::COUNTER", "crate::run");
    assert_used_by(&mut indexer, "crate::Id", "crate::other");
    // The module's own declaration edge is not a user.
    assert!(
        !used_by(&limit).iter().any(|(q, _)| q == "crate"),
        "{limit}"
    );
}

#[test]
fn python_const_and_type_alias_list_their_users() {
    let (_tmp, mut indexer) = index(PYTHON);
    assert_used_by(&mut indexer, "pkg.cfg.LIMIT", "pkg.use.run");
    assert_used_by(&mut indexer, "pkg.cfg.Id", "pkg.use.run");
}

#[test]
fn test_users_are_marked_covering_tests_and_sorted_after_production() {
    let (_tmp, mut indexer) = index(PYTHON);
    let explained = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "pkg.cfg.LIMIT"}),
    );
    let users = used_by(&explained);
    assert_eq!(users.first().map(|u| u.0.as_str()), Some("pkg.use.run"));
    assert!(
        users.iter().any(|(q, _)| q.ends_with("test_limit")),
        "{users:?}"
    );
    let tests = explained["tests"].as_array().unwrap();
    assert!(
        tests.iter().any(|t| t["symbol"]["qualname"]
            .as_str()
            .unwrap()
            .ends_with("test_limit")),
        "a test that uses the const covers it: {explained}"
    );
}

#[test]
fn used_by_respects_max_refs_and_reports_the_true_total() {
    let mut files: Vec<(String, String)> = vec![(
        "src/cfg.ts".to_string(),
        "export const LIMIT = 5;\n".to_string(),
    )];
    for i in 0..6 {
        files.push((
            format!("src/user{i}.ts"),
            format!(
                "import {{ LIMIT }} from './cfg';\nexport function user{i}(): number {{\n  return take(LIMIT);\n}}\nfunction take(v: number) {{ return v; }}\n"
            ),
        ));
    }
    let refs: Vec<(&str, &str)> = files
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    let (_tmp, mut indexer) = index(&refs);
    let explained = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "src/cfg.LIMIT", "max_refs": 2}),
    );
    assert_eq!(used_by(&explained).len(), 2, "{explained}");
    assert_eq!(explained["used_by_total"], 6, "{explained}");
    assert_eq!(explained["budget"]["truncated"], true, "{explained}");
    assert!(
        explained["next_hops"].as_array().unwrap().iter().any(|h| {
            h["method"] == "analyze_impact"
                && h["params"]["direction"] == "upstream"
                && h["params"]["kinds"] == json!(["USES", "REFERENCES"])
        }),
        "{explained}"
    );
}

#[test]
fn callables_gain_no_used_by_and_it_can_be_requested_by_name() {
    let (_tmp, mut indexer) = index(TS);
    let run = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "src/use.run"}),
    );
    assert!(run.get("used_by").is_none(), "{run}");
    let only = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "src/cfg.LIMIT", "sections": ["references"]}),
    );
    assert!(only.get("callers").is_none(), "{only}");
    assert_eq!(used_by(&only)[0].0, "src/use.run", "{only}");
}

#[test]
fn trace_flow_from_a_const_points_at_its_users() {
    let (_tmp, mut indexer) = index(TS);
    let trace = call(
        &mut indexer,
        "trace_flow",
        json!({"start_qualname": "src/cfg.LIMIT", "direction": "upstream"}),
    );
    assert!(trace["trace"].as_array().unwrap().is_empty(), "{trace}");
    assert!(
        trace["next_hops"].as_array().unwrap().iter().any(|h| {
            h["method"] == "explain_symbol" && h["params"]["sections"] == json!(["used_by"])
        }),
        "{trace}"
    );
}

#[test]
fn analyze_diff_on_a_changed_const_reports_its_users() {
    let (_tmp, mut indexer) = index(PYTHON);
    let diff = call(
        &mut indexer,
        "analyze_diff",
        json!({"paths": ["pkg/cfg.py"]}),
    );
    let upstream = diff["upstream"].as_array().unwrap();
    let user = upstream
        .iter()
        .find(|e| e["symbol"]["qualname"] == "pkg.use.run")
        .unwrap_or_else(|| panic!("run missing from upstream: {diff}"));
    assert_eq!(user["relationship"], "user", "{diff}");
}

#[test]
fn gather_context_on_a_const_includes_its_users() {
    let (_tmp, mut indexer) = index(TS);
    let ctx = call(
        &mut indexer,
        "gather_context",
        json!({"seeds": [{"type": "symbol", "qualname": "src/cfg.LIMIT"}]}),
    );
    assert!(ctx.to_string().contains("src/use.run"), "{ctx}");
}
