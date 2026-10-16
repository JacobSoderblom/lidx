//! Issue #529 review follow-ups: `used_by` has a real budget share, is
//! implicit only when non-empty and the symbol has no CALLS callers, and
//! `analyze_diff` keeps callers first with users capped separately and
//! weighted low in the risk level. A type-only test use counts as a covering
//! test: a test that mentions a const, static or type alias is listed under
//! `tests` just as a calling test is for a function.

mod common;

use lidx::indexer::Indexer;
use lidx::rpc;
use serde_json::{Value, json};

fn index(files: &[(String, String)]) -> (tempfile::TempDir, Indexer) {
    let refs: Vec<(&str, &str)> = files
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    let (tmp, _root, indexer) = common::live_indexer("lidx-value-budget-", &refs);
    (tmp, indexer)
}

fn call(indexer: &mut Indexer, method: &str, params: Value) -> Value {
    rpc::handle_method(indexer, method, params).unwrap()
}

fn used_by_len(explained: &Value) -> usize {
    explained["used_by"]
        .as_array()
        .unwrap_or_else(|| panic!("no used_by: {explained}"))
        .len()
}

/// A TS repo whose `LIMIT` const is used by `n` functions (each a USES user,
/// none a caller).
fn many_users(n: usize) -> Vec<(String, String)> {
    let mut files = vec![(
        "src/cfg.ts".to_string(),
        "export const LIMIT = 5;\n".to_string(),
    )];
    for i in 0..n {
        files.push((
            format!("src/user{i}.ts"),
            format!(
                "import {{ LIMIT }} from './cfg';\nexport function user{i}(): number {{\n  return take(LIMIT);\n}}\nfunction take(v: number) {{ return v; }}\n"
            ),
        ));
    }
    files
}

#[test]
fn used_by_requested_alone_is_not_starved_to_one_row() {
    let (_tmp, mut indexer) = index(&many_users(14));
    for sections in [["used_by"], ["users"], ["references"], ["callers"]] {
        let explained = call(
            &mut indexer,
            "explain_symbol",
            json!({"qualname": "src/cfg.LIMIT", "sections": sections, "max_refs": 50}),
        );
        assert_eq!(used_by_len(&explained), 14, "{sections:?}: {explained}");
        assert_eq!(explained["used_by_total"], 14, "{sections:?}");
    }
    // The default max_refs caps at 10 with the honest total.
    let explained = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "src/cfg.LIMIT", "sections": ["used_by"]}),
    );
    assert_eq!(used_by_len(&explained), 10, "{explained}");
    assert_eq!(explained["used_by_total"], 14, "{explained}");
}

#[test]
fn used_by_is_implicit_only_when_non_empty_and_without_call_callers() {
    let files = vec![
        (
            "src/cfg.ts".to_string(),
            "export const UNUSED = 1;\nexport const fn = () => 1;\n".to_string(),
        ),
        (
            "src/use.ts".to_string(),
            "import { fn } from './cfg';\nexport function run() {\n  return fn();\n}\n".to_string(),
        ),
    ];
    let (_tmp, mut indexer) = index(&files);
    let unused = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "src/cfg.UNUSED"}),
    );
    assert!(unused.get("used_by").is_none(), "{unused}");
    assert!(unused.get("used_by_total").is_none(), "{unused}");
    let callable = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "src/cfg.fn"}),
    );
    assert_eq!(callable["callers_total"], 1, "{callable}");
    assert!(callable.get("used_by").is_none(), "{callable}");
    // Asked for by name it is present even when empty.
    let asked = call(
        &mut indexer,
        "explain_symbol",
        json!({"qualname": "src/cfg.UNUSED", "sections": ["used_by"]}),
    );
    assert_eq!(asked["used_by"], json!([]), "{asked}");
}

#[test]
fn analyze_diff_keeps_callers_first_and_users_capped_and_low_risk() {
    let mut files = vec![
        ("pkg/__init__.py".to_string(), String::new()),
        (
            "pkg/cfg.py".to_string(),
            "LIMIT = 5\n\n\ndef target():\n    return 1\n".to_string(),
        ),
    ];
    for i in 0..15 {
        files.push((
            format!("pkg/u{i}.py"),
            format!(
                "from pkg.cfg import LIMIT, target\n\n\ndef c{i}():\n    return target()\n\n\ndef v{i}():\n    return take(LIMIT)\n\n\ndef take(x):\n    return x\n"
            ),
        ));
    }
    let (_tmp, mut indexer) = index(&files);
    let diff = call(
        &mut indexer,
        "analyze_diff",
        json!({"paths": ["pkg/cfg.py"]}),
    );
    let upstream = diff["upstream"].as_array().unwrap();
    let callers = upstream
        .iter()
        .filter(|e| e["relationship"] == "caller")
        .count();
    let users = upstream
        .iter()
        .filter(|e| e["relationship"] == "user")
        .count();
    assert_eq!(callers, 15, "{diff}");
    assert_eq!(users, 10, "user cap: {diff}");
    let factor = diff["risk"]["factors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["factor"] == "Users of changed values")
        .unwrap_or_else(|| panic!("{diff}"));
    assert_eq!(factor["description"], "15 users of changed values");
    assert_eq!(factor["severity"], "low");
}

#[test]
fn many_users_alone_never_raise_the_risk_level() {
    let mut files = many_users(30);
    files.push(("src/cfg.test.ts".to_string(), String::new()));
    let (_tmp, mut indexer) = index(&files);
    let diff = call(
        &mut indexer,
        "analyze_diff",
        json!({"paths": ["src/cfg.ts"]}),
    );
    let level = diff["risk"]["level"].as_str().unwrap();
    assert!(matches!(level, "low" | "medium"), "{diff}");
    let factors = diff["risk"]["factors"].to_string();
    assert!(!factors.contains("High fan-in"), "{diff}");
    assert!(!factors.contains("Wide blast radius"), "{diff}");
    assert!(factors.contains("30 users of changed values"), "{diff}");
    assert_eq!(diff["upstream"].as_array().unwrap().len(), 10, "{diff}");
}
